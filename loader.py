import csv
import io
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
import py7zr
import requests
import tempfile
import os


SKILL_TYPE_WHITELIST = {"skill/competence", "knowledge"}

SO_TAGS_URL    = "https://ia600508.us.archive.org/30/items/stackexchange/stackoverflow.com-Tags.7z"
SO_MIN_COUNT   = 1000
SO_VERSION_RE  = re.compile(r"[\-\.]\d")
SO_PREFIX_STRIPS = ["apache-", "amazon-"]
SO_FAMILY_ALIASES = {
    "aws-":      ("so::aws",      "aws"),
    "azure-":    ("so::azure",    "azure"),
    "gcp-":      ("so::gcp",      "gcp"),
    "google-":   ("so::google-cloud", "google cloud"),
    "apache-":   None,   # handled per-tag via prefix strip, not as a family
}
SO_BLOCKLIST = {
    "join", "using", "background", "model", "pipeline", "deployment",
    "string", "list", "function", "class", "type", "file", "data",
    "server", "client", "output", "input", "error", "exception",
    "performance", "security", "testing", "interface", "object",
    "array", "memory", "cache", "form", "image", "path", "date",
    "time", "email", "url", "http", "event", "timer", "token",
    "user", "table", "view", "query", "update", "delete", "insert",
}
MANUAL_TECH_SKILLS = {
    # Cloud data platforms (underrepresented in SO)
    "snowflake":        ["snowflake cloud", "snowflake data warehouse"],
    "bigquery":         ["google bigquery", "bq"],
    "databricks":       ["databricks platform"],
    "cloudformation":   ["aws cloudformation", "cfn"],
    "redshift":         ["amazon redshift", "aws redshift"],

    # DevOps / IaC
    "pulumi":           [],
    "ansible":          [],

    # Data tools
    "dbt":              ["data build tool"],
    "fivetran":         [],
    "airbyte":          [],
    "great expectations": [],
}


class ESCOLoader:
    def __init__(
        self,
        csv_path: str | Path,
        cache_path: Optional[str | Path] = None,
        skill_types: set[str] = SKILL_TYPE_WHITELIST,
        so_cache_path: Optional[str | Path] = None,
        include_so_tags: bool = True,
        so_min_count: int = SO_MIN_COUNT,
    ):
        self.csv_path      = Path(csv_path)
        self.cache_path    = Path(cache_path)    if cache_path    else None
        self.so_cache_path = Path(so_cache_path) if so_cache_path else None
        self.skill_types   = skill_types
        self.include_so_tags = include_so_tags
        self.so_min_count  = so_min_count
        self._db: dict     = {}

    def load(self) -> dict:
        """Load ESCO from CSV/cache, then merge SO tags if enabled."""
        if self.cache_path and self.cache_path.exists():
            db = self._load_cache()
        else:
            db = self._load_csv()

        if self.include_so_tags:
            so_db = self._load_so_tags()
            before = len(db)
            # SO tags fill gaps — don't overwrite ESCO entries
            for skill_id, entry in so_db.items():
                if skill_id not in db:
                    db[skill_id] = entry
            print(f"[ESCOLoader] Merged SO tags: {len(db) - before:,} new skills added "
                  f"(total {len(db):,}).")
        
        manual_db = self._load_manual_skills()
        db.update(manual_db)

        self._db = db
        return db

    @property
    def db(self) -> dict:
        if not self._db:
            self._db = self.load()
        return self._db

    def all_labels(self) -> list[tuple[str, str]]:
        """Return (skill_id, label) for every label variant (preferred + alt)."""
        rows = []
        for skill_id, entry in self.db.items():
            rows.append((skill_id, entry["label"]))
            for alt in entry["alt_labels"]:
                rows.append((skill_id, alt))
        return rows

    def _load_csv(self) -> dict:
        if not self.csv_path.exists():
            raise FileNotFoundError(f"ESCO CSV not found at '{self.csv_path}'.")

        db = {}
        with open(self.csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                skill_id = row["escoid"].strip()
                if skill_id in db:          # multiple sentences per skill — keep first
                    continue
                label = row["preferredLabel"].strip().lower()
                db[skill_id] = {
                    "label":      label,
                    "alt_labels": [],
                    "skill_type": "skill/competence",
                    "source":     "esco",
                }

        self._db = db
        if self.cache_path:
            self._save_cache()

        print(f"[ESCOLoader] Loaded {len(db):,} skills from CSV.")
        return db

    def _save_cache(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self._db, f, ensure_ascii=False)
        print(f"[ESCOLoader] ESCO cache saved => {self.cache_path}")

    def _load_cache(self) -> dict:
        with open(self.cache_path, encoding="utf-8") as f:
            self._db = json.load(f)
        print(f"[ESCOLoader] Loaded {len(self._db):,} skills from ESCO cache.")
        return self._db

    def _load_so_tags(self) -> dict:
        """Return SO tags as a skill DB dict, using cache when available."""
        if self.so_cache_path and self.so_cache_path.exists():
            return self._load_so_cache()
        return self._fetch_so_tags()

    def _fetch_so_tags(self) -> dict:
        """Download Tags.7z, extract XML, filter, and return skill DB dict."""
        print(f"[ESCOLoader] Fetching SO tags from Internet Archive…")
        response = requests.get(SO_TAGS_URL, stream=True, timeout=180)
        response.raise_for_status()

        total = int(response.headers.get("content-length", 0))
        downloaded, chunks = 0, []
        for chunk in response.iter_content(chunk_size=512 * 1024):
            chunks.append(chunk)
            downloaded += len(chunk)
            if total:
                print(f"  {downloaded / 1e6:.1f} / {total / 1e6:.1f} MB", end="\r")
        print(f"\n[ESCOLoader] Download complete ({downloaded / 1e6:.1f} MB).")

        raw_7z = b"".join(chunks)

        with tempfile.TemporaryDirectory() as tmpdir:
            with py7zr.SevenZipFile(io.BytesIO(raw_7z), mode="r") as archive:
                archive.extract(targets=["Tags.xml"], path=tmpdir)

            xml_path = os.path.join(tmpdir, "Tags.xml")

            with open(xml_path, "rb") as f:
                xml_bytes = f.read()

        db = self._parse_so_xml(xml_bytes)

        if self.so_cache_path:
            self.so_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.so_cache_path, "w", encoding="utf-8") as f:
                json.dump(db, f, ensure_ascii=False)
            print(f"[ESCOLoader] SO cache saved => {self.so_cache_path}")

        return db

    def _parse_so_xml(self, xml_bytes: bytes) -> dict:
        """Parse Tags.xml and return filtered skill DB dict."""
        root    = ET.fromstring(xml_bytes)
        db      = {}
        aliases = {}   # family_id => (skill_id, label) — built from seen tags

        for row in root:
            name  = row.get("TagName", "").strip()
            count = int(row.get("Count", 0))

            if count < self.so_min_count:
                continue
            if SO_VERSION_RE.search(name):
                continue

            label    = name.replace("-", " ").lower()
            skill_id = f"so::{name}"

            alt_labels = [name.lower()] if "-" in name else []

            # ── Prefix strip: apache-kafka => add "kafka" as alt ──────────────
            for prefix in SO_PREFIX_STRIPS:
                if name.lower().startswith(prefix):
                    stripped = name[len(prefix):].replace("-", " ").lower()
                    if stripped not in alt_labels:
                        alt_labels.append(stripped)

            # ── Family alias: any aws-* tag => synthesize "aws" entry ─────────
            for family_prefix, alias in SO_FAMILY_ALIASES.items():
                if alias and name.lower().startswith(family_prefix):
                    alias_id, alias_label = alias
                    if alias_id not in aliases:
                        aliases[alias_id] = {
                            "label":      alias_label,
                            "alt_labels": [],
                            "skill_type": "tool",
                            "source":     "stackoverflow-alias",
                        }

            if label not in SO_BLOCKLIST:
                db[skill_id] = {
                    "label":      label,
                    "alt_labels": alt_labels,
                    "skill_type": "tool",
                    "source":     "stackoverflow",
                    "count":      count,
                }

        # Merge aliases only if not already covered by a direct tag
        for alias_id, entry in aliases.items():
            if alias_id not in db:
                db[alias_id] = entry

        print(f"[ESCOLoader] Parsed {len(db):,} SO tags + {len(aliases):,} aliases.")
        return db

    def _load_so_cache(self) -> dict:
        with open(self.so_cache_path, encoding="utf-8") as f:
            db = json.load(f)
        print(f"[ESCOLoader] Loaded {len(db):,} SO tags from cache.")
        return db
    
    def _load_manual_skills(self) -> dict:
        db = {}
        for label, alt_labels in MANUAL_TECH_SKILLS.items():
            skill_id = f"manual::{label.replace(' ', '-')}"
            db[skill_id] = {
                "label":      label,
                "alt_labels": alt_labels,
                "skill_type": "tool",
                "source":     "manual",
            }
        print(f"[ESCOLoader] Loaded {len(db):,} manual skills.")
        return db