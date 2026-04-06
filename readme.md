# skill_extractor

A modular, fully local Python library for extracting and ranking tech keywords from job descriptions — no LLMs, no API calls.

Inspired by [SkillNER](https://github.com/AnasAito/SkillNER) but built on modern spaCy (v3.x) with a three-layer skill taxonomy:
- **[ESCO](https://esco.ec.europa.eu/en/use-esco/download)** — 14,000+ standardised competencies and knowledge areas
- **[StackOverflow Tags](https://stackoverflow.com/tags)** — 5,000+ mainstream tech tools (filtered from 65k total)
- **Manual list** — curated entries for commercial/cloud tools underrepresented in SO (Snowflake, BigQuery, etc.)

---

## Architecture

```
Job Description Text
        │
        ▼
┌───────────────────┐
│   Preprocessor    │  clean HTML/bullets, tokenise, lemmatise (spaCy)
└────────┬──────────┘
         │  spaCy Doc
         ▼
┌───────────────────┐
│ MatcherPipeline   │  4 strategies run in parallel
│  ├─ exact_matcher │  case-insensitive surface match    weight 1.00
│  ├─ lemma_matcher │  lemmatised match                  weight 0.85
│  ├─ unigram_match │  single-word skills                weight 0.65
│  └─ abbrev_match  │  acronyms (AWS, SQL, CI/CD)        weight 0.90
└────────┬──────────┘
         │  raw hits [(skill_id, label, type, start, end), ...]
         ▼
┌───────────────────┐
│     Scorer        │  base score + length bonus + frequency bonus
│   + Deduplicator  │  resolve overlapping spans, merge duplicate skills
└────────┬──────────┘
         │
         ▼
  ExtractionResult
   .ranked_keywords  => ['python', 'apache spark', 'aws', ...]
   .skills           => [MatchedSkill(...), ...]
   .to_dict()        => JSON-serialisable
```

---

## Taxonomy Layers

The skill DB is built by merging three sources in priority order:

```
ESCO CSV  ──────────────────────────┐
  14,000+ competencies & knowledge  │
                                    ▼
StackOverflow Tags ─────────►  Merged DB  ──► MatcherPipeline
  5,000+ tech tools                 ▲
  (min_count=1000, no versions)     │
                                    │
Manual Skills ──────────────────────┘
  Snowflake, BigQuery, dbt, etc.
  (highest priority — overwrites on collision)
```

### Why three layers?

| Source | Covers | Gap |
|---|---|---|
| ESCO | Soft skills, concepts, knowledge areas | Weak on vendor tech tools |
| SO Tags | Mainstream frameworks, languages, databases | New/commercial platforms have low SO activity |
| Manual | Snowflake, BigQuery, CloudFormation, etc. | Needs occasional human updates |

### StackOverflow tag filtering

Raw SO tags (65k) are filtered to ~5,000 using two rules:
- **`min_count=1000`** — drops rarely-used and hyper-specific tags
- **Version pattern filter** — drops `cassandra-0.7`, `python-3.x`, etc.

Tags are also normalised:
- `apache-kafka` → label `apache kafka`, alt_label `kafka`
- `aws-lambda`, `aws-sso`, ... → synthesizes a standalone `aws` entry
- `azure-functions`, ... → synthesizes a standalone `azure` entry

### Manual skills

Defined directly in `loader.py` as `MANUAL_TECH_SKILLS`. Add entries here for tools that are new, commercial, or niche enough that SO tags don't cover them:

```python
MANUAL_TECH_SKILLS = {
    # label             alt_labels
    "snowflake":        ["snowflake cloud", "snowflake data warehouse"],
    "bigquery":         ["google bigquery", "bq"],
    "databricks":       ["databricks platform"],
    "cloudformation":   ["aws cloudformation", "cfn"],
    "dbt":              ["data build tool"],
    "fivetran":         [],
    "airbyte":          [],
    ...
}
```

---

## Setup

### 1. Install dependencies

```bash
pip install spacy py7zr requests
python -m spacy download en_core_web_lg
```

### 2. Prepare your ESCO CSV

Your CSV must have these columns:

| Column | Description |
|---|---|
| `escoid` | Unique skill URI |
| `preferredLabel` | Canonical skill name |
| `description` | Skill description |
| `sentence` | Sample sentence using the skill |
| `sentence_type` | `explicit` or `implicit` |
| `extract` | Source: `course`, `cv`, or `job` |

> **Note:** The CSV is sentence-level — one row per sentence, multiple rows per skill.  
> The loader deduplicates by `escoid` automatically.

Place the file at `data/skills_en.csv`.

### 3. Run

```python
from skill_extractor import SkillExtractor

extractor = SkillExtractor(
    esco_csv="data/skills_en.csv",
    cache_path="data/esco_cache.json",       # ESCO cache — speeds up reload
    so_cache_path="data/so_cache.json",      # SO tags cache — avoids re-downloading
)

result = extractor.extract(your_job_description_text)
print(result.ranked_keywords[:20])
```

On **first run**, the loader will:
1. Parse `skills_en.csv` and save `esco_cache.json`
2. Download `Tags.7z` from Internet Archive (~1 MB), parse it, and save `so_cache.json`

On **subsequent runs**, both caches are loaded instantly from disk — no network calls.

---

## API Reference

### `SkillExtractor`

| Parameter | Type | Description |
|---|---|---|
| `esco_csv` | `str \| Path` | Path to your ESCO CSV file |
| `cache_path` | `str \| Path` | Optional ESCO cache path (JSON) |
| `so_cache_path` | `str \| Path` | Optional SO tags cache path (JSON) |
| `include_so_tags` | `bool` | Toggle SO enrichment (default: `True`) |
| `so_min_count` | `int` | Min SO question count threshold (default: `1000`) |
| `spacy_model` | `str` | spaCy model name (default: `en_core_web_lg`) |

| Method | Description |
|---|---|
| `extract(text)` | Extract from one JD → `ExtractionResult` |
| `extract_batch(texts)` | Extract from a list of JDs |
| `top_keywords(text, n=20)` | Shortcut — returns top-n labels directly |

### `ExtractionResult`

| Property | Type | Description |
|---|---|---|
| `ranked_keywords` | `list[str]` | Skill labels ranked by score |
| `skills` | `list[MatchedSkill]` | Full match objects |
| `to_dict()` | `dict` | JSON-serialisable output |

### `MatchedSkill`

| Field | Type | Description |
|---|---|---|
| `label` | `str` | Canonical skill name |
| `matched_text` | `str` | Exact text found in the JD |
| `match_type` | `str` | `exact` / `lemma` / `unigram` / `abbrev` |
| `score` | `float` | Confidence 0.0 – 1.0 |
| `start_char` / `end_char` | `int` | Character offsets in source text |

---

## Scoring

```
base_score   = STRATEGY_WEIGHTS[match_type]      # 0.65 - 1.0
length_bonus = min(span_tokens - 1, 4) * 0.05   # multi-word skills score higher
freq_bonus   = min(occurrences - 1, 5) * 0.03   # repeated mentions boost score
final_score  = min(base + length_bonus + freq_bonus, 1.0)
```

---

## File Structure

```
skill_extractor/
├── __init__.py         ← public exports
├── extractor.py        ← SkillExtractor (main API)
├── loader.py           ← ESCOLoader — ESCO CSV + SO tags + manual skills
├── matchers.py         ← MatcherPipeline (4 strategies)
├── preprocessor.py     ← text cleaning + spaCy tokenisation
├── scorer.py           ← scoring, overlap removal, deduplication
├── models.py           ← MatchedSkill, ExtractionResult
└── requirements.txt

example_usage.py        ← runnable demo
data/
├── skills_en.csv       ← your ESCO CSV
├── esco_cache.json     ← auto-generated on first run
└── so_cache.json       ← auto-generated on first run (downloaded from SO)
```