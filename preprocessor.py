import re
import spacy
from spacy.language import Language


# Common noise patterns in JDs (HTML tags, bullet markers, excessive whitespace)
_NOISE_RE = re.compile(r"<[^>]+>|[•·▪▸►●○–—]|[ \t]{2,}")
_ABBREV_RE = re.compile(r"\b([A-Z]{2,8})\b")  # capture abbreviations for matching


def clean_text(text: str) -> str:
    """Strip HTML, bullets, and normalise whitespace."""
    text = _NOISE_RE.sub(" ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


class Preprocessor:
    def __init__(self, model: str = "en_core_web_lg"):
        self.nlp: Language = spacy.load(model, disable=["ner", "parser"])

    def process(self, text: str):
        clean = clean_text(text)
        doc = self.nlp(clean)
        return doc, clean

    def extract_abbreviations(self, text: str) -> list[str]:
        """Pull uppercase abbreviations (e.g. AWS, SQL, CI/CD) from raw text."""
        return list(set(_ABBREV_RE.findall(text)))