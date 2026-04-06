from __future__ import annotations

from pathlib import Path
from typing import Optional

from loader import ESCOLoader
from matchers import MatcherPipeline
from models import ExtractionResult
from preprocessor import Preprocessor
from scorer import score_and_deduplicate


class SkillExtractor:
    """
    End-to-end skill extraction pipeline.

    Parameters
    ----------
    esco_csv    : path to the ESCO `skills_en.csv` file
    cache_path  : optional JSON cache (speeds up subsequent loads)
    spacy_model : spaCy model name (default: en_core_web_lg)
    skill_types : ESCO skill types to include
    """

    def __init__(
        self,
        esco_csv: str | Path,
        cache_path: Optional[str | Path] = None,
        spacy_model: str = "en_core_web_lg",
        skill_types: set[str] = frozenset({"skill/competence", "knowledge"}),
    ):
        print("[SkillExtractor] Initialising pipeline…")

        # 1. Load taxonomy
        self._loader = ESCOLoader(
            csv_path=esco_csv,
            cache_path=cache_path,
            skill_types=skill_types,
        )
        db = self._loader.load()

        # 2. Build preprocessor
        self._preprocessor = Preprocessor(model=spacy_model)

        # 3. Build matchers
        self._matchers = MatcherPipeline(nlp=self._preprocessor.nlp, db=db)

        print("[SkillExtractor] Ready.")


    def extract(self, text: str) -> ExtractionResult:
        """
        Extract and rank skills from a single job description.

        Parameters
        ----------
        text : raw job description string

        Returns
        -------
        ExtractionResult
            .ranked_keywords  => list of skill labels, best first
            .skills           => full MatchedSkill objects
            .to_dict()        => JSON-serialisable dict
        """
        doc, clean = self._preprocessor.process(text)
        raw_hits   = self._matchers.match(doc)
        skills     = score_and_deduplicate(doc, raw_hits)

        return ExtractionResult(raw_text=clean, skills=skills)

    def extract_batch(self, texts: list[str]) -> list[ExtractionResult]:
        """
        Extract skills from multiple job descriptions.

        Returns a list of ExtractionResult in the same order.
        """
        return [self.extract(t) for t in texts]

    def top_keywords(self, text: str, n: int = 20) -> list[str]:
        """
        Convenience method: return the top-n keyword labels directly.
        """
        return self.extract(text).ranked_keywords[:n]