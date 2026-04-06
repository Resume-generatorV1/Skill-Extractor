from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class MatchedSkill:
    """Represents a single matched skill from the job description."""
    skill_id: str
    label: str                  # canonical skill name from ESCO
    matched_text: str           # exact text found in the JD
    match_type: str             # "exact", "lemma", "unigram", "abbrev"
    score: float                # normalized confidence score (0.0 - 1.0)
    start_char: int             # character offset in source text
    end_char: int
    span_length: int            # number of tokens in the match

    def __repr__(self):
        return f"<MatchedSkill '{self.label}' [{self.match_type}] score={self.score:.2f}>"


@dataclass
class ExtractionResult:
    """Full result for one job description."""
    raw_text: str
    skills: List[MatchedSkill] = field(default_factory=list)

    @property
    def ranked_keywords(self) -> List[str]:
        """Return skill labels ranked by score (highest first)."""
        seen = set()
        result = []
        for s in sorted(self.skills, key=lambda x: x.score, reverse=True):
            if s.label not in seen:
                seen.add(s.label)
                result.append(s.label)
        return result

    @property
    def top_keywords(self, n: int = 20) -> List[str]:
        return self.ranked_keywords[:n]

    def to_dict(self) -> dict:
        return {
            "ranked_keywords": self.ranked_keywords,
            "skills": [
                {
                    "label": s.label,
                    "matched_text": s.matched_text,
                    "match_type": s.match_type,
                    "score": round(s.score, 4),
                }
                for s in self.skills
            ],
        }