"""
Scoring formula
---------------
  base_score     = STRATEGY_WEIGHTS[match_type]          # 0.65-1.0
  length_bonus   = min(span_length - 1, 4) * 0.05        # multi-word bonus
  freq_bonus     = min(frequency - 1, 5) * 0.03          # repeat appearances
  final_score    = min(base_score + length_bonus + freq_bonus, 1.0)

Deduplication
-------------
  1. Within-span: if two matches overlap in character range, keep the one
     with the higher score (longest / best match wins).
  2. Across-skill: if the same skill_id appears multiple times, merge into
     a single MatchedSkill and boost the score via freq_bonus.
"""

from __future__ import annotations

from collections import defaultdict
from typing import List, Tuple

from spacy.tokens import Doc

from matchers import STRATEGY_WEIGHTS
from models import MatchedSkill


def score_and_deduplicate(
    doc: Doc,
    raw_hits: List[Tuple[str, str, str, int, int]]
) -> List[MatchedSkill]:
    candidates: list[MatchedSkill] = []
    for skill_id, label, match_type, start_tok, end_tok in raw_hits:
        span = doc[start_tok:end_tok]
        span_length = end_tok - start_tok

        base = STRATEGY_WEIGHTS.get(match_type, 0.5)
        length_bonus = min(span_length - 1, 4) * 0.05
        score = min(base + length_bonus, 1.0)

        candidates.append(
            MatchedSkill(
                skill_id=skill_id,
                label=label,
                matched_text=span.text,
                match_type=match_type,
                score=score,
                start_char=span.start_char,
                end_char=span.end_char,
                span_length=span_length,
            )
        )

    candidates.sort(key=lambda m: (m.start_char, -m.span_length))
    non_overlapping = _remove_overlaps(candidates)

    grouped: dict[str, list[MatchedSkill]] = defaultdict(list)
    for m in non_overlapping:
        grouped[m.skill_id].append(m)

    merged: list[MatchedSkill] = []
    for skill_id, occurrences in grouped.items():
        best = max(occurrences, key=lambda x: x.score)
        freq_bonus = min(len(occurrences) - 1, 5) * 0.03
        best.score = min(best.score + freq_bonus, 1.0)
        merged.append(best)

    merged.sort(key=lambda m: m.score, reverse=True)
    return merged


def _remove_overlaps(candidates: list[MatchedSkill]) -> list[MatchedSkill]:
    """
    Greedy interval scheduling: iterate left-to-right; skip any match whose
    character range overlaps the previously accepted match.
    When two spans start at the same position, the longer one (higher score)
    was sorted first and wins.
    """
    result: list[MatchedSkill] = []
    last_end = -1
    for m in candidates:
        if m.start_char >= last_end:
            result.append(m)
            last_end = m.end_char
        else:
            # overlap — keep the one with higher score
            if result and m.score > result[-1].score:
                result[-1] = m
                last_end = m.end_char
    return result