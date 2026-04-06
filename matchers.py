from __future__ import annotations

import re
from typing import List, Tuple

import spacy
from spacy.language import Language
from spacy.matcher import PhraseMatcher
from spacy.tokens import Doc, Span


STRATEGY_WEIGHTS = {
    "exact":   1.0,
    "lemma":   0.85,
    "unigram": 0.65,
    "abbrev":  0.90,
}

_ABBREV_RE = re.compile(r"^[A-Z0-9]{2,8}([/\-][A-Z0-9]{1,6})*$")


class MatcherPipeline:
    def __init__(self, nlp: Language, db: dict):
        self.nlp = nlp
        self._exact_m   = PhraseMatcher(nlp.vocab, attr="LOWER")
        self._lemma_m   = PhraseMatcher(nlp.vocab, attr="LEMMA")
        self._uni_m     = PhraseMatcher(nlp.vocab, attr="LOWER")
        self._abbrev_m  = PhraseMatcher(nlp.vocab, attr="ORTH")
        self._id_to_label: dict[str, str] = {}

        self._build(db)

    def _build(self, db: dict):
        skill_ids = []
        all_label_lists = []

        for skill_id, entry in db.items():
            self._id_to_label[skill_id] = entry["label"]
            labels = [entry["label"]] + entry.get("alt_labels", [])
            skill_ids.append(skill_id)
            all_label_lists.append(labels)

        # Flatten all labels, run through pipeline once in batch
        flat_labels = [lbl for labels in all_label_lists for lbl in labels]
        flat_docs = list(self.nlp.pipe(flat_labels))

        idx = 0
        for skill_id, labels in zip(skill_ids, all_label_lists):
            for lbl in labels:
                doc = flat_docs[idx]; idx += 1
                make_doc = self.nlp.make_doc(lbl)

                self._exact_m.add(skill_id, [make_doc])
                self._lemma_m.add(skill_id, [doc])

                if len(lbl.split()) == 1:
                    self._uni_m.add(skill_id, [make_doc])

                if _ABBREV_RE.match(lbl.upper()) and len(lbl) <= 10:
                    self._abbrev_m.add(skill_id, [self.nlp.make_doc(lbl.upper())])

        print(f"[MatcherPipeline] Built matchers for {len(db):,} skills.")

    def match(self, doc: Doc) -> List[Tuple[str, str, str, int, int]]:
        """
        Run all strategies against *doc*.

        Returns a list of raw hits:
            (skill_id, label, match_type, start_token, end_token)
        """
        hits = []

        for match_id, start, end in self._exact_m(doc):
            skill_id = doc.vocab.strings[match_id]
            hits.append((skill_id, self._id_to_label[skill_id], "exact", start, end))

        for match_id, start, end in self._lemma_m(doc):
            skill_id = doc.vocab.strings[match_id]
            hits.append((skill_id, self._id_to_label[skill_id], "lemma", start, end))

        for match_id, start, end in self._uni_m(doc):
            skill_id = doc.vocab.strings[match_id]
            hits.append((skill_id, self._id_to_label[skill_id], "unigram", start, end))

        for match_id, start, end in self._abbrev_m(doc):
            skill_id = doc.vocab.strings[match_id]
            hits.append((skill_id, self._id_to_label[skill_id], "abbrev", start, end))

        return hits