#!/usr/bin/env python3
"""
Extract skills from preprocessed Faculty of Medical Sciences (FMS) courses using SkillNER.

Input : out/fms_courses_preprocessed.jsonl
Output: out/fms_courses_skills.jsonl

Expected input fields (per line):
- course_code
- clean_text

Output fields:
- course_code
- skills (list of skill strings, sorted by score desc)
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import spacy
from spacy.matcher import PhraseMatcher

from skillNer.general_params import SKILL_DB
from skillNer.skill_extractor_class import SkillExtractor


INPUT_FILE = Path("out/fms_courses_preprocessed.jsonl")
OUTPUT_FILE = Path("out/fms_courses_skills.jsonl")

# SkillNER works fine with sm; you can upgrade later to en_core_web_lg if desired.
SPACY_MODEL = "en_core_web_sm"


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_jsonl(path: Path, rows: Iterator[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def collect_skills(annotations: Dict[str, Any]) -> List[str]:
    """
    Combine full_matches + ngram_scored, keep best score per skill, return sorted skill list.
    """
    results = annotations.get("results", {}) if isinstance(annotations, dict) else {}
    full_matches = results.get("full_matches", []) or []
    ngram_scored = results.get("ngram_scored", []) or []

    best: Dict[str, float] = {}

    def ingest(items: List[Dict[str, Any]]) -> None:
        for it in items:
            name = (it.get("doc_node_value") or "").strip()
            if not name:
                continue
            score = float(it.get("score", 0.0) or 0.0)

            key = name.lower()
            if key not in best or score > best[key]:
                best[key] = score

    ingest(full_matches)
    ingest(ngram_scored)

    # Sort by score desc, then alphabetical
    sorted_items: List[Tuple[str, float]] = sorted(best.items(), key=lambda x: (-x[1], x[0]))
    return [name for name, _ in sorted_items]


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing file: {INPUT_FILE.resolve()}")

    # Load spaCy model
    try:
        nlp = spacy.load(SPACY_MODEL)
    except OSError as e:
        raise SystemExit(
            f"spaCy model '{SPACY_MODEL}' not installed.\n"
            f"Run:\n"
            f"  python -m spacy download {SPACY_MODEL}\n"
        ) from e

    # Initialize SkillNER
    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

    def gen_out() -> Iterator[Dict[str, Any]]:
        for rec in iter_jsonl(INPUT_FILE):
            course_code = (rec.get("course_code") or "").strip()
            text_for_skills = (rec.get("clean_text") or "").strip()

            # Keep technical punctuation (+/#/./-/etc) intact.
            # Convert underscores to spaces just in case.
            text_for_skills = text_for_skills.replace("_", " ")

            if not course_code:
                continue

            if not text_for_skills:
                yield {"course_code": course_code, "skills": []}
                continue

            annotations = skill_extractor.annotate(text_for_skills)
            skills = collect_skills(annotations)

            yield {
                "course_code": course_code,
                "skills": skills,
            }

    write_jsonl(OUTPUT_FILE, gen_out())
    print(f"Done. Skills saved to: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()