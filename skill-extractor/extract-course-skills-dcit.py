#!/usr/bin/env python3
"""
Extract skills from preprocessed DCIT courses using SkillNER.

Goal:
- keep useful skills
- reduce noise from low-quality ngram matches
- preserve important short technical skills like C and R

Input : out/processed/dcit_courses_preprocessed.jsonl
Output:
- out/dcit_courses_skills.jsonl
- out/cs_course_skills.jsonl
- out/it_course_skills.jsonl
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import spacy
from spacy.matcher import PhraseMatcher

from skillNer.general_params import SKILL_DB
from skillNer.skill_extractor_class import SkillExtractor


INPUT_FILE = Path("out/processed/dcit_courses_preprocessed.jsonl")
OUTPUT_FILE = Path("out/dcit_courses_skills.jsonl")
OUTPUT_FILE1 = Path("out/cs_course_skills.jsonl")
OUTPUT_FILE2 = Path("out/it_course_skills.jsonl")

SPACY_MODEL = "en_core_web_sm"

# -----------------------------
# Tuning knobs
# -----------------------------
NGRAM_SCORE_THRESHOLD = 0.55
MAX_SKILL_TOKENS = 6

# Valid one-token skills that should be preserved
VALID_SINGLE_TOKENS = {
    "c",   # C programming language
    "r",   # R language
}

# Valid phrases starting with 'e' (e-commerce style)
VALID_E_PHRASES = {
    "e business",
    "e commerce",
}

# Common junk one-letter tokens / function words
BAD_SINGLE_TOKENS = {
    "a", "an", "and", "or", "of", "to", "in", "on", "for", "by", "at",
    "e", "b", "d", "x", "y", "z"
}

# Broad generic words that often appear in noisy phrases
GENERIC_WORDS = {
    "introduction", "concept", "concepts", "topic", "topics",
    "area", "areas", "aspect", "aspects", "system", "systems",
    "study", "studies", "method", "methods", "use", "uses", "case", "cases"
}

# Normalize common variants so topic vocab is more consistent
REPLACEMENTS = {
    "c programming": "c",
    "r language": "r",
}


def canonicalize_symbolic_skills(text: str) -> str:
    """
    Normalize symbolic language names that may appear with inconsistent spacing.
    """
    # Examples handled: c++, c ++, c+ +, c + +
    text = re.sub(r"\bc\s*\+\s*\+(?=\s|$)", "c++", text)
    return text


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def normalize_skill_text(text: str) -> str:
    """
    Light normalization only.
    Assumes most text normalization already happened upstream.
    """
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = canonicalize_symbolic_skills(text)
    text = REPLACEMENTS.get(text, text)
    return text


def is_repeated_phrase(skill: str) -> bool:
    """
    Reject phrases like:
    - data data
    - analysis analysis
    """
    parts = skill.split()
    return len(parts) >= 2 and len(set(parts)) == 1


def has_too_many_short_tokens(skill: str) -> bool:
    """
    Reject phrases dominated by meaningless 1-character tokens,
    while preserving valid short skills like 'c' and 'r'.
    """
    parts = skill.split()
    if not parts:
        return True

    if len(parts) == 1:
        return parts[0] not in VALID_SINGLE_TOKENS and len(parts[0]) == 1

    short_count = sum(1 for p in parts if len(p) == 1 and p not in VALID_SINGLE_TOKENS)
    return short_count >= 2


def starts_or_ends_with_bad_short_token(skill: str) -> bool:
    """
    Reject awkward phrases like:
    - tools e
    - development e
    - businesses e
    - o big

    But allow known valid phrases like e commerce, e business, and valid leading/trailing short skills.
    """
    # Allow whitelisted e-commerce style phrases
    if skill in VALID_E_PHRASES:
        return False

    parts = skill.split()
    if not parts:
        return True

    first = parts[0]
    last = parts[-1]

    if len(first) == 1 and first not in VALID_SINGLE_TOKENS:
        return True
    if len(last) == 1 and last not in VALID_SINGLE_TOKENS:
        return True

    return False


def looks_like_noise(skill: str) -> bool:
    """
    Conservative filtering:
    - reject empty / clearly bad 1-char items
    - reject repeated phrases
    - reject overly long phrases
    - reject phrases with too many junk short tokens
    - reject phrases made only of generic filler
    - reject awkward phrases starting/ending with junk short tokens
    """
    if not skill:
        return True

    parts = skill.split()

    if len(parts) > MAX_SKILL_TOKENS:
        return True

    if len(skill) == 1 and skill not in VALID_SINGLE_TOKENS:
        return True

    if skill in BAD_SINGLE_TOKENS:
        return True

    if is_repeated_phrase(skill):
        return True

    if has_too_many_short_tokens(skill):
        return True

    if len(parts) > 1 and starts_or_ends_with_bad_short_token(skill):
        return True

    if all(p in GENERIC_WORDS for p in parts):
        return True

    # Allow common technical characters, reject other odd leftovers
    compact = re.sub(r"[a-z0-9+#.\-/ ]", "", skill)
    if compact:
        return True

    return False


def should_keep_match(name: str, score: float, source: str) -> bool:
    """
    Keep full matches unless they are clearly bad.
    Keep ngram matches only if they pass stronger filters.
    """
    skill = normalize_skill_text(name)

    if looks_like_noise(skill):
        return False

    if source == "full":
        return True

    if source == "ngram":
        if score < NGRAM_SCORE_THRESHOLD:
            return False

        parts = skill.split()
        if len(parts) == 1 and skill in GENERIC_WORDS:
            return False

        return True

    return False


def collect_skills(annotations: Dict[str, Any]) -> List[str]:
    """
    Combine full_matches + filtered ngram_scored,
    keep best score per normalized skill,
    return sorted skill list.
    """
    results = annotations.get("results", {}) if isinstance(annotations, dict) else {}
    full_matches = results.get("full_matches", []) or []
    ngram_scored = results.get("ngram_scored", []) or []

    best: Dict[str, float] = {}

    def ingest(items: List[Dict[str, Any]], source: str) -> None:
        for it in items:
            raw_name = (it.get("doc_node_value") or "").strip()
            if not raw_name:
                continue

            score = float(it.get("score", 0.0) or 0.0)
            skill = normalize_skill_text(raw_name)

            if not should_keep_match(skill, score, source):
                continue

            if skill not in best or score > best[skill]:
                best[skill] = score

    ingest(full_matches, "full")
    ingest(ngram_scored, "ngram")

    sorted_items: List[Tuple[str, float]] = sorted(
        best.items(),
        key=lambda x: (-x[1], x[0])
    )
    return [name for name, _ in sorted_items]


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing file: {INPUT_FILE.resolve()}")

    try:
        nlp = spacy.load(SPACY_MODEL)
    except OSError as e:
        raise SystemExit(
            f"spaCy model '{SPACY_MODEL}' not installed.\n"
            f"Run:\n"
            f"  python -m spacy download {SPACY_MODEL}\n"
        ) from e

    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

    rows: List[Dict[str, Any]] = []

    for rec in iter_jsonl(INPUT_FILE):
        course_code = (rec.get("course_code") or "").strip()
        course_name = (rec.get("course_name") or "").strip()
        thematic_areas = rec.get("thematic_areas") or []
        text_for_skills = (rec.get("clean_text") or "").strip()

        if not course_code:
            continue

        if not isinstance(thematic_areas, list):
            thematic_areas = [thematic_areas] if thematic_areas else []

        # Convert underscores to spaces for SkillNER matching
        text_for_skills = text_for_skills.replace("_", " ")

        if not text_for_skills:
            rows.append({
                "course_code": course_code,
                "course_name": course_name,
                "thematic_areas": thematic_areas,
                "skills": [],
            })
            continue

        annotations = skill_extractor.annotate(text_for_skills)
        skills = collect_skills(annotations)

        rows.append({
            "course_code": course_code,
            "course_name": course_name,
            "thematic_areas": thematic_areas,
            "skills": skills,
        })

    write_jsonl(OUTPUT_FILE, rows)

    cs_rows = [
        row for row in rows
        if "computer_science" in row.get("thematic_areas", [])
    ]

    it_rows = [
        row for row in rows
        if "information_technology" in row.get("thematic_areas", [])
    ]

    write_jsonl(OUTPUT_FILE1, cs_rows)
    write_jsonl(OUTPUT_FILE2, it_rows)

    print("Done. Skills saved to:")
    print(f"  - {OUTPUT_FILE.resolve()}")
    print(f"  - {OUTPUT_FILE1.resolve()}")
    print(f"  - {OUTPUT_FILE2.resolve()}")
    print(f"Processed {len(rows)} courses.")


if __name__ == "__main__":
    main()