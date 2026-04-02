"""
Workopolis Skill Extraction (Canada Only - Clean + Frequency Filtered)

Input : data/processed/processed_jobs.jsonl
Output: out/workopolis_skills.jsonl
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple
from collections import Counter

import spacy
from spacy.matcher import PhraseMatcher

from skillNer.general_params import SKILL_DB
from skillNer.skill_extractor_class import SkillExtractor


# ==============================
# CONFIG
# ==============================

INPUT_FILE = Path("data/processed/processed_jobs.jsonl")
OUTPUT_FILE = Path("out/workopolis_skills.jsonl")

SPACY_MODEL = "en_core_web_sm"
MIN_FREQ = 5   
VALID_SHORT_SKILLS = {"ai", "ml", "c", "r", "go", "c#", "c++"}
INVALID_EXACT = {"e", "etc", "eg", "ie", "tools e", "san"}

GENERIC_WORDS = {
    "build", "manage", "develop", "create", "support",
    "maintain", "implement", "monitor", "design", "test"
}


# ==============================
# JSONL IO
# ==============================

def read_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"[WARN] Skipping invalid JSON on line {line_no}")


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ==============================
# CLEANING
# ==============================

def normalize_skill(skill: str) -> str:
    s = skill.lower().strip()

    replacements = {
        "apis": "api",
        "restful apis": "api",
        "rest api": "api",
        "data integrations": "data integration",
        "systems": "system",
        "cloud services": "cloud",
        "sql azure": "azure sql",
    }

    return replacements.get(s, s)


def is_valid_skill(skill: str) -> bool:
    s = skill.strip().lower()

    if not s:
        return False

    if s in VALID_SHORT_SKILLS:
        return True

    if s in INVALID_EXACT:
        return False

    if len(s) == 1:
        return False

    if not any(c.isalpha() for c in s):
        return False

    words = s.split()

    if len(words) > 3:
        return False

    if all(word in GENERIC_WORDS for word in words):
        return False

    if re.fullmatch(r"[a-z]\s*[a-z]?", s):
        return False

    return True


def clean_skills(skills: List[str]) -> List[str]:
    cleaned = []

    for skill in skills:
        s = normalize_skill(skill)

        if not is_valid_skill(s):
            continue

        cleaned.append(s)

    return list(dict.fromkeys(cleaned))


# ==============================
# SKILL EXTRACTION
# ==============================

def extract_unique_skills(annotation: Dict[str, Any]) -> List[str]:
    if not annotation or not isinstance(annotation, dict):
        return []

    results = annotation.get("results") or {}
    full_matches = results.get("full_matches") or []
    ngram_scored = results.get("ngram_scored") or []

    best: Dict[str, float] = {}

    def ingest(items: List[Dict[str, Any]]) -> None:
        for it in items:
            name = (it.get("doc_node_value") or it.get("doc_node_id") or "").strip()
            if not name:
                continue

            score = float(it.get("score", 0.0) or 0.0)

            # 🔴 remove weak matches
            if score < 0.5:
                continue

            key = name.lower()

            if key not in best or score > best[key]:
                best[key] = score

    ingest(full_matches)
    ingest(ngram_scored)

    sorted_items: List[Tuple[str, float]] = sorted(
        best.items(),
        key=lambda x: (-x[1], x[0])
    )

    return [name for name, _ in sorted_items]


# ==============================
# MAIN PIPELINE
# ==============================

def main():

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE.resolve()}")

    # Load spaCy
    try:
        nlp = spacy.load(
            SPACY_MODEL,
            disable=["parser", "ner", "textcat", "tagger", "lemmatizer"]
        )
    except OSError:
        raise SystemExit(
            f"Run: python -m spacy download {SPACY_MODEL}"
        )

    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

    records = []
    skill_counts = Counter()

    # ==============================
    # PASS 1 — Extract everything
    # ==============================

    for rec in read_jsonl(INPUT_FILE):

        region = (rec.get("region") or "").strip()

        # 🇨🇦 ONLY Canada
        if region != "Canada":
            continue

        job_category = rec.get("job_category")
        job_title = rec.get("job_title")

        text = (rec.get("clean_text") or "").replace("_", " ").strip()

        skills = []

        if text:
            try:
                annotation = skill_extractor.annotate(text)

                raw_skills = extract_unique_skills(annotation)
                skills = clean_skills(raw_skills)

            except Exception as e:
                print(f"[WARN] Failed: {job_title}: {e}")

        skill_counts.update(skills)

        records.append({
            "region": region,
            "search_term": rec.get("search_term"),
            "job_category": job_category,
            "job_title": job_title,
            "skills": skills,
        })

    # ==============================
    # PASS 2 — Frequency filtering
    # ==============================

    for rec in records:
        rec["skills"] = [
            skill for skill in rec["skills"]
            if skill_counts[skill] >= MIN_FREQ
        ]

    # ==============================
    # SAVE
    # ==============================

    write_jsonl(OUTPUT_FILE, records)

    print(f"[DONE] Workopolis skills saved → {OUTPUT_FILE.resolve()}")
    print(f"[INFO] Unique skills kept: {len([s for s,c in skill_counts.items() if c >= MIN_FREQ])}")


if __name__ == "__main__":
    main()