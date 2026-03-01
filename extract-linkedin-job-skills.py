#!/usr/bin/env python3
"""
Skill extraction (SkillNER) for preprocessed LinkedIn jobs (SkillNER-first pipeline).

Input : out/linkedin_jobs_preprocessed.jsonl
Output: out/linkedin_jobs_skills.jsonl

Expected input fields per record:
- region
- job_category
- job_title
- clean_text

Output fields:
- region
- job_category
- job_title
- skills (unique list of extracted skill strings, ordered by confidence)
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import spacy
from spacy.matcher import PhraseMatcher

from skillNer.general_params import SKILL_DB
from skillNer.skill_extractor_class import SkillExtractor


INPUT_FILE = Path("out/linkedin_jobs_preprocessed.jsonl")
OUTPUT_FILE = Path("out/linkedin_jobs_skills.jsonl")

SPACY_MODEL = "en_core_web_sm"


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
                continue


def write_jsonl(path: Path, records: Iterator[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def extract_unique_skills(annotation: Dict[str, Any]) -> List[str]:
    """
    Combine full_matches + ngram_scored.
    Keep best score per skill (case-insensitive), then sort by score desc.
    Returns a simple list of skill strings.
    """
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

            key = name.lower()
            if key not in best or score > best[key]:
                best[key] = score

    ingest(full_matches)
    ingest(ngram_scored)

    # Sort by score desc then alpha for stability
    sorted_items: List[Tuple[str, float]] = sorted(best.items(), key=lambda x: (-x[1], x[0]))
    return [name for name, _ in sorted_items]


def main(
    input_path: str = str(INPUT_FILE),
    output_path: str = str(OUTPUT_FILE),
    spacy_model: str = SPACY_MODEL,
) -> None:
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file.resolve()}")

    # Load spaCy model (SkillNER uses it mainly for tokenization + PhraseMatcher)
    try:
        nlp = spacy.load(spacy_model, disable=["parser", "ner", "textcat", "tagger", "lemmatizer"])
    except OSError as e:
        raise SystemExit(
            f"spaCy model '{spacy_model}' not installed.\n"
            f"Run:\n"
            f"  python -m spacy download {spacy_model}\n"
        ) from e

    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

    def gen_out() -> Iterator[Dict[str, Any]]:
        count = 0

        for rec in read_jsonl(input_file):
            region = rec.get("region")
            job_category = rec.get("job_category")
            job_title = rec.get("job_title")
            text_for_skillner = (rec.get("clean_text") or "").strip()

            # If you ever used underscores in previous pipelines, convert them to spaces.
            # Do NOT touch +/#/. because we want C++/C#/.NET/Node.js preserved.
            text_for_skillner = text_for_skillner.replace("_", " ")

            skills: List[str] = []
            if text_for_skillner:
                try:
                    annotation = skill_extractor.annotate(text_for_skillner)
                    skills = extract_unique_skills(annotation)
                except Exception as e:
                    print(f"[WARN] Skill extraction failed for job_title={job_title!r}: {e}")
                    skills = []

            yield {
                "region": region,
                "job_category": job_category,
                "job_title": job_title,
                "skills": skills,
            }

            count += 1
            if count % 100 == 0:
                print(f"[INFO] Extracted skills for {count} records...")

    write_jsonl(output_file, gen_out())
    print(f"[DONE] Wrote output to: {output_file.resolve()}")


if __name__ == "__main__":
    main()