#!/usr/bin/env python3
"""
Preprocess Faculty of Medical Sciences (FMS) course descriptions for a SkillNER-first pipeline
(Skill Extraction -> Topic Modelling).

Input : out/fms_courses.json   (list[{"course_code": "...", "description": "..."}])
Output: out/fms_courses_preprocessed.jsonl

Output fields:
- course_code
- clean_text   (SkillNER-friendly cleaned text; preserves useful medical/technical forms like
               HbA1c, pH, Na+/K+, IgG, T-cell, SARS-CoV-2, PCR, ELISA, CT/MRI, etc.)

Rules:
- Minimal cleaning: remove HTML, normalize whitespace, remove odd characters
- Keep letters, numbers, spaces, and a small set of punctuation helpful for skill surface forms:
  +, #, ., -, /, _, %, (), :  and optionally the Greek letters μ α β γ
- Ignore specific course codes: FOUN 1101, FOUN 1102, FOUN 1301, SPAN 1013
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List


# -----------------------------
# Configuration
# -----------------------------

INPUT_FILE = Path("out/fms_courses.json")
OUTPUT_FILE = Path("out/fms_courses_preprocessed.jsonl")

IGNORE_CODES = {"FOUN 1101", "FOUN 1102", "FOUN 1301", "SPAN 1013"}


# -----------------------------
# Regex
# -----------------------------

HTML_TAG_RE = re.compile(r"<[^>]+>")
MULTISPACE_RE = re.compile(r"\s+")

# Keep letters, numbers, spaces, and punctuation that helps preserve medical/technical tokens:
# +  (Na+, K+)
# #  (rare, but safe)
# .  (HbA1c, e.g., abbreviations; also keeps dotted forms)
# -  (T-cell, evidence-based)
# /  (CT/MRI, mg/dL)
# _  (optional)
# %  (percent)
# () : (units, annotations)
# μ α β γ (common in science; safe for skills)
ALLOWED_CHARS_RE = re.compile(r"[^a-zA-Z0-9\s\+\#\.\-\/_%\(\):_μμβαγ]")


# -----------------------------
# SkillNER-safe normalizations
# (light touch; mainly unifies common variants)
# -----------------------------

NORMALIZATIONS = [
    # Normalize ampersand to "and" (SkillNER tends to prefer words)
    (r"&", " and "),

    # Common course-delivery acronyms / terms
    (r"\bproblem[-\s]?based[-\s]?learning\b", "PBL"),
    (r"\bteam[-\s]?based[-\s]?learning\b", "TBL"),
    (r"\bobjective[-\s]?structured[-\s]?clinical[-\s]?examination\b", "OSCE"),

    # Common lab/diagnostic terms spacing variants
    (r"\bpolymerase[-\s]?chain[-\s]?reaction\b", "PCR"),
    (r"\benzyme[-\s]?linked[-\s]?immunosorbent[-\s]?assay\b", "ELISA"),

    # Public health phrasing variants
    (r"\bprimary[-\s]?health[-\s]?care\b", "primary care"),
    (r"\bpublic[-\s]?health\b", "public health"),
]

NORMALIZATIONS = [(re.compile(p, re.IGNORECASE), r) for p, r in NORMALIZATIONS]


# -----------------------------
# Helpers
# -----------------------------

def normalize_course_code(code: str) -> str:
    """
    Make matching robust:
    - strip
    - collapse multiple spaces
    - uppercase
    """
    code = MULTISPACE_RE.sub(" ", (code or "").strip())
    return code.upper()


def normalize_text(text: str) -> str:
    for pattern, repl in NORMALIZATIONS:
        text = pattern.sub(repl, text)
    return text


def clean_text(text: str) -> str:
    # Remove HTML
    text = HTML_TAG_RE.sub(" ", text)

    # Normalize a few common variants first
    text = normalize_text(text)

    # Remove other weird characters but keep the important technical punctuation
    text = ALLOWED_CHARS_RE.sub(" ", text)

    # Normalize whitespace
    text = MULTISPACE_RE.sub(" ", text).strip()

    return text


def write_jsonl(path: Path, rows: Iterator[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input not found: {INPUT_FILE.resolve()}")

    with INPUT_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected the input JSON to be a list of course objects.")

    def gen_rows() -> Iterator[Dict[str, Any]]:
        for rec in data:
            if not isinstance(rec, dict):
                continue

            course_code_raw = rec.get("course_code", "")
            course_code = normalize_course_code(course_code_raw)

            if not course_code:
                continue

            # Ignore the specified course codes
            if course_code in IGNORE_CODES:
                continue

            desc = rec.get("description", "")
            if not isinstance(desc, str) or not desc.strip():
                continue

            cleaned = clean_text(desc)

            # Skip if cleaning wipes everything
            if not cleaned:
                continue

            yield {
                "course_code": course_code,
                "clean_text": cleaned,
            }

    write_jsonl(OUTPUT_FILE, gen_rows())
    print(f"Done. Output saved to: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()