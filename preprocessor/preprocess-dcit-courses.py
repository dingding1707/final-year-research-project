#!/usr/bin/env python3
"""
Preprocess DCIT course content for SkillNER-first pipeline (Skill Extraction -> Topic Modelling).

Input : out/outcomes_and_content.json
Output: out/dcit_courses_preprocessed.jsonl

Output fields:
- course_code
- clean_text   (SkillNER-friendly cleaned text; preserves C++, C#, .NET, Node.js, etc.)

Notes:
- Minimal cleaning: remove HTML, normalize whitespace, unify common variants
- Keep +, #, . to preserve technical surface forms for SkillNER
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List


# -----------------------------
# Configuration
# -----------------------------

INPUT_FILE = Path("out/outcomes_and_content.json")
OUTPUT_FILE = Path("out/dcit_courses_preprocessed.jsonl")


# -----------------------------
# Regex
# -----------------------------

HTML_TAG_RE = re.compile(r"<[^>]+>")
MULTISPACE_RE = re.compile(r"\s+")

# Keep letters, numbers, spaces, and tech punctuation needed for skills:
# + (C++), # (C#), . (.NET, Node.js), - (state-of-the-art), / (TCP/IP), _ (optional)
ALLOWED_CHARS_RE = re.compile(r"[^a-zA-Z0-9\s\+\#\.\-\/_]")


# -----------------------------
# SkillNER-safe normalizations
# (avoid cpp/csharp/dotnet/aspnet forms)
# -----------------------------

TECH_NORMALIZATIONS = [
    # Language formatting variants
    (r"\bc\s*\+\s*\+\b", "C++"),
    (r"\bc\s*#\b", "C#"),
    (r"\bf\s*#\b", "F#"),
    (r"\bobjective\s*-\s*c\b", "Objective-C"),
    (r"\bobjective\s+c\b", "Objective-C"),

    # JS/TS spacing variants
    (r"\bjava\s*script\b", "JavaScript"),
    (r"\btype\s*script\b", "TypeScript"),

    # .NET variants
    (r"\bdot\s*net\b", ".NET"),
    (r"\basp\s*\.?\s*net\b", "ASP.NET"),

    # Web framework punctuation variants
    (r"\bnode\s*\.?\s*js\b", "Node.js"),
    (r"\breact\s*\.?\s*js\b", "React.js"),
    (r"\bvue\s*\.?\s*js\b", "Vue.js"),
    (r"\bnext\s*\.?\s*js\b", "Next.js"),
    (r"\bnuxt\s*\.?\s*js\b", "Nuxt.js"),

    # AI/ML phrase variants (keep spaces, no underscores)
    (r"\bmachine[-\s]?learning\b", "machine learning"),
    (r"\bdeep[-\s]?learning\b", "deep learning"),
    (r"\breinforcement[-\s]?learning\b", "reinforcement learning"),
    (r"\bnatural[-\s]?language[-\s]?processing\b", "natural language processing"),
    (r"\bcomputer[-\s]?vision\b", "computer vision"),

    # Databases common spacing variants
    (r"\bpostgre\s*sql\b", "PostgreSQL"),
    (r"\bpostgres\s*sql\b", "PostgreSQL"),
    (r"\bmy\s*sql\b", "MySQL"),
    (r"\bmongo\s*db\b", "MongoDB"),
    (r"\bno\s*sql\b", "NoSQL"),

    # Cloud vendor long forms
    (r"\bamazon web services\b", "AWS"),
    (r"\bgoogle cloud platform\b", "GCP"),
    (r"\bmicrosoft azure\b", "Azure"),

    # Networking formatting
    (r"\btcp\s*\/\s*ip\b", "TCP/IP"),
]

TECH_NORMALIZATIONS = [(re.compile(p, re.IGNORECASE), r) for p, r in TECH_NORMALIZATIONS]


# -----------------------------
# Flatten course record
# -----------------------------

def flatten_record(rec: Dict[str, Any]) -> str:
    parts: List[str] = []

    for field in ("description", "rationale", "aims"):
        val = rec.get(field)
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())

    lo = rec.get("learning_outcomes")
    if isinstance(lo, list):
        lo_text = " ".join(str(x).strip() for x in lo if str(x).strip())
        if lo_text:
            parts.append(lo_text)

    cc = rec.get("course_content")
    if isinstance(cc, dict):
        for topic, bullets in cc.items():
            if topic:
                parts.append(str(topic).strip())
            if isinstance(bullets, list):
                for b in bullets:
                    b = str(b).strip()
                    if b:
                        parts.append(b)
            elif bullets:
                parts.append(str(bullets).strip())

    return "\n".join(parts).strip()


# -----------------------------
# Cleaning
# -----------------------------

def normalize_tech(text: str) -> str:
    for pattern, repl in TECH_NORMALIZATIONS:
        text = pattern.sub(repl, text)
    return text


def clean_text(text: str) -> str:
    # Remove HTML
    text = HTML_TAG_RE.sub(" ", text)

    # Normalize tech variants first (keeps meaningful punctuation like C++ / Node.js)
    text = normalize_tech(text)

    # Remove other weird characters but keep the important tech punctuation
    text = ALLOWED_CHARS_RE.sub(" ", text)

    # Normalize whitespace
    text = MULTISPACE_RE.sub(" ", text).strip()

    return text


# -----------------------------
# IO
# -----------------------------

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

    def gen_rows() -> Iterator[Dict[str, Any]]:
        for rec in data:
            course_code = (rec.get("course_code") or "").strip()
            raw = flatten_record(rec)
            cleaned = clean_text(raw)

            yield {
                "course_code": course_code,
                "clean_text": cleaned,
            }

    write_jsonl(OUTPUT_FILE, gen_rows())
    print(f"Done. Output saved to: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()