#!/usr/bin/env python3
"""
Preprocess job descriptions for SkillNER-first methodology:
Preprocessing -> Skill Extraction -> Topic Modelling -> Correlation

Input : out/linkedin_jobs.jsonl
Output: out/linkedin_jobs_preprocessed.jsonl

Output fields:
- region
- job_category
- job_title
- clean_text   (SkillNER-friendly; preserves C++, C#, .NET, Node.js, CI/CD, TCP/IP)

Cleaning:
- Remove HTML tags
- Normalize common tech variants to canonical SkillNER-friendly surface forms
- Keep + # . / - so skills remain detectable
- Normalize whitespace
"""

import json
import re
from pathlib import Path
from typing import Dict, Any, List, Iterator


# ----------------------------
# Canonical role lists
# ----------------------------

CS_ROLES = [
    "Software Engineer",
    "Software Developer",
    "Backend Developer",
    "Frontend Developer",
    "Full Stack Developer",
    "Mobile Application Developer",
    "Game Developer",
    "Embedded Systems Engineer",
    "DevOps Engineer",
    "Cloud Engineer",
]

IT_ROLES = [
    "IT Support Specialist",
    "Help Desk Technician",
    "Network Administrator",
    "Systems Administrator",
    "Cybersecurity Analyst",
    "Information Security Analyst",
    "Database Administrator",
    "IT Project Manager",
    "IT Operations Analyst",
    "Infrastructure Engineer",
]

AI_ROLES = [
    "Data Scientist",
    "Machine Learning Engineer",
    "AI Engineer",
    "Data Analyst",
    "Business Intelligence Analyst",
    "NLP Engineer",
    "Computer Vision Engineer",
    "Data Engineer",
    "MLOps Engineer",
    "Applied Scientist",
]


# ----------------------------
# Cleaning regex
# ----------------------------

_SPELLED_OUT_RE = re.compile(r"(?:\b[a-z]\s+){3,}[a-z]\b", flags=re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTISPACE_RE = re.compile(r"\s+")

# Keep letters, numbers, spaces, and important tech punctuation:
# + (C++), # (C#), . (.NET, Node.js), / (CI/CD, TCP/IP), - (real-time, end-to-end), _ (optional)
_ALLOWED_CHARS_RE = re.compile(r"[^a-zA-Z0-9\s\+\#\.\-\/_]")


# ----------------------------
# SkillNER-safe normalizations
# (do NOT convert to cpp/csharp/dotnet/nodejs etc.)
# ----------------------------

TECH_NORMALIZATIONS = [
    # C-family variants -> canonical forms
    (r"\bc\s*\+\s*\+\b", "C++"),
    (r"\bc\s*#\b", "C#"),
    (r"\bf\s*#\b", "F#"),
    (r"\bobjective\s*-\s*c\b", "Objective-C"),
    (r"\bobjective\s+c\b", "Objective-C"),

    # .NET ecosystem
    (r"\bdot\s*net\b", ".NET"),
    (r"\basp\s*\.?\s*net\b", "ASP.NET"),

    # JS ecosystem (keep punctuation)
    (r"\bnode\s*\.?\s*js\b", "Node.js"),
    (r"\breact\s*\.?\s*js\b", "React.js"),
    (r"\bvue\s*\.?\s*js\b", "Vue.js"),
    (r"\bnext\s*\.?\s*js\b", "Next.js"),
    (r"\bnuxt\s*\.?\s*js\b", "Nuxt.js"),

    # DevOps / common abbreviations
    (r"\bci\s*\/\s*cd\b", "CI/CD"),
    (r"\bdev\s*\/\s*sec\s*\/\s*ops\b", "DevSecOps"),

    # AI/ML phrase variants (keep spaces, no underscores)
    (r"\bmachine[-\s]?learning\b", "machine learning"),
    (r"\bdeep[-\s]?learning\b", "deep learning"),
    (r"\breinforcement[-\s]?learning\b", "reinforcement learning"),
    (r"\bnatural[-\s]?language[-\s]?processing\b", "natural language processing"),
    (r"\bcomputer[-\s]?vision\b", "computer vision"),
    (r"\bartificial intelligence\b", "artificial intelligence"),

    # Cloud long forms
    (r"\bamazon web services\b", "AWS"),
    (r"\bgoogle cloud platform\b", "GCP"),
    (r"\bmicrosoft azure\b", "Azure"),

    # DB spacing variants
    (r"\bpostgre\s*sql\b", "PostgreSQL"),
    (r"\bpostgres\s*sql\b", "PostgreSQL"),
    (r"\bmy\s*sql\b", "MySQL"),
    (r"\bmongo\s*db\b", "MongoDB"),
    (r"\bno\s*sql\b", "NoSQL"),

    # Networking
    (r"\btcp\s*\/\s*ip\b", "TCP/IP"),
    (r"\bwi[\-\s]?fi\b", "Wi-Fi"),
]

TECH_NORMALIZATIONS = [(re.compile(p, re.IGNORECASE), r) for p, r in TECH_NORMALIZATIONS]


def join_spelled_out_words(text: str) -> str:
    # Fix "c o d e" -> "code" style noise
    def _join(m: re.Match) -> str:
        return m.group(0).replace(" ", "")
    return _SPELLED_OUT_RE.sub(_join, text)


def normalize_tech(text: str) -> str:
    for pattern, repl in TECH_NORMALIZATIONS:
        text = pattern.sub(repl, text)
    return text


def clean_text(text: str) -> str:
    """SkillNER-friendly cleaning: keep important tech punctuation."""
    if not text:
        return ""

    # Remove HTML
    text = _HTML_TAG_RE.sub(" ", text)

    # Reduce letter-spaced noise early
    text = join_spelled_out_words(text)

    # Normalize known variants to canonical skill forms
    text = normalize_tech(text)

    # Remove odd characters but keep + # . / -
    text = _ALLOWED_CHARS_RE.sub(" ", text)

    # Normalize whitespace
    text = _MULTISPACE_RE.sub(" ", text).strip()

    return text


def infer_job_category(job_title: str, title_query: str) -> str:
    candidates = [title_query or "", job_title or ""]
    candidates = [c.lower() for c in candidates if c]

    for role in AI_ROLES:
        r = role.lower()
        if any(r in c for c in candidates):
            return "AI"

    for role in CS_ROLES:
        r = role.lower()
        if any(r in c for c in candidates):
            return "CS"

    for role in IT_ROLES:
        r = role.lower()
        if any(r in c for c in candidates):
            return "IT"

    return "Unknown"


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


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def main(
    input_path: str = "out/linkedin_jobs.jsonl",
    output_path: str = "out/linkedin_jobs_preprocessed.jsonl",
):
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file.resolve()}")

    processed: List[Dict[str, Any]] = []
    count = 0

    for rec in read_jsonl(input_file):
        region = rec.get("region")
        job_title = rec.get("job_title", "")
        title_query = rec.get("title_query", "")
        raw_desc = rec.get("description", "")

        job_category = infer_job_category(job_title=job_title, title_query=title_query)
        cleaned = clean_text(raw_desc)

        processed.append(
            {
                "region": region,
                "job_category": job_category,
                "job_title": job_title,
                "clean_text": cleaned,
            }
        )

        count += 1
        if count % 200 == 0:
            print(f"[INFO] Preprocessed {count} records...")

    write_jsonl(output_file, processed)
    print(f"[DONE] Wrote {len(processed)} records to: {output_file.resolve()}")


if __name__ == "__main__":
    main()