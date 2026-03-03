"""
Preprocess job descriptions for SkillNER + LDA

- Loads:
    caribbeanjobs_jobs.json
    workopolis_jobs.json
    reed_jobs.json

- Cleans text:
    * Remove HTML
    * Remove numbers & special characters
    * Convert to lowercase
    * Tokenise
    * Remove stopwords
    * Remove duplicates
    * NO stemming or lemmatisation

- Outputs:
    processed_jobs.json
"""

import json
import re
from pathlib import Path
from bs4 import BeautifulSoup
import spacy
import hashlib

# -----------------------------
# LOAD SPACY (light)
# -----------------------------
nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])

# -----------------------------
# FILE PATHS
# -----------------------------
DATA_DIR = Path("data/raw")

FILES = [
    "caribbeanjobs_jobs.json",
    "workopolis_jobs.json",
    "reed_jobs.json"
]

OUTPUT_FILE = Path("data/processed/processed_jobs.jsonl")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# -----------------------------
# REGION DETECTION
# -----------------------------
def detect_region(url):
    if not url:
        return None

    url = url.lower()

    if "caribbeanjobs.com" in url:
        return "Caribbean"
    elif "reed.co.uk" in url:
        return "UK"
    elif "workopolis.com" in url:
        return "Canada"
    else:
        return "Unknown"

# -----------------------------
# SkillNER-safe normalizations
# -----------------------------
TECH_NORMALIZATIONS = [
    (r"\bc\s*\+\s*\+\b", "C++"),
    (r"\bc\s*#\b", "C#"),
    (r"\bf\s*#\b", "F#"),
    (r"\bobjective\s*-\s*c\b", "Objective-C"),
    (r"\bobjective\s+c\b", "Objective-C"),

    (r"\bdot\s*net\b", ".NET"),
    (r"\basp\s*\.?\s*net\b", "ASP.NET"),

    (r"\bnode\s*\.?\s*js\b", "Node.js"),
    (r"\breact\s*\.?\s*js\b", "React.js"),
    (r"\bvue\s*\.?\s*js\b", "Vue.js"),
    (r"\bnext\s*\.?\s*js\b", "Next.js"),
    (r"\bnuxt\s*\.?\s*js\b", "Nuxt.js"),

    (r"\bci\s*\/\s*cd\b", "CI/CD"),
    (r"\bdev\s*\/\s*sec\s*\/\s*ops\b", "DevSecOps"),

    (r"\bmachine[-\s]?learning\b", "machine learning"),
    (r"\bdeep[-\s]?learning\b", "deep learning"),
    (r"\breinforcement[-\s]?learning\b", "reinforcement learning"),
    (r"\bnatural[-\s]?language[-\s]?processing\b", "natural language processing"),
    (r"\bcomputer[-\s]?vision\b", "computer vision"),
    (r"\bartificial intelligence\b", "artificial intelligence"),

    (r"\bamazon web services\b", "AWS"),
    (r"\bgoogle cloud platform\b", "GCP"),
    (r"\bmicrosoft azure\b", "Azure"),

    (r"\bpostgre\s*sql\b", "PostgreSQL"),
    (r"\bpostgres\s*sql\b", "PostgreSQL"),
    (r"\bmy\s*sql\b", "MySQL"),
    (r"\bmongo\s*db\b", "MongoDB"),
    (r"\bno\s*sql\b", "NoSQL"),

    (r"\btcp\s*\/\s*ip\b", "TCP/IP"),
    (r"\bwi[\-\s]?fi\b", "Wi-Fi"),
]

# -----------------------------
# CLEAN TEXT
# -----------------------------
def clean_text(text):

    if not text:
        return ""

    text = BeautifulSoup(text, "html.parser").get_text(separator=" ")

    # Apply SkillNER normalisations
    for pattern, replacement in TECH_NORMALIZATIONS:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    # Remove unwanted characters but KEEP tech punctuation
    text = re.sub(r"[^\w\+\#\./\-\s]", " ", text)

    text = re.sub(r"\s+", " ", text).strip()

    return text

# -----------------------------
# TOKENIZE
# -----------------------------
def tokenize(text):

    doc = nlp(text)

    tokens = []

    for token in doc:
        if token.is_stop:
            continue
        if token.is_punct:
            continue
        if len(token.text) < 2:
            continue

        tokens.append(token.text)

    return tokens

# -----------------------------
# LOAD JOBS (JSON safe)
# -----------------------------
def load_jobs():

    all_jobs = []

    for file in FILES:

        path = DATA_DIR / file

        if not path.exists():
            print(f"Warning: {path} not found.")
            continue

        print(f"Loading {path}...")

        with open(path, "r", encoding="utf-8") as f:
            try:
                jobs = json.load(f)   # <-- correct for JSON array
                all_jobs.extend(jobs)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in {file}: {e}")

    return all_jobs
# -----------------------------
# MAIN
# -----------------------------
def preprocess_jobs():

    jobs = load_jobs()
    print(f"Total raw jobs loaded: {len(jobs)}")

    total_processed = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:

        seen_urls = set()
        seen_hashes = set()

        for job in jobs:

            url = job.get("url")
            if not url:
                continue

            # --------
            # Layer 1: URL dedupe
            # --------
            if url in seen_urls:
                continue
            seen_urls.add(url)

            raw_desc = job.get("description", "")
            if not raw_desc:
                continue

            cleaned = clean_text(raw_desc)
            if not cleaned:
                continue

            tokens = tokenize(cleaned)
            if not tokens:
                continue

            clean_text_str = " ".join(tokens)

            # --------
            # Layer 2: Content hash dedupe
            # --------
            content_hash = hashlib.md5(clean_text_str.encode()).hexdigest()

            if content_hash in seen_hashes:
                continue
            seen_hashes.add(content_hash)

            region = detect_region(url)

            processed_job = {
                "region": region,
                "job_category": job.get("category"),
                "job_title": job.get("title") or job.get("job_title"),
                "url": url,
                "clean_text": clean_text_str
            }

            out.write(json.dumps(processed_job, ensure_ascii=False) + "\n")
            total_processed += 1

    print(f"Total processed jobs: {total_processed}")
    print(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_jobs()