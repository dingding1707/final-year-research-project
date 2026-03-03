import fitz  # PyMuPDF
import re
import json
import argparse
from typing import List, Dict, Optional


TARGET_SCHOOLS = {
    "SCHOOL OF MEDICINE",
    "SCHOOL OF DENTISTRY",
    "SCHOOL OF VETERINARY MEDICINE",
    "SCHOOL OF PHARMACY",
    "SCHOOL OF NURSING",
}

# Marks lines that usually indicate we've left a description block
END_LABEL_RE = re.compile(
    r"^(LEVEL|SEMESTER|COURSE CODE|COURSE TITLE|COURSE CREDITS|NUMBER OF CREDITS|"
    r"PREREQUISITE\(S\)|ASSESSMENT|PROGRAM NOTE|Return to Table of Contents|"
    r"UNDERGRADUATE REGULATIONS)",
    re.IGNORECASE
)

COURSE_CODE_RE = re.compile(r"^COURSE\s+CODE\s*:\s*(.+)\s*$", re.IGNORECASE)
COURSE_DESC_RE = re.compile(r"^COURSE\s+DESCRIPTION\s*:\s*(.*)\s*$", re.IGNORECASE)

COURSE_DESCRIPTIONS_SECTION_RE = re.compile(r"^COURSE\s+DESCRIPTIONS\b", re.IGNORECASE)


def clean_text(s: str) -> str:
    # Remove common odd PDF artifacts and normalize whitespace
    s = s.replace("\uFFFD", " ")  # replacement char
    s = s.replace("\u00ad", "")   # soft hyphen
    s = s.replace("\u2010", "-")  # hyphen
    s = s.replace("\u2011", "-")  # non-breaking hyphen
    s = s.replace("\uFEFF", "")   # BOM
    # This PDF sometimes has "￾" style artifacts; remove any non-printing-ish leftovers:
    s = re.sub(r"[\u0000-\u001F]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_courses(pdf_path: str) -> List[Dict[str, str]]:
    doc = fitz.open(pdf_path)

    in_course_descriptions = False
    current_school: Optional[str] = None

    current_code: Optional[str] = None
    collecting_desc = False
    desc_lines: List[str] = []

    results: List[Dict[str, str]] = []

    for page_idx in range(len(doc)):
        page = doc[page_idx]
        text = page.get_text("text") or ""
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        for line in lines:
            # 1) Only begin parsing once we reach the Course Descriptions section
            if not in_course_descriptions and COURSE_DESCRIPTIONS_SECTION_RE.match(line):
                in_course_descriptions = True
                current_school = None
                # reset any dangling state
                current_code = None
                collecting_desc = False
                desc_lines = []
                continue

            if not in_course_descriptions:
                continue

            # 2) Detect which school we are currently inside
            if line.upper() in TARGET_SCHOOLS:
                # If we were collecting a description, close it cleanly when a new school starts
                if collecting_desc and current_code and desc_lines:
                    results.append({
                        "course_code": clean_text(current_code),
                        "description": clean_text(" ".join(desc_lines)),
                    })
                current_school = line.upper()
                current_code = None
                collecting_desc = False
                desc_lines = []
                continue

            # We only collect courses for the target schools
            if current_school not in TARGET_SCHOOLS:
                continue

            # 3) If we are collecting a description, see if we should stop
            if collecting_desc:
                if END_LABEL_RE.match(line):
                    if current_code and desc_lines:
                        results.append({
                            "course_code": clean_text(current_code),
                            "description": clean_text(" ".join(desc_lines)),
                        })
                    # Reset description state
                    collecting_desc = False
                    desc_lines = []
                    # Do NOT clear current_school (still in same school)
                    # Do clear current_code because one description belongs to one course
                    current_code = None
                    # Continue processing this label line normally (it might contain a new course code later)
                    # but in practice labels are separate lines.
                    continue
                else:
                    desc_lines.append(line)
                    continue

            # 4) Capture course code
            m_code = COURSE_CODE_RE.match(line)
            if m_code:
                current_code = m_code.group(1).strip()
                continue

            # 5) Capture course description (must have a code already, but we won't strictly require it)
            m_desc = COURSE_DESC_RE.match(line)
            if m_desc:
                collecting_desc = True
                first_piece = m_desc.group(1).strip()
                desc_lines = [first_piece] if first_piece else []
                continue

    # Close any trailing description at EOF
    if collecting_desc and current_code and desc_lines:
        results.append({
            "course_code": clean_text(current_code),
            "description": clean_text(" ".join(desc_lines)),
        })

    doc.close()
    return results


def main():
    parser = argparse.ArgumentParser(description="Extract course_code + description JSON from FMS PDF using PyMuPDF.")
    parser.add_argument("--pdf", required=True, help="Path to input PDF")
    parser.add_argument("--out", required=True, help="Path to output JSON file")
    args = parser.parse_args()

    courses = extract_courses(args.pdf)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(courses, f, ensure_ascii=False, indent=2)

    print(f"Extracted {len(courses)} courses -> {args.out}")


if __name__ == "__main__":
    main()