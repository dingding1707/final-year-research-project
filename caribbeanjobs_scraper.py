#!/usr/bin/env python3
"""CaribbeanJobs scraper for ICT role metadata.

This script searches CaribbeanJobs using a role taxonomy and exports job metadata
as JSON and CSV files for downstream NLP / labour-market analysis.

The scraper is dependency-free (stdlib only) so it can run in constrained
research environments.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urljoin
from urllib.request import Request, urlopen

ROLE_TAXONOMY: Dict[str, List[str]] = {
    "Computer Science Roles": [
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
    ],
    "Information Technology Roles": [
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
    ],
    "AI / Data Science Roles": [
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
    ],
}

DEFAULT_SEARCH_URL_TEMPLATE = "https://www.caribbeanjobs.com/JobSearch/Jobs/?Keywords={query}"
DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


@dataclass
class JobRecord:
    role_category: str
    search_role: str
    title: str
    company: str
    location: str
    date_posted: str
    employment_type: str
    salary: str
    source_url: str
    search_url: str
    scraped_at_utc: str


class AnchorParser(HTMLParser):
    """Extract anchors from HTML without external dependencies."""

    def __init__(self) -> None:
        super().__init__()
        self.anchors: List[tuple[str, str]] = []
        self._active_href: str | None = None
        self._active_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href")
        if href:
            self._active_href = href
            self._active_text = []

    def handle_data(self, data: str) -> None:
        if self._active_href is not None:
            self._active_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._active_href is None:
            return
        text = unescape(" ".join(part.strip() for part in self._active_text if part.strip())).strip()
        self.anchors.append((self._active_href, text))
        self._active_href = None
        self._active_text = []


def fetch_html(url: str, timeout_s: int) -> str:
    req = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    with urlopen(req, timeout=timeout_s) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def extract_json_ld_job_postings(html: str) -> List[dict]:
    jobs: List[dict] = []
    scripts = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for script_content in scripts:
        text = unescape(script_content).strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        candidates: Iterable = payload if isinstance(payload, list) else [payload]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            if item.get("@type") == "JobPosting":
                jobs.append(item)
            if item.get("@graph") and isinstance(item["@graph"], list):
                for node in item["@graph"]:
                    if isinstance(node, dict) and node.get("@type") == "JobPosting":
                        jobs.append(node)
    return jobs


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", unescape(str(value))).strip()


def job_record_from_json_ld(job: dict, role_category: str, role: str, search_url: str) -> JobRecord:
    hiring_org = job.get("hiringOrganization") if isinstance(job.get("hiringOrganization"), dict) else {}
    location_block = job.get("jobLocation")
    location = ""
    if isinstance(location_block, dict):
        address = location_block.get("address")
        if isinstance(address, dict):
            location = ", ".join(
                normalize_text(address.get(piece))
                for piece in ("addressLocality", "addressRegion", "addressCountry")
                if normalize_text(address.get(piece))
            )
    return JobRecord(
        role_category=role_category,
        search_role=role,
        title=normalize_text(job.get("title")),
        company=normalize_text(hiring_org.get("name")),
        location=location,
        date_posted=normalize_text(job.get("datePosted")),
        employment_type=normalize_text(job.get("employmentType")),
        salary=normalize_text(str(job.get("baseSalary", ""))),
        source_url=normalize_text(job.get("url")),
        search_url=search_url,
        scraped_at_utc=datetime.now(timezone.utc).isoformat(),
    )


def extract_job_links(html: str, base_url: str) -> List[tuple[str, str]]:
    parser = AnchorParser()
    parser.feed(html)
    seen: set[str] = set()
    results: List[tuple[str, str]] = []
    for href, text in parser.anchors:
        absolute_url = urljoin(base_url, href)
        if absolute_url in seen:
            continue
        is_job_like = bool(re.search(r"/(job|jobs)/", absolute_url, flags=re.IGNORECASE))
        looks_like_title = len(text.split()) >= 2
        if is_job_like and looks_like_title:
            seen.add(absolute_url)
            results.append((absolute_url, text))
    return results


def scrape_role(
    role_category: str,
    role: str,
    search_url_template: str,
    timeout_s: int,
    sleep_s: float,
) -> List[JobRecord]:
    search_url = search_url_template.format(query=quote_plus(role))
    html = fetch_html(search_url, timeout_s=timeout_s)
    records: List[JobRecord] = []

    json_ld_jobs = extract_json_ld_job_postings(html)
    if json_ld_jobs:
        for job in json_ld_jobs:
            records.append(job_record_from_json_ld(job, role_category, role, search_url))
    else:
        # Fallback: keep minimal metadata from link cards when JobPosting schema is absent.
        for url, title in extract_job_links(html, base_url=search_url):
            records.append(
                JobRecord(
                    role_category=role_category,
                    search_role=role,
                    title=normalize_text(title),
                    company="",
                    location="",
                    date_posted="",
                    employment_type="",
                    salary="",
                    source_url=url,
                    search_url=search_url,
                    scraped_at_utc=datetime.now(timezone.utc).isoformat(),
                )
            )

    time.sleep(sleep_s)
    return records


def deduplicate_records(records: List[JobRecord]) -> List[JobRecord]:
    seen: set[tuple[str, str]] = set()
    deduped: List[JobRecord] = []
    for record in records:
        key = (record.search_role.lower(), record.source_url.lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped


def write_outputs(records: List[JobRecord], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "caribbeanjobs_ict_roles.json"
    csv_path = output_dir / "caribbeanjobs_ict_roles.csv"

    payload = [asdict(record) for record in records]
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    fieldnames = list(JobRecord.__dataclass_fields__.keys())
    with csv_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(payload)

    return json_path, csv_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape CaribbeanJobs metadata for ICT roles.")
    parser.add_argument("--output-dir", default="data", help="Directory for CSV/JSON output files.")
    parser.add_argument(
        "--search-url-template",
        default=DEFAULT_SEARCH_URL_TEMPLATE,
        help="Search URL template with a {query} placeholder.",
    )
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--sleep", type=float, default=1.0, help="Delay between role searches in seconds.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)

    all_records: List[JobRecord] = []
    failures: List[str] = []

    for category, roles in ROLE_TAXONOMY.items():
        for role in roles:
            try:
                records = scrape_role(
                    role_category=category,
                    role=role,
                    search_url_template=args.search_url_template,
                    timeout_s=args.timeout,
                    sleep_s=args.sleep,
                )
                all_records.extend(records)
                print(f"[OK] {role}: {len(records)} records")
            except (HTTPError, URLError, TimeoutError, ValueError) as exc:
                failures.append(f"{role}: {exc}")
                print(f"[WARN] {role}: {exc}")

    deduped = deduplicate_records(all_records)
    json_path, csv_path = write_outputs(deduped, output_dir)

    print(f"\nSaved {len(deduped)} deduplicated records")
    print(f"JSON: {json_path}")
    print(f"CSV : {csv_path}")

    if failures:
        print("\nCompleted with warnings:")
        for failure in failures:
            print(f" - {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
