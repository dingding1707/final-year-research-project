# final-year-research-project

Repo for the final year research project.

## CaribbeanJobs ICT scraper

This repo now includes a standalone scraper for collecting metadata from
[CaribbeanJobs](https://www.caribbeanjobs.com) across 30 ICT roles split into:

- Computer Science roles (10)
- Information Technology roles (10)
- AI / Data Science roles (10)

The structure is designed for research workflows similar to
`ECE-Curriculum-NLP-Analysis`, but focused on ICT labour-market postings.

### File

- `caribbeanjobs_scraper.py` – dependency-free Python scraper (stdlib only).

### What gets collected

For each role query, the scraper attempts to collect:

- role category
- search role term
- job title
- company
- location
- date posted
- employment type
- salary (when published)
- source job URL
- source search URL
- scrape timestamp (UTC)

### Run

```bash
python caribbeanjobs_scraper.py --output-dir data
```

Optional parameters:

- `--search-url-template` (default: `https://www.caribbeanjobs.com/JobSearch/Jobs/?Keywords={query}`)
- `--timeout` (default: `20`)
- `--sleep` (default: `1.0` seconds between requests)

Example with explicit URL template:

```bash
python caribbeanjobs_scraper.py \
  --output-dir data \
  --search-url-template "https://www.caribbeanjobs.com/JobSearch/Jobs/?Keywords={query}"
```

### Output

The scraper writes:

- `data/caribbeanjobs_ict_roles.json`
- `data/caribbeanjobs_ict_roles.csv`

### Notes

- It first parses JSON-LD `JobPosting` schema where available.
- It falls back to link/card extraction when structured metadata is absent.
- In restricted environments, HTTP requests may fail due network proxy rules.
