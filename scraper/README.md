# scraper

This folder contains job data collection scripts.

## What it does

- Scrapes job postings from multiple platforms.
- Saves raw job data for preprocessing.

## Main scripts

- `caribbeanjobs-scraper.py`
- `linkedin-scraper.py`
- `reed-scraper.py`
- `workopolis-scraper.py`

LinkedIn helper files are in:

- `linkedin-scraper-helper-files/`

## Typical usage

From repo root (run as needed):

```bash
python scraper/caribbeanjobs-scraper.py
python scraper/linkedin-scraper.py
python scraper/reed-scraper.py
python scraper/workopolis-scraper.py
```

## Output

Scrapers produce raw job data that is then consumed by scripts in `preprocessor/`.

After scraping, continue with preprocessing and then skill extraction.
