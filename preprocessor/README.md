# preprocessor

This folder contains scripts that clean and standardize raw curriculum/job text before skill extraction.

## What it does

- Loads raw course/job data.
- Normalizes text fields.
- Produces structured JSONL files for downstream extraction.

## Main scripts

- `preprocess-dcit-courses.py`: Preprocess DCIT course text.
- `preprocess_jobs.py`: Preprocess job data from general sources.
- `preprocess-linkedin-jobs.py`: Preprocess LinkedIn job data.

## Typical usage

From repo root:

```bash
python preprocessor/preprocess-dcit-courses.py
python preprocessor/preprocess_jobs.py
python preprocessor/preprocess-linkedin-jobs.py
```

## Inputs and outputs

- Inputs: raw files in `out/raw/` and/or source data directories.
- Outputs: cleaned JSONL files in `out/processed/` or `data/processed/`.

Run these scripts before any `skill-extractor/` script.
