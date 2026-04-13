# DCIT Curriculum-Industry Alignment Pipeline

This repository contains a reproducible NLP pipeline for quantifying curriculum-industry alignment for the Department of Computing and Information Technology (DCIT).

The project focuses on comparing skills represented in DCIT course content with skills represented in job advertisements, using topic modeling and alignment metrics.

## Project Goal

Build an end-to-end, reproducible workflow that:

- collects curriculum and job market data,
- preprocesses text into analysis-ready form,
- extracts standardized skill signals,
- evaluates topic quality with coherence,
- computes curriculum-industry closeness/alignment metrics.

This README documents the pipeline and how to rerun it. It intentionally does not include experimental results.

## Methodology (Five Stages)

### 1) Data Collection

Collect raw data from curriculum sources and multiple job sources.

- Curriculum source material is stored under `out/raw/outcomes_and_content.json`.
- Job sources are collected via scrapers in `scraper/`:
	- `caribbeanjobs-scraper.py`
	- `linkedin-scraper.py`
	- `reed-scraper.py`
	- `workopolis-scraper.py`

### 2) Preprocessing

Normalize and clean text before skill extraction.

- Course preprocessing:
	- `preprocessor/preprocess-dcit-courses.py`
	- Input: raw course content
	- Output: `out/processed/dcit_courses_preprocessed.jsonl`
- Job preprocessing:
	- `preprocessor/preprocess_jobs.py`
	- `preprocessor/preprocess-linkedin-jobs.py`
	- Outputs in `data/processed/` and/or `out/processed/`

### 3) Skill Extraction

Convert cleaned text into skill lists using the extraction scripts in `skill-extractor/`.

- Course skills:
	- `skill-extractor/extract-course-skills-dcit.py`
	- Outputs:
		- `out/dcit_courses_skills.jsonl`
		- `out/cs_course_skills.jsonl`
		- `out/it_course_skills.jsonl`
- Job skills:
	- `extract-caribbeanjobs-job-skills.py`
	- `extract-linkedin-job-skills.py`
	- `extract-reed-job-skills.py`
	- `extract-workopolis-job-skills.py`
	- plus derived grouped outputs in `out/` (local/international/unrelated/ai skill files)

### 4) Coherence Scores

Evaluate topic-model quality and choose suitable topic counts.

- Notebook: `notebooks/coherence-score.ipynb`
- Uses course skill corpora to assess LDA coherence across candidate topic numbers.

### 5) Closeness Metric

Measure curriculum-industry alignment by comparing job-skill documents against course-derived topics.

- Notebook: `notebooks/closeness-metric.ipynb`
- Includes CS and IT alignment flows and weighted summaries.

## Repository Layout

```text
.
├── data/
│   └── processed/
├── notebooks/
│   ├── closeness-metric.ipynb
│   ├── coherence-score.ipynb
│   ├── lda-topic-modelling.ipynb
│   ├── thematic-count.ipynb
├── preprocessor/
├── scraper/
├── skill-extractor/
├── out/
├── requirements.txt
└── README.md
```

## Environment Setup

### 1) Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Install spaCy model (required for skill extraction)

```bash
python -m spacy download en_core_web_sm
```

## Reproducible Run Order

Run stages in this order to reproduce the full pipeline.

### Stage A: Collect data

Run the required scrapers and place raw files under expected input locations.

### Stage B: Preprocess

```bash
python preprocessor/preprocess-dcit-courses.py
python preprocessor/preprocess_jobs.py
python preprocessor/preprocess-linkedin-jobs.py
```

### Stage C: Extract skills

```bash
python skill-extractor/extract-course-skills-dcit.py
python skill-extractor/extract-caribbeanjobs-job-skills.py
python skill-extractor/extract-linkedin-job-skills.py
python skill-extractor/extract-reed-job-skills.py
python skill-extractor/extract-workopolis-job-skills.py
```

### Stage D: Evaluate topic quality

Open and run:

- `notebooks/coherence-score.ipynb`

### Stage E: Compute alignment metrics

Open and run:

- `notebooks/closeness-metric.ipynb`

## Reproducibility Notes

- Keep input/output file paths consistent across scripts and notebooks.
- Run all notebook cells from top to bottom in a fresh kernel.
- Prefer using the same Python environment for all stages.
- Ensure generated files in `out/` are refreshed when rerunning experiments.

## Scope and Non-Goals

This repository is intended as a methodological and reproducible pipeline.

- Includes: data processing, NLP feature generation, topic evaluation, and metric computation.
- Excludes from README: numeric outcomes, interpretation of specific runs, and final reported results.

## License and Usage

If this repository is used in publications or derivatives, cite the project context appropriately and respect source data terms for scraped job data.
