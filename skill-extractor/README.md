# skill-extractor

This folder contains scripts that extract skills from preprocessed course and job text.

## What it does

- Reads cleaned JSONL files (from `preprocessor/` outputs).
- Uses NLP-based skill extraction.
- Writes skill lists back to `out/`.

## Main scripts

- `extract-course-skills-dcit.py`: Extract skills from DCIT course content.
- `extract-caribbeanjobs-job-skills.py`: Extract Caribbean job skills.
- `extract-linkedin-job-skills.py`: Extract LinkedIn job skills.
- `extract-reed-job-skills.py`: Extract Reed UK job skills.
- `extract-workopolis-job-skills.py`: Extract Workopolis/Canada job skills.

## Typical usage

From repo root:

```bash
python skill-extractor/extract-course-skills-dcit.py
python skill-extractor/extract-caribbeanjobs-job-skills.py
python skill-extractor/extract-linkedin-job-skills.py
python skill-extractor/extract-reed-job-skills.py
python skill-extractor/extract-workopolis-job-skills.py
```

## Inputs and outputs

- Inputs: preprocessed JSONL text files.
- Outputs: JSONL files with a `skills` list per course/job.

Keep path conventions in scripts and notebooks aligned when rerunning the pipeline.
