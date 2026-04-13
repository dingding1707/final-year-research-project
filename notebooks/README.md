# notebooks

This folder contains analysis notebooks for topic quality and curriculum-industry alignment.

## Notebook overview

- `coherence-score.ipynb`: Finds suitable topic counts using coherence.
- `closeness-metric.ipynb`: Computes curriculum-industry closeness/alignment metrics.
- `lda-topic-modelling.ipynb`: Topic modeling exploration.
- `thematic-count.ipynb`: Thematic/statistical summaries.

## How to use

1. Run preprocessing and skill extraction first.
2. Open notebooks in order:
   - `coherence-score.ipynb`
   - `closeness-metric.ipynb`
   - optional deeper diagnostics in `result-evaluation.ipynb`
3. Run each notebook top-to-bottom in a fresh kernel.

## Notes

- Notebooks expect files in `out/` to exist.
- Use the same Python environment for consistent results.
