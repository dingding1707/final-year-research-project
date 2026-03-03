"""
CS Special ↔ Industry Alignment Analysis
Generates full multi-panel alignment report image.
"""

import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
from scipy.spatial.distance import jensenshannon, cosine

# ── PATHS ─────────────────────────────────────
DATA_DIR = os.path.join("data", "processed")
PROGRAMMES_CSV = os.path.join(DATA_DIR, "programmes.csv")
COURSES_JSON = os.path.join(DATA_DIR, "courses.json")
JOBS_JSONL = os.path.join(DATA_DIR, "processed_jobs.jsonl")
OUTPUT_IMG = "alignment_report.png"


# ── LOAD CS SPECIAL COURSE CODES ─────────────
def load_cs_special_codes():
    df = pd.read_csv(PROGRAMMES_CSV)
    df = df[df["CS Special*"].notna()]
    df = df[df["CS Special*"] != "N/A"]
    return set(df["Code"].astype(str).str.strip())


# ── BUILD COURSE DOCUMENTS ───────────────────
def build_course_documents(cs_codes):
    with open(COURSES_JSON, encoding="utf-8") as f:
        courses = json.load(f)

    docs = []
    for c in courses:
        code = c.get("course_code", "").strip()
        if code not in cs_codes:
            continue

        parts = []

        for field in ["title", "description", "rationale", "aims"]:
            if c.get(field):
                parts.append(c[field])

        parts.extend(c.get("learning_outcomes", []))

        cc = c.get("course_content", {})
        for section, items in cc.items():
            parts.append(section)
            if isinstance(items, list):
                parts.extend(items)

        text = " ".join(parts).lower().strip()

        if text:
            docs.append({
                "code": code,
                "text": text,
                "source": "course"
            })

    return docs


# ── LOAD JOB DOCUMENTS FROM JSONL ────────────
def load_job_documents():
    docs = []
    with open(JOBS_JSONL, encoding="utf-8") as f:
        for line in f:
            job = json.loads(line)
            text = job.get("clean_text", "").strip()
            if text:
                docs.append({
                    "code": job.get("job_title", "job"),
                    "text": text,
                    "source": "job"
                })
    return docs


# ── RUN TOPIC MODEL ──────────────────────────
def run_topic_model(corpus, n_topics=8):

    vectorizer = TfidfVectorizer(
        max_df=0.9,
        min_df=2,
        stop_words="english",
        ngram_range=(1, 2),
        max_features=3000
    )

    tfidf = vectorizer.fit_transform(corpus)

    nmf = NMF(
        n_components=n_topics,
        random_state=42,
        max_iter=500
    )

    W = nmf.fit_transform(tfidf)
    H = nmf.components_

    feature_names = vectorizer.get_feature_names_out()

    topic_labels = []
    for topic in H:
        top_words = [feature_names[i] for i in topic.argsort()[:-11:-1]]
        topic_labels.append(", ".join(top_words[:4]))

    return W, topic_labels


# ── ALIGNMENT METRICS ─────────────────────────
def compute_alignment(W, sources):

    W_norm = W / (W.sum(axis=1, keepdims=True) + 1e-10)

    course_mask = np.array([s == "course" for s in sources])
    job_mask = np.array([s == "job" for s in sources])

    avg_course = W_norm[course_mask].mean(axis=0)
    avg_job = W_norm[job_mask].mean(axis=0)

    avg_course /= avg_course.sum()
    avg_job /= avg_job.sum()

    gap = avg_job - avg_course

    jsd = jensenshannon(avg_course, avg_job)
    cos_sim = 1 - cosine(avg_course, avg_job)

    return avg_course, avg_job, gap, jsd, cos_sim


# ── FULL REPORT PLOT ──────────────────────────
def generate_report(avg_course, avg_job, gap, jsd, cos_sim,
                    topic_labels, W, sources, doc_labels):

    n_topics = len(topic_labels)
    short_labels = [f"T{i}" for i in range(n_topics)]

    fig = plt.figure(figsize=(22, 16), facecolor="white")
    fig.suptitle("CS Special Programme  ↔  Industry Alignment Report",
                 fontsize=20, fontweight="bold")

    gs = gridspec.GridSpec(3, 2, hspace=0.4, wspace=0.3)

    # ── Panel 1
    ax1 = fig.add_subplot(gs[0, 0])
    x = np.arange(n_topics)
    width = 0.35

    ax1.bar(x - width/2, avg_course, width, label="Curriculum")
    ax1.bar(x + width/2, avg_job, width, label="Job Posting")

    ax1.set_xticks(x)
    ax1.set_xticklabels(short_labels)
    ax1.set_title("Topic Prevalence: Curriculum vs Industry")
    ax1.legend()

    # ── Panel 2
    ax2 = fig.add_subplot(gs[0, 1])
    sorted_idx = np.argsort(gap)

    ax2.hlines(range(n_topics),
               xmin=0,
               xmax=gap[sorted_idx])

    ax2.scatter(gap[sorted_idx],
                range(n_topics))

    ax2.set_yticks(range(n_topics))
    ax2.set_yticklabels([short_labels[i] for i in sorted_idx])
    ax2.axvline(0, linestyle="--")
    ax2.set_title("Alignment Gap per Topic")

    # ── Panel 3
    ax3 = fig.add_subplot(gs[1, :])
    W_norm = W / (W.sum(axis=1, keepdims=True) + 1e-10)

    df_heat = pd.DataFrame(W_norm,
                           columns=short_labels,
                           index=doc_labels)

    sns.heatmap(df_heat,
                cmap="Blues",
                ax=ax3,
                cbar_kws={"label": "Weight"})

    ax3.set_title("Document × Topic Heatmap")

    # ── Panel 4
    ax4 = fig.add_subplot(gs[2, 0])
    ax4.axis("off")

    table_data = []
    for i, lbl in enumerate(topic_labels):
        table_data.append([f"T{i}", lbl])

    table = ax4.table(cellText=table_data,
                      colLabels=["Topic", "Top Keywords"],
                      loc="center",
                      cellLoc="left")

    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.4)

    # ── Panel 5 (FULL SUMMARY)
    ax5 = fig.add_subplot(gs[2, 1])
    ax5.axis("off")

    biggest_gap_idx = np.argmax(gap)
    biggest_surplus_idx = np.argmin(gap)

    summary = f"""
Cosine Similarity: {cos_sim:.3f}
Jensen-Shannon Divergence: {jsd:.3f}

Biggest Industry Gap:
T{biggest_gap_idx}: {topic_labels[biggest_gap_idx]}
({gap[biggest_gap_idx]:+.3f})

Biggest Curriculum Surplus:
T{biggest_surplus_idx}: {topic_labels[biggest_surplus_idx]}
({gap[biggest_surplus_idx]:+.3f})

Total Courses: {sum(1 for s in sources if s == "course")}
Total Jobs: {sum(1 for s in sources if s == "job")}
Topics (k): {n_topics}
"""

    ax5.text(0.02, 0.98,
             summary,
             fontsize=9,
             verticalalignment="top",
             fontfamily="monospace",
             bbox=dict(boxstyle="round"))

    plt.savefig(OUTPUT_IMG, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"Report saved to {OUTPUT_IMG}")


# ── MAIN ─────────────────────────────────────
def main():

    cs_codes = load_cs_special_codes()
    course_docs = build_course_documents(cs_codes)
    job_docs = load_job_documents()

    if not course_docs or not job_docs:
        print("Missing data.")
        return

    all_docs = course_docs + job_docs
    corpus = [d["text"] for d in all_docs]
    sources = [d["source"] for d in all_docs]
    doc_labels = [
        f"[C] {d['code']}" if d["source"] == "course"
        else f"[J] {d['code']}"
        for d in all_docs
    ]

    n_topics = min(8, max(3, len(corpus)//3))

    W, topic_labels = run_topic_model(corpus, n_topics)
    avg_course, avg_job, gap, jsd, cos_sim = compute_alignment(W, sources)

    generate_report(avg_course, avg_job, gap, jsd, cos_sim,
                    topic_labels, W, sources, doc_labels)


if __name__ == "__main__":
    main()