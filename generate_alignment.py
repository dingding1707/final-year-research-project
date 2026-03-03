import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import jensenshannon

# ==============================
# CONFIG
# ==============================

K = 5
RANDOM_STATE = 42

JOB_FILE = "out/international_job_skills.jsonl"
COURSE_FILE = "data/processed/dcit_courses_skills.jsonl"
PROGRAMMES_FILE = "data/processed/programmes.csv"

# ==============================
# LOAD DATA
# ==============================

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f]

jobs = load_jsonl(JOB_FILE)
courses = load_jsonl(COURSE_FILE)
programmes = pd.read_csv(PROGRAMMES_FILE)

# ==============================
# GROUP COURSES BY PROGRAMME
# ==============================

# Clean headers
programmes.columns = programmes.columns.str.strip()

COURSE_CODE_COL = "Code"

TARGET_PROGRAMMES = ["IT Special", "CS Special*"]

programme_map = {}

for prog in TARGET_PROGRAMMES:

    if prog not in programmes.columns:
        raise ValueError(f"Column '{prog}' not found in CSV")

    mask = (
        programmes[prog].notna() &
        (programmes[prog].astype(str).str.strip().str.upper() != "N/A")
    )

    codes = programmes.loc[mask, COURSE_CODE_COL].astype(str).tolist()

    programme_map[prog] = codes

print("Filtered Programmes:")
for k, v in programme_map.items():
    print(k, "→", len(v), "courses")

# Column that contains course codes
COURSE_CODE_COL = "Code"

# Programme columns (everything after Pre-requisites)
programme_columns = [
    "IT General",
    "IT Special",
    "CS Special*",
    "CS General*",
    "CS & Mgmt",
    "CS Minor*",
    "IT Minor"
]

programme_map = {}

for prog in programme_columns:
    if prog not in programmes.columns:
        continue

    # Select rows where this programme column is NOT empty
    mask = programmes[prog].notna() & (programmes[prog] != "")
    codes = programmes.loc[mask, COURSE_CODE_COL].astype(str).tolist()

    programme_map[prog] = codes

print("Programmes detected:")
for k, v in programme_map.items():
    print(k, "→", len(v), "courses")

programme_docs = {}

for programme, codes in programme_map.items():
    skills = []
    for c in courses:
        if c["course_code"] in codes:
            skills.extend(c.get("skills", []))
    programme_docs[programme] = " ".join(skills)

# Industry document
job_skills = []
for j in jobs:
    job_skills.extend(j.get("skills", []))
industry_doc = " ".join(job_skills)

# ==============================
# BUILD DOCUMENT MATRIX
# ==============================

# Build curriculum documents (one per course)
curriculum_docs = []

for course in courses:
    if course["course_code"] in programme_map["IT Special"] or \
       course["course_code"] in programme_map["CS Special*"]:

        skills = course.get("skills", [])
        if skills:
            curriculum_docs.append(" ".join(skills))

# Build industry documents (one per job)
industry_docs = []

for job in jobs:
    skills = job.get("skills", [])
    if skills:
        industry_docs.append(" ".join(skills))

all_docs = curriculum_docs + industry_docs

doc_labels = (
    ["Course"] * len(curriculum_docs) +
    ["Job"] * len(industry_docs)
)
doc_labels = list(programme_docs.keys()) + ["Industry"]

vectorizer = CountVectorizer(
    lowercase=True,
    stop_words="english",
    min_df=3,
    max_df=0.85
)
X = vectorizer.fit_transform(all_docs)

# ==============================
# LDA MODEL
# ==============================

lda = LatentDirichletAllocation(
    n_components=K,
    random_state=RANDOM_STATE
)

doc_topic = lda.fit_transform(X)
topic_word = lda.components_

# ==============================
# TOPIC PREVALENCE
# ==============================

curriculum_topic = doc_topic[:-1].mean(axis=0)
industry_topic = doc_topic[-1]

gap = industry_topic - curriculum_topic

# ==============================
# ALIGNMENT METRICS
# ==============================

cos_sim = cosine_similarity(
    curriculum_topic.reshape(1, -1),
    industry_topic.reshape(1, -1)
)[0][0]

js_div = jensenshannon(curriculum_topic, industry_topic)

# ==============================
# PLOT REPORT
# ==============================

fig = plt.figure(figsize=(18, 12))
fig.suptitle("CS Special Programme  ↔  Industry Alignment Report", fontsize=18)

# ---- 1. Topic Prevalence
ax1 = plt.subplot2grid((3,2), (0,0))
x = np.arange(K)

ax1.bar(x - 0.2, curriculum_topic, width=0.4, label="Curriculum")
ax1.bar(x + 0.2, industry_topic, width=0.4, label="Job Posting")
ax1.set_title("Topic Prevalence: Curriculum vs Industry")
ax1.set_xticks(x)
ax1.set_xticklabels([f"T{i}" for i in range(K)])
ax1.legend()

# ---- 2. Alignment Gap
ax2 = plt.subplot2grid((3,2), (0,1))
for i in range(K):
    ax2.plot([0, gap[i]], [i, i], marker='o')

ax2.axvline(0, linestyle="--")
ax2.set_yticks(range(K))
ax2.set_yticklabels([f"T{i}" for i in range(K)])
ax2.set_title("Alignment Gap per Topic")
ax2.set_xlabel("Gap (→ industry needs more | ← curriculum supplies more)")

# ---- 3. Heatmap
ax3 = plt.subplot2grid((3,2), (1,0), colspan=2)
sns.heatmap(
    doc_topic,
    cmap="YlOrRd",
    ax=ax3,
    cbar=True
)
ax3.set_yticklabels(doc_labels, rotation=0)
ax3.set_xticklabels([f"T{i}" for i in range(K)])
ax3.set_title("Document × Topic Heatmap")

# ---- 4. Topic Keywords
ax4 = plt.subplot2grid((3,2), (2,0))
feature_names = vectorizer.get_feature_names_out()

topic_keywords = []
for i, topic in enumerate(topic_word):
    top_words = [feature_names[j] for j in topic.argsort()[:-6:-1]]
    topic_keywords.append((f"T{i}", ", ".join(top_words)))

table_data = pd.DataFrame(topic_keywords, columns=["Topic", "Top Keywords"])
ax4.axis("off")
tbl = ax4.table(
    cellText=table_data.values,
    colLabels=table_data.columns,
    loc="center"
)
tbl.auto_set_font_size(False)
tbl.set_fontsize(8)
ax4.set_title("Topic Keywords")

# ---- 5. Alignment Summary
ax5 = plt.subplot2grid((3,2), (2,1))
ax5.axis("off")

biggest_gap_idx = np.argmax(np.abs(gap))

summary_text = f"""
Cosine Similarity: {cos_sim:.3f}
Jensen-Shannon Divergence: {js_div:.3f}

Biggest Industry Gap: T{biggest_gap_idx}
Gap Value: {gap[biggest_gap_idx]:.3f}

Total programmes analysed: {len(programme_docs)}
Total job descriptions: {len(jobs)}
Number of topics (K): {K}
"""

ax5.text(0.05, 0.6, summary_text)

plt.tight_layout(rect=[0,0,1,0.95])
plt.savefig("alignment_report_generated.png", dpi=300)
plt.show()