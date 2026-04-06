from extractor import SkillExtractor

# ── Sample Job Description ────────────────────────────────────────────────────
JD = """
We are looking for a Senior Data Engineer to join our platform team.

Requirements:
- 4+ years of experience with Python and SQL
- Strong background in Apache Spark, Kafka, and Airflow
- Experience building data pipelines on AWS (S3, Glue, Redshift)
- Familiarity with dbt, Snowflake, or BigQuery
- Proficiency in Docker and Kubernetes for containerised deployments
- Knowledge of CI/CD practices using GitHub Actions or Jenkins
- Experience with PostgreSQL and NoSQL databases (MongoDB, DynamoDB)
- Strong understanding of data modelling, ETL/ELT patterns
- Excellent communication skills and ability to collaborate cross-functionally
- Familiarity with machine learning model deployment is a plus
- Experience with Terraform or CloudFormation (IaC) is a bonus
"""

# ── Initialise (do this once — it loads the taxonomy into memory) ─────────────
extractor = SkillExtractor(
    esco_csv="dataset/skills_en.csv",      # path to ESCO CSV
    cache_path="dataset/esco_cache.json",  # speeds up subsequent runs
    spacy_model="en_core_web_lg",
)

# ── Extract ───────────────────────────────────────────────────────────────────
result = extractor.extract(JD)

# ── Output ────────────────────────────────────────────────────────────────────
print("=" * 60)
print("TOP KEYWORDS (ranked)")
print("=" * 60)
for i, kw in enumerate(result.ranked_keywords[:25], 1):
    print(f"  {i:>2}. {kw}")

print()
print("=" * 60)
print("DETAILED MATCH BREAKDOWN")
print("=" * 60)
for skill in result.skills[:15]:
    print(
        f"  [{skill.match_type:<8}] score={skill.score:.2f}  "
        f"'{skill.matched_text}' => {skill.label}"
    )

# ── Batch usage ───────────────────────────────────────────────────────────────
JD_LIST = [JD, "Looking for a React and TypeScript developer with GraphQL experience."]
batch_results = extractor.extract_batch(JD_LIST)
for i, r in enumerate(batch_results, 1):
    print(f"\n[JD {i}] Top 10 keywords: {r.ranked_keywords[:10]}")

# ── Convenience shortcut ──────────────────────────────────────────────────────
keywords = extractor.top_keywords(JD, n=15)
print(f"\nQuick top-15: {keywords}")