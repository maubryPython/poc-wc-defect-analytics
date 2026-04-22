# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup: Databases, Delta Tables & Dependencies
# MAGIC Run this notebook **once** before any other notebook.

# COMMAND ----------
# MAGIC %md ## 1. Install dependencies

# COMMAND ----------

%pip install langgraph langchain langchain-community huggingface-hub great-expectations dbt-core pyyaml python-dotenv -q

# COMMAND ----------
# MAGIC %md ## 2. Set your HuggingFace API token

# COMMAND ----------

import os
# Option A: Set directly (fine for personal CE workspace)
os.environ["HF_API_TOKEN"] = "hf_YOUR_TOKEN_HERE"   # ← replace this

# Option B: Use Databricks Secrets (recommended)
# dbutils.secrets.put(scope="nike-wc-poc", key="hf_api_token", string_value="hf_...")

# COMMAND ----------
# MAGIC %md ## 3. Create databases

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver COMMENT 'Validated source data'")
spark.sql("CREATE DATABASE IF NOT EXISTS gold   COMMENT 'Analytics-ready outputs'")
print("Databases created: silver, gold")

# COMMAND ----------
# MAGIC %md ## 4. Create Silver tables

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_supplier (
  supplier_id          STRING NOT NULL,
  supplier_name        STRING,
  region               STRING,
  material_type        STRING,
  contract_tier        STRING,
  baseline_defect_rate DOUBLE,
  ingested_at          STRING,
  run_id               STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_batch (
  batch_id        STRING NOT NULL,
  supplier_id     STRING,
  production_date STRING,
  line_id         STRING,
  unit_count      LONG,
  material_lot    STRING,
  material_type   STRING,
  sku             STRING,
  ingested_at     STRING,
  run_id          STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.fact_qc_inspections (
  inspection_id    STRING NOT NULL,
  batch_id         STRING,
  supplier_id      STRING,
  line_id          STRING,
  inspection_date  STRING,
  inspector_id     STRING,
  units_inspected  LONG,
  defect_count     LONG,
  defect_type      STRING,
  pass_fail        STRING,
  notes            STRING,
  ingested_at      STRING,
  run_id           STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.fact_sales_returns (
  transaction_id STRING NOT NULL,
  date           STRING,
  sku            STRING,
  channel        STRING,
  units_sold     LONG,
  units_returned LONG,
  return_rate    DOUBLE,
  return_reason  STRING,
  revenue_usd    DOUBLE,
  refund_usd     DOUBLE,
  ingested_at    STRING,
  run_id         STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.supplier_defect_history (
  supplier_id      STRING,
  line_id          STRING,
  month            STRING,
  units_inspected  LONG,
  defect_rate      DOUBLE,
  defects_found    LONG,
  ingested_at      STRING,
  run_id           STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.social_sentiment (
  date                  STRING,
  platform              STRING,
  topic                 STRING,
  volume                LONG,
  sentiment_score       DOUBLE,
  mentions_nike_jersey  BOOLEAN,
  ingested_at           STRING,
  run_id                STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.ingest_log (
  source_path      STRING,
  table_name       STRING,
  rows_written     LONG,
  rows_quarantined LONG,
  run_id           STRING,
  status           STRING,
  ingested_at      STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.bad_records (
  error_reason    STRING,
  source_file     STRING,
  quarantined_at  STRING,
  run_id          STRING
) USING DELTA
""")

print("Silver tables created.")

# COMMAND ----------
# MAGIC %md ## 5. Create Gold tables

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.defect_alerts (
  batch_id              STRING,
  supplier_id           STRING,
  line_id               STRING,
  observed_defect_rate  DOUBLE,
  units_flagged         LONG,
  anomaly_score         DOUBLE,
  alert_status          STRING,
  defect_types          STRING,
  fail_inspections      LONG,
  material_lot          STRING,
  production_date       STRING,
  reasoning_chain       STRING,
  tools_called          STRING,
  run_id                STRING,
  alerted_at            STRING,
  created_at            STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.rca_findings (
  batch_id            STRING,
  supplier_id         STRING,
  line_id             STRING,
  rca_status          STRING,
  top_hypothesis      STRING,
  top_confidence      DOUBLE,
  overall_confidence  DOUBLE,
  hypotheses_json     STRING,
  evidence_sources    STRING,
  data_gaps           STRING,
  tools_called        STRING,
  run_id              STRING,
  analyzed_at         STRING,
  created_at          STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.scenario_projections (
  scenario_id              STRING,
  scenario_name            STRING,
  scenario_description     STRING,
  is_recommended           BOOLEAN,
  confidence_flag          STRING,
  units_affected           LONG,
  recall_cost_per_unit     DOUBLE,
  discount_pct             DOUBLE,
  logistics_cost_usd       DOUBLE,
  mean_total_cost_usd      DOUBLE,
  p10_total_cost_usd       DOUBLE,
  p50_total_cost_usd       DOUBLE,
  p90_total_cost_usd       DOUBLE,
  mean_revenue_impact_usd  DOUBLE,
  mean_timeline_days       LONG,
  p10_timeline_days        LONG,
  p90_timeline_days        LONG,
  mean_returns_volume      LONG,
  sensitivity_rank         STRING,
  most_sensitive_param     STRING,
  run_id                   STRING,
  modeled_at               STRING,
  created_at               STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.executive_briefings (
  briefing_id            STRING,
  run_id                 STRING,
  published_at           STRING,
  briefing_text          STRING,
  word_count             LONG,
  rca_confidence         DOUBLE,
  rca_status             STRING,
  recommended_scenario   STRING,
  units_flagged          LONG,
  flagged_batches        LONG,
  verified_claims        STRING,
  unverified_claims      STRING,
  all_claims_verified    BOOLEAN,
  data_sources_json      STRING,
  tools_called           STRING,
  created_at             STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.escalation_queue (
  escalation_id      STRING,
  agent_id           STRING,
  run_id             STRING,
  escalated_at       STRING,
  question           STRING,
  context_json       STRING,
  recovery_attempted BOOLEAN,
  resolved           BOOLEAN,
  resolution_notes   STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.run_log (
  run_id            STRING,
  run_timestamp     STRING,
  agents_invoked    STRING,
  tools_called      STRING,
  escalations       LONG,
  duration_seconds  DOUBLE,
  confidence_scores STRING,
  notes             STRING,
  created_at        STRING
) USING DELTA
""")

print("Gold tables created.")

# COMMAND ----------
# MAGIC %md ## 6. Upload synthetic data to landing zone

# COMMAND ----------

# MAGIC %md
# MAGIC Upload the files from `data/synthetic/` to `/dbfs/mnt/landing/` using:
# MAGIC ```
# MAGIC dbutils.fs.cp("file:/path/to/supplier_manifest.csv", "dbfs:/mnt/landing/supplier_manifest.csv")
# MAGIC ```
# MAGIC Or drag-and-drop via the Databricks File Browser (top-left menu → Data → DBFS).
# MAGIC
# MAGIC **Remember:** strip the `is_defective` column from `manufacturing_batches.csv` before uploading.

# COMMAND ----------
# MAGIC %md ## 7. Verify setup

# COMMAND ----------

silver_tables = spark.sql("SHOW TABLES IN silver").collect()
gold_tables   = spark.sql("SHOW TABLES IN gold").collect()
print(f"Silver tables: {[t.tableName for t in silver_tables]}")
print(f"Gold tables:   {[t.tableName for t in gold_tables]}")
print("\nSetup complete. Run notebook 01_ingestion_agent next.")
