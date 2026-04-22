# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestion & Validation Agent (A-01)
# MAGIC Ingests raw CSV/Excel files from the landing zone into Silver Delta tables.
# MAGIC Run after uploading files to `/dbfs/mnt/landing/`.

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%pip install "typing_extensions>=4.9.0" langgraph langchain langchain-community anthropic pyyaml python-dotenv openpyxl scikit-learn --quiet

# COMMAND ----------
# MAGIC %md ## Run the agent

# COMMAND ----------

import uuid
from agents.ingestion_agent import run

# List files in landing zone
raw_files = [f.path for f in dbutils.fs.ls("/Volumes/workspace/default/landing/")]
files = [p.replace("dbfs:", "") for p in raw_files]
print(f"Files found: {files}")

result = run({
    "run_id": str(uuid.uuid4()),
    "files":  files,
})

import json
print(json.dumps(result, indent=2))

# COMMAND ----------
# MAGIC %md ## Verify Silver tables

# COMMAND ----------

for tbl in ["dim_supplier", "dim_batch", "fact_qc_inspections",
            "fact_sales_returns", "supplier_defect_history"]:
    count = spark.table(f"silver.{tbl}").count()
    print(f"silver.{tbl}: {count:,} rows")

# COMMAND ----------

spark.sql("SELECT * FROM silver.fact_qc_inspections LIMIT 10").display()

# COMMAND ----------
# MAGIC %md ## View quarantined records (if any)

# COMMAND ----------

spark.sql("SELECT * FROM silver.bad_records ORDER BY quarantined_at DESC").display()

# COMMAND ----------
# MAGIC %md ## View ingest log

# COMMAND ----------

spark.sql("SELECT * FROM silver.ingest_log ORDER BY ingested_at DESC").display()
