# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestion & Validation Agent (A-01)
# MAGIC Ingests raw CSV/Excel files from the landing zone into Silver Delta tables.
# MAGIC Now handles 3 additional prevention-story data files:
# MAGIC - `supplier_risk_scores.csv` → silver.supplier_risk_scores
# MAGIC - `material_lots.csv`        → silver.material_lots
# MAGIC - `batch_lot_mapping.csv`    → silver.batch_lot_mapping

# COMMAND ----------

%pip install pyyaml python-dotenv openpyxl scikit-learn --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid
from agents.ingestion_agent import run

raw_files = [f.path for f in dbutils.fs.ls("/Volumes/workspace/default/landing/")]
files = [p.replace("dbfs:", "") for p in raw_files]
print(f"Files found: {len(files)}")
for f in files:
    print(" ", f.split("/")[-1])

# COMMAND ----------

result = run({
    "run_id": str(uuid.uuid4()),
    "files":  files,
})

import json
print(json.dumps(result, indent=2))

# COMMAND ----------
# MAGIC %md ## Verify Silver tables (original + prevention-story tables)

# COMMAND ----------

for tbl in ["dim_supplier", "dim_batch", "fact_qc_inspections",
            "fact_sales_returns", "supplier_defect_history",
            "supplier_risk_scores", "material_lots", "batch_lot_mapping"]:
    try:
        count = spark.table(f"silver.{tbl}").count()
        print(f"silver.{tbl}: {count:,} rows")
    except Exception as e:
        print(f"silver.{tbl}: NOT FOUND — {e}")

# COMMAND ----------
# MAGIC %md ## Preview: supplier risk scores (pre-production assessment)

# COMMAND ----------

spark.sql("""
  SELECT supplier_id, supplier_name, risk_tier,
         ROUND(historical_defect_score, 2)      as defect_score,
         ROUND(material_lot_stability_score, 2) as lot_stability,
         ROUND(capacity_utilization_score, 2)   as capacity_util,
         ROUND(composite_risk_score, 2)         as composite,
         would_trigger_audit
  FROM silver.supplier_risk_scores
  ORDER BY composite_risk_score ASC
""").display()

# COMMAND ----------
# MAGIC %md ## Preview: material lots (defective vs normal)

# COMMAND ----------

spark.sql("""
  SELECT lot_id, supplier_id, material_type,
         entered_production_date,
         is_defective, hours_to_first_alert,
         SUBSTR(defect_root_cause, 1, 60) as root_cause_summary
  FROM silver.material_lots
  ORDER BY is_defective DESC, entered_production_date
""").display()

# COMMAND ----------
# MAGIC %md ## View quarantined records (if any)

# COMMAND ----------

spark.sql("SELECT * FROM silver.bad_records ORDER BY quarantined_at DESC").display()

# COMMAND ----------
# MAGIC %md ## View ingest log

# COMMAND ----------

spark.sql("SELECT source_path, table_name, rows_written, rows_quarantined, status, ingested_at FROM silver.ingest_log ORDER BY ingested_at DESC").display()
