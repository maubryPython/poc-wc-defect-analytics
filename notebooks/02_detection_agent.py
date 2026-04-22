# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Defect Detection Agent (A-02)
# MAGIC Scans Silver QC data for anomalous batches.
# MAGIC **Prevention story additions:**
# MAGIC - Enriches alerts with pre-production supplier risk tier (HIGH flags get lower threshold)
# MAGIC - Writes `gold.weekly_defect_trend` (Chart 1 — the detection window)
# MAGIC - Writes `gold.lot_defect_rates` (Chart 3 input — consumed by RCA agent)

# COMMAND ----------

%pip install pyyaml python-dotenv scikit-learn --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid, json
from agents.detection_agent import run

result = run({"run_id": str(uuid.uuid4())})
print(json.dumps({k: v for k, v in result.items() if k \!= "tools_called"}, indent=2))
print(f"\nTools called: {result.get('tools_called')}")

# COMMAND ----------
# MAGIC %md ## Flagged batches with supplier risk tier (AC-12)

# COMMAND ----------

spark.sql("""
  SELECT batch_id, supplier_id, line_id, supplier_risk_tier,
         ROUND(observed_defect_rate, 4) as defect_rate,
         ROUND(anomaly_score, 2)        as z_score,
         alert_status, material_lot,
         SUBSTR(reasoning_chain, 1, 100) as reasoning
  FROM gold.defect_alerts
  WHERE alert_status IN ('FLAGGED','REVIEW','UNRESOLVED')
  ORDER BY anomaly_score DESC
""").display()

# COMMAND ----------
# MAGIC %md ## Chart 1 data: Weekly defect trend — the detection window

# COMMAND ----------

spark.sql("""
  SELECT supplier_id, production_week,
         ROUND(weekly_defect_rate, 4) as defect_rate,
         total_defects, total_units_inspected,
         above_ai_threshold,
         ai_threshold
  FROM gold.weekly_defect_trend
  WHERE supplier_id IN ('S007','S008')
  ORDER BY production_week
""").display()

# COMMAND ----------
# MAGIC %md ## First week the AI threshold was crossed

# COMMAND ----------

spark.sql("""
  SELECT MIN(production_week) as first_alert_week,
         MIN(weekly_defect_rate) as rate_at_crossing,
         ai_threshold
  FROM gold.weekly_defect_trend
  WHERE above_ai_threshold = true
    AND supplier_id IN ('S007','S008')
  GROUP BY ai_threshold
""").display()

# COMMAND ----------
# MAGIC %md ## Lot-level defect rates (Chart 3 input — highest first)

# COMMAND ----------

spark.sql("""
  SELECT lot_id, supplier_id,
         ROUND(mean_defect_rate, 4) as mean_defect_rate,
         ROUND(max_defect_rate, 4)  as max_defect_rate,
         batch_count, total_units,
         above_ai_threshold
  FROM gold.lot_defect_rates
  ORDER BY mean_defect_rate DESC
  LIMIT 15
""").display()

# COMMAND ----------
# MAGIC %md ## Alert status distribution

# COMMAND ----------

spark.sql("""
  SELECT alert_status, supplier_risk_tier,
         COUNT(*)                              as batch_count,
         ROUND(AVG(observed_defect_rate), 4)  as avg_defect_rate,
         ROUND(AVG(anomaly_score), 2)         as avg_zscore
  FROM gold.defect_alerts
  GROUP BY alert_status, supplier_risk_tier
  ORDER BY avg_zscore DESC
""").display()
