# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — RCA Agent (A-03)
# MAGIC Investigates flagged batches. Demonstrates source-selection by defect type,
# MAGIC LLM reasoning over evidence, and confidence gating.
# MAGIC **Prevention story addition:**
# MAGIC - Writes `gold.lot_defect_traceability` (Chart 3 — material lot traceability)
# MAGIC   showing which lots were defective, when they entered production, and how
# MAGIC   quickly an alert would have fired.

# COMMAND ----------

%pip install pyyaml python-dotenv --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid, json
from agents.rca_agent import run

flagged = [
    r["batch_id"]
    for r in spark.sql(
        "SELECT batch_id FROM gold.defect_alerts WHERE alert_status='FLAGGED'"
    ).collect()
]
print(f"Investigating {len(flagged)} flagged batches: {flagged}")

run_id = str(uuid.uuid4())
result = run({"run_id": run_id, "flagged_batch_ids": flagged})
print(json.dumps({k: v for k, v in result.items() if k \!= "tools_called"}, indent=2))
print(f"\nTools called: {result.get('tools_called')}")

# COMMAND ----------
# MAGIC %md ## RCA findings — hypotheses and confidence

# COMMAND ----------

spark.sql("""
  SELECT batch_id, supplier_id, rca_status, confidence_tier,
         ROUND(top_confidence, 3)  as confidence,
         SUBSTR(top_hypothesis, 1, 80) as hypothesis,
         material_lot
  FROM gold.rca_findings
  ORDER BY top_confidence DESC
""").display()

# COMMAND ----------
# MAGIC %md ## Chart 3 data: Material lot traceability

# COMMAND ----------

spark.sql("""
  SELECT lot_id, supplier_id, material_type,
         entered_production_date,
         ROUND(mean_defect_rate, 4)  as mean_defect_rate,
         batch_count, batches_flagged,
         is_defective,
         hours_to_first_alert,
         rca_status,
         ROUND(rca_top_confidence, 3) as rca_confidence,
         SUBSTR(defect_root_cause, 1, 70) as root_cause
  FROM gold.lot_defect_traceability
  ORDER BY is_defective DESC, mean_defect_rate DESC
""").display()

# COMMAND ----------
# MAGIC %md ## Hours to detection: defective lots vs. normal lots

# COMMAND ----------

spark.sql("""
  SELECT
    is_defective,
    COUNT(*)                               as lot_count,
    ROUND(AVG(mean_defect_rate), 4)        as avg_defect_rate,
    ROUND(AVG(hours_to_first_alert), 1)    as avg_hours_to_alert,
    MIN(hours_to_first_alert)              as fastest_alert_hrs,
    MAX(hours_to_first_alert)              as slowest_alert_hrs
  FROM gold.lot_defect_traceability
  GROUP BY is_defective
""").display()

# COMMAND ----------
# MAGIC %md ## Confidence distribution

# COMMAND ----------

spark.sql("""
  SELECT rca_status, confidence_tier,
         COUNT(*) as count,
         ROUND(AVG(top_confidence), 3) as avg_confidence
  FROM gold.rca_findings
  GROUP BY rca_status, confidence_tier
  ORDER BY avg_confidence DESC
""").display()
