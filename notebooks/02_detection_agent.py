# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Defect Detection Agent (A-02)
# MAGIC Scans Silver QC data for anomalous batches using supplier-specific baselines.
# MAGIC Demonstrates: tool use, conditional routing (borderline secondary lookup), confidence, escalation.

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%pip install langgraph langchain langchain-community huggingface-hub pyyaml python-dotenv scikit-learn --quiet

# COMMAND ----------

import uuid, json
from agents.detection_agent import run

result = run({"run_id": str(uuid.uuid4())})
print(json.dumps({k: v for k, v in result.items() if k != "tools_called"}, indent=2))
print(f"\nTools called: {result.get('tools_called')}")

# COMMAND ----------
# MAGIC %md ## View flagged batches with reasoning chains (AC-12)

# COMMAND ----------

spark.sql("""
  SELECT batch_id, supplier_id, line_id, observed_defect_rate,
         anomaly_score, alert_status, reasoning_chain, tools_called
  FROM gold.defect_alerts
  WHERE alert_status IN ('FLAGGED', 'REVIEW', 'UNRESOLVED')
  ORDER BY anomaly_score DESC
""").display()

# COMMAND ----------
# MAGIC %md ## Distribution: normal vs flagged defect rates

# COMMAND ----------

spark.sql("""
  SELECT alert_status,
         ROUND(AVG(observed_defect_rate), 4) as avg_rate,
         ROUND(AVG(anomaly_score), 2)         as avg_zscore,
         COUNT(*) as count
  FROM gold.defect_alerts
  GROUP BY alert_status
  ORDER BY avg_zscore DESC
""").display()

# COMMAND ----------
# MAGIC %md ## Escalations requiring analyst review

# COMMAND ----------

spark.sql("""
  SELECT agent_id, question, context_json
  FROM gold.escalation_queue
  WHERE agent_id = 'A-02' AND resolved = false
""").display()
