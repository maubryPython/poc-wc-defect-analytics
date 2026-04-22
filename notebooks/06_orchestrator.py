# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Full Pipeline Orchestrator
# MAGIC Runs agents A-01 → A-02 → A-03 → A-04 → A-05 in sequence.
# MAGIC Checks table freshness before each step; surfaces escalations.
# MAGIC After completing, all four chart-ready gold tables are populated.

# COMMAND ----------

%pip install pyyaml python-dotenv scikit-learn langgraph langchain-core --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid, json, time
from agents.orchestrator import build_graph, run_pipeline

run_id = str(uuid.uuid4())
print(f"Pipeline run ID: {run_id}")
start  = time.time()

result = run_pipeline({
    "run_id":  run_id,
    "files":   [f.path.replace("dbfs:","") for f in dbutils.fs.ls("/Volumes/workspace/default/landing/")],
})

elapsed = time.time() - start
print(f"\nCompleted in {elapsed:.1f}s")
print(json.dumps({k: v for k, v in result.items() if k \!= "tools_called"}, indent=2))

# COMMAND ----------
# MAGIC %md ## Gold table row counts after full pipeline run

# COMMAND ----------

for tbl in ["defect_alerts","weekly_defect_trend","lot_defect_rates",
            "rca_findings","lot_defect_traceability",
            "scenario_projections","prevention_roi_curves","executive_briefings"]:
    try:
        count = spark.table(f"gold.{tbl}").count()
        print(f"gold.{tbl}: {count:,} rows")
    except Exception as e:
        print(f"gold.{tbl}: NOT FOUND — {e}")

# COMMAND ----------
# MAGIC %md ## Any escalations requiring analyst review?

# COMMAND ----------

spark.sql("""
  SELECT agent_id, COUNT(*) as open_escalations
  FROM gold.escalation_queue
  WHERE resolved = false
  GROUP BY agent_id
""").display()
