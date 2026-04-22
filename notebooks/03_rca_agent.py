# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — RCA Agent (A-03)
# MAGIC Investigates flagged batches. Demonstrates: source selection by defect type,
# MAGIC LLM reasoning over evidence, confidence gating, escalation on insufficient evidence.
# MAGIC POC-1: LLM calls HuggingFace Inference API.

# COMMAND ----------

%pip install "typing_extensions>=4.9.0" langgraph langchain langchain-community anthropic pyyaml python-dotenv --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid, json
from agents.rca_agent import run

# Pull flagged IDs from the most recent detection run
flagged = [
    r["batch_id"]
    for r in spark.sql(
        "SELECT batch_id FROM gold.defect_alerts WHERE alert_status='FLAGGED'"
    ).collect()
]
print(f"Investigating {len(flagged)} flagged batch(es): {flagged}")

run_id = str(uuid.uuid4())
result = run({"run_id": run_id, "flagged_batch_ids": flagged})
print(json.dumps({k: v for k, v in result.items() if k != "tools_called"}, indent=2))
print(f"\nTools called: {result.get('tools_called')}")

# COMMAND ----------
# MAGIC %md ## View RCA findings with hypotheses and confidence scores (AC-16)

# COMMAND ----------

spark.sql("""
  SELECT batch_id, supplier_id, rca_status,
         top_hypothesis, ROUND(top_confidence, 3) as confidence,
         evidence_sources, tools_called
  FROM gold.rca_findings
  ORDER BY top_confidence DESC
""").display()

# COMMAND ----------
# MAGIC %md ## Show confidence gating in action

# COMMAND ----------

spark.sql("""
  SELECT rca_status, COUNT(*) as count,
         ROUND(AVG(top_confidence), 3) as avg_confidence
  FROM gold.rca_findings
  GROUP BY rca_status
""").display()

# COMMAND ----------
# MAGIC %md ## View top hypothesis detail for highest-confidence finding

# COMMAND ----------

import json as _json
top = spark.sql("""
  SELECT batch_id, hypotheses_json, data_gaps
  FROM gold.rca_findings
  ORDER BY top_confidence DESC
  LIMIT 1
""").collect()

if top:
    row = top[0]
    print(f"Batch: {row['batch_id']}")
    hyps = _json.loads(row['hypotheses_json'])
    for h in hyps:
        print(f"\nRank {h.get('rank')}: {h.get('hypothesis')}")
        print(f"  Confidence: {h.get('confidence_score'):.0%}")
        print(f"  Evidence:   {h.get('supporting_evidence')}")
        print(f"  Action:     {h.get('recommended_action')}")
    print(f"\nData gaps: {_json.loads(row['data_gaps'])}")
