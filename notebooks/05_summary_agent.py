# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Executive Summary Agent (A-05)
# MAGIC Generates and self-validates an executive briefing.
# MAGIC POC-1: LLM via HuggingFace API. POC-3: exports CSVs for Tableau.

# COMMAND ----------

%pip install anthropic pyyaml python-dotenv --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid
from agents.summary_agent import run

run_id = str(uuid.uuid4())
result = run({
    "run_id":             run_id,
    "rca_status":         "COMPLETE",
    "max_top_confidence": 0.78,   # pass actual value from RCA result in orchestrated run
})

import json
print(json.dumps({k: v for k, v in result.items() if k != "tools_called"}, indent=2))
print(f"\nTools called: {result.get('tools_called')}")

# COMMAND ----------
# MAGIC %md ## Read the generated executive briefing

# COMMAND ----------

briefing_row = spark.sql("""
  SELECT briefing_text, rca_confidence, recommended_scenario,
         all_claims_verified, unverified_claims, word_count, published_at
  FROM gold.executive_briefings
  ORDER BY published_at DESC
  LIMIT 1
""").collect()

if briefing_row:
    b = briefing_row[0]
    print("=" * 60)
    print(b["briefing_text"])
    print("=" * 60)
    print(f"\nWord count: {b['word_count']}")
    print(f"All claims verified: {b['all_claims_verified']}")
    print(f"Unverified claims: {b['unverified_claims']}")
    print(f"RCA confidence: {b['rca_confidence']:.0%}")
    print(f"Recommended: {b['recommended_scenario']}")

# COMMAND ----------
# MAGIC %md ## Check dashboard CSV exports (POC-3)

# COMMAND ----------

export_path = "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics/dashboard/exports/"
for f in dbutils.fs.ls(export_path):
    print(f.name, f.size)

# COMMAND ----------
# MAGIC %md ## Fact-check summary

# COMMAND ----------

spark.sql("""
  SELECT briefing_id, published_at,
         all_claims_verified,
         SIZE(FROM_JSON(verified_claims, 'ARRAY<STRING>'))   as verified_count,
         SIZE(FROM_JSON(unverified_claims, 'ARRAY<STRING>')) as unverified_count
  FROM gold.executive_briefings
  ORDER BY published_at DESC
""").display()
