# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Executive Summary Agent (A-05)
# MAGIC Generates and self-validates an executive briefing.
# MAGIC Prevention story framing: the briefing now leads with the "never again"
# MAGIC thesis — what the AI system detected, when it would have fired, and
# MAGIC what the data shows about preventing recurrence.

# COMMAND ----------

%pip install pyyaml python-dotenv --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid
from agents.summary_agent import run

run_id = str(uuid.uuid4())

# Pull max confidence from RCA findings
rca_row = spark.sql("""
  SELECT MAX(top_confidence) as max_conf FROM gold.rca_findings
""").collect()
max_conf = float(rca_row[0]["max_conf"] or 0.78)

# Pull prevention ROI savings
roi_row = spark.sql("""
  SELECT savings_vs_detect_late
  FROM gold.prevention_roi_curves
  WHERE scenario = 'detect_early'
    AND stage_sequence = (SELECT MAX(stage_sequence) FROM gold.prevention_roi_curves)
  LIMIT 1
""").collect()
savings = int(roi_row[0]["savings_vs_detect_late"]) if roi_row else 1_400_000

result = run({
    "run_id":             run_id,
    "rca_status":         "COMPLETE",
    "max_top_confidence": max_conf,
    "prevention_savings_usd": savings,
})

import json
print(json.dumps({k: v for k, v in result.items() if k \!= "tools_called"}, indent=2))
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
    print("=" * 70)
    print(b["briefing_text"])
    print("=" * 70)
    print(f"\nWord count:          {b['word_count']}")
    print(f"All claims verified: {b['all_claims_verified']}")
    print(f"Unverified claims:   {b['unverified_claims']}")
    print(f"RCA confidence:      {b['rca_confidence']:.0%}")
    print(f"Recommended:         {b['recommended_scenario']}")

# COMMAND ----------
# MAGIC %md ## Fact-check log

# COMMAND ----------

spark.sql("""
  SELECT briefing_id, published_at,
         all_claims_verified,
         SIZE(FROM_JSON(verified_claims,   'ARRAY<STRING>')) as verified_count,
         SIZE(FROM_JSON(unverified_claims, 'ARRAY<STRING>')) as unverified_count
  FROM gold.executive_briefings
  ORDER BY published_at DESC
""").display()
