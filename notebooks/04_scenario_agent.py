# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Scenario Modeling Agent (A-04)
# MAGIC Monte Carlo projections for 3 response strategies.
# MAGIC **Prevention story addition:**
# MAGIC - Writes `gold.prevention_roi_curves` (Chart 4 — prevention ROI)
# MAGIC   showing cumulative cost at each of 6 pipeline stages for all 3 scenarios
# MAGIC   plus the AI system investment line.

# COMMAND ----------

%pip install pyyaml python-dotenv --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

import uuid, json
from agents.scenario_agent import run

run_id = str(uuid.uuid4())
result = run({"run_id": run_id})
print(json.dumps({k: v for k, v in result.items() if k \!= "tools_called"}, indent=2))
print(f"\nTools called: {result['tools_called']}")

# COMMAND ----------
# MAGIC %md ## Scenario cost comparison (P10 / P50 / P90)

# COMMAND ----------

spark.sql("""
  SELECT scenario_label, is_recommended,
         FORMAT_NUMBER(mean_total_cost_usd,  0) as mean_cost,
         FORMAT_NUMBER(p10_total_cost_usd,   0) as p10_cost,
         FORMAT_NUMBER(p90_total_cost_usd,   0) as p90_cost,
         mean_timeline_days,
         p10_timeline_days,
         p90_timeline_days,
         mean_returns_volume,
         most_sensitive_param
  FROM gold.scenario_projections
  ORDER BY mean_total_cost_usd ASC
""").display()

# COMMAND ----------
# MAGIC %md ## Chart 4 data: Prevention ROI curves — cumulative cost by stage

# COMMAND ----------

spark.sql("""
  SELECT scenario, stage_sequence, stage_name,
         FORMAT_NUMBER(stage_cost_usd,      0) as stage_cost,
         FORMAT_NUMBER(cumulative_cost_usd,  0) as cumulative_cost,
         FORMAT_NUMBER(p10_cumulative_usd,   0) as p10,
         FORMAT_NUMBER(p90_cumulative_usd,   0) as p90,
         FORMAT_NUMBER(savings_vs_detect_late,0) as savings_vs_status_quo
  FROM gold.prevention_roi_curves
  ORDER BY scenario, stage_sequence
""").display()

# COMMAND ----------
# MAGIC %md ## Savings summary: AI system vs. status quo at each stage

# COMMAND ----------

spark.sql("""
  SELECT stage_name,
         MAX(CASE WHEN scenario='detect_early'  THEN cumulative_cost_usd END) as detect_early,
         MAX(CASE WHEN scenario='detect_late'   THEN cumulative_cost_usd END) as detect_late,
         MAX(CASE WHEN scenario='do_nothing'    THEN cumulative_cost_usd END) as do_nothing,
         MAX(CASE WHEN scenario='detect_late'   THEN cumulative_cost_usd END)
           - MAX(CASE WHEN scenario='detect_early' THEN cumulative_cost_usd END)
                                                     as savings_usd
  FROM gold.prevention_roi_curves
  GROUP BY stage_name, stage_sequence
  ORDER BY stage_sequence
""").display()
