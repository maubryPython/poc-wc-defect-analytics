# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Scenario Modeling Agent (A-04)
# MAGIC Monte Carlo projections for 3 response strategies.
# MAGIC POC-3: pre-computes parameter variants for Tableau parameter actions.

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/<your-repo>/nike-wc-defect-analytics")

# COMMAND ----------

import uuid, json
from agents.scenario_agent import run

run_id = str(uuid.uuid4())
result = run({"run_id": run_id})
print(json.dumps({k: v for k, v in result.items() if k != "tools_called"}, indent=2))
print(f"\nTools called: {result['tools_called']}")

# COMMAND ----------
# MAGIC %md ## Scenario comparison — cost and timeline (AC-09, AC-23)

# COMMAND ----------

spark.sql("""
  SELECT scenario_name, is_recommended, confidence_flag,
         FORMAT_NUMBER(mean_total_cost_usd, 0)     as mean_cost_usd,
         FORMAT_NUMBER(p10_total_cost_usd, 0)      as p10_cost_usd,
         FORMAT_NUMBER(p90_total_cost_usd, 0)      as p90_cost_usd,
         mean_timeline_days,
         mean_returns_volume,
         most_sensitive_param
  FROM gold.scenario_projections
  ORDER BY mean_total_cost_usd ASC
""").display()

# COMMAND ----------
# MAGIC %md ## POC-3: Pre-compute parameter variants for Tableau sliders

# COMMAND ----------

# MAGIC %md
# MAGIC Pre-compute scenarios across a range of return_rate assumptions
# MAGIC so Tableau Public can switch between them with parameter actions.

from agents.scenario_agent import run as run_scenario

variants = []
for return_rate_mult in [0.7, 0.85, 1.0, 1.15, 1.30]:
    r = run_scenario({
        "run_id": f"variant_{return_rate_mult}",
        "param_overrides": {
            "do_nothing":         {"return_rate_multiplier": return_rate_mult},
            "partial_remediation":{"return_rate_multiplier": return_rate_mult},
            "full_recall":        {"return_rate_multiplier": return_rate_mult},
        }
    })
    variants.append({"return_rate_assumption": return_rate_mult, **r})

import pandas as pd
pd.DataFrame(variants)[["return_rate_assumption", "recommended_scenario",
                          "recommended_cost_usd", "recommended_timeline_days"]].display()

# COMMAND ----------
# MAGIC %md ## Sensitivity analysis

# COMMAND ----------

spark.sql("""
  SELECT DISTINCT most_sensitive_param,
         sensitivity_rank,
         confidence_flag
  FROM gold.scenario_projections
  LIMIT 3
""").display()
