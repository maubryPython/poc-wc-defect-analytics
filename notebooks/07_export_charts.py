# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Chart Export Notebook
# MAGIC Exports exactly the four CSV files consumed by the prevention-story
# MAGIC Tableau dashboard (or the matplotlib charts for the POC demo).
# MAGIC
# MAGIC | Export file                  | Gold table                   | Chart |
# MAGIC |------------------------------|------------------------------|-------|
# MAGIC | weekly_defect_trend.csv      | gold.weekly_defect_trend     | 1 — Detection Window |
# MAGIC | supplier_risk_scores.csv     | silver.supplier_risk_scores  | 2 — Supplier Risk Radar |
# MAGIC | lot_traceability.csv         | gold.lot_defect_traceability | 3 — Lot Traceability |
# MAGIC | prevention_roi.csv           | gold.prevention_roi_curves   | 4 — Prevention ROI |
# MAGIC
# MAGIC Run this notebook after `06_orchestrator` completes (or after running
# MAGIC notebooks 01–05 individually). The CSVs are written to:
# MAGIC `/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics/dashboard/exports/`

# COMMAND ----------

%pip install pyyaml python-dotenv --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")

# COMMAND ----------

%run ./00_env

# COMMAND ----------

EXPORT_PATH = "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics/dashboard/exports/"

# COMMAND ----------
# MAGIC %md ## Export 1 — Weekly Defect Trend (Chart 1: Detection Window)
# MAGIC Columns: supplier_id, production_week, weekly_defect_rate,
# MAGIC          total_defects, total_units_inspected, above_ai_threshold, ai_threshold

# COMMAND ----------

trend_df = spark.sql("""
  SELECT supplier_id, production_week,
         ROUND(weekly_defect_rate, 5)   as weekly_defect_rate,
         total_defects,
         total_units_inspected,
         above_ai_threshold,
         ai_threshold,
         -- Add helper columns for Chart 1 annotation
         CASE WHEN above_ai_threshold THEN production_week ELSE NULL END as alert_week,
         run_id
  FROM gold.weekly_defect_trend
  ORDER BY supplier_id, production_week
""")
trend_df.toPandas().to_csv(EXPORT_PATH + "weekly_defect_trend.csv", index=False)
print(f"weekly_defect_trend.csv: {trend_df.count()} rows")
trend_df.filter("supplier_id IN ('S007','S008') AND above_ai_threshold = true").display()

# COMMAND ----------
# MAGIC %md ## Export 2 — Supplier Risk Scores (Chart 2: Risk Radar)
# MAGIC Columns: all 5 dimension scores + composite + risk_tier

# COMMAND ----------

risk_df = spark.sql("""
  SELECT supplier_id, supplier_name, region, material_type, contract_tier,
         historical_defect_score,
         on_time_delivery_score,
         audit_score,
         material_lot_stability_score,
         capacity_utilization_score,
         composite_risk_score,
         risk_tier,
         would_trigger_audit,
         scored_at
  FROM silver.supplier_risk_scores
  ORDER BY composite_risk_score ASC
""")
risk_df.toPandas().to_csv(EXPORT_PATH + "supplier_risk_scores.csv", index=False)
print(f"supplier_risk_scores.csv: {risk_df.count()} rows")
risk_df.display()

# COMMAND ----------
# MAGIC %md ## Export 3 — Lot Defect Traceability (Chart 3: Lot Timeline)
# MAGIC Columns: lot_id, supplier_id, entered_production_date, mean_defect_rate,
# MAGIC          batch_count, is_defective, hours_to_first_alert, defect_root_cause

# COMMAND ----------

lot_df = spark.sql("""
  SELECT lot_id, supplier_id, material_type,
         entered_production_date,
         ROUND(mean_defect_rate, 5) as mean_defect_rate,
         batch_count,
         batches_flagged,
         is_defective,
         hours_to_first_alert,
         rca_status,
         ROUND(rca_top_confidence, 3) as rca_confidence,
         defect_root_cause,
         traced_at
  FROM gold.lot_defect_traceability
  ORDER BY is_defective DESC, mean_defect_rate DESC
""")
lot_df.toPandas().to_csv(EXPORT_PATH + "lot_traceability.csv", index=False)
print(f"lot_traceability.csv: {lot_df.count()} rows")
lot_df.display()

# COMMAND ----------
# MAGIC %md ## Export 4 — Prevention ROI Curves (Chart 4: ROI)
# MAGIC Columns: scenario, stage_sequence, stage_name, cumulative_cost_usd,
# MAGIC          p10/p90 uncertainty bands, savings_vs_detect_late

# COMMAND ----------

roi_df = spark.sql("""
  SELECT scenario, stage_sequence, stage_name,
         stage_cost_usd,
         cumulative_cost_usd,
         p10_cumulative_usd,
         p90_cumulative_usd,
         savings_vs_detect_late,
         computed_at
  FROM gold.prevention_roi_curves
  ORDER BY scenario, stage_sequence
""")
roi_df.toPandas().to_csv(EXPORT_PATH + "prevention_roi.csv", index=False)
print(f"prevention_roi.csv: {roi_df.count()} rows")
roi_df.display()

# COMMAND ----------
# MAGIC %md ## Verify all four exports exist

# COMMAND ----------

for fname in ["weekly_defect_trend.csv","supplier_risk_scores.csv",
              "lot_traceability.csv","prevention_roi.csv"]:
    try:
        info = dbutils.fs.ls(EXPORT_PATH + fname)
        print(f"✓ {fname} — {info[0].size:,} bytes")
    except Exception as e:
        print(f"✗ {fname} — MISSING: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next step: Connect Tableau
# MAGIC
# MAGIC Connect Tableau Desktop or Tableau Public to Databricks SQL via:
# MAGIC `Connectors → Databricks → SQL Warehouse endpoint`
# MAGIC
# MAGIC Then use these four gold/silver tables directly (live connection),
# MAGIC or open the CSV exports from this directory for the offline POC demo.
# MAGIC
# MAGIC **Dashboard story arc:**
# MAGIC 1. Chart 1 — *The Detection Window*: 10 weeks the data was trying to tell us
# MAGIC 2. Chart 2 — *Supplier Risk Radar*: S007 & S008 were already red-flag pre-production
# MAGIC 3. Chart 3 — *Lot Traceability*: 3 adhesive lots drove all 14 defective batches — detectable in 72 hours
# MAGIC 4. Chart 4 — *Prevention ROI*: AI monitoring saves $1.4M vs. status quo detection
