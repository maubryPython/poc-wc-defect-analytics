"""
db_tools.py — Delta Lake Tool Functions
========================================
All Delta Lake reads/writes used by the agent layer.
Designed to run inside Databricks; uses SparkSession.getActiveSession().

Tool contract: every function returns a dict with at minimum:
  { "status": "ok" | "error", "tool": "<function_name>", ... }
"""

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# ── Spark session (Databricks provides this in notebook scope) ─────────────────
def _spark():
    return SparkSession.getActiveSession()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── Landing Zone ─────────────────────────────────────────────────────────────

def landing_zone_poll(path: str) -> dict:
    """
    Tool: landing_zone_poll
    Check the landing zone for files not yet recorded in the ingest log.
    Returns list of new file paths.
    """
    try:
        spark = _spark()
        all_files = [f.path for f in spark.sparkContext._jvm.org.apache.hadoop
                     .fs.FileSystem
                     .get(spark.sparkContext._jsc.hadoopConfiguration())
                     .listStatus(
                         spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
                     )]
    except Exception:
        # Fallback: use dbutils if available
        try:
            from pyspark.dbutils import DBUtils  # noqa
            dbutils = DBUtils(_spark())
            all_files = [f.path for f in dbutils.fs.ls(path)]
        except Exception as e:
            return {"status": "error", "tool": "landing_zone_poll", "message": str(e)}

    # Filter to supported formats
    supported = [f for f in all_files if f.endswith((".csv", ".xlsx", ".json"))]

    # Check against ingest log
    try:
        logged = _spark().sql(
            "SELECT source_path FROM silver.ingest_log"
        ).toPandas()["source_path"].tolist()
        new_files = [f for f in supported if f not in logged]
    except Exception:
        new_files = supported  # table doesn't exist yet → all files are new

    return {
        "status": "ok",
        "tool": "landing_zone_poll",
        "new_files": new_files,
        "total_found": len(supported),
        "new_count": len(new_files),
        "checked_at": _now(),
    }


# ─── Pipeline State ───────────────────────────────────────────────────────────

def pipeline_state_read() -> dict:
    """
    Tool: pipeline_state_read
    Returns freshness of Silver and Gold tables so the Orchestrator can
    decide which agents need to run this cycle.
    """
    tables = [
        ("silver", "dim_batch"),
        ("silver", "dim_supplier"),
        ("silver", "fact_qc_inspections"),
        ("silver", "fact_sales_returns"),
        ("gold",   "defect_alerts"),
        ("gold",   "rca_findings"),
        ("gold",   "scenario_projections"),
        ("gold",   "executive_briefings"),
    ]
    state = {}
    spark = _spark()
    for db, tbl in tables:
        try:
            row = spark.sql(
                f"SELECT MAX(ingested_at) as last_updated FROM {db}.{tbl}"
            ).collect()[0]
            state[f"{db}.{tbl}"] = str(row["last_updated"]) if row["last_updated"] else None
        except Exception:
            state[f"{db}.{tbl}"] = None  # table doesn't exist yet

    return {"status": "ok", "tool": "pipeline_state_read", "table_state": state}


# ─── Silver writes ────────────────────────────────────────────────────────────

def silver_write(df: DataFrame, table_name: str, run_id: str) -> dict:
    """
    Tool: silver_write
    Write a validated DataFrame to the Silver Delta database.
    Uses merge-on-pk to avoid duplicates.
    """
    try:
        df = df.withColumn("ingested_at", F.lit(_now())) \
               .withColumn("run_id", F.lit(run_id))
        df.write.format("delta").mode("append") \
          .saveAsTable(f"silver.{table_name}")
        count = df.count()
        return {
            "status": "ok", "tool": "silver_write",
            "table": f"silver.{table_name}", "rows_written": count,
        }
    except Exception as e:
        return {"status": "error", "tool": "silver_write", "message": str(e)}


def silver_read(table_name: str, filter_expr: str = None) -> dict:
    """
    Tool: silver_read
    Read from a Silver Delta table, optionally filtered.
    Returns a Spark DataFrame in 'data' key (None on error).
    """
    try:
        df = _spark().table(f"silver.{table_name}")
        if filter_expr:
            df = df.filter(filter_expr)
        return {"status": "ok", "tool": "silver_read", "table": f"silver.{table_name}", "data": df}
    except Exception as e:
        return {"status": "error", "tool": "silver_read", "message": str(e), "data": None}


# ─── Gold writes / reads ──────────────────────────────────────────────────────

def gold_write(df: DataFrame, table_name: str, run_id: str, mode: str = "append") -> dict:
    """
    Tool: gold_write
    Write analytics-ready data to the Gold Delta database.
    """
    try:
        df = df.withColumn("created_at", F.lit(_now())) \
               .withColumn("run_id", F.lit(run_id))
        df.write.format("delta").mode(mode).saveAsTable(f"gold.{table_name}")
        return {
            "status": "ok", "tool": "gold_write",
            "table": f"gold.{table_name}", "rows_written": df.count(),
        }
    except Exception as e:
        return {"status": "error", "tool": "gold_write", "message": str(e)}


def gold_read(table_name: str, filter_expr: str = None) -> dict:
    """
    Tool: gold_read
    Read from a Gold Delta table.
    """
    try:
        df = _spark().table(f"gold.{table_name}")
        if filter_expr:
            df = df.filter(filter_expr)
        return {"status": "ok", "tool": "gold_read", "table": f"gold.{table_name}", "data": df}
    except Exception as e:
        return {"status": "error", "tool": "gold_read", "message": str(e), "data": None}


# ─── Quarantine ───────────────────────────────────────────────────────────────

def quarantine_write(df: DataFrame, source_file: str, error_reason: str, run_id: str) -> dict:
    """
    Tool: quarantine_write
    Write invalid rows (or entire file records) to the bad_records table.
    """
    try:
        df = df.withColumn("error_reason", F.lit(error_reason)) \
               .withColumn("source_file", F.lit(source_file)) \
               .withColumn("quarantined_at", F.lit(_now())) \
               .withColumn("run_id", F.lit(run_id))
        df.write.format("delta").mode("append").saveAsTable("silver.bad_records")
        return {
            "status": "ok", "tool": "quarantine_write",
            "rows_quarantined": df.count(), "reason": error_reason,
        }
    except Exception as e:
        return {"status": "error", "tool": "quarantine_write", "message": str(e)}


# ─── Run Log ──────────────────────────────────────────────────────────────────

def run_log_write(run_id: str, agents_invoked: list, tools_called: list,
                  escalations: int, duration_seconds: float,
                  confidence_scores: dict = None, notes: str = "") -> dict:
    """
    Tool: run_log_write
    Persist an Orchestrator run summary to the run_log Delta table.
    """
    spark = _spark()
    try:
        record = spark.createDataFrame([{
            "run_id":            run_id,
            "run_timestamp":     _now(),
            "agents_invoked":    json.dumps(agents_invoked),
            "tools_called":      json.dumps(tools_called),
            "escalations":       escalations,
            "duration_seconds":  float(duration_seconds),
            "confidence_scores": json.dumps(confidence_scores or {}),
            "notes":             notes,
        }])
        record.write.format("delta").mode("append").saveAsTable("gold.run_log")
        return {"status": "ok", "tool": "run_log_write", "run_id": run_id}
    except Exception as e:
        return {"status": "error", "tool": "run_log_write", "message": str(e)}


# ─── Escalation Queue ─────────────────────────────────────────────────────────

def escalation_write(agent_id: str, run_id: str, question: str,
                     context: dict, recovery_attempted: bool = False) -> dict:
    """
    Tool: escalate_to_human
    Write a structured escalation record to the escalation_queue table.
    The Orchestrator surfaces this; analyst must resolve before the
    pipeline can proceed on the affected branch.
    """
    spark = _spark()
    try:
        record = spark.createDataFrame([{
            "escalation_id":      str(uuid.uuid4()),
            "agent_id":           agent_id,
            "run_id":             run_id,
            "escalated_at":       _now(),
            "question":           question,
            "context_json":       json.dumps(context),
            "recovery_attempted": recovery_attempted,
            "resolved":           False,
            "resolution_notes":   None,
        }])
        record.write.format("delta").mode("append").saveAsTable("gold.escalation_queue")
        return {"status": "ok", "tool": "escalate_to_human", "agent_id": agent_id, "question": question}
    except Exception as e:
        return {"status": "error", "tool": "escalate_to_human", "message": str(e)}


# ─── Ingest Log ───────────────────────────────────────────────────────────────

def ingest_log_write(source_path: str, table_name: str, rows_written: int,
                     rows_quarantined: int, run_id: str, status: str) -> dict:
    """Log each successfully processed file so the Orchestrator knows what's been seen."""
    spark = _spark()
    try:
        record = spark.createDataFrame([{
            "source_path":      source_path,
            "table_name":       table_name,
            "rows_written":     rows_written,
            "rows_quarantined": rows_quarantined,
            "run_id":           run_id,
            "status":           status,
            "ingested_at":      _now(),
        }])
        record.write.format("delta").mode("append").saveAsTable("silver.ingest_log")
        return {"status": "ok", "tool": "ingest_log_write"}
    except Exception as e:
        return {"status": "error", "tool": "ingest_log_write", "message": str(e)}


# ─── Convenience queries used by specialist agents ────────────────────────────

def qc_query(batch_id: str = None, date_range: tuple = None) -> dict:
    """Tool: qc_query — retrieve QC inspection records."""
    try:
        df = _spark().table("silver.fact_qc_inspections")
        if batch_id:
            df = df.filter(F.col("batch_id") == batch_id)
        if date_range:
            df = df.filter(F.col("inspection_date").between(*date_range))
        rows = df.toPandas().to_dict("records")
        return {"status": "ok", "tool": "qc_query", "rows": rows, "count": len(rows)}
    except Exception as e:
        return {"status": "error", "tool": "qc_query", "message": str(e), "rows": []}


def baseline_lookup(supplier_id: str, line_id: str = None) -> dict:
    """
    Tool: baseline_lookup
    Retrieve a supplier's historical defect rate from the Silver history table.
    Used by the Detection Agent before scoring anomalies.
    """
    try:
        df = _spark().table("silver.supplier_defect_history")
        df = df.filter(F.col("supplier_id") == supplier_id)
        if line_id:
            df = df.filter(F.col("line_id") == line_id)
        agg = df.agg(
            F.mean("defect_rate").alias("mean_rate"),
            F.stddev("defect_rate").alias("std_rate"),
            F.max("defect_rate").alias("max_rate"),
            F.count("*").alias("months_on_record"),
        ).collect()[0]
        if agg["mean_rate"] is None:
            return {
                "status": "not_found", "tool": "baseline_lookup",
                "supplier_id": supplier_id, "line_id": line_id,
            }
        return {
            "status": "ok", "tool": "baseline_lookup",
            "supplier_id": supplier_id, "line_id": line_id,
            "mean_rate": round(float(agg["mean_rate"]), 5),
            "std_rate":  round(float(agg["std_rate"] or 0), 5),
            "max_rate":  round(float(agg["max_rate"]), 5),
            "months_on_record": int(agg["months_on_record"]),
        }
    except Exception as e:
        return {"status": "error", "tool": "baseline_lookup", "message": str(e)}


def material_lot_query(lot_id: str) -> dict:
    """Tool: material_lot_query — retrieve material lot metadata for borderline batches."""
    try:
        df = _spark().table("silver.dim_batch") \
                     .filter(F.col("material_lot") == lot_id) \
                     .select("batch_id", "supplier_id", "material_lot",
                             "material_type", "line_id", "production_date")
        rows = df.toPandas().to_dict("records")
        return {"status": "ok", "tool": "material_lot_query", "lot_id": lot_id, "batches": rows}
    except Exception as e:
        return {"status": "error", "tool": "material_lot_query", "message": str(e), "batches": []}


def supplier_history_query(supplier_id: str) -> dict:
    """Tool: supplier_history_query — full monthly defect history for RCA."""
    try:
        df = _spark().table("silver.supplier_defect_history") \
                     .filter(F.col("supplier_id") == supplier_id) \
                     .orderBy("month")
        rows = df.toPandas().to_dict("records")
        return {"status": "ok", "tool": "supplier_history_query",
                "supplier_id": supplier_id, "months": rows}
    except Exception as e:
        return {"status": "error", "tool": "supplier_history_query", "message": str(e)}


def production_conditions_query(line_id: str, month: str) -> dict:
    """
    Tool: production_conditions_query
    Returns aggregated production stats for a line in a given month.
    (Derived from Silver batch + QC tables — no separate conditions table needed.)
    """
    try:
        spark = _spark()
        df = spark.sql(f"""
            SELECT b.line_id, DATE_FORMAT(b.production_date,'yyyy-MM') as month,
                   COUNT(DISTINCT b.batch_id) as batches_produced,
                   SUM(b.unit_count) as total_units,
                   AVG(q.defect_count) as avg_defects_per_inspection,
                   MAX(q.defect_count) as max_defects,
                   COUNT(CASE WHEN q.pass_fail='FAIL' THEN 1 END) as fail_inspections
            FROM silver.dim_batch b
            LEFT JOIN silver.fact_qc_inspections q ON b.batch_id = q.batch_id
            WHERE b.line_id = '{line_id}'
              AND DATE_FORMAT(b.production_date,'yyyy-MM') = '{month}'
            GROUP BY b.line_id, DATE_FORMAT(b.production_date,'yyyy-MM')
        """)
        rows = df.toPandas().to_dict("records")
        return {"status": "ok", "tool": "production_conditions_query",
                "line_id": line_id, "month": month, "data": rows}
    except Exception as e:
        return {"status": "error", "tool": "production_conditions_query", "message": str(e)}


def defect_scope_query(run_id: str = None) -> dict:
    """Tool: defect_scope_query — aggregate total flagged units for the Scenario Agent."""
    try:
        filter_clause = f"AND run_id = '{run_id}'" if run_id else ""
        df = _spark().sql(f"""
            SELECT COUNT(DISTINCT batch_id) as flagged_batches,
                   SUM(units_flagged) as total_units_flagged,
                   AVG(anomaly_score) as avg_anomaly_score,
                   COLLECT_SET(supplier_id) as affected_suppliers
            FROM gold.defect_alerts
            WHERE alert_status = 'FLAGGED' {filter_clause}
        """)
        row = df.collect()[0]
        return {
            "status": "ok", "tool": "defect_scope_query",
            "flagged_batches": int(row["flagged_batches"] or 0),
            "total_units_flagged": int(row["total_units_flagged"] or 0),
            "avg_anomaly_score": float(row["avg_anomaly_score"] or 0),
            "affected_suppliers": row["affected_suppliers"] or [],
        }
    except Exception as e:
        return {"status": "error", "tool": "defect_scope_query", "message": str(e)}


def sales_return_query(date_range: tuple = None) -> dict:
    """Tool: sales_return_query — aggregate sales/returns for Scenario Agent baseline."""
    try:
        df = _spark().table("silver.fact_sales_returns")
        if date_range:
            df = df.filter(F.col("date").between(*date_range))
        agg = df.agg(
            F.sum("units_sold").alias("total_sold"),
            F.sum("units_returned").alias("total_returned"),
            F.mean("return_rate").alias("avg_return_rate"),
            F.sum("revenue_usd").alias("total_revenue"),
            F.sum("refund_usd").alias("total_refunds"),
        ).collect()[0]
        return {
            "status": "ok", "tool": "sales_return_query",
            "total_sold":       int(agg["total_sold"] or 0),
            "total_returned":   int(agg["total_returned"] or 0),
            "avg_return_rate":  round(float(agg["avg_return_rate"] or 0), 4),
            "total_revenue":    round(float(agg["total_revenue"] or 0), 2),
            "total_refunds":    round(float(agg["total_refunds"] or 0), 2),
        }
    except Exception as e:
        return {"status": "error", "tool": "sales_return_query", "message": str(e)}


def rca_findings_query(run_id: str = None, supplier_id: str = None) -> dict:
    """Tool: rca_findings_query — retrieve RCA results for Summary Agent."""
    try:
        df = _spark().table("gold.rca_findings")
        if run_id:
            df = df.filter(F.col("run_id") == run_id)
        if supplier_id:
            df = df.filter(F.col("supplier_id") == supplier_id)
        rows = df.orderBy(F.col("confidence_score").desc()).toPandas().to_dict("records")
        return {"status": "ok", "tool": "rca_findings_query", "findings": rows}
    except Exception as e:
        return {"status": "error", "tool": "rca_findings_query", "message": str(e), "findings": []}


def scenario_summary_query(run_id: str) -> dict:
    """Tool: scenario_summary_query — retrieve scenario projections for Summary Agent."""
    try:
        df = _spark().table("gold.scenario_projections") \
                     .filter(F.col("run_id") == run_id)
        rows = df.toPandas().to_dict("records")
        recommended = next((r for r in rows if r.get("is_recommended")), None)
        return {
            "status": "ok", "tool": "scenario_summary_query",
            "scenarios": rows, "recommended": recommended,
        }
    except Exception as e:
        return {"status": "error", "tool": "scenario_summary_query", "message": str(e)}


# ─── Prevention Story Query Tools ─────────────────────────────────────────────
# Powers the four dashboard charts that make the "never again" case.

def supplier_risk_score_query(supplier_id: str = None, risk_tier: str = None) -> dict:
    """
    Tool: supplier_risk_score_query
    Retrieve pre-production supplier risk scores from silver.supplier_risk_scores.
    Used by Detection Agent to enrich defect context and by export notebook for Chart 2.
    Optionally filter by supplier_id or risk_tier (HIGH / ELEVATED / NORMAL).
    """
    try:
        df = _spark().table("silver.supplier_risk_scores")
        if supplier_id:
            df = df.filter(F.col("supplier_id") == supplier_id)
        if risk_tier:
            df = df.filter(F.col("risk_tier") == risk_tier)
        rows = df.orderBy("composite_risk_score").toPandas().to_dict("records")
        return {
            "status": "ok", "tool": "supplier_risk_score_query",
            "scores": rows, "count": len(rows),
            "high_risk_suppliers": [r["supplier_id"] for r in rows if r["risk_tier"] == "HIGH"],
        }
    except Exception as e:
        return {"status": "error", "tool": "supplier_risk_score_query", "message": str(e), "scores": []}


def lot_traceability_query(lot_id: str = None, supplier_id: str = None,
                           defective_only: bool = False) -> dict:
    """
    Tool: lot_traceability_query
    Retrieve material lot traceability data from gold.lot_defect_traceability.
    Used by RCA Agent to link batches → lots → root cause, and by export for Chart 3.
    """
    try:
        df = _spark().table("gold.lot_defect_traceability")
        if lot_id:
            df = df.filter(F.col("lot_id") == lot_id)
        if supplier_id:
            df = df.filter(F.col("supplier_id") == supplier_id)
        if defective_only:
            df = df.filter(F.col("is_defective") == True)
        rows = df.orderBy(F.col("mean_defect_rate").desc()).toPandas().to_dict("records")
        return {
            "status": "ok", "tool": "lot_traceability_query",
            "lots": rows, "count": len(rows),
            "defective_lots": [r["lot_id"] for r in rows if r.get("is_defective")],
        }
    except Exception as e:
        return {"status": "error", "tool": "lot_traceability_query", "message": str(e), "lots": []}


def weekly_defect_trend_query(supplier_ids: list = None, weeks: int = 24) -> dict:
    """
    Tool: weekly_defect_trend_query
    Retrieve rolling weekly defect rates from gold.weekly_defect_trend.
    Used by Detection Agent and export notebook for Chart 1 (the detection window).
    """
    try:
        df = _spark().table("gold.weekly_defect_trend")
        if supplier_ids:
            df = df.filter(F.col("supplier_id").isin(supplier_ids))
        df = df.orderBy("production_week").limit(weeks * (len(supplier_ids) if supplier_ids else 8))
        rows = df.toPandas().to_dict("records")
        # Find the first week the aggregate rate crossed the AI threshold
        agg = df.groupBy("production_week") \
                .agg(F.mean("weekly_defect_rate").alias("avg_rate")) \
                .orderBy("production_week")
        threshold = 0.038
        crossed = agg.filter(F.col("avg_rate") >= threshold).limit(1).collect()
        first_alert_week = crossed[0]["production_week"] if crossed else None
        return {
            "status": "ok", "tool": "weekly_defect_trend_query",
            "trend": rows, "count": len(rows),
            "ai_threshold": threshold,
            "first_alert_week": first_alert_week,
        }
    except Exception as e:
        return {"status": "error", "tool": "weekly_defect_trend_query", "message": str(e), "trend": []}


def prevention_roi_query(run_id: str = None) -> dict:
    """
    Tool: prevention_roi_query
    Retrieve 6-stage cumulative cost curves from gold.prevention_roi_curves.
    Used by Scenario Agent and export notebook for Chart 4 (Prevention ROI).
    """
    try:
        df = _spark().table("gold.prevention_roi_curves")
        if run_id:
            df = df.filter(F.col("run_id") == run_id)
        else:
            # Most recent run
            latest = df.agg(F.max("run_id").alias("latest")).collect()[0]["latest"]
            df = df.filter(F.col("run_id") == latest)
        rows = df.orderBy("stage_sequence").toPandas().to_dict("records")
        # Compute savings: detect_early vs detect_late at final stage
        by_scenario = {r["scenario"]: r["cumulative_cost_usd"]
                       for r in rows if r["stage_sequence"] == max(r2["stage_sequence"] for r2 in rows)}
        savings = round(by_scenario.get("detect_late", 0) - by_scenario.get("detect_early", 0), 2)
        return {
            "status": "ok", "tool": "prevention_roi_query",
            "curves": rows, "count": len(rows),
            "savings_vs_status_quo_usd": savings,
            "final_costs_by_scenario": by_scenario,
        }
    except Exception as e:
        return {"status": "error", "tool": "prevention_roi_query", "message": str(e), "curves": []}
