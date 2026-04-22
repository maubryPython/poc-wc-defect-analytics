"""
A-02 — Defect Detection Agent
==============================
Observe: Silver QC inspection data + batch_lot_mapping + supplier_risk_scores
Reason:  Z-score vs. supplier baseline; Isolation Forest pre-screen;
         AGENTIC: borderline cases trigger material_lot_query secondary lookup;
         NEW: computes weekly_defect_trend and lot_defect_rates for Chart 1 & 3
Act:     write gold.defect_alerts, gold.weekly_defect_trend, gold.lot_defect_rates
Escalate: missing baseline → UNRESOLVED + escalation_queue

Prevention story output:
  gold.weekly_defect_trend  → Chart 1 (detection window)
  gold.lot_defect_rates     → Chart 3 (lot traceability — consumed by RCA agent)
"""

import uuid
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tools.db_tools import (
    silver_read, gold_write, escalation_write,
    baseline_lookup, material_lot_query, qc_query,
    supplier_risk_score_query,
)
from tools.validation_tools import config_read
from sklearn.ensemble import IsolationForest

AI_ALERT_THRESHOLD = 0.038   # 3.8% — defect rate that triggers an AI alert (Chart 1)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _zscore(rate: float, mean: float, std: float) -> float:
    if std == 0:
        return 0.0
    return (rate - mean) / std


def run(task_payload: dict) -> dict:
    run_id    = task_payload.get("run_id", str(uuid.uuid4()))
    batch_ids = task_payload.get("batch_ids")

    cfg = config_read()["config"].get("detection", {})
    FLAG_THRESH    = cfg.get("flag_threshold",    2.0)
    BORDERLINE_LOW = cfg.get("borderline_low",    1.5)
    MIN_UNITS      = cfg.get("min_units_inspected", 50)
    tools_called   = ["config_read"]

    spark = SparkSession.getActiveSession()

    # ── Observe: QC data ────────────────────────────────────────────────────
    qc_result = silver_read("fact_qc_inspections")
    tools_called.append("silver_read:fact_qc_inspections")
    if qc_result["status"] \!= "ok":
        return {"agent_id": "A-02", "run_id": run_id, "status": "FAILED",
                "message": "Could not read fact_qc_inspections"}

    qc_df = qc_result["data"].toPandas()
    if batch_ids:
        qc_df = qc_df[qc_df["batch_id"].isin(batch_ids)]
    qc_df = qc_df[qc_df["units_inspected"] >= MIN_UNITS]

    # Batch-level aggregation
    batch_agg = (
        qc_df.groupby(["batch_id","supplier_id","line_id"])
        .agg(
            avg_defect_count=("defect_count","mean"),
            avg_units_inspected=("units_inspected","mean"),
            inspection_count=("inspection_id","count"),
            fail_count=("pass_fail", lambda x: (x == "FAIL").sum()),
            defect_types=("defect_type", lambda x: list(x.unique())),
        )
        .reset_index()
    )
    batch_agg["observed_defect_rate"] = (
        batch_agg["avg_defect_count"] / batch_agg["avg_units_inspected"]
    )

    # Join batch metadata
    batch_meta = silver_read("dim_batch")["data"].toPandas()
    tools_called.append("silver_read:dim_batch")
    batch_agg = batch_agg.merge(
        batch_meta[["batch_id","unit_count","material_lot","production_date"]],
        on="batch_id", how="left"
    )

    # ── NEW: Enrich with pre-production supplier risk scores ────────────────
    risk_result = supplier_risk_score_query()
    tools_called.append("supplier_risk_score_query")
    risk_map = {}
    if risk_result["status"] == "ok":
        risk_map = {r["supplier_id"]: r for r in risk_result["scores"]}

    # ── Isolation Forest pre-screen ─────────────────────────────────────────
    if len(batch_agg) >= 5:
        iso = IsolationForest(contamination=0.1, random_state=42)
        batch_agg["iso_outlier"] = iso.fit_predict(
            batch_agg[["observed_defect_rate","fail_count"]].fillna(0)
        ) == -1
    else:
        batch_agg["iso_outlier"] = False

    # ── Reason & Act: per-batch alert logic ─────────────────────────────────
    alert_records = []
    escalations   = 0

    for _, row in batch_agg.iterrows():
        batch_id    = row["batch_id"]
        supplier_id = row["supplier_id"]
        line_id     = row["line_id"]
        obs_rate    = float(row["observed_defect_rate"])
        units_flagged = int(row["unit_count"]) if pd.notna(row["unit_count"]) else 0
        batch_tools = []

        # Factor in pre-production risk tier (AGENTIC enrichment)
        supplier_risk = risk_map.get(supplier_id, {})
        risk_tier     = supplier_risk.get("risk_tier", "UNKNOWN")
        risk_composite = float(supplier_risk.get("composite_risk_score", 0.8))

        bl = baseline_lookup(supplier_id, line_id)
        batch_tools.append("baseline_lookup")
        tools_called.append("baseline_lookup")

        if bl["status"] == "not_found":
            escalation_write(
                agent_id="A-02", run_id=run_id,
                question=(f"Batch {batch_id} (supplier {supplier_id}) has no historical baseline."),
                context={"batch_id": batch_id, "supplier_id": supplier_id,
                         "observed_rate": obs_rate},
                recovery_attempted=False,
            )
            batch_tools.append("escalate_to_human")
            tools_called.append("escalate_to_human")
            alert_records.append(_build_alert(
                batch_id, supplier_id, line_id, obs_rate, units_flagged,
                row, zscore=None, status="UNRESOLVED", risk_tier=risk_tier,
                reasoning="No historical baseline available.",
                tools_called=batch_tools, run_id=run_id,
            ))
            escalations += 1
            continue

        mean_rate = bl["mean_rate"]
        std_rate  = bl["std_rate"]
        zscore    = _zscore(obs_rate, mean_rate, std_rate)
        iso_flag  = bool(row["iso_outlier"])

        # AGENTIC: HIGH-risk supplier gets lower effective threshold
        effective_threshold = FLAG_THRESH * (0.85 if risk_tier == "HIGH" else 1.0)

        if zscore >= effective_threshold or (iso_flag and zscore >= BORDERLINE_LOW):
            reasoning = (
                f"Defect rate {obs_rate:.4f} is {zscore:.1f} SD above supplier baseline "
                f"(mean={mean_rate:.4f}, SD={std_rate:.4f}). "
                f"IsolationForest: {'outlier' if iso_flag else 'normal'}. "
                f"Supplier risk tier: {risk_tier} (composite={risk_composite:.2f})."
            )
            status = "FLAGGED"

        elif BORDERLINE_LOW <= zscore < effective_threshold:
            ml = material_lot_query(str(row["material_lot"]))
            batch_tools.append("material_lot_query")
            tools_called.append("material_lot_query")
            lot_batches = ml.get("batches", [])
            lot_rates = []
            for lb in lot_batches:
                lb_qc = qc_df[qc_df["batch_id"] == lb["batch_id"]]
                if len(lb_qc):
                    lr = lb_qc["defect_count"].mean() / max(lb_qc["units_inspected"].mean(), 1)
                    lot_rates.append(lr)
            lot_avg = float(np.mean(lot_rates)) if lot_rates else obs_rate

            if lot_avg >= mean_rate + BORDERLINE_LOW * std_rate:
                reasoning = (
                    f"Borderline Z-score {zscore:.1f}. "
                    f"Secondary lot query for '{row['material_lot']}' found elevated "
                    f"lot-level rate {lot_avg:.4f}. Supplier risk: {risk_tier}. Upgraded to FLAGGED."
                )
                status = "FLAGGED"
            else:
                reasoning = (
                    f"Borderline Z-score {zscore:.1f}. Lot rates normal ({lot_avg:.4f}). "
                    f"Classified REVIEW."
                )
                status = "REVIEW"
        else:
            reasoning = (
                f"Defect rate {obs_rate:.4f} within {zscore:.1f} SD of baseline. Normal."
            )
            status = "NORMAL"

        alert_records.append(_build_alert(
            batch_id, supplier_id, line_id, obs_rate, units_flagged,
            row, zscore=zscore, status=status, risk_tier=risk_tier,
            reasoning=reasoning, tools_called=batch_tools, run_id=run_id,
        ))

    # ── Act: write gold.defect_alerts ────────────────────────────────────────
    if alert_records:
        alert_df = spark.createDataFrame(
            [{k: v for k, v in r.items()} for r in alert_records]
        )
        gold_write(alert_df, "defect_alerts", run_id)
        tools_called.append("gold_write:defect_alerts")

    # ── NEW: Compute and write gold.weekly_defect_trend (Chart 1) ────────────
    _write_weekly_trend(spark, qc_df, batch_meta, run_id, tools_called)

    # ── NEW: Compute and write gold.lot_defect_rates (Chart 3 input) ─────────
    _write_lot_defect_rates(spark, batch_agg, run_id, tools_called)

    flagged  = sum(1 for r in alert_records if r["alert_status"] == "FLAGGED")
    reviewed = sum(1 for r in alert_records if r["alert_status"] == "REVIEW")

    return {
        "agent_id":     "A-02",
        "run_id":       run_id,
        "status":       "COMPLETE" if escalations == 0 else "PARTIAL",
        "batches_evaluated": len(batch_agg),
        "flagged":      flagged,
        "review":       reviewed,
        "unresolved":   escalations,
        "ai_threshold": AI_ALERT_THRESHOLD,
        "tools_called": list(dict.fromkeys(tools_called)),
        "flagged_batch_ids": [r["batch_id"] for r in alert_records
                              if r["alert_status"] == "FLAGGED"],
    }


def _write_weekly_trend(spark, qc_df, batch_meta, run_id, tools_called):
    """
    Aggregate QC data to weekly defect rate per supplier.
    Adds AI threshold flag so Chart 1 can show exactly when the signal crossed.
    Writes to gold.weekly_defect_trend.
    """
    try:
        merged = qc_df.merge(
            batch_meta[["batch_id","production_date","supplier_id"]],
            on=["batch_id","supplier_id"], how="left", suffixes=("","_meta")
        )
        merged["production_date"] = pd.to_datetime(merged["production_date"], errors="coerce")
        merged["production_week"] = merged["production_date"].dt.to_period("W").astype(str)
        merged["defect_rate"]     = merged["defect_count"] / merged["units_inspected"].clip(lower=1)

        weekly = (
            merged.groupby(["supplier_id","production_week"])
            .agg(
                weekly_defect_rate=("defect_rate","mean"),
                inspections=("inspection_id","count"),
                total_units_inspected=("units_inspected","sum"),
                total_defects=("defect_count","sum"),
            )
            .reset_index()
        )
        weekly["above_ai_threshold"] = weekly["weekly_defect_rate"] >= AI_ALERT_THRESHOLD
        weekly["ai_threshold"]       = AI_ALERT_THRESHOLD
        weekly["run_id"]             = run_id

        sdf = spark.createDataFrame(weekly)
        gold_write(sdf, "weekly_defect_trend", run_id, mode="overwrite")
        tools_called.append("gold_write:weekly_defect_trend")
    except Exception as e:
        tools_called.append(f"weekly_defect_trend:FAILED:{e}")


def _write_lot_defect_rates(spark, batch_agg, run_id, tools_called):
    """
    Aggregate batch-level defect rates up to lot level.
    Used by RCA agent to build lot_defect_traceability gold table.
    Writes to gold.lot_defect_rates.
    """
    try:
        lot_agg = (
            batch_agg.groupby(["material_lot","supplier_id"])
            .agg(
                mean_defect_rate=("observed_defect_rate","mean"),
                max_defect_rate=("observed_defect_rate","max"),
                batch_count=("batch_id","count"),
                total_units=("unit_count","sum"),
            )
            .reset_index()
            .rename(columns={"material_lot":"lot_id"})
        )
        lot_agg["above_ai_threshold"] = lot_agg["mean_defect_rate"] >= AI_ALERT_THRESHOLD
        lot_agg["run_id"]             = run_id

        sdf = spark.createDataFrame(lot_agg)
        gold_write(sdf, "lot_defect_rates", run_id, mode="overwrite")
        tools_called.append("gold_write:lot_defect_rates")
    except Exception as e:
        tools_called.append(f"lot_defect_rates:FAILED:{e}")


def _build_alert(batch_id, supplier_id, line_id, obs_rate, units_flagged,
                 row, zscore, status, risk_tier, reasoning, tools_called, run_id):
    return {
        "batch_id":             batch_id,
        "supplier_id":          supplier_id,
        "line_id":              line_id,
        "observed_defect_rate": round(obs_rate, 6),
        "units_flagged":        units_flagged,
        "anomaly_score":        round(float(zscore), 4) if zscore is not None else None,
        "alert_status":         status,
        "supplier_risk_tier":   risk_tier,
        "defect_types":         json.dumps(list(row.get("defect_types", []))),
        "fail_inspections":     int(row.get("fail_count", 0)),
        "material_lot":         str(row.get("material_lot", "")),
        "production_date":      str(row.get("production_date", "")),
        "reasoning_chain":      reasoning,
        "tools_called":         json.dumps(tools_called),
        "run_id":               run_id,
        "alerted_at":           _now(),
    }
