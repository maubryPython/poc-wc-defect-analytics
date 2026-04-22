"""
A-02 — Defect Detection Agent
==============================
Observe: newly written Silver QC inspection data
Reason:  compare batch defect rate against supplier-specific historical baseline
         AGENTIC: borderline cases (1.5–2.0 SD) trigger secondary material_lot_query
Act:     write flagged batches to gold.defect_alerts with full reasoning chain
Escalate: if no historical baseline exists for a supplier (new supplier / missing data)
"""

import uuid
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from tools.db_tools import (
    silver_read, gold_write, escalation_write,
    baseline_lookup, material_lot_query, qc_query
)
from tools.validation_tools import config_read

# Isolation Forest (available in Databricks runtime)
from sklearn.ensemble import IsolationForest


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _zscore(rate: float, mean: float, std: float) -> float:
    """Signed Z-score: how many SDs above the supplier mean is this rate?"""
    if std == 0:
        return 0.0
    return (rate - mean) / std


def run(task_payload: dict) -> dict:
    """
    Agent entry point.
    task_payload = {
      "run_id": str,
      "batch_ids": list[str] | None   # None = process all new batches this run
    }
    """
    run_id    = task_payload.get("run_id", str(uuid.uuid4()))
    batch_ids = task_payload.get("batch_ids")

    # ── Read config thresholds ───────────────────────────────────────────────
    cfg = config_read()["config"].get("detection", {})
    FLAG_THRESH       = cfg.get("flag_threshold",   2.0)
    BORDERLINE_LOW    = cfg.get("borderline_low",   1.5)
    MIN_UNITS         = cfg.get("min_units_inspected", 50)
    tools_called      = ["config_read"]

    # ── Observe: load QC data ────────────────────────────────────────────────
    qc_result = silver_read("fact_qc_inspections")
    tools_called.append("silver_read")
    if qc_result["status"] != "ok":
        return {"agent_id": "A-02", "run_id": run_id, "status": "FAILED",
                "message": "Could not read fact_qc_inspections"}

    qc_df = qc_result["data"].toPandas()

    # Apply batch_id filter if provided by Orchestrator
    if batch_ids:
        qc_df = qc_df[qc_df["batch_id"].isin(batch_ids)]

    # Only process batches with adequate sample size
    qc_df = qc_df[qc_df["units_inspected"] >= MIN_UNITS]

    # Aggregate to batch level: mean defect rate across inspections
    batch_agg = (
        qc_df.groupby(["batch_id", "supplier_id", "line_id"])
        .agg(
            avg_defect_count=("defect_count", "mean"),
            avg_units_inspected=("units_inspected", "mean"),
            inspection_count=("inspection_id", "count"),
            fail_count=("pass_fail", lambda x: (x == "FAIL").sum()),
            defect_types=("defect_type", lambda x: list(x.unique())),
        )
        .reset_index()
    )
    batch_agg["observed_defect_rate"] = (
        batch_agg["avg_defect_count"] / batch_agg["avg_units_inspected"]
    )

    # Also join unit_count from dim_batch
    batch_meta = silver_read("dim_batch")["data"].toPandas()
    tools_called.append("silver_read")
    batch_agg = batch_agg.merge(
        batch_meta[["batch_id", "unit_count", "material_lot", "production_date"]],
        on="batch_id", how="left"
    )

    # ── Isolation Forest: global anomaly pre-screen ──────────────────────────
    if len(batch_agg) >= 5:
        iso = IsolationForest(contamination=0.1, random_state=42)
        iso_scores = iso.fit_predict(
            batch_agg[["observed_defect_rate", "fail_count"]].fillna(0)
        )
        batch_agg["iso_outlier"] = iso_scores == -1   # -1 = anomaly
    else:
        batch_agg["iso_outlier"] = False

    # ── Reason & Act: per-batch evaluation ──────────────────────────────────
    alert_records = []
    escalations   = 0

    for _, row in batch_agg.iterrows():
        batch_id    = row["batch_id"]
        supplier_id = row["supplier_id"]
        line_id     = row["line_id"]
        obs_rate    = float(row["observed_defect_rate"])
        units_flagged = int(row["unit_count"]) if pd.notna(row["unit_count"]) else 0
        batch_tools = []

        # ── Tool: baseline_lookup (always called — AC-10) ─────────────
        bl = baseline_lookup(supplier_id, line_id)
        batch_tools.append("baseline_lookup")
        tools_called.append("baseline_lookup")

        if bl["status"] == "not_found":
            # AGENTIC: no baseline → can't score → UNRESOLVED + escalate (AC-13)
            escalation_write(
                agent_id="A-02", run_id=run_id,
                question=(
                    f"Batch {batch_id} (supplier {supplier_id}, line {line_id}) "
                    f"has no historical defect baseline on record. "
                    f"Cannot apply supplier-specific threshold. "
                    f"Recommend: collect at least 3 months of QC data for this supplier."
                ),
                context={
                    "batch_id": batch_id, "supplier_id": supplier_id,
                    "line_id": line_id, "observed_rate": obs_rate,
                },
                recovery_attempted=False,
            )
            batch_tools.append("escalate_to_human")
            tools_called.append("escalate_to_human")
            alert_records.append(_build_alert(
                batch_id, supplier_id, line_id, obs_rate, units_flagged,
                row, zscore=None, status="UNRESOLVED",
                reasoning="No historical baseline available for this supplier/line.",
                tools_called=batch_tools, run_id=run_id,
            ))
            escalations += 1
            continue

        mean_rate = bl["mean_rate"]
        std_rate  = bl["std_rate"]
        zscore    = _zscore(obs_rate, mean_rate, std_rate)
        iso_flag  = bool(row["iso_outlier"])

        # ── AGENTIC: Conditional routing ─────────────────────────────
        if zscore >= FLAG_THRESH or (iso_flag and zscore >= BORDERLINE_LOW):
            # Clear anomaly — flag immediately
            reasoning = (
                f"Defect rate {obs_rate:.4f} is {zscore:.1f} SD above supplier "
                f"baseline (mean={mean_rate:.4f}, SD={std_rate:.4f}). "
                f"Isolation Forest: {'outlier' if iso_flag else 'normal'}."
            )
            status = "FLAGGED"

        elif BORDERLINE_LOW <= zscore < FLAG_THRESH:
            # AGENTIC: Borderline → secondary material lot lookup (AC-11)
            ml = material_lot_query(str(row["material_lot"]))
            batch_tools.append("material_lot_query")
            tools_called.append("material_lot_query")

            lot_batches = ml.get("batches", [])
            lot_fail_rates = []
            for lb in lot_batches:
                lb_qc = qc_df[qc_df["batch_id"] == lb["batch_id"]]
                if len(lb_qc):
                    lr = lb_qc["defect_count"].mean() / max(lb_qc["units_inspected"].mean(), 1)
                    lot_fail_rates.append(lr)

            lot_avg = float(np.mean(lot_fail_rates)) if lot_fail_rates else obs_rate

            if lot_avg >= mean_rate + BORDERLINE_LOW * std_rate:
                # Lot-level corroboration → upgrade to FLAGGED
                reasoning = (
                    f"Borderline Z-score {zscore:.1f}. "
                    f"Secondary material_lot_query for lot '{row['material_lot']}' "
                    f"found {len(lot_batches)} batches with avg defect rate {lot_avg:.4f} "
                    f"(also elevated). Upgraded to FLAGGED."
                )
                status = "FLAGGED"
            else:
                reasoning = (
                    f"Borderline Z-score {zscore:.1f}. "
                    f"Secondary material_lot_query for lot '{row['material_lot']}' "
                    f"showed normal lot-level rates (avg {lot_avg:.4f}). "
                    f"Classified as REVIEW (monitor next cycle)."
                )
                status = "REVIEW"
        else:
            # Normal range
            reasoning = (
                f"Defect rate {obs_rate:.4f} within {zscore:.1f} SD of supplier baseline. "
                f"No anomaly detected."
            )
            status = "NORMAL"

        alert_records.append(_build_alert(
            batch_id, supplier_id, line_id, obs_rate, units_flagged,
            row, zscore=zscore, status=status,
            reasoning=reasoning, tools_called=batch_tools, run_id=run_id,
        ))

    # ── Act: write all alert records to Gold ─────────────────────────────────
    if alert_records:
        spark = SparkSession.getActiveSession()
        alert_df = spark.createDataFrame(
            [{k: v for k, v in r.items() if k != "_tools"} for r in alert_records]
        )
        gold_write(alert_df, "defect_alerts", run_id)
        tools_called.append("gold_write")

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
        "tools_called": list(dict.fromkeys(tools_called)),
        "flagged_batch_ids": [r["batch_id"] for r in alert_records if r["alert_status"] == "FLAGGED"],
    }


def _build_alert(batch_id, supplier_id, line_id, obs_rate, units_flagged,
                 row, zscore, status, reasoning, tools_called, run_id) -> dict:
    """Construct a complete alert record including the reasoning chain (AC-12)."""
    return {
        "batch_id":          batch_id,
        "supplier_id":       supplier_id,
        "line_id":           line_id,
        "observed_defect_rate": round(obs_rate, 6),
        "units_flagged":     units_flagged,
        "anomaly_score":     round(float(zscore), 4) if zscore is not None else None,
        "alert_status":      status,    # FLAGGED | REVIEW | NORMAL | UNRESOLVED
        "defect_types":      json.dumps(list(row.get("defect_types", []))),
        "fail_inspections":  int(row.get("fail_count", 0)),
        "material_lot":      str(row.get("material_lot", "")),
        "production_date":   str(row.get("production_date", "")),
        # AC-12: full reasoning chain — not just a flag
        "reasoning_chain":   reasoning,
        "tools_called":      json.dumps(tools_called),
        "run_id":            run_id,
        "alerted_at":        _now(),
    }
