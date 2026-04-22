"""
A-03 — Root Cause Analysis (RCA) Agent
========================================
Observe: flagged batch IDs from gold.defect_alerts
Reason:  source-selection by defect type → evidence bundle → LLM reasoning → hypotheses
Act:     write gold.rca_findings (existing) + gold.lot_defect_traceability (NEW)
Escalate: top confidence < 50% → INSUFFICIENT_EVIDENCE

Prevention story output:
  gold.lot_defect_traceability → Chart 3 (material lot traceability)
    - lot_id, supplier_id, mean_defect_rate, batch_count, entered_production_date,
      hours_to_first_alert, is_defective, defect_root_cause
"""

import uuid
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tools.db_tools import (
    gold_read, gold_write, escalation_write, silver_read,
    baseline_lookup, supplier_history_query,
    material_lot_query, production_conditions_query, qc_query,
    lot_traceability_query,
)
from tools.llm_tools import llm_reason
from tools.validation_tools import config_read


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


DEFECT_SOURCE_MAP = {
    "seam_bulge":            ["production_conditions","material_lot","supplier_history"],
    "heat_transfer_failure": ["material_lot","supplier_history","production_conditions"],
    "material_deformation":  ["supplier_history","material_lot"],
    "minor_thread":          ["production_conditions"],
    "minor_color_variation": ["supplier_history"],
    "none":                  [],
    "default":               ["supplier_history","material_lot"],
}


def _select_sources(defect_types: list, max_sources: int) -> list:
    sources = []
    for dt in defect_types:
        for s in DEFECT_SOURCE_MAP.get(dt, DEFECT_SOURCE_MAP["default"]):
            if s not in sources:
                sources.append(s)
    if not sources:
        sources = DEFECT_SOURCE_MAP["default"]
    return sources[:max_sources]


def _gather_evidence(batch_id, supplier_id, line_id, material_lot,
                     production_date, sources, tools_called) -> list:
    evidence = []
    month = str(production_date)[:7] if production_date else None

    for source in sources:
        if source == "supplier_history":
            result = supplier_history_query(supplier_id)
            tools_called.append("supplier_history_query")
            if result["status"] == "ok":
                months = result["months"]
                recent = months[-6:] if len(months) >= 6 else months
                rates  = [m["defect_rate"] for m in recent]
                trend  = "increasing" if len(rates) > 1 and rates[-1] > rates[0] else "stable"
                evidence.append({
                    "source": "supplier_history_query",
                    "supplier_id": supplier_id,
                    "months_on_record": len(months),
                    "recent_avg_defect_rate": round(sum(rates)/len(rates), 5) if rates else None,
                    "trend": trend,
                    "max_historical_rate": max(rates) if rates else None,
                })

        elif source == "material_lot":
            result = material_lot_query(str(material_lot))
            tools_called.append("material_lot_query")
            if result["status"] == "ok":
                evidence.append({
                    "source": "material_lot_query",
                    "lot_id": material_lot,
                    "batches_using_lot": len(result["batches"]),
                    "other_batches": [b["batch_id"] for b in result["batches"]
                                      if b["batch_id"] \!= batch_id],
                })

        elif source == "production_conditions":
            if month:
                result = production_conditions_query(line_id, month)
                tools_called.append("production_conditions_query")
                if result["status"] == "ok" and result["data"]:
                    d = result["data"][0]
                    evidence.append({
                        "source": "production_conditions_query",
                        "line_id": line_id, "month": month,
                        "batches_produced": d.get("batches_produced"),
                        "avg_defects_per_inspection": d.get("avg_defects_per_inspection"),
                        "fail_inspections": d.get("fail_inspections"),
                    })

    return evidence


def run(task_payload: dict) -> dict:
    run_id           = task_payload.get("run_id", str(uuid.uuid4()))
    flagged_batch_ids = task_payload.get("flagged_batch_ids", [])

    cfg = config_read()["config"].get("rca", {})
    CONFIDENCE_THRESHOLD = cfg.get("min_confidence",   0.50)
    MAX_SOURCES          = cfg.get("max_sources",       3)
    tools_called         = ["config_read"]

    spark = SparkSession.getActiveSession()

    # ── Read flagged batches from gold.defect_alerts ─────────────────────────
    alerts_result = gold_read("defect_alerts", "alert_status = 'FLAGGED'")
    tools_called.append("gold_read:defect_alerts")
    if alerts_result["status"] \!= "ok":
        return {"agent_id": "A-03", "run_id": run_id, "status": "FAILED",
                "message": "Could not read gold.defect_alerts"}

    alerts_df = alerts_result["data"].toPandas()
    if flagged_batch_ids:
        alerts_df = alerts_df[alerts_df["batch_id"].isin(flagged_batch_ids)]

    # Also load lot-level defect rates computed by A-02
    lot_rates_result = gold_read("lot_defect_rates")
    tools_called.append("gold_read:lot_defect_rates")
    lot_rates_map = {}
    if lot_rates_result["status"] == "ok":
        for r in lot_rates_result["data"].toPandas().to_dict("records"):
            lot_rates_map[r["lot_id"]] = r

    # Also load material_lots metadata from Silver
    lots_meta_result = silver_read("material_lots")
    tools_called.append("silver_read:material_lots")
    lots_meta_map = {}
    if lots_meta_result["status"] == "ok":
        for r in lots_meta_result["data"].toPandas().to_dict("records"):
            lots_meta_map[r["lot_id"]] = r

    # ── Per-batch RCA ────────────────────────────────────────────────────────
    rca_records   = []
    traceability  = {}  # lot_id → aggregated traceability record
    escalations   = 0

    for _, alert in alerts_df.iterrows():
        batch_id    = alert["batch_id"]
        supplier_id = alert["supplier_id"]
        line_id     = alert["line_id"]
        material_lot = alert.get("material_lot", "")
        production_date = alert.get("production_date", "")
        batch_tools = []

        try:
            defect_types_raw = alert.get("defect_types", "[]")
            defect_types = json.loads(defect_types_raw) if isinstance(defect_types_raw, str) else defect_types_raw
        except Exception:
            defect_types = []

        # AGENTIC: select data sources based on defect type
        sources = _select_sources(defect_types, MAX_SOURCES)
        evidence = _gather_evidence(batch_id, supplier_id, line_id,
                                    material_lot, production_date,
                                    sources, batch_tools)
        tools_called.extend(batch_tools)

        # Enrich evidence with lot-level rate
        lot_rate_info = lot_rates_map.get(material_lot, {})
        if lot_rate_info:
            evidence.append({
                "source": "lot_defect_rates",
                "lot_id": material_lot,
                "mean_defect_rate": lot_rate_info.get("mean_defect_rate"),
                "batch_count_on_lot": lot_rate_info.get("batch_count"),
                "above_ai_threshold": lot_rate_info.get("above_ai_threshold"),
            })

        # LLM reasoning
        rca_result = llm_reason(
            batch_id=batch_id,
            supplier_id=supplier_id,
            defect_types=defect_types,
            evidence=evidence,
            anomaly_score=float(alert.get("anomaly_score", 0) or 0),
        )
        tools_called.append("llm_reason")

        hypotheses    = rca_result.get("hypotheses", [])
        top           = hypotheses[0] if hypotheses else {}
        top_conf      = float(top.get("confidence_score", 0))
        top_hypothesis = top.get("hypothesis", "")
        rca_status    = "COMPLETE" if top_conf >= CONFIDENCE_THRESHOLD else "COMPLETE_LOW_CONFIDENCE"

        if top_conf < CONFIDENCE_THRESHOLD:
            escalation_write(
                agent_id="A-03", run_id=run_id,
                question=(f"RCA for batch {batch_id} has low confidence ({top_conf:.0%}). "
                          f"Manual review required."),
                context={"batch_id": batch_id, "supplier_id": supplier_id,
                         "top_confidence": top_conf, "hypotheses": hypotheses},
                recovery_attempted=True,
            )
            batch_tools.append("escalate_to_human")
            tools_called.append("escalate_to_human")
            escalations += 1

        # Build RCA record
        rca_records.append({
            "batch_id":         batch_id,
            "supplier_id":      supplier_id,
            "line_id":          line_id,
            "material_lot":     material_lot,
            "rca_status":       rca_status,
            "top_hypothesis":   top_hypothesis,
            "top_confidence":   round(top_conf, 4),
            "overall_confidence": round(
                sum(h.get("confidence_score",0) for h in hypotheses) / max(len(hypotheses),1), 4
            ),
            "hypotheses_json":  json.dumps(hypotheses),
            "evidence_sources": json.dumps(sources),
            "data_gaps":        json.dumps(rca_result.get("data_gaps", [])),
            "confidence_tier":  "HIGH" if top_conf >= 0.75 else "MEDIUM" if top_conf >= 0.60 else "LOW",
            "model_used":       rca_result.get("model_used", "claude-sonnet-4-6"),
            "tools_called":     json.dumps(batch_tools),
            "run_id":           run_id,
            "analyzed_at":      _now(),
        })

        # ── Build lot traceability record ──────────────────────────────────
        if material_lot and material_lot not in traceability:
            lot_meta = lots_meta_map.get(material_lot, {})
            traceability[material_lot] = {
                "lot_id":                  material_lot,
                "supplier_id":             supplier_id,
                "material_type":           lot_meta.get("material_type", ""),
                "entered_production_date": lot_meta.get("entered_production_date", ""),
                "is_defective":            lot_meta.get("is_defective", False),
                "defect_root_cause":       lot_meta.get("defect_root_cause") or top_hypothesis,
                "hours_to_first_alert":    lot_meta.get("hours_to_first_alert"),
                "mean_defect_rate":        lot_rate_info.get("mean_defect_rate", float(alert.get("observed_defect_rate",0))),
                "batch_count":             lot_rate_info.get("batch_count", 1),
                "batches_flagged":         1,
                "rca_top_confidence":      round(top_conf, 4),
                "rca_status":              rca_status,
                "run_id":                  run_id,
                "traced_at":               _now(),
            }
        elif material_lot in traceability:
            traceability[material_lot]["batches_flagged"] += 1

    # ── Write gold.rca_findings ──────────────────────────────────────────────
    if rca_records:
        rca_df = spark.createDataFrame(rca_records)
        gold_write(rca_df, "rca_findings", run_id)
        tools_called.append("gold_write:rca_findings")

    # ── Write gold.lot_defect_traceability (NEW — Chart 3) ──────────────────
    if traceability:
        lot_df = spark.createDataFrame(list(traceability.values()))
        gold_write(lot_df, "lot_defect_traceability", run_id, mode="overwrite")
        tools_called.append("gold_write:lot_defect_traceability")

    complete = sum(1 for r in rca_records if r["rca_status"] == "COMPLETE")
    top_conf_max = max((r["top_confidence"] for r in rca_records), default=0)

    return {
        "agent_id":           "A-03",
        "run_id":             run_id,
        "status":             "COMPLETE" if escalations == 0 else "PARTIAL",
        "batches_investigated": len(rca_records),
        "rca_complete":       complete,
        "rca_low_confidence": len(rca_records) - complete,
        "escalations":        escalations,
        "lots_traced":        len(traceability),
        "max_top_confidence": round(top_conf_max, 4),
        "rca_status":         "COMPLETE" if top_conf_max >= 0.5 else "INSUFFICIENT_EVIDENCE",
        "tools_called":       list(dict.fromkeys(tools_called)),
    }
