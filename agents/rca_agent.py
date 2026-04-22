"""
A-03 — Root Cause Analysis (RCA) Agent
========================================
Observe: flagged batch IDs from gold.defect_alerts
Reason:  select data sources based on defect type (not all queried every run)
         accumulate evidence → LLM reasoning → ranked hypotheses
Act:     write to gold.rca_findings
Escalate: if all hypothesis confidence < 50% → INSUFFICIENT_EVIDENCE (AC-18)
Auto-trigger: if top confidence > 70% → signal Orchestrator to run Summary Agent (AC-17)

POC-1: llm_reason calls HuggingFace Inference API
"""

import uuid
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from tools.db_tools import (
    gold_read, gold_write, escalation_write,
    baseline_lookup, supplier_history_query,
    material_lot_query, production_conditions_query,
    qc_query
)
from tools.llm_tools import llm_reason
from tools.validation_tools import config_read


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Defect-type → data source routing ────────────────────────────────────────
# AGENTIC (AC-15): agent selects sources based on defect signature
DEFECT_SOURCE_MAP = {
    "seam_bulge":           ["production_conditions", "material_lot", "supplier_history"],
    "heat_transfer_failure":["material_lot", "supplier_history", "production_conditions"],
    "material_deformation": ["supplier_history", "material_lot"],
    "minor_thread":         ["production_conditions"],
    "minor_color_variation":["supplier_history"],
    "none":                 [],
    "default":              ["supplier_history", "material_lot"],
}


def _select_sources(defect_types: list, max_sources: int) -> list:
    """
    AGENTIC DECISION: choose which data sources to query based on defect type.
    Returns an ordered list of source names (capped at max_sources).
    """
    sources = []
    for dt in defect_types:
        for s in DEFECT_SOURCE_MAP.get(dt, DEFECT_SOURCE_MAP["default"]):
            if s not in sources:
                sources.append(s)
    if not sources:
        sources = DEFECT_SOURCE_MAP["default"]
    return sources[:max_sources]


def _gather_evidence(batch_id: str, supplier_id: str, line_id: str,
                     material_lot: str, production_date: str,
                     sources: list, tools_called: list) -> list:
    """Query each selected source and return structured evidence bundle."""
    evidence = []
    month = str(production_date)[:7] if production_date else None

    for source in sources:
        if source == "supplier_history":
            result = supplier_history_query(supplier_id)
            tools_called.append("supplier_history_query")
            if result["status"] == "ok":
                months = result["months"]
                # Summarise trend rather than dumping all rows
                recent = months[-6:] if len(months) >= 6 else months
                rates  = [m["defect_rate"] for m in recent]
                trend  = "increasing" if len(rates) > 1 and rates[-1] > rates[0] else "stable"
                evidence.append({
                    "source": "supplier_history_query",
                    "relevance": "high",
                    "data": {
                        "supplier_id": supplier_id,
                        "recent_monthly_rates": rates,
                        "trend": trend,
                        "months_on_record": len(months),
                    },
                })

        elif source == "material_lot":
            result = material_lot_query(material_lot)
            tools_called.append("material_lot_query")
            if result["status"] == "ok":
                lot_batches = result["batches"]
                evidence.append({
                    "source": "material_lot_query",
                    "relevance": "high",
                    "data": {
                        "lot_id": material_lot,
                        "batches_in_lot": len(lot_batches),
                        "suppliers_in_lot": list({b["supplier_id"] for b in lot_batches}),
                        "lines_in_lot":     list({b["line_id"] for b in lot_batches}),
                    },
                })

        elif source == "production_conditions":
            if month:
                result = production_conditions_query(line_id, month)
                tools_called.append("production_conditions_query")
                if result["status"] == "ok" and result.get("data"):
                    evidence.append({
                        "source": "production_conditions_query",
                        "relevance": "medium",
                        "data": result["data"][0] if result["data"] else {},
                    })

    # Always add QC detail for the specific batch
    qc_result = qc_query(batch_id=batch_id)
    tools_called.append("qc_query")
    if qc_result["status"] == "ok":
        rows = qc_result["rows"]
        evidence.insert(0, {
            "source": "qc_query",
            "relevance": "critical",
            "data": {
                "batch_id": batch_id,
                "inspection_count": len(rows),
                "fail_count": sum(1 for r in rows if r.get("pass_fail") == "FAIL"),
                "avg_defect_count": round(
                    sum(r.get("defect_count", 0) for r in rows) / max(len(rows), 1), 1
                ),
                "defect_types_seen": list({r.get("defect_type") for r in rows}),
            },
        })

    return evidence


def run(task_payload: dict) -> dict:
    """
    Agent entry point.
    task_payload = {
      "run_id": str,
      "flagged_batch_ids": list[str]
    }
    """
    run_id           = task_payload.get("run_id", str(uuid.uuid4()))
    flagged_batch_ids = task_payload.get("flagged_batch_ids", [])

    cfg = config_read()["config"].get("rca", {})
    CONF_THRESHOLD  = cfg.get("confidence_threshold",    0.70)
    INSUFF_THRESH   = cfg.get("insufficient_threshold",  0.50)
    MAX_SOURCES     = cfg.get("max_sources_to_query",    3)
    LLM_MODEL       = cfg.get("llm_model",              "mistralai/Mistral-7B-Instruct-v0.2")
    LLM_MAX_TOKENS  = cfg.get("llm_max_tokens",         512)

    tools_called = ["config_read"]

    if not flagged_batch_ids:
        return {"agent_id": "A-03", "run_id": run_id, "status": "NO_ALERTS",
                "message": "No flagged batch IDs provided."}

    # ── Load alert records for context ───────────────────────────────────────
    alerts_result = gold_read("defect_alerts", f"alert_status = 'FLAGGED' AND run_id = '{run_id}'")
    tools_called.append("gold_read")
    if alerts_result["status"] != "ok":
        return {"agent_id": "A-03", "run_id": run_id, "status": "FAILED",
                "message": "Could not read defect_alerts"}

    alerts_df = alerts_result["data"].toPandas()
    alerts_df = alerts_df[alerts_df["batch_id"].isin(flagged_batch_ids)]

    # Load batch metadata
    from tools.db_tools import silver_read
    batch_meta = silver_read("dim_batch")["data"].toPandas()
    tools_called.append("silver_read")

    rca_records = []
    overall_rca_status = "COMPLETE"
    max_top_confidence = 0.0

    for _, alert in alerts_df.iterrows():
        batch_id    = alert["batch_id"]
        supplier_id = alert["supplier_id"]
        line_id     = alert["line_id"]
        batch_tools = list(tools_called)

        meta_row = batch_meta[batch_meta["batch_id"] == batch_id]
        material_lot   = str(meta_row["material_lot"].values[0]) if len(meta_row) else "UNKNOWN"
        production_date = str(meta_row["production_date"].values[0]) if len(meta_row) else None

        # Parse defect types from alert
        try:
            defect_types = json.loads(alert.get("defect_types", "[]"))
        except (json.JSONDecodeError, TypeError):
            defect_types = ["default"]

        # ── AGENTIC: select data sources based on defect type (AC-15) ────
        sources = _select_sources(defect_types, MAX_SOURCES)
        batch_tools.append(f"source_selection({sources})")

        # ── Gather evidence from selected sources ─────────────────────────
        evidence = _gather_evidence(
            batch_id, supplier_id, line_id,
            material_lot, production_date, sources, batch_tools
        )

        # ── Tool: llm_reason (AC-16) ─────────────────────────────────────
        llm_result = llm_reason(evidence, model=LLM_MODEL, max_tokens=LLM_MAX_TOKENS)
        batch_tools.append("llm_reason")

        hypotheses         = llm_result.get("hypotheses", [])
        overall_confidence = llm_result.get("overall_confidence", 0.0)
        data_gaps          = llm_result.get("data_gaps", [])

        # ── AGENTIC: Confidence gating (AC-17, AC-18) ────────────────────
        if not hypotheses or overall_confidence < INSUFF_THRESH:
            rca_status = "INSUFFICIENT_EVIDENCE"
            overall_rca_status = "ESCALATED"

            escalation_write(
                agent_id="A-03", run_id=run_id,
                question=(
                    f"RCA for batch {batch_id} (supplier {supplier_id}) produced no hypothesis "
                    f"above the confidence threshold ({INSUFF_THRESH:.0%}). "
                    f"Confidence achieved: {overall_confidence:.0%}. "
                    f"Data gaps identified: {data_gaps}. "
                    f"Recommended: collect additional QC records for this supplier's L2/L3 lines."
                ),
                context={
                    "batch_id": batch_id, "supplier_id": supplier_id,
                    "hypotheses_attempted": len(hypotheses),
                    "confidence_scores": [h.get("confidence_score") for h in hypotheses],
                    "data_gaps": data_gaps,
                    "evidence_sources_queried": sources,
                },
                recovery_attempted=True,
            )
            batch_tools.append("escalate_to_human")

        elif overall_confidence >= CONF_THRESHOLD:
            # High confidence → Summary Agent can auto-trigger (AC-17)
            rca_status = "COMPLETE"
            max_top_confidence = max(max_top_confidence, overall_confidence)
        else:
            rca_status = "COMPLETE_LOW_CONFIDENCE"
            max_top_confidence = max(max_top_confidence, overall_confidence)

        # Write findings to Gold
        top = hypotheses[0] if hypotheses else {}
        record = {
            "batch_id":              batch_id,
            "supplier_id":           supplier_id,
            "line_id":               line_id,
            "rca_status":            rca_status,
            "top_hypothesis":        top.get("hypothesis", ""),
            "top_confidence":        float(top.get("confidence_score", 0.0)),
            "overall_confidence":    float(overall_confidence),
            "hypotheses_json":       json.dumps(hypotheses),
            "evidence_sources":      json.dumps(sources),
            "data_gaps":             json.dumps(data_gaps),
            "tools_called":          json.dumps(batch_tools),
            "run_id":                run_id,
            "analyzed_at":           _now(),
        }
        rca_records.append(record)

    # ── Write all RCA records ─────────────────────────────────────────────────
    if rca_records:
        spark = SparkSession.getActiveSession()
        rca_df = spark.createDataFrame(rca_records)
        gold_write(rca_df, "rca_findings", run_id)
        tools_called.append("gold_write")

    complete_count = sum(1 for r in rca_records if "COMPLETE" in r["rca_status"])
    escalated_count = sum(1 for r in rca_records if r["rca_status"] == "INSUFFICIENT_EVIDENCE")

    return {
        "agent_id":          "A-03",
        "run_id":            run_id,
        "status":            overall_rca_status,
        "batches_analyzed":  len(rca_records),
        "complete":          complete_count,
        "escalated":         escalated_count,
        "max_top_confidence": round(max_top_confidence, 3),
        # AC-17: signal for Orchestrator → trigger Summary Agent if confidence is high enough
        "trigger_summary":   max_top_confidence >= CONF_THRESHOLD,
        "tools_called":      list(dict.fromkeys(tools_called)),
    }
