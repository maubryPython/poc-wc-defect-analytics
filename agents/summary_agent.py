"""
A-05 — Executive Summary Agent
================================
Observe: gold.rca_findings + gold.scenario_projections
Reason:  severity check → proactive trigger decision (AC-24)
         llm_draft → fact_check every numeric claim (AC-25)
         disclose uncertainty if RCA was INSUFFICIENT_EVIDENCE (AC-26)
Act:     write validated briefing to gold.executive_briefings + CSV export

POC-1: llm_draft calls HuggingFace Inference API
POC-3: exports CSV for Tableau Public (no live JDBC connection)
"""

import uuid
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from tools.db_tools import (
    defect_scope_query, rca_findings_query,
    scenario_summary_query, gold_write, escalation_write
)
from tools.llm_tools import llm_draft, fact_check
from tools.validation_tools import config_read

EXPORT_DIR = Path(__file__).parent.parent / "dashboard" / "exports"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _severity_check(scope: dict, cfg: dict) -> bool:
    """
    Tool: severity_check
    AGENTIC (AC-24): decide whether to proactively publish a briefing
    based on defect scope, without waiting for a manual request.
    """
    unit_thresh = cfg.get("severity_unit_threshold", 5000)
    rate_thresh = cfg.get("severity_rate_threshold", 0.03)
    units = scope.get("total_units_flagged", 0)
    rate  = scope.get("avg_anomaly_score", 0)
    return units >= unit_thresh or rate >= rate_thresh


def run(task_payload: dict) -> dict:
    """
    task_payload = {
      "run_id": str,
      "rca_status": str,          # from RCA Agent output
      "max_top_confidence": float  # from RCA Agent output
    }
    """
    run_id          = task_payload.get("run_id", str(uuid.uuid4()))
    rca_status      = task_payload.get("rca_status", "COMPLETE")
    top_confidence  = task_payload.get("max_top_confidence", 0.0)

    cfg         = config_read()["config"].get("summary", {})
    MAX_WORDS   = cfg.get("max_briefing_words", 300)
    LLM_MODEL   = cfg.get("llm_model", "mistralai/Mistral-7B-Instruct-v0.2")
    LLM_TOKENS  = cfg.get("llm_max_tokens", 768)
    tools_called = ["config_read"]

    # ── Observe: defect scope ─────────────────────────────────────────────────
    scope = defect_scope_query(run_id=run_id)
    tools_called.append("defect_scope_query")

    # ── AGENTIC: proactive trigger check (AC-24) ──────────────────────────────
    should_publish = _severity_check(scope, cfg)
    tools_called.append("severity_check")
    if not should_publish:
        return {
            "agent_id": "A-05", "run_id": run_id,
            "status": "SKIPPED",
            "reason": (
                f"Severity thresholds not met "
                f"(units={scope.get('total_units_flagged', 0)}, "
                f"rate={scope.get('avg_anomaly_score', 0):.3f}). "
                "Briefing will publish on next run if scope increases."
            ),
        }

    # ── Observe: RCA findings ─────────────────────────────────────────────────
    rca_result = rca_findings_query(run_id=run_id)
    tools_called.append("rca_findings_query")
    findings = rca_result.get("findings", [])
    top_finding = findings[0] if findings else {}

    # ── Observe: scenario projections ────────────────────────────────────────
    scen_result = scenario_summary_query(run_id=run_id)
    tools_called.append("scenario_summary_query")
    recommended_scenario = scen_result.get("recommended") or {}

    # ── Build gold_data for fact-check (AC-25) ───────────────────────────────
    gold_data = {
        "total_units_flagged": scope.get("total_units_flagged", 0),
        "flagged_batches":     scope.get("flagged_batches", 0),
        "affected_suppliers":  len(scope.get("affected_suppliers", [])),
        "rca_confidence_pct":  round(top_confidence * 100, 1),
        "recommended_cost_usd": recommended_scenario.get("mean_total_cost_usd", 0),
        "recommended_timeline_days": recommended_scenario.get("mean_timeline_days", 0),
        "recommended_returns_volume": recommended_scenario.get("mean_returns_volume", 0),
    }

    # ── AC-26: handle INSUFFICIENT_EVIDENCE from RCA ──────────────────────────
    rca_disclosure = ""
    if rca_status in ("ESCALATED", "INSUFFICIENT_EVIDENCE"):
        rca_disclosure = (
            f"⚠ Root cause analysis is inconclusive at this time. "
            f"Confidence achieved: {top_confidence:.0%}. "
            f"Analyst review has been requested."
        )

    # ── Tool: llm_draft (POC-1: HuggingFace API) ─────────────────────────────
    draft_result = llm_draft(
        scope=scope,
        rca=top_finding,
        scenario=recommended_scenario,
        confidence=top_confidence,
        max_words=MAX_WORDS,
        model=LLM_MODEL,
        max_tokens=LLM_TOKENS,
    )
    tools_called.append("llm_draft")
    draft_text = draft_result.get("draft", "")

    # Append uncertainty disclosure if RCA was inconclusive
    if rca_disclosure:
        draft_text = draft_text.rstrip() + "\n\n" + rca_disclosure

    # ── Tool: fact_check — verify every numeric claim (AC-25) ────────────────
    fc_result = fact_check(draft_text, gold_data)
    tools_called.append("fact_check")

    final_draft = fc_result["revised_draft"]
    unverified  = fc_result["unverified_claims"]

    # Self-correction: if unverified claims remain, append source table disclosure
    if unverified:
        final_draft += (
            "\n\n[Note: The following figures could not be automatically verified "
            f"against Gold table values and are marked for review: {unverified}. "
            "Please cross-check before sharing externally.]"
        )
        tools_called.append("self_correction")

    # ── Act: write to gold.executive_briefings ────────────────────────────────
    spark = SparkSession.getActiveSession()
    record = spark.createDataFrame([{
        "briefing_id":          str(uuid.uuid4()),
        "run_id":               run_id,
        "published_at":         _now(),
        "briefing_text":        final_draft,
        "word_count":           len(final_draft.split()),
        "rca_confidence":       float(top_confidence),
        "rca_status":           rca_status,
        "recommended_scenario": recommended_scenario.get("scenario_name", ""),
        "units_flagged":        scope.get("total_units_flagged", 0),
        "flagged_batches":      scope.get("flagged_batches", 0),
        "verified_claims":      json.dumps(fc_result["verified_claims"]),
        "unverified_claims":    json.dumps(unverified),
        "all_claims_verified":  fc_result["all_verified"],
        "data_sources_json":    json.dumps({k: str(v) for k, v in gold_data.items()}),
        "tools_called":         json.dumps(tools_called),
    }])
    gold_write(record, "executive_briefings", run_id, mode="append")
    tools_called.append("gold_write")

    # ── POC-3: Export to CSV for Tableau Public ───────────────────────────────
    _export_dashboard_csvs(run_id, spark)
    tools_called.append("csv_export")

    return {
        "agent_id":          "A-05",
        "run_id":            run_id,
        "status":            "COMPLETE",
        "briefing_published": True,
        "word_count":         len(final_draft.split()),
        "all_claims_verified": fc_result["all_verified"],
        "unverified_count":   len(unverified),
        "rca_disclosure_included": bool(rca_disclosure),
        "tools_called":      list(dict.fromkeys(tools_called)),
    }


def _export_dashboard_csvs(run_id: str, spark) -> None:
    """
    POC-3: Export Gold tables to CSV for Tableau Public ingestion.
    In production this would be a live Databricks JDBC connection.
    """
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    exports = {
        "defect_alerts":        "gold.defect_alerts",
        "rca_findings":         "gold.rca_findings",
        "scenario_projections": "gold.scenario_projections",
        "executive_briefings":  "gold.executive_briefings",
        "run_log":              "gold.run_log",
    }

    for filename, table in exports.items():
        try:
            df = spark.table(table).toPandas()
            df.to_csv(EXPORT_DIR / f"{filename}.csv", index=False)
        except Exception:
            pass  # table may not exist yet on first run

    # Write a metadata file so Tableau knows when data was last refreshed
    import json as _json
    meta = {
        "last_export": _now(),
        "run_id": run_id,
        "poc_constraint": "POC-3: CSV extracts replace live Databricks JDBC connection",
    }
    with open(EXPORT_DIR / "_metadata.json", "w") as f:
        _json.dump(meta, f, indent=2)
