"""
A-04 — Scenario Modeling Agent
=================================
Observe: gold.defect_alerts scope + gold.rca_findings confidence
Reason:  Monte Carlo projections for 3 response strategies
Act:     write gold.scenario_projections (existing)
         write gold.prevention_roi_curves (NEW — Chart 4)

Prevention ROI model — 6 pipeline stages:
  1. Pre-Production Risk Scoring   (supplier_risk_score investment)
  2. In-Production Lot Monitoring  (continuous AI monitoring cost)
  3. Defect Detection (AI Agent)   (cumulative: monitoring + alert processing)
  4. RCA & Scenario Analysis       (agent run cost + analyst review)
  5. Remediation Execution         (actual remediation cost)
  6. Reputational Recovery         (brand / customer recovery cost)

Three scenarios at each stage:
  detect_early  — AI catches it at Week 13 (our system)
  detect_late   — Status quo: fan complaints Week 20
  do_nothing    — No remediation; returns + brand damage compound
Plus: ai_investment — cumulative cost of building and running the AI system
"""

import uuid
import json
import numpy as np
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from tools.db_tools import (
    gold_read, gold_write, silver_read,
    defect_scope_query, sales_return_query,
)
from tools.validation_tools import config_read


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Scenario parameters ──────────────────────────────────────────────────────
SCENARIO_PARAMS = {
    "do_nothing": {
        "label":               "Do Nothing",
        "return_rate_multiplier": 1.0,
        "remediation_cost_per_unit": 0.0,
        "recall_cost_fixed":   0,
        "brand_damage_factor": 3.0,   # reputation cost = 3× returns × avg unit price
        "timeline_days_mean":  90.5,
        "timeline_days_std":   17.0,
        "is_recommended":      False,
        "description":         "Accept current defect exposure. No proactive action.",
    },
    "partial_remediation": {
        "label":               "Partial Remediation",
        "return_rate_multiplier": 0.40,   # 60% reduction via targeted repair
        "remediation_cost_per_unit": 8.50,
        "recall_cost_fixed":   85_000,
        "brand_damage_factor": 0.5,
        "timeline_days_mean":  44.7,
        "timeline_days_std":   9.0,
        "is_recommended":      True,
        "description":         "Targeted replacement of defective lots + consumer advisory.",
    },
    "full_recall": {
        "label":               "Full Recall",
        "return_rate_multiplier": 0.0,
        "remediation_cost_per_unit": 22.00,
        "recall_cost_fixed":   220_000,
        "brand_damage_factor": 0.2,
        "timeline_days_mean":  74.5,
        "timeline_days_std":   15.0,
        "is_recommended":      False,
        "description":         "Full product recall across all WC jersey SKUs.",
    },
}

# Prevention ROI stage costs (USD) by scenario — cumulative at each stage
# These are the expected values; Monte Carlo adds uncertainty
ROI_STAGE_PARAMS = {
    "detect_early": {
        # AI catches defect at Week 13 — only 9 flagged batches vs. 14 if missed
        "Pre-Production Risk Scoring":  15_000,
        "In-Production Lot Monitoring": 25_000,   # incremental AI monitoring cost
        "Defect Detection (AI Agent)":  30_000,   # agent infrastructure
        "RCA & Scenario Analysis":      40_000,   # + analyst review
        "Remediation Execution":       350_000,   # smaller scope (9 batches caught early)
        "Reputational Recovery":        420_000,  # minimal brand impact
    },
    "detect_late": {
        # Status quo — fan complaints April 2026
        "Pre-Production Risk Scoring":  0,
        "In-Production Lot Monitoring": 0,
        "Defect Detection (AI Agent)":  0,
        "RCA & Scenario Analysis":    250_000,   # crisis response team
        "Remediation Execution":     1_200_000,  # 14 batches + larger scope
        "Reputational Recovery":     1_850_000,  # media cycle, returns spike
    },
    "do_nothing": {
        "Pre-Production Risk Scoring":  0,
        "In-Production Lot Monitoring": 0,
        "Defect Detection (AI Agent)":  0,
        "RCA & Scenario Analysis":      0,
        "Remediation Execution":       580_000,  # returns absorbed
        "Reputational Recovery":     2_600_000,  # sustained brand damage
    },
    "ai_investment": {
        # Total cost of building and operating the AI prevention system
        "Pre-Production Risk Scoring":   15_000,
        "In-Production Lot Monitoring":  25_000,
        "Defect Detection (AI Agent)":   30_000,
        "RCA & Scenario Analysis":       40_000,
        "Remediation Execution":        350_000,
        "Reputational Recovery":        420_000,
    },
}

STAGE_ORDER = [
    "Pre-Production Risk Scoring",
    "In-Production Lot Monitoring",
    "Defect Detection (AI Agent)",
    "RCA & Scenario Analysis",
    "Remediation Execution",
    "Reputational Recovery",
]


def run(task_payload: dict) -> dict:
    run_id = task_payload.get("run_id", str(uuid.uuid4()))
    param_overrides = task_payload.get("param_overrides", {})
    n_simulations   = task_payload.get("n_simulations", 10_000)

    cfg = config_read()["config"].get("scenario", {})
    AVG_UNIT_PRICE   = cfg.get("avg_unit_price_usd",    85.0)
    RETURN_RATE_BASE = cfg.get("base_return_rate",       0.24)
    tools_called     = ["config_read"]

    # ── Observe: scope from detection agent ─────────────────────────────────
    scope = defect_scope_query()
    tools_called.append("defect_scope_query")
    total_units = int(scope.get("total_units_flagged", 17190))

    # ── Monte Carlo for gold.scenario_projections ────────────────────────────
    rng = np.random.default_rng(42)
    projection_records = []

    for scenario_key, params in SCENARIO_PARAMS.items():
        p = {**params, **param_overrides.get(scenario_key, {})}

        unit_costs  = rng.normal(p["remediation_cost_per_unit"], p["remediation_cost_per_unit"] * 0.12, n_simulations)
        return_vols = rng.normal(total_units * RETURN_RATE_BASE * p["return_rate_multiplier"],
                                 total_units * 0.04, n_simulations).clip(0)
        timelines   = rng.normal(p["timeline_days_mean"], p["timeline_days_std"], n_simulations).clip(14)

        total_costs = (
            unit_costs * total_units
            + return_vols * AVG_UNIT_PRICE * p["brand_damage_factor"]
            + p["recall_cost_fixed"]
        )

        projection_records.append({
            "scenario_name":         scenario_key,
            "scenario_label":        p["label"],
            "is_recommended":        p["is_recommended"],
            "description":           p["description"],
            "total_units_in_scope":  total_units,
            "mean_total_cost_usd":   round(float(np.mean(total_costs)), 2),
            "p10_total_cost_usd":    round(float(np.percentile(total_costs, 10)), 2),
            "p50_total_cost_usd":    round(float(np.percentile(total_costs, 50)), 2),
            "p90_total_cost_usd":    round(float(np.percentile(total_costs, 90)), 2),
            "mean_timeline_days":    round(float(np.mean(timelines)), 1),
            "p10_timeline_days":     round(float(np.percentile(timelines, 10)), 1),
            "p90_timeline_days":     round(float(np.percentile(timelines, 90)), 1),
            "mean_returns_volume":   round(float(np.mean(return_vols)), 0),
            "return_rate_assumption":RETURN_RATE_BASE,
            "most_sensitive_param":  "return_rate",
            "n_simulations":         n_simulations,
            "confidence_flag":       "HIGH" if n_simulations >= 10_000 else "LOW",
            "run_id":                run_id,
        })

    spark = SparkSession.getActiveSession()
    proj_df = spark.createDataFrame(projection_records)
    gold_write(proj_df, "scenario_projections", run_id)
    tools_called.append("gold_write:scenario_projections")

    # ── NEW: Prevention ROI curves (Chart 4) ─────────────────────────────────
    roi_records = []
    uncertainty_pct = 0.08   # ±8% uncertainty band on each stage

    for scenario, stage_costs in ROI_STAGE_PARAMS.items():
        cumulative = 0
        for seq, stage in enumerate(STAGE_ORDER, start=1):
            stage_cost = stage_costs[stage]
            cumulative += stage_cost
            roi_records.append({
                "scenario":           scenario,
                "stage_name":         stage,
                "stage_sequence":     seq,
                "stage_cost_usd":     stage_cost,
                "cumulative_cost_usd":cumulative,
                "p10_cumulative_usd": round(cumulative * (1 - uncertainty_pct), 0),
                "p90_cumulative_usd": round(cumulative * (1 + uncertainty_pct), 0),
                "savings_vs_detect_late": None,  # computed post-hoc in notebook
                "run_id":             run_id,
                "computed_at":        _now(),
            })

    # Compute savings vs detect_late at each stage
    late_cumulative = {r["stage_sequence"]: r["cumulative_cost_usd"]
                       for r in roi_records if r["scenario"] == "detect_late"}
    for r in roi_records:
        late_cost = late_cumulative.get(r["stage_sequence"], 0)
        r["savings_vs_detect_late"] = round(late_cost - r["cumulative_cost_usd"], 0)

    roi_df = spark.createDataFrame(roi_records)
    gold_write(roi_df, "prevention_roi_curves", run_id, mode="overwrite")
    tools_called.append("gold_write:prevention_roi_curves")

    recommended = next(
        (r for r in projection_records if r["is_recommended"]), projection_records[0]
    )

    return {
        "agent_id":                "A-04",
        "run_id":                  run_id,
        "status":                  "COMPLETE",
        "scenarios_modeled":       len(projection_records),
        "roi_stages_computed":     len(roi_records),
        "recommended_scenario":    recommended["scenario_name"],
        "recommended_cost_usd":    recommended["mean_total_cost_usd"],
        "recommended_timeline_days": recommended["mean_timeline_days"],
        "ai_system_total_cost":    ROI_STAGE_PARAMS["ai_investment"]["Reputational Recovery"],
        "savings_vs_status_quo":   round(
            next(r["mean_total_cost_usd"] for r in projection_records if r["scenario_name"]=="do_nothing") -
            recommended["mean_total_cost_usd"], 0
        ),
        "tools_called":            list(dict.fromkeys(tools_called)),
    }
