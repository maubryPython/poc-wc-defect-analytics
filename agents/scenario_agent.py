"""
A-04 — Scenario Modeling Agent
================================
Observe: defect scope from gold.defect_alerts + Silver sales/returns
Reason:  sensitivity_check → Monte Carlo simulation for 3 scenarios
Act:     write gold.scenario_projections with confidence bands + recommended scenario
Escalate: if scenario CIs overlap → LOW_CONFIDENCE flag (AC-22)

POC-3: Parameter tuning pre-computes variants; Tableau parameter actions switch between rows.
"""

import uuid
import json
from datetime import datetime, timezone

import numpy as np
from pyspark.sql import SparkSession

from tools.db_tools import (
    defect_scope_query, sales_return_query,
    gold_write, gold_read, escalation_write
)
from tools.validation_tools import config_read


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _monte_carlo(units_affected: int, avg_unit_price: float,
                 return_rate_base: float, scenario_params: dict,
                 n_sim: int = 1000, seed: int = 42) -> dict:
    """
    Run Monte Carlo simulation for a single scenario.
    Returns: mean_cost, p10, p50, p90, revenue_impact (all USD).
    """
    rng = np.random.default_rng(seed)

    # Sample uncertain inputs
    return_rates = rng.normal(return_rate_base, return_rate_base * 0.15, n_sim).clip(0, 1)
    unit_costs   = rng.normal(
        scenario_params.get("recall_cost_per_unit_usd", 0),
        max(scenario_params.get("recall_cost_per_unit_usd", 0) * 0.1, 0.5),
        n_sim
    ).clip(0)

    returns_volume   = (units_affected * return_rates).astype(int)
    refund_costs     = returns_volume * avg_unit_price
    recall_costs     = units_affected * unit_costs
    logistics        = rng.normal(
        scenario_params.get("logistics_cost_usd", 0),
        scenario_params.get("logistics_cost_usd", 0) * 0.1 + 1,
        n_sim
    ).clip(0)
    discount_cost    = (
        units_affected
        * avg_unit_price
        * scenario_params.get("discount_pct", 0)
    )
    total_cost = refund_costs + recall_costs + logistics + discount_cost

    # Revenue impact = direct sales loss + reputation discount
    revenue_at_risk  = units_affected * avg_unit_price
    recovered_revenue = revenue_at_risk * (1 - scenario_params.get("discount_pct", 0))

    timeline_days_base = scenario_params.get("timeline_days", 30)
    timeline_days = rng.normal(timeline_days_base, timeline_days_base * 0.2, n_sim).clip(7).astype(int)

    return {
        "mean_total_cost_usd":     round(float(np.mean(total_cost)), 0),
        "p10_total_cost_usd":      round(float(np.percentile(total_cost, 10)), 0),
        "p50_total_cost_usd":      round(float(np.percentile(total_cost, 50)), 0),
        "p90_total_cost_usd":      round(float(np.percentile(total_cost, 90)), 0),
        "mean_revenue_impact_usd": round(float(np.mean(refund_costs + discount_cost)), 0),
        "mean_timeline_days":      int(np.mean(timeline_days)),
        "p10_timeline_days":       int(np.percentile(timeline_days, 10)),
        "p90_timeline_days":       int(np.percentile(timeline_days, 90)),
        "mean_returns_volume":     int(np.mean(returns_volume)),
    }


def _sensitivity_check(units: int, base_return_rate: float,
                        avg_price: float, cfg_scenarios: dict) -> dict:
    """
    Tool: sensitivity_check
    Identify which parameter drives the most variance in total cost.
    AGENTIC: agent uses this to focus attention and frame its recommendation.
    """
    base = _monte_carlo(units, avg_price, base_return_rate,
                        cfg_scenarios["partial_remediation"], n_sim=200)
    base_cost = base["mean_total_cost_usd"]

    deltas = {}
    # Vary return rate ±30%
    high = _monte_carlo(units, avg_price, base_return_rate * 1.3,
                        cfg_scenarios["partial_remediation"], n_sim=200)
    deltas["return_rate"] = abs(high["mean_total_cost_usd"] - base_cost)

    # Vary recall cost ±30%
    p2 = dict(cfg_scenarios["partial_remediation"])
    p2["recall_cost_per_unit_usd"] *= 1.3
    high2 = _monte_carlo(units, avg_price, base_return_rate, p2, n_sim=200)
    deltas["recall_cost_per_unit"] = abs(high2["mean_total_cost_usd"] - base_cost)

    # Vary logistics ±30%
    p3 = dict(cfg_scenarios["partial_remediation"])
    p3["logistics_cost_usd"] = int(p3["logistics_cost_usd"] * 1.3)
    high3 = _monte_carlo(units, avg_price, base_return_rate, p3, n_sim=200)
    deltas["logistics_cost"] = abs(high3["mean_total_cost_usd"] - base_cost)

    ranked = sorted(deltas.items(), key=lambda x: x[1], reverse=True)
    return {
        "sensitivity_rank": [r[0] for r in ranked],
        "sensitivity_deltas_usd": {r[0]: round(r[1], 0) for r in ranked},
        "most_sensitive_param": ranked[0][0],
    }


def _cis_overlap(r1: dict, r2: dict, r3: dict) -> bool:
    """Check if all three scenario p10-p90 cost intervals overlap (AC-22)."""
    intervals = [
        (r["p10_total_cost_usd"], r["p90_total_cost_usd"])
        for r in [r1, r2, r3]
    ]
    # All overlap if every pair has a common region
    for i in range(len(intervals)):
        for j in range(i + 1, len(intervals)):
            lo = max(intervals[i][0], intervals[j][0])
            hi = min(intervals[i][1], intervals[j][1])
            if lo > hi:
                return False
    return True


def run(task_payload: dict) -> dict:
    """
    task_payload = {
      "run_id": str,
      "param_overrides": dict | None   # optional: caller can override YAML defaults
    }
    """
    run_id          = task_payload.get("run_id", str(uuid.uuid4()))
    param_overrides = task_payload.get("param_overrides") or {}

    cfg = config_read()["config"]
    scenario_cfg  = cfg.get("scenario", {})
    N_SIM         = scenario_cfg.get("n_simulations", 1000)
    scenarios_def = scenario_cfg.get("scenarios", {})
    tools_called  = ["config_read"]

    # Apply any runtime overrides (POC-3: pre-computes multiple variant rows)
    for scenario_key, overrides in param_overrides.items():
        if scenario_key in scenarios_def:
            scenarios_def[scenario_key].update(overrides)

    # ── Observe: defect scope ─────────────────────────────────────────────────
    scope = defect_scope_query(run_id=run_id)
    tools_called.append("defect_scope_query")
    if scope["status"] != "ok":
        return {"agent_id": "A-04", "run_id": run_id, "status": "FAILED",
                "message": "Could not read defect scope"}

    units_affected = scope["total_units_flagged"]
    if units_affected == 0:
        return {"agent_id": "A-04", "run_id": run_id, "status": "NO_ALERTS",
                "message": "No flagged units to model."}

    # ── Observe: sales/returns baseline ──────────────────────────────────────
    sales = sales_return_query(date_range=("2026-02-01", "2026-04-07"))  # pre-discovery
    tools_called.append("sales_return_query")
    base_return_rate = sales.get("avg_return_rate", 0.04)
    avg_unit_price   = (
        sales["total_revenue"] / max(sales["total_sold"], 1)
    ) if sales.get("total_revenue") else 99.0

    # ── Reason: sensitivity check (AC-20) ─────────────────────────────────────
    sensitivity = _sensitivity_check(units_affected, base_return_rate,
                                     avg_unit_price, scenarios_def)
    tools_called.append("sensitivity_check")

    # ── Reason: Monte Carlo for each scenario ────────────────────────────────
    SCENARIO_PARAMS = {
        "do_nothing": {**scenarios_def.get("do_nothing", {}), "timeline_days": 90},
        "partial_remediation": {**scenarios_def.get("partial_remediation", {}), "timeline_days": 45},
        "full_recall": {**scenarios_def.get("full_recall", {}), "timeline_days": 21},
    }

    results = {}
    for name, params in SCENARIO_PARAMS.items():
        results[name] = _monte_carlo(
            units_affected, avg_unit_price, base_return_rate, params, N_SIM
        )
    tools_called.append("monte_carlo_run")

    # ── AGENTIC: Confidence gating — do CIs overlap? (AC-22) ─────────────────
    overlap = _cis_overlap(
        results["do_nothing"],
        results["partial_remediation"],
        results["full_recall"],
    )
    confidence_flag = "LOW_CONFIDENCE" if overlap else "OK"

    # ── Recommend best scenario ───────────────────────────────────────────────
    # Prefer lowest mean cost as primary criterion; tie-break on shortest timeline
    ranked = sorted(
        results.items(),
        key=lambda x: (x[1]["mean_total_cost_usd"], x[1]["mean_timeline_days"])
    )
    recommended_scenario = ranked[0][0]
    recommended = results[recommended_scenario]

    # ── Act: write projection records ─────────────────────────────────────────
    records = []
    for scenario_name, res in results.items():
        params = SCENARIO_PARAMS[scenario_name]
        records.append({
            "scenario_id":              f"{run_id}_{scenario_name}",
            "scenario_name":            scenario_name,
            "scenario_description":     scenarios_def.get(scenario_name, {}).get("description", ""),
            "is_recommended":           scenario_name == recommended_scenario,
            "confidence_flag":          confidence_flag,
            "units_affected":           units_affected,
            "recall_cost_per_unit":     float(params.get("recall_cost_per_unit_usd", 0)),
            "discount_pct":             float(params.get("discount_pct", 0)),
            "logistics_cost_usd":       float(params.get("logistics_cost_usd", 0)),
            "mean_total_cost_usd":      res["mean_total_cost_usd"],
            "p10_total_cost_usd":       res["p10_total_cost_usd"],
            "p50_total_cost_usd":       res["p50_total_cost_usd"],
            "p90_total_cost_usd":       res["p90_total_cost_usd"],
            "mean_revenue_impact_usd":  res["mean_revenue_impact_usd"],
            "mean_timeline_days":       res["mean_timeline_days"],
            "p10_timeline_days":        res["p10_timeline_days"],
            "p90_timeline_days":        res["p90_timeline_days"],
            "mean_returns_volume":      res["mean_returns_volume"],
            "sensitivity_rank":         json.dumps(sensitivity["sensitivity_rank"]),
            "most_sensitive_param":     sensitivity["most_sensitive_param"],
            "run_id":                   run_id,
            "modeled_at":               _now(),
        })

    spark = SparkSession.getActiveSession()
    proj_df = spark.createDataFrame(records)
    gold_write(proj_df, "scenario_projections", run_id)
    tools_called.append("gold_write")

    # Escalate if LOW_CONFIDENCE (AC-22)
    if overlap:
        escalation_write(
            agent_id="A-04", run_id=run_id,
            question=(
                "Scenario cost confidence intervals overlap significantly — "
                "no scenario is statistically distinguishable from another. "
                f"The most sensitive parameter is '{sensitivity['most_sensitive_param']}'. "
                "Recommend: obtain tighter estimates on this parameter before presenting to executive."
            ),
            context={
                "most_sensitive_param":     sensitivity["most_sensitive_param"],
                "sensitivity_deltas":       sensitivity["sensitivity_deltas_usd"],
                "scenario_cost_ranges_usd": {
                    k: (v["p10_total_cost_usd"], v["p90_total_cost_usd"])
                    for k, v in results.items()
                },
            },
            recovery_attempted=False,
        )
        tools_called.append("escalate_to_human")

    return {
        "agent_id":           "A-04",
        "run_id":             run_id,
        "status":             "COMPLETE",
        "confidence_flag":    confidence_flag,
        "units_affected":     units_affected,
        "recommended_scenario": recommended_scenario,
        "recommended_cost_usd": recommended["mean_total_cost_usd"],
        "recommended_timeline_days": recommended["mean_timeline_days"],
        "most_sensitive_param": sensitivity["most_sensitive_param"],
        "tools_called":       list(dict.fromkeys(tools_called)),
    }
