"""
A-00 — Orchestrator Agent (LangGraph)
=======================================
Master coordinator. Implements the observe–plan–delegate–monitor–synthesize–reflect loop.

POC-2: Uses scheduled polling (landing_zone_poll) instead of event-driven Auto Loader.

LangGraph StateGraph:
  Nodes:  poll → plan → ingest → detect → rca → scenario → summary → log
  Edges:  conditional routing based on agent output status
"""

import uuid
import time
import json
from datetime import datetime, timezone
from typing import TypedDict, Annotated, Literal
import operator

from langgraph.graph import StateGraph, END

from tools.db_tools import (
    landing_zone_poll, pipeline_state_read, run_log_write, escalation_write
)
from tools.validation_tools import config_read

import agents.ingestion_agent  as ingestion_agent
import agents.detection_agent  as detection_agent
import agents.rca_agent        as rca_agent
import agents.scenario_agent   as scenario_agent
import agents.summary_agent    as summary_agent


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Agent State ───────────────────────────────────────────────────────────────
class PipelineState(TypedDict):
    run_id:              str
    started_at:          str
    new_files:           list[str]
    pipeline_state:      dict

    # Per-agent outputs
    ingest_result:       dict
    detect_result:       dict
    rca_result:          dict
    scenario_result:     dict
    summary_result:      dict

    # Orchestrator bookkeeping
    agents_invoked:      Annotated[list[str], operator.add]
    tools_called:        Annotated[list[str], operator.add]
    escalations:         int
    retry_counts:        dict
    errors:              list[str]

    # Routing signals
    should_ingest:       bool
    should_detect:       bool
    should_rca:          bool
    should_scenario:     bool
    should_summary:      bool


# ── Node: poll ────────────────────────────────────────────────────────────────
def node_poll(state: PipelineState) -> dict:
    """
    Observe: check landing zone for new files.
    POC-2: polling replaces event-driven Auto Loader trigger.
    """
    cfg = config_read()["config"]
    landing_path = cfg.get("pipeline", {}).get("landing_zone_path", "/dbfs/mnt/landing")

    poll_result = landing_zone_poll(landing_path)
    new_files   = poll_result.get("new_files", [])

    return {
        "new_files":    new_files,
        "should_ingest": len(new_files) > 0,
        "tools_called":  ["landing_zone_poll"],
    }


# ── Node: plan ────────────────────────────────────────────────────────────────
def node_plan(state: PipelineState) -> dict:
    """
    Plan: decide which downstream agents need to run this cycle.
    AGENTIC (AC-05): not all agents run every cycle — only stale/needed ones.
    """
    ps = pipeline_state_read()
    table_state = ps.get("table_state", {})

    # Determine staleness: if a table has no data it needs to run
    silver_stale = table_state.get("silver.fact_qc_inspections") is None
    gold_alerts_stale = table_state.get("gold.defect_alerts") is None
    gold_rca_stale    = table_state.get("gold.rca_findings") is None
    gold_scen_stale   = table_state.get("gold.scenario_projections") is None

    return {
        "pipeline_state":  table_state,
        "should_detect":   state.get("should_ingest", False) or silver_stale,
        "should_rca":      state.get("should_ingest", False) or gold_alerts_stale,
        "should_scenario": state.get("should_ingest", False) or gold_rca_stale,
        "should_summary":  state.get("should_ingest", False) or gold_scen_stale,
        "tools_called":    ["pipeline_state_read"],
    }


# ── Node: ingest ──────────────────────────────────────────────────────────────
def node_ingest(state: PipelineState) -> dict:
    """Delegate to Ingestion Agent (A-01)."""
    cfg = config_read()["config"]
    result = ingestion_agent.run({
        "run_id":               state["run_id"],
        "files":                state["new_files"],
        "quarantine_threshold": cfg.get("pipeline", {}).get("quarantine_failure_rate", 0.30),
    })
    tools_called = ["ingestion_agent.run"] + result.get("tools_called", [])
    esc = sum(1 for r in result.get("results", []) if r.get("status") == "FILE_QUARANTINED")
    return {
        "ingest_result":  result,
        "agents_invoked": ["A-01"],
        "tools_called":   tools_called,
        "escalations":    state.get("escalations", 0) + esc,
    }


# ── Node: detect ──────────────────────────────────────────────────────────────
def node_detect(state: PipelineState) -> dict:
    """Delegate to Defect Detection Agent (A-02)."""
    result = detection_agent.run({"run_id": state["run_id"]})
    tools_called = ["detection_agent.run"] + result.get("tools_called", [])
    esc = result.get("unresolved", 0)
    return {
        "detect_result":  result,
        "agents_invoked": ["A-02"],
        "tools_called":   tools_called,
        "escalations":    state.get("escalations", 0) + esc,
    }


# ── Node: rca ─────────────────────────────────────────────────────────────────
def node_rca(state: PipelineState) -> dict:
    """Delegate to RCA Agent (A-03)."""
    flagged_ids = state.get("detect_result", {}).get("flagged_batch_ids", [])
    result = rca_agent.run({
        "run_id":           state["run_id"],
        "flagged_batch_ids": flagged_ids,
    })
    tools_called = ["rca_agent.run"] + result.get("tools_called", [])
    esc = result.get("escalated", 0)
    return {
        "rca_result":     result,
        "agents_invoked": ["A-03"],
        "tools_called":   tools_called,
        "escalations":    state.get("escalations", 0) + esc,
    }


# ── Node: scenario ────────────────────────────────────────────────────────────
def node_scenario(state: PipelineState) -> dict:
    """Delegate to Scenario Modeling Agent (A-04)."""
    result = scenario_agent.run({"run_id": state["run_id"]})
    tools_called = ["scenario_agent.run"] + result.get("tools_called", [])
    esc = 1 if result.get("confidence_flag") == "LOW_CONFIDENCE" else 0
    return {
        "scenario_result": result,
        "agents_invoked":  ["A-04"],
        "tools_called":    tools_called,
        "escalations":     state.get("escalations", 0) + esc,
    }


# ── Node: summary ─────────────────────────────────────────────────────────────
def node_summary(state: PipelineState) -> dict:
    """Delegate to Executive Summary Agent (A-05)."""
    rca_out = state.get("rca_result", {})
    result  = summary_agent.run({
        "run_id":              state["run_id"],
        "rca_status":          rca_out.get("status", "COMPLETE"),
        "max_top_confidence":  rca_out.get("max_top_confidence", 0.0),
    })
    tools_called = ["summary_agent.run"] + result.get("tools_called", [])
    return {
        "summary_result": result,
        "agents_invoked": ["A-05"],
        "tools_called":   tools_called,
    }


# ── Node: log ─────────────────────────────────────────────────────────────────
def node_log(state: PipelineState) -> dict:
    """
    Reflect: write the complete run log to gold.run_log (AC-04).
    """
    duration = (
        datetime.now(timezone.utc)
        - datetime.fromisoformat(state["started_at"])
    ).total_seconds()

    confidence_scores = {}
    if state.get("rca_result"):
        confidence_scores["rca_max_confidence"] = state["rca_result"].get("max_top_confidence", 0)
    if state.get("detect_result"):
        confidence_scores["flagged_batches"] = state["detect_result"].get("flagged", 0)

    run_log_write(
        run_id=state["run_id"],
        agents_invoked=state.get("agents_invoked", []),
        tools_called=list(dict.fromkeys(state.get("tools_called", []))),
        escalations=state.get("escalations", 0),
        duration_seconds=duration,
        confidence_scores=confidence_scores,
        notes=(
            f"new_files={len(state.get('new_files', []))} | "
            f"flagged={state.get('detect_result', {}).get('flagged', 0)} | "
            f"rca_status={state.get('rca_result', {}).get('status', 'N/A')} | "
            f"summary={state.get('summary_result', {}).get('status', 'N/A')}"
        ),
    )
    return {"tools_called": ["run_log_write"]}


# ── Routing functions (AGENTIC: conditional edges) ────────────────────────────

def route_after_poll(state: PipelineState) -> Literal["plan", "log"]:
    """If no new files and tables are fresh, skip to log (AC-05)."""
    return "plan"  # always plan; plan node decides which agents are needed


def route_after_plan(state: PipelineState) -> Literal["ingest", "detect", "log"]:
    if state.get("should_ingest"):
        return "ingest"
    if state.get("should_detect"):
        return "detect"
    return "log"


def route_after_ingest(state: PipelineState) -> Literal["detect", "log"]:
    """Continue to detection if ingestion succeeded (at least partially)."""
    result = state.get("ingest_result", {})
    if result.get("status") in ("COMPLETE", "PARTIAL"):
        return "detect"
    # Complete failure: retry logic handled inside agent; escalate & log
    return "log"


def route_after_detect(state: PipelineState) -> Literal["rca", "log"]:
    """Only run RCA if there are flagged batches."""
    result = state.get("detect_result", {})
    if result.get("flagged", 0) > 0:
        return "rca"
    return "log"


def route_after_rca(state: PipelineState) -> Literal["scenario", "log"]:
    """
    AGENTIC (AC-17): if RCA is ESCALATED with no complete findings, skip scenario.
    If any findings completed, proceed.
    """
    result = state.get("rca_result", {})
    if result.get("complete", 0) > 0:
        return "scenario"
    return "log"


def route_after_scenario(state: PipelineState) -> Literal["summary", "log"]:
    """
    AGENTIC: run Summary Agent if RCA confidence threshold reached (AC-17),
    OR if scenario completed (we'll let the Summary Agent decide severity).
    """
    rca_out  = state.get("rca_result", {})
    scen_out = state.get("scenario_result", {})
    if scen_out.get("status") == "COMPLETE":
        return "summary"
    return "log"


def route_after_summary(state: PipelineState) -> Literal["log"]:
    return "log"


# ── Build LangGraph ───────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    g = StateGraph(PipelineState)

    g.add_node("poll",     node_poll)
    g.add_node("plan",     node_plan)
    g.add_node("ingest",   node_ingest)
    g.add_node("detect",   node_detect)
    g.add_node("rca",      node_rca)
    g.add_node("scenario", node_scenario)
    g.add_node("summary",  node_summary)
    g.add_node("log",      node_log)

    g.set_entry_point("poll")

    g.add_conditional_edges("poll",     route_after_poll,     {"plan": "plan", "log": "log"})
    g.add_conditional_edges("plan",     route_after_plan,     {"ingest": "ingest", "detect": "detect", "log": "log"})
    g.add_conditional_edges("ingest",   route_after_ingest,   {"detect": "detect", "log": "log"})
    g.add_conditional_edges("detect",   route_after_detect,   {"rca": "rca", "log": "log"})
    g.add_conditional_edges("rca",      route_after_rca,      {"scenario": "scenario", "log": "log"})
    g.add_conditional_edges("scenario", route_after_scenario, {"summary": "summary", "log": "log"})
    g.add_conditional_edges("summary",  route_after_summary,  {"log": "log"})

    g.add_edge("log", END)

    return g.compile()


# ── Entry point ───────────────────────────────────────────────────────────────

def run_pipeline() -> dict:
    """
    Called by the Databricks Workflow scheduler (POC-2: every 15 minutes).
    Initialises state and runs the full LangGraph.
    """
    run_id    = str(uuid.uuid4())
    graph     = build_graph()

    initial_state: PipelineState = {
        "run_id":         run_id,
        "started_at":     _now(),
        "new_files":      [],
        "pipeline_state": {},
        "ingest_result":  {},
        "detect_result":  {},
        "rca_result":     {},
        "scenario_result":{},
        "summary_result": {},
        "agents_invoked": [],
        "tools_called":   [],
        "escalations":    0,
        "retry_counts":   {},
        "errors":         [],
        "should_ingest":  False,
        "should_detect":  False,
        "should_rca":     False,
        "should_scenario":False,
        "should_summary": False,
    }

    print(f"[Orchestrator] Run {run_id} starting at {_now()}")
    final_state = graph.invoke(initial_state)

    summary = {
        "run_id":         run_id,
        "status":         "COMPLETE",
        "agents_invoked": list(dict.fromkeys(final_state.get("agents_invoked", []))),
        "files_processed":len(final_state.get("new_files", [])),
        "flagged_batches":final_state.get("detect_result", {}).get("flagged", 0),
        "rca_status":     final_state.get("rca_result", {}).get("status", "N/A"),
        "summary_status": final_state.get("summary_result", {}).get("status", "N/A"),
        "escalations":    final_state.get("escalations", 0),
    }
    print(f"[Orchestrator] Run complete: {json.dumps(summary, indent=2)}")
    return summary


if __name__ == "__main__":
    run_pipeline()
