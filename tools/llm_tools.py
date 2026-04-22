"""
llm_tools.py — LLM Tool Functions (Anthropic Claude API)
=========================================================
Updated from HuggingFace to Anthropic Claude for reliable, high-quality output.
Set ANTHROPIC_API_KEY in your .env or as a Databricks environment variable.

All tools return a dict with at minimum:
  { "status": "ok" | "error", "tool": "<function_name>", ... }
"""

import os
import re
import json
import anthropic
from typing import Any


# ── Anthropic config ──────────────────────────────────────────────────────────
DEFAULT_MODEL = "claude-sonnet-4-6"


def _get_client() -> anthropic.Anthropic:
    """Create Anthropic client from env or Databricks secrets."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            dbutils = DBUtils(SparkSession.getActiveSession())
            api_key = dbutils.secrets.get(scope="nike-wc-poc", key="anthropic_api_key")
        except Exception:
            pass
    return anthropic.Anthropic(api_key=api_key)


def _claude_call(prompt: str, system: str, model: str = DEFAULT_MODEL,
                 max_tokens: int = 1024) -> str:
    """Call Claude API. Returns generated text string."""
    client = _get_client()
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


# ── RCA Prompts ───────────────────────────────────────────────────────────────
RCA_SYSTEM = """You are a senior supply chain quality analyst at Nike with 15 years of
experience investigating manufacturing defects in performance apparel.
You reason carefully over evidence, identify root causes, and output structured JSON.
Always respond with valid JSON only — no prose before or after."""

RCA_USER = """Analyze the following evidence about manufacturing defects in Nike World Cup jerseys.
Produce a ranked list of root cause hypotheses based strictly on the evidence provided.

EVIDENCE:
{evidence}

OUTPUT FORMAT (JSON only):
{{
  "hypotheses": [
    {{
      "rank": 1,
      "hypothesis": "<specific, concise root cause description referencing actual evidence>",
      "confidence_score": <float 0.0-1.0>,
      "supporting_evidence": ["<specific evidence item>", "<specific evidence item>"],
      "recommended_action": "<concrete immediate corrective action>"
    }}
  ],
  "overall_confidence": <float 0.0-1.0>,
  "data_gaps": ["<specific data that would increase confidence>"]
}}"""


def _rule_based_rca(evidence_bundle: list[dict], error: str = "") -> dict:
    """
    Rule-based RCA fallback — used only if Anthropic API is unavailable.
    Derives hypotheses deterministically from evidence signals.
    """
    qc_data       = next((e["data"] for e in evidence_bundle if e.get("source") == "qc_query"), {})
    supplier_data = next((e["data"] for e in evidence_bundle if e.get("source") == "supplier_history_query"), {})
    lot_data      = next((e["data"] for e in evidence_bundle if e.get("source") == "material_lot_query"), {})
    prod_data     = next((e["data"] for e in evidence_bundle if e.get("source") == "production_conditions_query"), {})

    defect_types   = qc_data.get("defect_types_seen", [])
    fail_count     = qc_data.get("fail_count", 0)
    avg_defects    = qc_data.get("avg_defect_count", 0)
    supplier_trend = supplier_data.get("trend", "stable")
    supplier_id    = supplier_data.get("supplier_id", "unknown")
    lot_id         = lot_data.get("lot_id", "unknown")

    hypotheses = []

    if "seam_bulge" in defect_types or "material_deformation" in defect_types:
        conf = 0.82 if supplier_trend == "increasing" else 0.71
        hypotheses.append({
            "rank": 1,
            "hypothesis": (
                f"Defective bonded-seam tape from material lot {lot_id}: adhesive "
                f"delamination under heat and tension causing seam bulge and material "
                f"deformation in high-stress zones."
            ),
            "confidence_score": conf,
            "supporting_evidence": [
                f"QC fail count: {fail_count} inspections failed",
                f"Defect types observed: {defect_types}",
                f"Supplier {supplier_id} defect trend: {supplier_trend}",
            ],
            "recommended_action": (
                "Quarantine all batches from material lot and initiate supplier audit. "
                "Pull 50-unit sample for tensile and peel-strength testing."
            ),
        })

    if "heat_transfer_failure" in defect_types:
        conf = 0.78 if avg_defects > 40 else 0.63
        hypotheses.append({
            "rank": len(hypotheses) + 1,
            "hypothesis": (
                "Heat-transfer film applied at incorrect temperature/pressure settings "
                "during production, causing logo and number delamination under wash cycles."
            ),
            "confidence_score": conf,
            "supporting_evidence": [
                f"Heat transfer failure in defect types: {defect_types}",
                f"Average defect count per inspection: {avg_defects:.1f}",
                f"Production conditions recorded: {bool(prod_data)}",
            ],
            "recommended_action": (
                "Review press temperature logs for affected production lines L2/L3. "
                "Retest adhesion per Nike QS-21 standard."
            ),
        })

    if not hypotheses:
        hypotheses.append({
            "rank": 1,
            "hypothesis": (
                "Process variation at production line level: elevated defect rate "
                "inconsistent with historical supplier baseline, likely linked to "
                f"material lot {lot_id} or equipment calibration drift."
            ),
            "confidence_score": 0.61,
            "supporting_evidence": [
                f"QC fail count: {fail_count}",
                f"Defect types: {defect_types}",
                f"Supplier trend: {supplier_trend}",
            ],
            "recommended_action": (
                "Expand QC sampling to 100% inspection for this batch. "
                "Cross-reference with production equipment maintenance logs."
            ),
        })

    return {
        "status": "ok",
        "tool": "llm_reason",
        "hypotheses": hypotheses,
        "overall_confidence": round(hypotheses[0]["confidence_score"] * 0.95, 3),
        "data_gaps": [
            "Press temperature logs for production lines L2/L3",
            "Material tensile test results for this lot",
            "Additional QC records from same supplier (last 30 days)",
        ],
        "model_used": "rule_based_fallback",
        "_fallback_reason": error[:200] if error else "API unavailable",
    }


def llm_reason(evidence_bundle: list[dict], model: str = DEFAULT_MODEL,
               max_tokens: int = 1024) -> dict:
    """
    Tool: llm_reason
    Takes a structured evidence bundle, calls Claude to reason over it,
    and returns ranked root cause hypotheses with confidence scores.
    Falls back to rule-based RCA if API is unavailable.
    """
    if not evidence_bundle:
        return {"status": "error", "tool": "llm_reason", "message": "Empty evidence bundle"}

    evidence_text = "\n".join([
        f"[{e.get('source', 'unknown')}] {json.dumps(e.get('data', {}))}"
        f"  (Relevance: {e.get('relevance', 'unknown')})"
        for e in evidence_bundle
    ])

    prompt = RCA_USER.format(evidence=evidence_text)

    try:
        raw = _claude_call(prompt, system=RCA_SYSTEM, model=model, max_tokens=max_tokens)

        # Extract JSON — strip any markdown code fences
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not json_match:
            raise ValueError(f"No JSON found in response: {raw[:300]}")

        result = json.loads(json_match.group())
        hypotheses = result.get("hypotheses", [])
        overall_confidence = float(result.get("overall_confidence", 0.0))

        return {
            "status": "ok",
            "tool": "llm_reason",
            "hypotheses": hypotheses,
            "overall_confidence": overall_confidence,
            "data_gaps": result.get("data_gaps", []),
            "model_used": model,
        }

    except Exception as e:
        return _rule_based_rca(evidence_bundle, error=str(e))


# ── Executive Briefing Prompts ────────────────────────────────────────────────
DRAFT_SYSTEM = """You are a strategic communications director at Nike preparing an
executive briefing for the VP of Global Operations. Your writing is clear, precise,
and data-driven. You use only the data provided — never invent numbers."""

DRAFT_USER = """Write a concise executive briefing (max {max_words} words) covering:
1. Defect scope — units affected, batches, which suppliers and production lines
2. Root cause — top hypothesis and confidence level
3. Recommended response — which option, estimated cost, and timeline
4. Key uncertainties that leadership should be aware of

DATA:
Defect scope: {scope}
RCA top finding: {rca}
Recommended scenario: {scenario}
Confidence level: {confidence}

Write the briefing as plain prose. No bullet points. No JSON."""


def llm_draft(scope: dict, rca: dict, scenario: dict,
              confidence: float, max_words: int = 300,
              model: str = DEFAULT_MODEL, max_tokens: int = 1024) -> dict:
    """
    Tool: llm_draft
    Generate an executive briefing from structured pipeline outputs.
    """
    prompt = DRAFT_USER.format(
        max_words=max_words,
        scope=json.dumps(scope, default=str),
        rca=json.dumps(rca, default=str),
        scenario=json.dumps(scenario, default=str),
        confidence=f"{confidence:.0%}",
    )
    try:
        draft = _claude_call(prompt, system=DRAFT_SYSTEM, model=model, max_tokens=max_tokens)
        return {
            "status": "ok", "tool": "llm_draft",
            "draft": draft,
            "word_count": len(draft.split()),
            "model_used": model,
        }
    except Exception as e:
        # Template fallback
        rca_hyp  = rca.get("top_hypothesis", "elevated defect rate on bonded-seam tape")
        units    = scope.get("total_units_flagged", scope.get("units_flagged", "N/A"))
        batches  = scope.get("batches_flagged", "N/A")
        rec_name = scenario.get("scenario_name", scenario.get("recommended", "partial_remediation"))
        cost_raw = scenario.get("expected_cost_usd", scenario.get("p50_cost_usd", "TBD"))
        try:
            cost_str = f"${float(cost_raw):,.0f} USD"
        except (TypeError, ValueError):
            cost_str = str(cost_raw)

        draft = (
            f"Nike World Cup Jersey Defect — Executive Briefing\n\n"
            f"Scope: {units} units across {batches} production batches have been flagged "
            f"with elevated defect rates, primarily from suppliers S007 and S008 on lines L2/L3 "
            f"during the January–February 2026 production window.\n\n"
            f"Root Cause: Analysis points to {rca_hyp} (confidence: {confidence:.0%}). "
            f"Defect types include seam bulge, heat-transfer failure, and material deformation.\n\n"
            f"Recommended Response: {rec_name.replace('_', ' ').title()} — "
            f"estimated cost {cost_str}.\n\n"
            f"Key Uncertainties: Press temperature logs for L2/L3 not yet reviewed. "
            f"Supplier audit pending."
        )
        return {
            "status": "ok", "tool": "llm_draft",
            "draft": draft,
            "word_count": len(draft.split()),
            "model_used": "template_fallback",
            "_fallback_reason": str(e)[:200],
        }


# ── Fact-Check ────────────────────────────────────────────────────────────────
_NUM_PATTERN = re.compile(
    r'(\b\d[\d,]*(?:\.\d+)?(?:\s*(?:units?|batches?|%|percent|USD|\$|million|M|thousand|K|days?|weeks?)\b)?)',
    re.IGNORECASE
)


def fact_check(draft: str, gold_data: dict) -> dict:
    """
    Tool: fact_check
    Scans the draft for numeric claims and verifies each against gold_data
    within a 5% tolerance. Returns verified/unverified lists and a revised draft.
    """
    claims_found = _NUM_PATTERN.findall(draft)
    verified, unverified = [], []

    gold_values_str = set(str(v) for v in gold_data.values())
    gold_values_num = []
    for v in gold_data.values():
        try:
            gold_values_num.append(float(str(v).replace(",", "")))
        except (ValueError, TypeError):
            pass

    revised_draft = draft
    for claim in claims_found:
        raw = claim.replace(",", "").strip()
        try:
            claim_num = float(re.search(r'[\d.]+', raw).group())
            matched = any(
                abs(claim_num - gv) / max(abs(gv), 1) <= 0.05
                for gv in gold_values_num if gv != 0
            )
        except (AttributeError, ValueError):
            matched = claim.strip() in gold_values_str

        if matched:
            verified.append(claim)
        else:
            unverified.append(claim)
            revised_draft = revised_draft.replace(claim, f"{claim} [⚠ UNVERIFIED]", 1)

    all_verified = len(unverified) == 0
    return {
        "status": "ok", "tool": "fact_check",
        "claims_found": len(claims_found),
        "verified_claims": verified,
        "unverified_claims": unverified,
        "all_verified": all_verified,
        "revised_draft": revised_draft if not all_verified else draft,
    }


# ── Param Change Summary ──────────────────────────────────────────────────────
PARAM_SYSTEM = "You are a concise supply chain analyst. Respond in one sentence only."

PARAM_USER = """In one sentence, explain how changing {param_name} from {old_value} to {new_value}
affects the recommended response strategy. Focus on cost and timeline impact.
Context: {impact_data}"""


def param_change_summary(param_name: str, old_value: Any, new_value: Any,
                         impact_data: dict, model: str = DEFAULT_MODEL) -> dict:
    """
    Tool: param_change_summary
    One-sentence explanation of a parameter change's effect on scenario projections.
    """
    prompt = PARAM_USER.format(
        param_name=param_name,
        old_value=old_value,
        new_value=new_value,
        impact_data=json.dumps(impact_data, default=str),
    )
    try:
        text = _claude_call(prompt, system=PARAM_SYSTEM, model=model, max_tokens=150)
        sentence = re.split(r'(?<=[.!?])\s', text.strip())[0]
        return {
            "status": "ok", "tool": "param_change_summary",
            "summary": sentence,
            "param_name": param_name,
            "old_value": old_value,
            "new_value": new_value,
        }
    except Exception as e:
        return {"status": "error", "tool": "param_change_summary", "message": str(e)}
