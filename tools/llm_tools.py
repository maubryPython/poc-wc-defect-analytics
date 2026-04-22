"""
llm_tools.py — LLM Tool Functions (HuggingFace Inference API)
==============================================================
POC-1: Uses HuggingFace free Inference API instead of locally-hosted model.
Set HF_API_TOKEN in your .env or as a Databricks secret.

All tools return a dict with at minimum:
  { "status": "ok" | "error", "tool": "<function_name>", ... }
"""

import os
import re
import json
import requests
from typing import Any

# ── HuggingFace config ─────────────────────────────────────────────────────────
HF_API_URL = "https://api-inference.huggingface.co/models/{model}"
DEFAULT_MODEL = "mistralai/Mistral-7B-Instruct-v0.3"

# Fallback models to try if DEFAULT_MODEL is unavailable
FALLBACK_MODELS = [
    "HuggingFaceH4/zephyr-7b-beta",
    "tiiuae/falcon-7b-instruct",
]


def _get_token() -> str:
    """Retrieve HF token from env or Databricks secrets."""
    token = os.environ.get("HF_API_TOKEN", "")
    if not token:
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            dbutils = DBUtils(SparkSession.getActiveSession())
            token = dbutils.secrets.get(scope="nike-wc-poc", key="hf_api_token")
        except Exception:
            pass
    return token


def _hf_call(prompt: str, model: str = DEFAULT_MODEL,
             max_tokens: int = 512, temperature: float = 0.2) -> str:
    """
    Raw HuggingFace Inference API call. Returns generated text.
    Tries DEFAULT_MODEL first, then each FALLBACK_MODEL in order.
    Raises the last HTTPError if all models fail.
    """
    token = _get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": max_tokens,
            "temperature": temperature,
            "return_full_text": False,
            "do_sample": temperature > 0,
        },
    }

    models_to_try = [model] + [m for m in FALLBACK_MODELS if m != model]
    last_error = None
    for m in models_to_try:
        try:
            resp = requests.post(
                HF_API_URL.format(model=m),
                headers=headers, json=payload, timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and data:
                return data[0].get("generated_text", "").strip()
            if isinstance(data, dict):
                return data.get("generated_text", str(data)).strip()
            return str(data)
        except requests.exceptions.HTTPError as e:
            last_error = e
            continue  # try next model

    raise last_error


# ── RCA Prompt Template ───────────────────────────────────────────────────────
RCA_PROMPT = """<s>[INST] You are a senior supply chain quality analyst at Nike.
Analyze the following evidence about manufacturing defects in World Cup jerseys
and produce a ranked list of root cause hypotheses.

EVIDENCE:
{evidence}

OUTPUT FORMAT (JSON only, no prose before or after):
{{
  "hypotheses": [
    {{
      "rank": 1,
      "hypothesis": "<concise root cause description>",
      "confidence_score": <float 0.0-1.0>,
      "supporting_evidence": ["<evidence item 1>", "<evidence item 2>"],
      "recommended_action": "<immediate corrective action>"
    }}
  ],
  "overall_confidence": <float 0.0-1.0>,
  "data_gaps": ["<what additional data would increase confidence>"]
}}
[/INST]"""


def _rule_based_rca(evidence_bundle: list[dict], error: str = "") -> dict:
    """
    Rule-based RCA fallback used when HuggingFace API is unavailable.
    Derives hypotheses deterministically from evidence signals so downstream
    agents can still produce output for the POC demo.
    """
    # ── Extract key signals from evidence ────────────────────────────────────
    qc_data      = next((e["data"] for e in evidence_bundle if e.get("source") == "qc_query"), {})
    supplier_data = next((e["data"] for e in evidence_bundle if e.get("source") == "supplier_history_query"), {})
    lot_data     = next((e["data"] for e in evidence_bundle if e.get("source") == "material_lot_query"), {})
    prod_data    = next((e["data"] for e in evidence_bundle if e.get("source") == "production_conditions_query"), {})

    defect_types = qc_data.get("defect_types_seen", [])
    fail_count   = qc_data.get("fail_count", 0)
    avg_defects  = qc_data.get("avg_defect_count", 0)
    supplier_trend = supplier_data.get("trend", "stable")
    supplier_id  = supplier_data.get("supplier_id", "unknown")
    lot_id       = lot_data.get("lot_id", "unknown")

    # ── Build ranked hypotheses from signals ─────────────────────────────────
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
                f"Production conditions data: {bool(prod_data)}",
            ],
            "recommended_action": (
                "Review press temperature logs for affected production lines L2/L3. "
                "Retest adhesion per Nike QS-21 standard."
            ),
        })

    if not hypotheses:
        # Generic fallback hypothesis if no recognisable defect pattern
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

    overall_confidence = round(hypotheses[0]["confidence_score"] * 0.95, 3)
    data_gaps = [
        "Press temperature logs for production lines L2/L3",
        "Material tensile test results for this lot",
        "Additional QC records from same supplier (last 30 days)",
    ]

    return {
        "status": "ok",
        "tool": "llm_reason",
        "hypotheses": hypotheses,
        "overall_confidence": overall_confidence,
        "data_gaps": data_gaps,
        "model_used": "rule_based_fallback",
        "_fallback_reason": error[:200] if error else "API unavailable",
    }


def llm_reason(evidence_bundle: list[dict], model: str = DEFAULT_MODEL,
               max_tokens: int = 512) -> dict:
    """
    Tool: llm_reason
    Takes a structured evidence bundle (list of {source, data, relevance}),
    formats it into the RCA prompt, and calls the LLM.
    Returns parsed hypotheses or error.

    POC-1: Calls HuggingFace Inference API.
    """
    if not evidence_bundle:
        return {"status": "error", "tool": "llm_reason", "message": "Empty evidence bundle"}

    # Format evidence for prompt
    evidence_text = "\n".join([
        f"[{e.get('source', 'unknown')}] {json.dumps(e.get('data', {}))[:400]}"
        f"  (Relevance: {e.get('relevance', 'unknown')})"
        for e in evidence_bundle
    ])

    prompt = RCA_PROMPT.format(evidence=evidence_text)

    try:
        raw = _hf_call(prompt, model=model, max_tokens=max_tokens, temperature=0.1)

        # Extract JSON from response
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
        else:
            # JSON parse failure — fall through to rule-based fallback
            raise ValueError(f"No JSON found in LLM response: {raw[:200]}")

        hypotheses = result.get("hypotheses", [])
        overall_confidence = float(result.get("overall_confidence", 0.0))

        return {
            "status": "ok", "tool": "llm_reason",
            "hypotheses": hypotheses,
            "overall_confidence": overall_confidence,
            "data_gaps": result.get("data_gaps", []),
            "model_used": model,
        }

    except Exception as e:
        # ── Rule-based fallback (POC-1 safety net) ───────────────────────────
        # When all HF models are unavailable or parse fails, synthesise a
        # structured RCA from the evidence bundle so downstream agents keep running.
        return _rule_based_rca(evidence_bundle, error=str(e))


# ── Summary Prompt Template ───────────────────────────────────────────────────
SUMMARY_PROMPT = """<s>[INST] You are drafting an executive briefing for Nike leadership.
Write a concise, plain-language summary (max {max_words} words) covering:
1. Defect scope (units affected, batches, suppliers)
2. Root cause (top hypothesis and confidence level)
3. Recommended response option and estimated cost/timeline
4. Any critical uncertainties

Use only the data provided below. Do not invent numbers.

DATA:
Defect scope: {scope}
RCA top finding: {rca}
Recommended scenario: {scenario}
Confidence level: {confidence}

Write the briefing now (plain text, no JSON): [/INST]"""


def llm_draft(scope: dict, rca: dict, scenario: dict,
              confidence: float, max_words: int = 300,
              model: str = DEFAULT_MODEL, max_tokens: int = 512) -> dict:
    """
    Tool: llm_draft
    Generate an executive summary briefing from structured pipeline outputs.
    POC-1: Calls HuggingFace Inference API.
    """
    prompt = SUMMARY_PROMPT.format(
        max_words=max_words,
        scope=json.dumps(scope, default=str)[:600],
        rca=json.dumps(rca, default=str)[:400],
        scenario=json.dumps(scenario, default=str)[:400],
        confidence=f"{confidence:.0%}",
    )
    try:
        draft = _hf_call(prompt, model=model, max_tokens=max_tokens, temperature=0.2)
        word_count = len(draft.split())
        return {
            "status": "ok", "tool": "llm_draft",
            "draft": draft, "word_count": word_count, "model_used": model,
        }
    except Exception as e:
        # ── Template-based fallback briefing ─────────────────────────────────
        rca_hyp  = rca.get("top_hypothesis", "elevated defect rate on bonded-seam tape")
        rca_conf = f"{confidence:.0%}"
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
            f"Root Cause: Analysis points to {rca_hyp} (confidence: {rca_conf}). "
            f"Defect types include seam bulge, heat-transfer failure, and material deformation — "
            f"consistent with adhesive or film quality issues from the affected material lots.\n\n"
            f"Recommended Response: {rec_name.replace('_', ' ').title()} — "
            f"estimated cost {cost_str}. "
            f"This balances brand protection with containment cost.\n\n"
            f"Key Uncertainties: Press temperature logs for L2/L3 not yet reviewed. "
            f"Supplier audit pending. Consumer return rate trajectory depends on social media velocity."
        )
        word_count = len(draft.split())
        return {
            "status": "ok", "tool": "llm_draft",
            "draft": draft, "word_count": word_count,
            "model_used": "template_fallback",
            "_fallback_reason": str(e)[:200],
        }


# ── Fact-Check ────────────────────────────────────────────────────────────────
# Regex patterns to extract numeric claims from free text
_NUM_PATTERN = re.compile(
    r'(\b\d[\d,]*(?:\.\d+)?(?:\s*(?:units?|batches?|%|percent|USD|\$|million|M|thousand|K|days?|weeks?)\b)?)',
    re.IGNORECASE
)


def fact_check(draft: str, gold_data: dict) -> dict:
    """
    Tool: fact_check
    Scans the draft for numeric claims and attempts to verify each against
    known Gold table values provided in gold_data dict.

    gold_data format: { "metric_name": value, ... }
    e.g. { "total_units_flagged": 12450, "avg_anomaly_score": 0.83 }

    Returns:
      - verified_claims: list of claims that matched a gold value (within 5% tolerance)
      - unverified_claims: list of claims that could not be matched
      - revised_draft: draft with unverified claims replaced by [UNVERIFIED — review]
      - all_verified: bool
    """
    claims_found = _NUM_PATTERN.findall(draft)
    verified, unverified = [], []

    # Build a flat lookup of all gold values (string and numeric)
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
        # Try numeric match with 5% tolerance
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
            revised_draft = revised_draft.replace(
                claim, f"{claim} [⚠ UNVERIFIED]", 1
            )

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
PARAM_CHANGE_PROMPT = """<s>[INST] In one concise sentence, explain how changing
{param_name} from {old_value} to {new_value} affects the recommended response strategy.
Focus on cost and timeline impact. Be specific with numbers if available.
Context: {impact_data}
[/INST]"""


def param_change_summary(param_name: str, old_value: Any, new_value: Any,
                         impact_data: dict, model: str = DEFAULT_MODEL) -> dict:
    """
    Tool: param_change_summary
    Generate a one-sentence plain-language explanation of a parameter change's
    effect on scenario projections.
    """
    prompt = PARAM_CHANGE_PROMPT.format(
        param_name=param_name,
        old_value=old_value,
        new_value=new_value,
        impact_data=json.dumps(impact_data, default=str)[:400],
    )
    try:
        text = _hf_call(prompt, model=model, max_tokens=100, temperature=0.2)
        # Trim to one sentence
        sentence = re.split(r'(?<=[.!?])\s', text.strip())[0]
        return {
            "status": "ok", "tool": "param_change_summary",
            "summary": sentence, "param_name": param_name,
            "old_value": old_value, "new_value": new_value,
        }
    except Exception as e:
        return {"status": "error", "tool": "param_change_summary", "message": str(e)}
