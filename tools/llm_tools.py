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
DEFAULT_MODEL = "mistralai/Mistral-7B-Instruct-v0.2"


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
    """Raw HuggingFace Inference API call. Returns generated text."""
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
    resp = requests.post(
        HF_API_URL.format(model=model),
        headers=headers, json=payload, timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list) and data:
        return data[0].get("generated_text", "").strip()
    if isinstance(data, dict):
        return data.get("generated_text", str(data)).strip()
    return str(data)


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
            # Fallback: structured parse failure
            return {
                "status": "parse_error", "tool": "llm_reason",
                "raw_response": raw[:500],
                "hypotheses": [],
                "overall_confidence": 0.0,
            }

        hypotheses = result.get("hypotheses", [])
        overall_confidence = float(result.get("overall_confidence", 0.0))

        return {
            "status": "ok", "tool": "llm_reason",
            "hypotheses": hypotheses,
            "overall_confidence": overall_confidence,
            "data_gaps": result.get("data_gaps", []),
            "model_used": model,
        }

    except requests.exceptions.HTTPError as e:
        return {"status": "error", "tool": "llm_reason",
                "message": f"HuggingFace API error: {e}", "hypotheses": []}
    except json.JSONDecodeError as e:
        return {"status": "parse_error", "tool": "llm_reason",
                "message": f"JSON parse failed: {e}", "hypotheses": []}
    except Exception as e:
        return {"status": "error", "tool": "llm_reason",
                "message": str(e), "hypotheses": []}


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
        return {"status": "error", "tool": "llm_draft", "message": str(e), "draft": ""}


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
