"""
validation_tools.py — Schema Validation & Business Rule Tools
=============================================================
Used by the Ingestion Agent (A-01) to enforce data quality before Silver writes.
"""

import yaml
from pathlib import Path
from typing import Any

import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "config" / "agent_config.yaml"


def config_read(path: str = None) -> dict:
    """
    Tool: config_read
    Read the agent configuration YAML. Agents call this at runtime so
    threshold changes take effect without code changes.
    """
    p = Path(path) if path else CONFIG_PATH
    try:
        with open(p) as f:
            cfg = yaml.safe_load(f)
        return {"status": "ok", "tool": "config_read", "config": cfg}
    except Exception as e:
        return {"status": "error", "tool": "config_read", "message": str(e), "config": {}}


# ── Schema definitions ────────────────────────────────────────────────────────
SCHEMAS = {
    # Keys match the Silver table names passed by the Ingestion Agent
    "dim_batch": {
        "required_cols": [
            "batch_id", "supplier_id", "production_date",
            "line_id", "unit_count", "material_lot", "material_type", "sku"
        ],
        "types": {
            "batch_id": str, "supplier_id": str, "production_date": str,
            "line_id": str, "unit_count": (int, float),
            "material_lot": str, "material_type": str, "sku": str,
        },
        "not_null": ["batch_id", "supplier_id", "production_date", "line_id", "unit_count"],
    },
    "fact_qc_inspections": {
        "required_cols": [
            "inspection_id", "batch_id", "supplier_id", "line_id",
            "inspection_date", "inspector_id", "units_inspected",
            "defect_count", "defect_type", "pass_fail"
        ],
        "types": {
            "inspection_id": str, "batch_id": str, "defect_count": (int, float),
            "units_inspected": (int, float), "pass_fail": str,
        },
        "not_null": ["inspection_id", "batch_id", "defect_count", "units_inspected"],
    },
    "dim_supplier": {
        "required_cols": [
            "supplier_id", "supplier_name", "region",
            "material_type", "contract_tier", "baseline_defect_rate"
        ],
        "types": {"baseline_defect_rate": float},
        "not_null": ["supplier_id", "supplier_name"],
    },
    "fact_sales_returns": {
        "required_cols": [
            "transaction_id", "date", "sku", "channel",
            "units_sold", "units_returned", "return_rate",
            "return_reason", "revenue_usd", "refund_usd"
        ],
        "types": {
            "units_sold": (int, float), "units_returned": (int, float),
            "return_rate": float, "revenue_usd": float, "refund_usd": float,
        },
        "not_null": ["transaction_id", "date", "units_sold"],
    },
    "supplier_defect_history": {
        "required_cols": [
            "supplier_id", "line_id", "month",
            "units_inspected", "defect_rate", "defects_found"
        ],
        "types": {"defect_rate": float, "units_inspected": (int, float)},
        "not_null": ["supplier_id", "line_id", "month", "defect_rate"],
    },
}


def schema_validate(df: pd.DataFrame, table_name: str) -> dict:
    """
    Tool: schema_validate
    Check that a DataFrame has the required columns and non-null constraints
    for the target table. Returns per-row validation results.
    """
    schema = SCHEMAS.get(table_name)
    if not schema:
        return {
            "status": "error", "tool": "schema_validate",
            "message": f"No schema defined for table '{table_name}'",
        }

    errors = []
    total = len(df)

    # Missing columns
    missing = [c for c in schema["required_cols"] if c not in df.columns]
    if missing:
        return {
            "status": "schema_mismatch", "tool": "schema_validate",
            "missing_columns": missing,
            "valid_rows": 0, "invalid_rows": total,
            "failure_rate": 1.0,
        }

    # Per-row checks
    invalid_mask = pd.Series([False] * total, index=df.index)

    # Null checks
    for col in schema.get("not_null", []):
        if col in df.columns:
            null_mask = df[col].isna()
            invalid_mask |= null_mask
            n_null = null_mask.sum()
            if n_null:
                errors.append({"col": col, "issue": "null_value", "count": int(n_null)})

    # Type / range checks
    if "unit_count" in df.columns:
        bad = df["unit_count"] <= 0
        invalid_mask |= bad
        if bad.any():
            errors.append({"col": "unit_count", "issue": "non_positive", "count": int(bad.sum())})

    if "defect_count" in df.columns:
        bad = df["defect_count"] < 0
        invalid_mask |= bad
        if bad.any():
            errors.append({"col": "defect_count", "issue": "negative", "count": int(bad.sum())})

    if "defect_rate" in df.columns:
        bad = (df["defect_rate"] < 0) | (df["defect_rate"] > 1)
        invalid_mask |= bad
        if bad.any():
            errors.append({"col": "defect_rate", "issue": "out_of_range_0_1", "count": int(bad.sum())})

    if "return_rate" in df.columns:
        bad = (df["return_rate"] < 0) | (df["return_rate"] > 1)
        invalid_mask |= bad
        if bad.any():
            errors.append({"col": "return_rate", "issue": "out_of_range_0_1", "count": int(bad.sum())})

    invalid_rows = int(invalid_mask.sum())
    valid_rows   = total - invalid_rows
    failure_rate = invalid_rows / max(total, 1)

    return {
        "status": "ok" if invalid_rows == 0 else "partial_invalid",
        "tool": "schema_validate",
        "table": table_name,
        "total_rows": total,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "failure_rate": round(failure_rate, 4),
        "errors": errors,
        "invalid_mask": invalid_mask,   # caller uses this to split valid/invalid rows
    }


# ── Business Rules ────────────────────────────────────────────────────────────
BUSINESS_RULES = {
    "manufacturing_batches": [
        {
            "rule": "valid_line_id",
            "description": "line_id must be L1, L2, L3, or L4",
            "check": lambda df: ~df["line_id"].isin(["L1", "L2", "L3", "L4"]),
        },
        {
            "rule": "unit_count_range",
            "description": "unit_count must be between 100 and 5000",
            "check": lambda df: (df["unit_count"] < 100) | (df["unit_count"] > 5000),
        },
        {
            "rule": "valid_sku",
            "description": "sku must be a known World Cup SKU",
            "check": lambda df: ~df["sku"].isin(
                ["WC-HOME-M", "WC-AWAY-M", "WC-HOME-W", "WC-AWAY-W"]
            ),
        },
    ],
    "qc_inspection_logs": [
        {
            "rule": "valid_pass_fail",
            "description": "pass_fail must be PASS, REVIEW, or FAIL",
            "check": lambda df: ~df["pass_fail"].isin(["PASS", "REVIEW", "FAIL"]),
        },
        {
            "rule": "defect_count_le_units",
            "description": "defect_count cannot exceed units_inspected",
            "check": lambda df: df["defect_count"] > df["units_inspected"],
        },
        {
            "rule": "min_units_inspected",
            "description": "units_inspected must be >= 10",
            "check": lambda df: df["units_inspected"] < 10,
        },
    ],
    "sales_returns": [
        {
            "rule": "returns_le_sold",
            "description": "units_returned cannot exceed units_sold",
            "check": lambda df: df["units_returned"] > df["units_sold"],
        },
        {
            "rule": "positive_revenue",
            "description": "revenue_usd must be positive",
            "check": lambda df: df["revenue_usd"] <= 0,
        },
        {
            "rule": "valid_channel",
            "description": "channel must be a known sales channel",
            "check": lambda df: ~df["channel"].isin(
                ["nike.com", "retail_partner", "own_store", "wholesale"]
            ),
        },
    ],
}


def business_rule_check(df: pd.DataFrame, table_name: str) -> dict:
    """
    Tool: business_rule_check
    Apply domain-specific business rules to a DataFrame.
    Returns per-rule failure counts and a combined invalid mask.
    """
    rules = BUSINESS_RULES.get(table_name, [])
    if not rules:
        return {
            "status": "ok", "tool": "business_rule_check",
            "table": table_name, "rules_applied": 0,
            "violations": [], "invalid_mask": pd.Series([False] * len(df)),
        }

    combined_mask = pd.Series([False] * len(df), index=df.index)
    violations = []

    for rule in rules:
        try:
            bad = rule["check"](df)
            combined_mask |= bad
            n_bad = int(bad.sum())
            if n_bad:
                violations.append({
                    "rule": rule["rule"],
                    "description": rule["description"],
                    "violations": n_bad,
                })
        except Exception as e:
            violations.append({
                "rule": rule["rule"],
                "description": rule["description"],
                "error": str(e),
                "violations": 0,
            })

    invalid_rows = int(combined_mask.sum())
    return {
        "status": "ok" if invalid_rows == 0 else "violations_found",
        "tool": "business_rule_check",
        "table": table_name,
        "rules_applied": len(rules),
        "total_rows": len(df),
        "invalid_rows": invalid_rows,
        "failure_rate": round(invalid_rows / max(len(df), 1), 4),
        "violations": violations,
        "invalid_mask": combined_mask,
    }


def duplicate_check(df: pd.DataFrame, pk_col: str, existing_ids: list) -> dict:
    """
    Tool: duplicate_check
    Flag rows whose primary key already exists in the Silver table.
    """
    dupes = df[df[pk_col].isin(existing_ids)]
    return {
        "status": "ok", "tool": "duplicate_check",
        "pk_col": pk_col,
        "total_rows": len(df),
        "duplicate_count": len(dupes),
        "duplicate_ids": dupes[pk_col].tolist()[:20],   # cap preview
        "duplicate_mask": df[pk_col].isin(existing_ids),
    }
