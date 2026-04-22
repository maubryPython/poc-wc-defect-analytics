"""
A-01 — Ingestion & Validation Agent
=====================================
Observe:  files in the landing zone (CSV / Excel / JSON)
Reason:   validate schema, types, referential integrity
Act:      write clean rows to Silver Delta tables; quarantine invalids
New (prevention story): also ingests supplier_risk_scores.csv,
                         material_lots.csv, batch_lot_mapping.csv
"""

import uuid
import json
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tools.db_tools import (
    silver_write, quarantine_write, ingest_log_write,
    landing_zone_poll, gold_write,
)
from tools.validation_tools import config_read, validate_dataframe

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _spark():
    return SparkSession.getActiveSession()


# ── Schema registry: expected columns per file type ─────────────────────────
SCHEMAS = {
    "manufacturing_batches": {
        "required": ["batch_id","supplier_id","production_date","line_id",
                     "unit_count","material_lot","material_type","sku"],
        "types":    {"unit_count": "int"},
    },
    "qc_inspection_logs": {
        "required": ["inspection_id","batch_id","supplier_id","line_id",
                     "inspection_date","defect_count","units_inspected",
                     "pass_fail","defect_type"],
        "types":    {"defect_count":"int","units_inspected":"int"},
    },
    "sales_returns": {
        "required": ["date","supplier_id","sku","units_sold","units_returned",
                     "return_rate","revenue_usd","refund_usd"],
        "types":    {"units_sold":"int","units_returned":"int"},
    },
    "supplier_defect_history": {
        "required": ["supplier_id","line_id","month","units_inspected",
                     "defect_rate","defects_found"],
        "types":    {"units_inspected":"int","defects_found":"int"},
    },
    "supplier_manifest": {
        "required": ["supplier_id","supplier_name","region","material_type",
                     "contract_tier","baseline_defect_rate"],
        "types":    {"baseline_defect_rate":"float"},
    },
    # ── NEW: prevention data ──────────────────────────────────────────────
    "supplier_risk_scores": {
        "required": ["supplier_id","historical_defect_score","on_time_delivery_score",
                     "audit_score","material_lot_stability_score",
                     "capacity_utilization_score","composite_risk_score","risk_tier"],
        "types":    {"composite_risk_score":"float"},
    },
    "material_lots": {
        "required": ["lot_id","supplier_id","material_type","entered_production_date",
                     "is_defective"],
        "types":    {"is_defective":"bool"},
    },
    "batch_lot_mapping": {
        "required": ["batch_id","supplier_id","lot_id","production_date"],
        "types":    {},
    },
}

# Map file-stem → Silver table name
FILE_TABLE_MAP = {
    "manufacturing_batches":  "dim_batch",
    "supplier_manifest":      "dim_supplier",
    "qc_inspection_logs":     "fact_qc_inspections",
    "sales_returns":          "fact_sales_returns",
    "supplier_defect_history":"supplier_defect_history",
    "supplier_risk_scores":   "supplier_risk_scores",
    "material_lots":          "material_lots",
    "batch_lot_mapping":      "batch_lot_mapping",
}


def _file_key(path: str) -> str:
    """Derive schema key from file path stem."""
    stem = path.rstrip("/").split("/")[-1].lower()
    stem = stem.replace(".csv","").replace(".xlsx","").replace(".json","")
    for key in SCHEMAS:
        if key in stem:
            return key
    return None


def _read_file(path: str) -> pd.DataFrame:
    """Read CSV or Excel from DBFS/Volumes path into a Pandas DataFrame."""
    if path.endswith(".xlsx"):
        return pd.read_excel(path)
    elif path.endswith(".json"):
        with open(path) as f:
            data = json.load(f)
        return pd.DataFrame(data if isinstance(data, list) else [data])
    else:
        return pd.read_csv(path)


def run(task_payload: dict) -> dict:
    """
    Agent entry point.
    task_payload = {
      "run_id": str,
      "files":  list[str]   # DBFS or Volumes paths
    }
    """
    run_id      = task_payload.get("run_id", str(uuid.uuid4()))
    files       = task_payload.get("files", [])
    tools_called = []
    results      = []

    spark = _spark()

    for path in files:
        file_key = _file_key(path)
        if not file_key:
            results.append({"file": path, "status": "SKIPPED", "reason": "Unrecognised file type"})
            continue

        schema      = SCHEMAS[file_key]
        table_name  = FILE_TABLE_MAP.get(file_key)
        if not table_name:
            results.append({"file": path, "status": "SKIPPED", "reason": "No target table mapping"})
            continue

        # ── Observe: read raw file ──────────────────────────────────────
        try:
            df_raw = _read_file(path)
        except Exception as e:
            results.append({"file": path, "status": "ERROR", "reason": str(e)})
            continue

        # ── Reason: validate schema ──────────────────────────────────────
        missing_cols = [c for c in schema["required"] if c not in df_raw.columns]
        if missing_cols:
            # AGENTIC: quarantine the whole file and escalate if critical table
            qdf = spark.createDataFrame(df_raw.astype(str).head(5))
            quarantine_write(qdf, path,
                             f"Missing required columns: {missing_cols}", run_id)
            tools_called.append("quarantine_write")
            results.append({"file": path, "status": "QUARANTINED",
                             "reason": f"Missing columns: {missing_cols}"})
            ingest_log_write(path, table_name, 0, len(df_raw), run_id, "QUARANTINED")
            continue

        # Type coercion with bad-row quarantine
        bad_rows = pd.DataFrame()
        for col, dtype in schema.get("types", {}).items():
            if col not in df_raw.columns:
                continue
            try:
                if dtype == "int":
                    df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")
                    bad = df_raw[df_raw[col].isna()]
                    bad_rows = pd.concat([bad_rows, bad])
                    df_raw = df_raw.dropna(subset=[col])
                    df_raw[col] = df_raw[col].astype(int)
                elif dtype == "float":
                    df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")
                    bad = df_raw[df_raw[col].isna()]
                    bad_rows = pd.concat([bad_rows, bad])
                    df_raw = df_raw.dropna(subset=[col])
                elif dtype == "bool":
                    df_raw[col] = df_raw[col].astype(str).str.lower().map(
                        {"true":"True","false":"False","1":"True","0":"False",
                         "yes":"True","no":"False"}
                    )
            except Exception:
                pass

        # Quarantine bad rows
        rows_quarantined = 0
        if len(bad_rows):
            qdf = spark.createDataFrame(bad_rows.astype(str))
            quarantine_write(qdf, path, "Type coercion failure", run_id)
            tools_called.append("quarantine_write")
            rows_quarantined = len(bad_rows)

        # ── Act: write clean rows to Silver ─────────────────────────────
        if len(df_raw):
            sdf = spark.createDataFrame(df_raw)
            result = silver_write(sdf, table_name, run_id)
            tools_called.append("silver_write")
            rows_written = result.get("rows_written", len(df_raw))
        else:
            rows_written = 0

        ingest_log_write(path, table_name, rows_written, rows_quarantined, run_id, "OK")
        tools_called.append("ingest_log_write")

        results.append({
            "file":             path,
            "table":            f"silver.{table_name}",
            "status":           "OK",
            "rows_written":     rows_written,
            "rows_quarantined": rows_quarantined,
        })

    ok_count  = sum(1 for r in results if r["status"] == "OK")
    err_count = sum(1 for r in results if r["status"] in ("ERROR","QUARANTINED"))

    return {
        "agent_id":     "A-01",
        "run_id":       run_id,
        "status":       "COMPLETE" if err_count == 0 else "PARTIAL",
        "files_processed": len(files),
        "files_ok":     ok_count,
        "files_errored":err_count,
        "details":      results,
        "tools_called": list(dict.fromkeys(tools_called)),
    }
