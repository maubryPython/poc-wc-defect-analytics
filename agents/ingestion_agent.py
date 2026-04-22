"""
A-01 — Ingestion & Validation Agent
=====================================
Observe: new files in landing zone (dispatched by Orchestrator)
Reason:  schema + business rule validation; duplicate detection
Act:     write valid rows to Silver; quarantine invalid rows
Escalate: if file-level failure rate > 30% (config: quarantine_failure_rate)
"""

import uuid
import pandas as pd

from tools.validation_tools import config_read, schema_validate, business_rule_check, duplicate_check
from tools.db_tools import silver_write, quarantine_write, ingest_log_write, escalation_write

# Table routing: file keywords → (table_name, pk_col)
FILE_ROUTING = {
    "manufacturing_batches": ("dim_batch",             "batch_id"),
    "qc_inspection":         ("fact_qc_inspections",   "inspection_id"),
    "supplier_manifest":     ("dim_supplier",           "supplier_id"),
    "supplier_defect":       ("supplier_defect_history","supplier_id"),
    "sales_returns":         ("fact_sales_returns",     "transaction_id"),
    "social_sentiment":      ("social_sentiment",       None),
}

# Strip ground-truth column before ingestion (POC artefact)
STRIP_COLS = ["is_defective"]


def _detect_table(filename: str) -> tuple:
    fn = filename.lower()
    for key, val in FILE_ROUTING.items():
        if key in fn:
            return val
    return None, None


def _load_file(path: str) -> pd.DataFrame:
    """Load CSV, Excel, or JSON from a local or DBFS path."""
    if path.endswith(".xlsx"):
        return pd.read_excel(path)
    elif path.endswith(".json"):
        import json
        with open(path.replace("dbfs:", "/dbfs")) as f:
            data = json.load(f)
        return pd.DataFrame(data) if isinstance(data, list) else pd.json_normalize(data)
    else:
        return pd.read_csv(path)


def run(task_payload: dict) -> dict:
    """
    Agent entry point. Called by the Orchestrator with:
      task_payload = {
        "run_id": str,
        "files": [str],          # file paths to process
        "quarantine_threshold": float  # from config (default 0.30)
      }
    Returns status dict consumed by Orchestrator.
    """
    run_id    = task_payload.get("run_id", str(uuid.uuid4()))
    files     = task_payload.get("files", [])
    threshold = task_payload.get("quarantine_threshold", 0.30)

    cfg_result = config_read()
    if cfg_result["status"] == "ok":
        threshold = cfg_result["config"].get("pipeline", {}).get(
            "quarantine_failure_rate", threshold
        )

    results = []
    tools_called = []

    for filepath in files:
        filename = filepath.split("/")[-1]
        table_name, pk_col = _detect_table(filename)

        if not table_name:
            results.append({
                "file": filename, "status": "SKIPPED",
                "reason": "No routing match for filename",
            })
            continue

        # ── Observe: load file ───────────────────────────────────────────
        try:
            df = _load_file(filepath)
        except Exception as e:
            results.append({"file": filename, "status": "LOAD_ERROR", "message": str(e)})
            continue

        # Strip ground-truth column if present (POC-only)
        for col in STRIP_COLS:
            if col in df.columns:
                df = df.drop(columns=[col])

        # ── Reason: schema validation ────────────────────────────────────
        sv = schema_validate(df, table_name)
        tools_called.append("schema_validate")

        # File-level quarantine if too many invalid rows
        if sv["failure_rate"] >= threshold:
            qw = quarantine_write(
                _spark_from_pandas(df), filepath,
                f"FILE_QUARANTINED: {sv['failure_rate']:.1%} rows failed schema validation",
                run_id
            )
            tools_called.append("quarantine_write")
            escalation_write(
                agent_id="A-01", run_id=run_id,
                question=f"File '{filename}' had {sv['failure_rate']:.1%} schema failure rate "
                         f"(threshold {threshold:.0%}). Should it be reprocessed or rejected?",
                context={"file": filename, "table": table_name, "errors": sv["errors"],
                         "failure_rate": sv["failure_rate"]},
                recovery_attempted=False,
            )
            tools_called.append("escalate_to_human")
            results.append({
                "file": filename, "status": "FILE_QUARANTINED",
                "failure_rate": sv["failure_rate"],
                "rows": len(df),
            })
            continue

        # Row-level split: valid vs invalid
        if "invalid_mask" in sv:
            invalid_mask = sv["invalid_mask"]
            df_invalid = df[invalid_mask].copy()
            df_valid   = df[~invalid_mask].copy()
        else:
            df_invalid = pd.DataFrame()
            df_valid   = df.copy()

        # ── Reason: business rule check on valid rows ────────────────────
        br = business_rule_check(df_valid, table_name)
        tools_called.append("business_rule_check")
        if br["status"] == "violations_found" and "invalid_mask" in br:
            br_mask      = br["invalid_mask"]
            df_invalid   = pd.concat([df_invalid, df_valid[br_mask]])
            df_valid     = df_valid[~br_mask]

        # ── Reason: duplicate check ──────────────────────────────────────
        rows_quarantined = 0
        if pk_col and len(df_valid):
            try:
                from pyspark.sql import SparkSession
                existing_ids = [
                    r[pk_col] for r in
                    SparkSession.getActiveSession()
                    .table(f"silver.{table_name}")
                    .select(pk_col).collect()
                ]
            except Exception:
                existing_ids = []

            dc = duplicate_check(df_valid, pk_col, existing_ids)
            tools_called.append("duplicate_check")
            if dc["duplicate_count"]:
                dupe_mask = dc["duplicate_mask"]
                df_invalid = pd.concat([df_invalid, df_valid[dupe_mask]])
                df_valid   = df_valid[~dupe_mask]

        # ── Act: quarantine invalid rows ─────────────────────────────────
        if len(df_invalid):
            df_invalid["error_reason"] = "schema_or_rule_violation"
            quarantine_write(
                _spark_from_pandas(df_invalid), filepath,
                "ROW_QUARANTINED: schema or business rule violation", run_id
            )
            tools_called.append("quarantine_write")
            rows_quarantined = len(df_invalid)

        # ── Act: write valid rows to Silver ──────────────────────────────
        rows_written = 0
        if len(df_valid):
            sdf = _spark_from_pandas(df_valid)
            sw = silver_write(sdf, table_name, run_id)
            tools_called.append("silver_write")
            rows_written = sw.get("rows_written", len(df_valid))

        # Log to ingest log
        ingest_log_write(
            source_path=filepath, table_name=table_name,
            rows_written=rows_written, rows_quarantined=rows_quarantined,
            run_id=run_id, status="COMPLETE"
        )
        tools_called.append("ingest_log_write")

        results.append({
            "file": filename, "status": "COMPLETE",
            "table": table_name, "rows_written": rows_written,
            "rows_quarantined": rows_quarantined,
            "schema_errors": sv.get("errors", []),
            "business_rule_violations": br.get("violations", []),
        })

    overall_status = "COMPLETE"
    if any(r["status"] in ("FILE_QUARANTINED", "LOAD_ERROR") for r in results):
        overall_status = "PARTIAL"
    if all(r["status"] in ("FILE_QUARANTINED", "LOAD_ERROR", "SKIPPED") for r in results):
        overall_status = "FAILED"

    return {
        "agent_id": "A-01",
        "run_id":   run_id,
        "status":   overall_status,
        "files_processed": len(files),
        "results":  results,
        "tools_called": list(dict.fromkeys(tools_called)),  # deduplicated, ordered
    }


def _spark_from_pandas(df: pd.DataFrame):
    """Convert pandas DataFrame to Spark DataFrame for Delta writes."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    # Coerce object columns to string for Delta compatibility
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)
    return spark.createDataFrame(df)
