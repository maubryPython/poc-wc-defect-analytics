# POC Constraints & Implementation Notes

This document records the four places where the proof-of-concept implementation
deliberately diverges from the full production design. These are scoping
decisions, not design gaps. The agentic logic, data architecture, and business
narrative are identical in both cases.

---

## POC-1 — LLM Hosting

| | |
|---|---|
| **Full design intent** | Llama 3 (or equivalent OSS LLM) hosted within the Databricks cluster, invoked as a local model via MLflow Model Serving |
| **POC implementation** | [HuggingFace free Inference API](https://huggingface.co/inference-api) called via HTTP from the notebook. Model: `mistralai/Mistral-7B-Instruct-v0.2`. No model is hosted locally. |
| **Why the difference** | Databricks Community Edition has no GPU and insufficient RAM to run a 7B+ parameter model. Local hosting requires a paid GPU cluster. |
| **Production path** | Databricks Model Serving (MLflow) with a GPU instance, or a Databricks Marketplace model endpoint on a paid tier. |
| **Affected criteria** | AC-16, AC-25 |

**Setup:** Add your HuggingFace API token to `.env`:
```
HF_API_TOKEN=hf_xxxxxxxxxxxxxxxxxxxxxxxx
```

---

## POC-2 — Event-Driven Pipeline Trigger

| | |
|---|---|
| **Full design intent** | Orchestrator detects a file landing in `/landing` via Auto Loader (`cloudFiles`) or cloud storage events (S3 EventBridge / ADLS Event Grid) |
| **POC implementation** | Databricks Workflow scheduled every 15 minutes. The Orchestrator notebook polls the landing zone directory; if new files are found, the pipeline runs. If not, it exits cleanly and logs a NO_NEW_FILES entry. |
| **Why the difference** | Auto Loader's event-driven mode and cloud storage triggers are not available on Databricks Community Edition. |
| **Production path** | Auto Loader with `cloudFiles` format on paid Databricks; or a cloud event trigger (e.g., AWS Lambda on S3 `PutObject`) calling the Databricks Jobs API webhook. |
| **Affected criteria** | AC-01, AC-24 |

---

## POC-3 — Tableau Live Connection

| | |
|---|---|
| **Full design intent** | Live Tableau → Databricks connection via JDBC. Dashboard reflects current Gold table state on load; Scenario Agent recalculates on parameter change and dashboard updates live. |
| **POC implementation** | Gold Delta tables exported to CSV on pipeline completion (`dashboard/exports/`). Tableau Public workbook built on CSV extract. Scenario sliders implemented as Tableau parameter actions switching between pre-computed scenario rows. |
| **Why the difference** | Tableau Public does not support live database connections. Real-time agent recalculation from a Tableau parameter event requires Tableau Server/Cloud with a live connector. |
| **Production path** | Tableau Desktop with the [Databricks connector](https://www.tableau.com/solutions/databricks) (paid); or Power BI Desktop with DirectQuery to Databricks (free Power BI Desktop supports this). |
| **Affected criteria** | AC-21, AC-27 |

---

## POC-4 — Cross-Run Agent Memory

| | |
|---|---|
| **Full design intent** | Agents maintain persistent memory across runs: RCA Agent references prior findings for the same supplier; Summary Agent generates delta commentary comparing current to last briefing. |
| **POC implementation** | Single-run context only. Cross-run lookups (`rca_history_query`, prior briefing comparison) are stubbed with seeded historical Delta records to demonstrate the pattern exists in the data model, but multi-run accumulation is not executed in the POC. |
| **Why the difference** | Implementing true cross-run memory requires multiple full pipeline runs with consistent synthetic data, adding complexity disproportionate to POC demo value. AC-19 and AC-28 are P2 (stretch) items. |
| **Production path** | No infrastructure change required — the Delta table schema already supports it. Implementation requires running the pipeline multiple times with varied input files and validating the lookup logic against accumulated history. |
| **Affected criteria** | AC-19 (P2), AC-28 (P2) |

---

## What the POC Fully Demonstrates

Despite the four constraints above, the following agentic properties are
implemented and demonstrable end-to-end:

- ✅ **Tool Use** — Agents call named tool functions based on data conditions
- ✅ **Conditional Routing** — Borderline anomaly scores trigger secondary lookups
- ✅ **Confidence Gating** — Low-confidence RCA routes to escalation, not Summary Agent
- ✅ **Orchestrator Delegation** — Not all agents run every cycle
- ✅ **Human-in-the-Loop** — Structured escalation records with agent context
- ✅ **Self-Correction / Retry** — Failed agents retry before escalating
- ✅ **Proactive Trigger** — Summary Agent triggers on severity, not manual request
- ⏳ **State & Memory** — Within-run only (cross-run deferred, see POC-4)
