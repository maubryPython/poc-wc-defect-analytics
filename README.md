# Nike World Cup Jersey Defect Analytics
### Multi-Agent AI Analytics System — Proof of Concept

A LangGraph-orchestrated multi-agent system that autonomously detects manufacturing
defects, diagnoses root causes, and models response scenarios for Nike's World Cup
jersey supply chain challenges (April 2026).

Built as a candidate concept project for the **Nike Lead Data Analyst** role.


---

## System Overview

```
Landing Zone (CSV/Excel)
        │
        ▼
  [A-00] Orchestrator Agent          ← LangGraph orchestrator
        │  (polls every 15 min)
        ├──► [A-01] Ingestion & Validation Agent   → Silver Delta tables
        ├──► [A-02] Defect Detection Agent          → gold_defect_alerts
        ├──► [A-03] RCA Agent                       → gold_rca_findings
        ├──► [A-04] Scenario Modeling Agent         → gold_scenario_projections
        └──► [A-05] Executive Summary Agent         → executive_briefings + CSV export
                                                              │
                                                              ▼
                                                    Tableau Public Dashboard
```

## Project Structure

```
nike-wc-defect-analytics/
├── README.md
├── Nike_AI_Analytics_UserStories_AC_v1
├── Nike_AI_Analytics_UserStories_AC_v3
├── POC_CONSTRAINTS.md          ← 4 deliberate POC simplifications documented
├── DATA_SOURCES.md             ← Full synthetic data inventory
├── requirements.txt
│
├── config/
│   └── agent_config.yaml       ← All thresholds & settings (no code changes needed)
│
├── data/
│   ├── generate_synthetic_data.py
│   └── synthetic/              ← Generated datasets (6 files)
│       ├── supplier_manifest.csv
│       ├── supplier_defect_history.csv
│       ├── manufacturing_batches.csv      ← strip 'is_defective' before upload
│       ├── qc_inspection_logs.xlsx
│       ├── sales_returns.csv
│       └── social_sentiment.json
│
├── agents/                     ← LangGraph agent definitions
│   ├── orchestrator.py
│   ├── ingestion_agent.py
│   ├── detection_agent.py
│   ├── rca_agent.py
│   ├── scenario_agent.py
│   └── summary_agent.py
│
├── tools/                      ← Tool functions called by agents
│   ├── db_tools.py             ← Delta table reads/writes
│   ├── llm_tools.py            ← HuggingFace API + fact-check
│   └── validation_tools.py     ← Schema validation, business rules
│
├── notebooks/                  ← Databricks-ready notebooks (one per agent)
│   ├── 00_setup.ipynb
│   ├── 01_ingestion_agent.ipynb
│   ├── 02_detection_agent.ipynb
│   ├── 03_rca_agent.ipynb
│   ├── 04_scenario_agent.ipynb
│   ├── 05_summary_agent.ipynb
│   └── 06_orchestrator.ipynb
│
├── dbt/
│   └── models/
│       ├── silver/             ← Validated source tables
│       └── gold/               ← Business-ready analytics tables
│
└── dashboard/
    ├── exports/                ← CSV exports for Tableau Public
    └── README.md               ← Dashboard setup instructions
```

## Quick Start

### 0. Initial Prompts
```bash
Non-ai-assisted requirement creation:
        Nike_AI_Analytics_UserStories_AC_v1

Final ai-assisted requirements document that was used:
        Nike_AI_Analytics_UserStories_AC_v3

```

### 1. Generate Synthetic Data
```bash
python data/generate_synthetic_data.py
```

### 2. Set Up Environment Variables
```bash
cp .env.example .env
# Add your HuggingFace API token to .env
```

### 3. Upload to Databricks
- Upload `data/synthetic/` files to `/dbfs/mnt/landing/` (strip `is_defective` from batches first)
- Import notebooks from `notebooks/` into your Databricks CE workspace
- Run `notebooks/00_setup.ipynb` to create databases and Delta tables

### 4. Run the Pipeline
- Import and trigger `notebooks/06_orchestrator.ipynb`
- Or run notebooks 01–05 in sequence manually

### 5. View Dashboard
- Open `dashboard/exports/` CSVs in Tableau Public
- See `dashboard/README.md` for workbook setup

---

## POC Constraints Summary

| Constraint | Production Design | POC Implementation |
|---|---|---|
| POC-1 | Local LLM in Databricks | HuggingFace free Inference API |
| POC-2 | Event-driven Auto Loader | 15-min scheduled polling |
| POC-3 | Live Tableau JDBC | Pre-computed CSV extracts |
| POC-4 | Cross-run agent memory | Single-run context only |

See [POC_CONSTRAINTS.md](POC_CONSTRAINTS.md) for full details and production paths.

---

## Agentic Patterns Demonstrated

| Pattern | Where |
|---|---|
| Tool Use | All agents invoke named tool functions based on data |
| Conditional Routing | Detection Agent secondary lookup on borderline scores |
| Confidence Gating | RCA Agent routes low-confidence outputs to escalation |
| Orchestrator Delegation | A-00 dispatches only needed agents each cycle |
| Human-in-the-Loop | escalation_queue Delta table with structured context |
| Self-Correction / Retry | Orchestrator retries failed agents before escalating |
| Proactive Trigger | Summary Agent triggers on severity, not manual request |

---

*Candidate concept project — all data synthetic — not for commercial use.*
