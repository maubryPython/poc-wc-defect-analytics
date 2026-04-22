# Tableau Public Dashboard — Setup Guide

This guide walks you through connecting the pipeline's CSV exports to Tableau Public,
building the four-chart World Cup Defect Analytics dashboard, and wiring up the
scenario-slider parameter actions so stakeholders can explore cost projections interactively.

---

## 0. Prerequisites

| Requirement | Notes |
|---|---|
| Tableau Public Desktop | Free download at public.tableau.com |
| Agent pipeline run | Run notebook `06_orchestrator.py` at least once |
| CSV exports present | Verify at `dashboard/exports/` — see Step 1 |

---

## 1. Verify CSV Exports

After each pipeline run, the Summary Agent (A-05) exports four Gold tables to
`dashboard/exports/`. Check that these files exist and are non-empty:

```
dashboard/exports/
├── defect_alerts.csv          ← Detection Agent output
├── rca_findings.csv           ← RCA Agent output
├── scenario_projections.csv   ← Scenario Agent output (includes all return_rate variants)
└── executive_briefings.csv    ← Summary Agent output
```

To regenerate from Databricks:
```python
# In notebook 05_summary_agent.py, the _export_dashboard_csvs() call
# writes all four files. Re-run cell 3 to refresh.
```

**POC-3 note:** These are static exports, not a live connection. Tableau Public
does not support JDBC connections to Databricks CE. Refresh the CSVs after each
pipeline run, then republish the workbook.

---

## 2. Connect Data Sources in Tableau Public

1. Open Tableau Public Desktop.
2. On the Start page, click **Connect → Text File**.
3. Navigate to `dashboard/exports/defect_alerts.csv` and click Open.
4. On the Data Source canvas, verify:
   - `anomaly_score` is **Number (decimal)**
   - `observed_defect_rate` is **Number (decimal)**
   - `alerted_at` is **Date & Time**
   - `alert_status` is **String**
5. Repeat **Connect → Add** for each remaining CSV:
   - `rca_findings.csv`
   - `scenario_projections.csv`
   - `executive_briefings.csv`
6. Create relationships between data sources using `batch_id` as the join key
   between `defect_alerts` and `rca_findings`.

---

## 3. Build the Four Dashboard Views

### View 1 — Defect Rate Heatmap (Supplier × Line)

**Purpose:** Show which supplier/line combinations are producing anomalous defect rates.

1. Drag `supplier_id` to **Rows**.
2. Drag `line_id` to **Columns**.
3. Drag `observed_defect_rate` to **Color** (use sequential color palette — orange to red).
4. Drag `alert_status` to **Filters** → Show Filter → allow multi-select.
5. Drag `anomaly_score` to **Label**.
6. Right-click the color legend → **Edit Colors** → set min at 0.00, max at 0.20.
7. Add a **Reference Line** at `baseline_mean_rate` (right-click axis → Add Reference Line).
8. Title: `Defect Rate by Supplier & Line — WC 2026`

**What to highlight during demo:** S007 and S008 will appear dark red on L2/L3. Explain
that the Z-score (anomaly_score) was computed against each supplier's own historical
baseline, not a global average — this is the Detection Agent's supplier-specific
baseline lookup (AC-10).

---

### View 2 — RCA Confidence Waterfall

**Purpose:** Show how many batches were investigated and at what confidence level.

1. Connect to `rca_findings.csv`.
2. Create a calculated field:
   ```
   Confidence Tier
   IF [top_confidence] >= 0.70 THEN "HIGH (≥70%)"
   ELSEIF [top_confidence] >= 0.50 THEN "MEDIUM (50–70%)"
   ELSE "LOW (<50%)"
   END
   ```
3. Drag `confidence_tier` to **Columns**, `COUNTD(batch_id)` to **Rows**.
4. Drag `rca_status` to **Color**.
5. Drag `top_hypothesis` to **Tooltip**.
6. Sort columns: HIGH → MEDIUM → LOW.
7. Title: `RCA Findings — Investigation Confidence Distribution`

**What to highlight during demo:** The HIGH bar shows batches where the RCA Agent
had enough evidence (>70% confidence) to recommend action without escalation (AC-17).
The LOW/ESCALATED bars show where data gaps triggered human-in-the-loop escalation (AC-18).

---

### View 3 — Scenario Cost Comparison (with parameter actions)

**Purpose:** Allow stakeholders to compare the three response strategies and explore
how different return-rate assumptions affect total cost.

#### 3a. Base chart

1. Connect to `scenario_projections.csv`.
2. Filter `return_rate_assumption = 1.0` initially (baseline variant).
3. Drag `scenario_name` to **Rows**.
4. Drag `mean_total_cost_usd` to **Columns** (bar chart).
5. Drag `p10_total_cost_usd` and `p90_total_cost_usd` to add error bars:
   - Drag `p10_total_cost_usd` → Measure Values shelf.
   - Drag `p90_total_cost_usd` → Measure Values shelf.
   - Right-click x-axis → **Add Reference Line** → Per Cell → using Measure Values.
6. Color `is_recommended = TRUE` bars in Nike orange (#FF6600); others in grey.
7. Drag `confidence_flag` to **Detail**. Add annotation: if `confidence_flag = LOW_CONFIDENCE`,
   show a ⚠ symbol on the bar.

#### 3b. Return Rate Assumption parameter

1. Create a **Parameter**: `Return Rate Assumption`
   - Data type: Float
   - Allowable values: List → 0.70, 0.85, 1.00, 1.15, 1.30
   - Display as: "×0.70 (Optimistic)", "×0.85", "×1.00 (Base)", "×1.15", "×1.30 (Stress)"
   - Default: 1.00
2. Create a calculated field: `Selected Return Rate Variant`
   ```
   [return_rate_assumption] = [Return Rate Assumption]
   ```
3. Drag `Selected Return Rate Variant` to **Filters** → True.
4. Right-click the Parameter control → **Show Parameter**.
5. Show the parameter slider on the dashboard. When a stakeholder moves it, the chart
   re-filters to the pre-computed variant — no re-query needed (POC-3 design).

**What to highlight during demo:** The scenario projections come from the Scenario Agent's
Monte Carlo simulation (1,000 runs per scenario). The slider uses pre-computed variants —
explain that the agent ran the simulation for five return-rate assumptions so Tableau
Public can switch instantly without a live database connection.

---

### View 4 — Pipeline Operations Summary

**Purpose:** Show run health, escalation queue, and briefing verification status.

1. Connect to `executive_briefings.csv` and `defect_alerts.csv`.
2. Build a **Text Table** with:
   - Row: most recent `published_at` from `executive_briefings`
   - Columns: `word_count`, `all_claims_verified`, `unverified_claim_count`, `recommended_scenario`
3. Add a second **Text Table** from `defect_alerts`:
   - `COUNT(batch_id)` total batches scanned
   - `COUNTD(batch_id) WHERE alert_status = 'FLAGGED'` flagged count
   - `COUNTD(batch_id) WHERE alert_status = 'REVIEW'` borderline count
4. Add a **KPI tile** for `unverified_claim_count`:
   - Green if 0, amber if 1–2, red if ≥ 3.
5. Title: `Pipeline Run Summary`

**What to highlight during demo:** The `all_claims_verified` field shows the Summary
Agent's self-correction loop at work (AC-25). Every numeric claim in the briefing was
regex-extracted and verified within ±5% of the Gold tables before publishing.

---

## 4. Assemble the Dashboard

1. Click the **New Dashboard** tab.
2. Set Size to **1200 × 800 px** (Automatic works for Tableau Public).
3. Drag sheets in this layout:
   ```
   ┌──────────────────────────┬──────────────────────────┐
   │  View 1: Defect Heatmap  │  View 2: RCA Confidence  │
   ├──────────────────────────┴──────────────────────────┤
   │           View 3: Scenario Cost Comparison          │
   │           (Return Rate Assumption slider)            │
   ├──────────────────────────────────────────────────────┤
   │           View 4: Pipeline Operations Summary        │
   └──────────────────────────────────────────────────────┘
   ```
4. Add a **Text box** at the top:
   ```
   Nike WC 2026 — AI Defect Analytics POC
   Data refreshed: <today's date>  |  Pipeline run: <run_id from run_log>
   ```
5. Add a **Dashboard Action**: Filter → Source: View 1 → Target: View 2
   → Fields: `batch_id`. Clicking a heatmap cell shows that batch's RCA finding.

---

## 5. Publish to Tableau Public

1. Sign in to your Tableau Public account from within the app.
2. **File → Save to Tableau Public As…**
3. Name: `Nike WC 2026 — AI Defect Analytics`
4. Check **Show workbook sheets as tabs**.
5. Click Save. Tableau Public will open the published view in your browser.
6. Copy the public URL and add it to your portfolio/resume.

**To refresh data after a new pipeline run:**
1. Re-run notebook `05_summary_agent.py` (last cell exports new CSVs).
2. In Tableau Public Desktop, open the saved workbook.
3. **Data → [datasource name] → Refresh** for each of the four CSVs.
4. Re-publish (File → Save to Tableau Public As… → overwrite existing).

---

## 6. Demo Script Talking Points

Use these to connect dashboard visuals to the agentic patterns during interviews:

| Dashboard Element | Agentic Pattern | AC |
|---|---|---|
| Heatmap Z-scores vs baseline | Tool Use: `baseline_lookup` per supplier | AC-10 |
| Heatmap cells showing REVIEW status | Conditional Routing: borderline secondary lookup | AC-11 |
| Reasoning chain in tooltip | State & Memory: full chain persisted | AC-12 |
| RCA confidence waterfall | Confidence Gating: ≥70% COMPLETE, <50% ESCALATE | AC-17/18 |
| Scenario bars with P10/P90 range | Monte Carlo uncertainty quantification | AC-21 |
| ⚠ on LOW_CONFIDENCE scenarios | Self-Correction: CI overlap detection | AC-22 |
| Return Rate Assumption slider | Pre-computed variants: POC-3 Tableau workaround | POC-3 |
| `all_claims_verified = FALSE` KPI | Self-Correction: LLM fact-check loop | AC-25 |
| Escalation count | Human-in-the-Loop: agent defers to analyst | AC-07 |

---

## 7. File Reference

```
dashboard/
├── exports/                    ← Agent-generated CSVs (refreshed each run)
│   ├── defect_alerts.csv
│   ├── rca_findings.csv
│   ├── scenario_projections.csv
│   └── executive_briefings.csv
└── README.md                   ← This file
```

The export logic lives in `agents/summary_agent.py` → `_export_dashboard_csvs()`.
To add a new export, add a `spark.sql(...).toPandas().to_csv(...)` call there and
re-run the Summary Agent notebook.
