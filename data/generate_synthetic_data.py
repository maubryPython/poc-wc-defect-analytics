"""
Nike World Cup Jersey Defect Analytics — Synthetic Data Generator
=================================================================
Generates all six datasets used in the POC pipeline.
All data is entirely synthetic. Fixed random seeds ensure reproducibility.

Run:  python data/generate_synthetic_data.py
Output: data/synthetic/
"""

import random
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ── Reproducibility ───────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

OUT = Path(__file__).parent / "synthetic"
OUT.mkdir(exist_ok=True)

# ── Constants ─────────────────────────────────────────────────────────────────
PRODUCTION_START = date(2026, 1, 5)
PRODUCTION_END   = date(2026, 3, 28)
SALES_START      = date(2026, 2, 1)
SALES_END        = date(2026, 4, 20)
DEFECT_DISCOVERY = date(2026, 4, 8)   # Real news date

LINES = ["L1", "L2", "L3", "L4"]
SKUS  = ["WC-HOME-M", "WC-AWAY-M", "WC-HOME-W", "WC-AWAY-W"]

# ── 1. SUPPLIER MANIFEST ──────────────────────────────────────────────────────
def make_suppliers():
    suppliers = [
        # (id, name, region, material_type, contract_tier, baseline_defect_rate)
        ("S001", "Viet Textile Partners",    "Vietnam",    "jersey_fabric",       "tier1", 0.008),
        ("S002", "Mekong Apparel Co.",       "Vietnam",    "bonded_seam_tape",    "tier1", 0.011),
        ("S003", "Jakarta Sportswear Ltd.",  "Indonesia",  "jersey_fabric",       "tier2", 0.014),
        ("S004", "Bandung Heat-Transfer",    "Indonesia",  "heat_transfer_film",  "tier2", 0.009),
        ("S005", "Dhaka Precision Fabric",   "Bangladesh", "jersey_fabric",       "tier1", 0.007),
        ("S006", "Chittagong Seam Works",    "Bangladesh", "bonded_seam_tape",    "tier2", 0.013),
        # Two suppliers involved in the defect incident — elevated baseline
        ("S007", "Surabaya Bonding Tech",    "Indonesia",  "bonded_seam_tape",    "tier2", 0.021),
        ("S008", "Ho Chi Minh Film Coat",    "Vietnam",    "heat_transfer_film",  "tier2", 0.019),
    ]
    df = pd.DataFrame(suppliers, columns=[
        "supplier_id", "supplier_name", "region",
        "material_type", "contract_tier", "baseline_defect_rate"
    ])
    df.to_csv(OUT / "supplier_manifest.csv", index=False)
    print(f"  supplier_manifest.csv  — {len(df)} rows")
    return df


# ── 2. SUPPLIER DEFECT HISTORY (monthly, Jan 2025 – Mar 2026) ────────────────
def make_supplier_history(suppliers):
    months = pd.date_range("2025-01-01", "2026-03-01", freq="MS")
    rows = []
    for _, sup in suppliers.iterrows():
        base = sup["baseline_defect_rate"]
        for m in months:
            for line in LINES:
                # S007 and S008 show a creeping trend on L2/L3 from Dec 2025
                if sup["supplier_id"] in ("S007", "S008") and m >= pd.Timestamp("2025-12-01"):
                    if line in ("L2", "L3"):
                        rate = base * np.random.uniform(1.8, 3.2)
                    else:
                        rate = base * np.random.uniform(0.9, 1.3)
                else:
                    rate = base * np.random.uniform(0.7, 1.4)
                rate = round(min(rate, 0.15), 5)
                inspected = random.randint(400, 1200)
                rows.append({
                    "supplier_id":    sup["supplier_id"],
                    "line_id":        line,
                    "month":          m.strftime("%Y-%m"),
                    "units_inspected": inspected,
                    "defect_rate":    rate,
                    "defects_found":  int(inspected * rate),
                })
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "supplier_defect_history.csv", index=False)
    print(f"  supplier_defect_history.csv — {len(df)} rows")
    return df


# ── 3. MANUFACTURING BATCHES ──────────────────────────────────────────────────
def make_batches(suppliers):
    # Defective batches: S007 + S008 on L2/L3, produced Jan 15 – Feb 28
    defective_suppliers = {"S007", "S008"}
    defective_lines     = {"L2", "L3"}
    defective_window    = (date(2026, 1, 15), date(2026, 2, 28))

    all_suppliers = suppliers["supplier_id"].tolist()
    sup_materials = dict(zip(suppliers["supplier_id"], suppliers["material_type"]))

    days_range = (PRODUCTION_END - PRODUCTION_START).days
    rows = []
    for i in range(1, 201):
        batch_id   = f"B-{i:03d}"
        prod_date  = PRODUCTION_START + timedelta(days=random.randint(0, days_range))
        supplier   = random.choice(all_suppliers)
        line       = random.choice(LINES)
        unit_count = random.randint(500, 2000)
        lot_id     = f"LOT-{supplier}-{prod_date.strftime('%Y%m')}-{random.randint(1,5):02d}"
        sku        = random.choice(SKUS)

        # Flag defective batches
        is_defective = (
            supplier in defective_suppliers
            and line in defective_lines
            and defective_window[0] <= prod_date <= defective_window[1]
        )

        rows.append({
            "batch_id":    batch_id,
            "supplier_id": supplier,
            "production_date": prod_date.isoformat(),
            "line_id":     line,
            "unit_count":  unit_count,
            "material_lot": lot_id,
            "material_type": sup_materials[supplier],
            "sku":         sku,
            "is_defective": is_defective,   # ground truth — NOT visible to agents
        })

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "manufacturing_batches.csv", index=False)
    n_defective = df["is_defective"].sum()
    print(f"  manufacturing_batches.csv — {len(df)} rows  ({n_defective} defective, hidden from agents)")
    return df


# ── 4. QC INSPECTION LOGS ────────────────────────────────────────────────────
def make_qc_inspections(batches):
    defect_types_normal    = ["none", "none", "none", "minor_thread", "minor_color_variation"]
    defect_types_defective = ["seam_bulge", "heat_transfer_failure", "seam_bulge",
                              "material_deformation", "seam_bulge"]
    inspectors = [f"QC-{i:03d}" for i in range(1, 21)]

    rows = []
    insp_id = 1
    for _, batch in batches.iterrows():
        n_inspections = random.randint(1, 3)
        for _ in range(n_inspections):
            is_def = batch["is_defective"]
            if is_def:
                defect_count = random.randint(45, 180)
                defect_type  = random.choice(defect_types_defective)
                pass_fail    = "FAIL"
            else:
                defect_count = random.randint(0, 12)
                defect_type  = random.choice(defect_types_normal)
                pass_fail    = "PASS" if defect_count < 10 else "REVIEW"

            # Inspection date: 1–5 days after production
            prod_date   = date.fromisoformat(batch["production_date"])
            insp_date   = prod_date + timedelta(days=random.randint(1, 5))
            rows.append({
                "inspection_id":  f"INS-{insp_id:05d}",
                "batch_id":       batch["batch_id"],
                "supplier_id":    batch["supplier_id"],
                "line_id":        batch["line_id"],
                "inspection_date": insp_date.isoformat(),
                "inspector_id":   random.choice(inspectors),
                "units_inspected": min(batch["unit_count"], random.randint(80, 200)),
                "defect_count":   defect_count,
                "defect_type":    defect_type,
                "pass_fail":      pass_fail,
                "notes":          f"Routine inspection. Lot: {batch['material_lot']}",
            })
            insp_id += 1

    df = pd.DataFrame(rows)
    # Save as Excel (as per acceptance criteria — raw QC data arrives as Excel)
    df.to_excel(OUT / "qc_inspection_logs.xlsx", index=False)
    print(f"  qc_inspection_logs.xlsx — {len(df)} rows")
    return df


# ── 5. SALES & RETURNS ────────────────────────────────────────────────────────
def make_sales_returns():
    channels = ["nike.com", "retail_partner", "own_store", "wholesale"]
    return_reasons_normal   = ["wrong_size", "changed_mind", "gift_duplicate"]
    return_reasons_defective = ["product_defect", "product_defect", "item_damaged"]

    rows = []
    txn_id = 1
    current = SALES_START
    while current <= SALES_END:
        for sku in SKUS:
            for channel in channels:
                units_sold = random.randint(200, 800)

                # Returns spike after defect discovery
                if current >= DEFECT_DISCOVERY:
                    return_rate = random.uniform(0.18, 0.35)   # elevated post-discovery
                    reason_pool = return_reasons_defective
                else:
                    return_rate = random.uniform(0.02, 0.06)   # normal returns
                    reason_pool = return_reasons_normal

                units_returned = int(units_sold * return_rate)
                rows.append({
                    "transaction_id":  f"TXN-{txn_id:06d}",
                    "date":            current.isoformat(),
                    "sku":             sku,
                    "channel":         channel,
                    "units_sold":      units_sold,
                    "units_returned":  units_returned,
                    "return_rate":     round(units_returned / units_sold, 4),
                    "return_reason":   random.choice(reason_pool),
                    "revenue_usd":     round(units_sold * random.uniform(88, 110), 2),
                    "refund_usd":      round(units_returned * random.uniform(88, 110), 2),
                })
                txn_id += 1
        current += timedelta(days=1)

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "sales_returns.csv", index=False)
    print(f"  sales_returns.csv — {len(df)} rows")
    return df


# ── 6. SOCIAL SENTIMENT ───────────────────────────────────────────────────────
def make_sentiment():
    platforms = ["twitter_x", "reddit", "instagram", "news_media"]
    rows = []
    current = date(2026, 3, 1)
    while current <= SALES_END:
        days_since_discovery = (current - DEFECT_DISCOVERY).days
        for platform in platforms:
            if current < DEFECT_DISCOVERY:
                sentiment = round(random.uniform(0.55, 0.85), 3)
                volume    = random.randint(800, 2500)
                topic     = "world_cup_jersey_hype"
            elif days_since_discovery <= 3:
                # Viral moment — high volume, very negative
                sentiment = round(random.uniform(0.05, 0.25), 3)
                volume    = random.randint(8000, 25000)
                topic     = "nike_jersey_defect"
            elif days_since_discovery <= 10:
                sentiment = round(random.uniform(0.15, 0.35), 3)
                volume    = random.randint(4000, 12000)
                topic     = "nike_jersey_defect"
            else:
                # Fading but still negative
                sentiment = round(random.uniform(0.25, 0.50), 3)
                volume    = random.randint(1500, 5000)
                topic     = "nike_jersey_recall_response"

            rows.append({
                "date":      current.isoformat(),
                "platform":  platform,
                "topic":     topic,
                "volume":    volume,
                "sentiment_score": sentiment,    # 0 = very negative, 1 = very positive
                "mentions_nike_jersey": True,
            })
        current += timedelta(days=1)

    # Save as JSON (as per spec)
    with open(OUT / "social_sentiment.json", "w") as f:
        json.dump(rows, f, indent=2)
    print(f"  social_sentiment.json — {len(rows)} records")
    return rows


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("\nGenerating synthetic datasets (seed=42)...\n")
    suppliers = make_suppliers()
    make_supplier_history(suppliers)
    batches   = make_batches(suppliers)
    make_qc_inspections(batches)
    make_sales_returns()
    make_sentiment()
    print("\nAll datasets written to data/synthetic/")
    print("\nNOTE: manufacturing_batches.csv includes an 'is_defective' ground-truth column.")
    print("This column is NOT ingested into the pipeline — it is used only for evaluation.")
    print("Remove it or rename it before uploading to Databricks landing zone.\n")


if __name__ == "__main__":
    main()
