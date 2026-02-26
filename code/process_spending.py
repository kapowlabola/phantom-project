"""
Process USAspending FY2017-2025 contract transaction CSVs.
- Standardize columns to a common schema
- Convert action_date to datetime, compute fiscal_year
- Merge all valid years into a single parquet file
"""

import os
import glob
import pandas as pd

EXTRACTED_DIR = "backups/extracted"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "combined_spending_2017_2025.parquet")

# Mapping: desired column name -> actual CSV column name
COLUMN_MAP = {
    "award_amount": "federal_action_obligation",
    "action_date": "action_date",
    "period_of_performance_start_date": "period_of_performance_start_date",
    "naics_code": "naics_code",
    "product_or_service_code": "product_or_service_code",
    "recipient_uei": "recipient_uei",
    "awarding_sub_agency_name": "awarding_sub_agency_name",
    "type_of_contract_pricing": "type_of_contract_pricing",
    "extent_competed": "extent_competed",
    "primary_place_of_performance_zip_5": "primary_place_of_performance_zip_4",
    "contract_description": "transaction_description",
    "parent_award_agency_id": "parent_award_agency_id",
}

# Only the source columns we need to read
USE_COLS = list(COLUMN_MAP.values())

FY_FOLDERS = [
    "FY2017", "FY2018", "FY2019", "FY2020",
    "FY2021", "FY2022", "FY2023", "FY2024", "FY2025",
]


def compute_fiscal_year(date_series):
    """Federal fiscal year: FY starts Oct 1 of prior calendar year.
    e.g. Oct 1 2024 -> FY2025, Sep 30 2025 -> FY2025."""
    return date_series.dt.year + (date_series.dt.month >= 10).astype(int)


def process_fy(fy_label):
    """Read and clean a single FY CSV."""
    fy_dir = os.path.join(EXTRACTED_DIR, fy_label)
    csvs = glob.glob(os.path.join(fy_dir, "*.csv"))
    if not csvs:
        print(f"  WARNING: No CSV found in {fy_dir}")
        return None

    csv_path = csvs[0]
    print(f"  Reading {csv_path}")

    df = pd.read_csv(csv_path, usecols=USE_COLS, dtype=str, low_memory=False)

    # Rename to standardized column names
    reverse_map = {v: k for k, v in COLUMN_MAP.items()}
    df = df.rename(columns=reverse_map)

    # Convert action_date to datetime
    df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")

    # Convert period_of_performance_start_date to datetime
    df["period_of_performance_start_date"] = pd.to_datetime(
        df["period_of_performance_start_date"], errors="coerce"
    )

    # Convert award_amount to numeric
    df["award_amount"] = pd.to_numeric(df["award_amount"], errors="coerce")

    # Extract 5-digit zip from zip+4 field
    df["primary_place_of_performance_zip_5"] = (
        df["primary_place_of_performance_zip_5"]
        .astype(str)
        .str[:5]
        .replace("nan", pd.NA)
    )

    # Compute fiscal year from action_date
    df["fiscal_year"] = compute_fiscal_year(df["action_date"])

    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- Process each FY ---
    print("=" * 60)
    print("PROCESSING VALID FISCAL YEARS")
    print("=" * 60)

    frames = []
    for fy in FY_FOLDERS:
        print(f"\nProcessing {fy}...")
        df = process_fy(fy)
        if df is not None:
            print(f"  Rows: {len(df):,}")
            frames.append(df)

    # --- Merge ---
    print("\n" + "=" * 60)
    print("MERGING ALL YEARS")
    print("=" * 60)

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total combined rows: {len(combined):,}")

    # --- Export ---
    combined.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nExported to: {OUTPUT_FILE}")

    # --- Verification summary ---
    print("\n" + "=" * 60)
    print("VERIFICATION: Row count per Fiscal Year")
    print("=" * 60)

    fy_counts = combined.groupby("fiscal_year").size().sort_index()
    for fy, count in fy_counts.items():
        label = f"FY{int(fy)}"
        print(f"  {label}: {count:>10,} rows")

    print(f"\n  {'TOTAL':}: {len(combined):>10,} rows")

    # Check for missing FYs
    expected = set(range(2017, 2026))
    actual = set(fy_counts.index.dropna().astype(int))
    missing = expected - actual
    if missing:
        print(f"\n  MISSING fiscal years: {sorted(missing)}")
        print("  -> You need to download the data for these years.")
    else:
        print("\n  All fiscal years 2017-2025 are present.")


if __name__ == "__main__":
    main()
