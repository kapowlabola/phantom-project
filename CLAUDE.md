# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Air Force contract cost analysis research project. The goal is to build a "fair price" prediction model: given a service type (e.g., Cloud Computing) and economic climate (FRED indicators), predict what a federal contract should cost. This supports the government's independent cost estimate process, where parametric cost prediction is currently underused due to lack of data.

**Status**: Data collection and cleaning phase. FRED integration not yet implemented.

## Data Architecture

- **Source**: USAspending.gov bulk download CSVs (Air Force prime contract transactions, FY2017-2025)
- **Raw data**: `backups/` contains FY20XX.zip files; `backups/extracted/` has unzipped CSVs per fiscal year
- **Cleaned output**: `data/combined_spending_2017_2025.parquet` — the single merged dataset (973K rows, 13 columns)
- **Other CSVs in `data/`**: PrimeAwardSummaries and Subawards files from an earlier download (different schema, not used in the main pipeline)

### Standardized Column Schema (in parquet)

The `process_spending.py` script maps raw USAspending column names to these standardized names:

| Standardized Name | Source Column |
|---|---|
| award_amount | federal_action_obligation |
| action_date | action_date |
| period_of_performance_start_date | period_of_performance_start_date |
| naics_code | naics_code |
| product_or_service_code | product_or_service_code |
| recipient_uei | recipient_uei |
| awarding_sub_agency_name | awarding_sub_agency_name |
| type_of_contract_pricing | type_of_contract_pricing |
| extent_competed | extent_competed |
| primary_place_of_performance_zip_5 | primary_place_of_performance_zip_4 (truncated to 5 digits) |
| contract_description | transaction_description |
| parent_award_agency_id | parent_award_agency_id |
| fiscal_year | computed from action_date (Oct 1 = new FY) |

## Key Commands

```bash
# Re-run the data processing pipeline (extract, clean, merge to parquet)
python process_spending.py

# Activate the virtual environment
.venv\Scripts\activate   # Windows
```

## Workspace Rules

- **Data safety**: DO NOT read files in `data/` or `backups/` unless explicitly asked. They are large (40MB+ zips, 200MB+ CSVs, ~1GB parquet).
- **Conciseness**: Provide brief, direct answers. Skip conversational filler to save tokens.
- **Tech stack**: Python 3.9, Pandas, pyarrow (for Parquet), matplotlib/seaborn (for viz). FRED API integration planned but not yet built.
- **Fiscal year convention**: Federal FY starts Oct 1. FY2025 = Oct 1 2024 through Sep 30 2025. The `compute_fiscal_year()` function in `process_spending.py` handles this.

## Key Files

- `process_spending.py` — Main ETL pipeline: reads per-FY CSVs from `backups/extracted/`, standardizes columns, computes fiscal year, exports merged parquet
- `cleaning.ipynb` — Exploratory data analysis notebook (top recipients, contract pricing distribution, timeline trends). Uses the PrimeAwardSummaries CSV, not the main parquet
- `data/combined_spending_2017_2025.parquet` — The cleaned, merged dataset ready for modeling
