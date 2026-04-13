"""
Central configuration for all data source paths, Google Sheet URLs,
and credential locations.

Update the paths and URLs below to match your environment before running.
"""

# ---------------------------------------------------------------------------
# Google Service-Account Credentials
# Path to the JSON key file that allows programmatic access to Google Sheets.
# ---------------------------------------------------------------------------
GSHEET_CREDENTIALS_PATH = (
    r"G:/.shortcut-targets-by-id/1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em/"
    r"Planning Team/Chandramita/causal-flame-452312-q9-1b4341ee87db.json"
)

# OAuth scopes required for reading Google Sheets via gspread.
GSHEET_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# ---------------------------------------------------------------------------
# RDS data files – keyed by year label.
#
# Each key is a year string ("2023", "2024", …).  The value is a list of
# file paths to RDS files containing the daily product-level data for that
# year.  The tool needs data spanning roughly 8-10 weeks before each
# festival date so that baselines (5-week same-weekday median) can be
# computed.
#
# *** UPDATE THESE PATHS to point at your actual RDS files. ***
# ---------------------------------------------------------------------------
RDS_PATHS: dict[str, list[str]] = {
    "2026": [
        r"G:/.shortcut-targets-by-id/1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em/"
        r"Planning Team/25. Planning_Database/01_all_day_reporting/"
        r"04_6w_rolling_data/6w_v3.RDS",
    ],
}

# ---------------------------------------------------------------------------
# Pre-merged Parquet files for archive years (2023-2025) and live (2026).
#
# 2023-2025: Produced by scripts/merge_archive_rds.py (Sales + Forecast + Availability).
# 2026: Produced by scripts/convert_6w_to_parquet.py from 6w_v3.RDS.
# The data_loader prefers parquet when it exists; falls back to RDS for 2026.
# ---------------------------------------------------------------------------
_HIST_DIR = (
    r"G:/.shortcut-targets-by-id/1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em/"
    r"Planning Team/01. Forecasting/Weekly Forecasts/2026/Historicals Festive Tool"
)

PARQUET_PATHS: dict[str, str] = {
    "2023": rf"{_HIST_DIR}/merged_2023.parquet",
    "2024": rf"{_HIST_DIR}/merged_2024.parquet",
    "2025": rf"{_HIST_DIR}/merged_2025.parquet",
    "2026": rf"{_HIST_DIR}/merged_2026.parquet",
}

# ---------------------------------------------------------------------------
# Google Sheet URLs and worksheet names for reference data.
#
# P_Master         → maps product_id to Anchor ID / Anchor Name
# Avl_Flag         → per-product availability flag + SKU Class
# Subcat_Type_Map  → maps Sub-category to a "Type" (used for sell-through)
# SellThroughFactor→ hourly sell-through factors by Type × day × city
# Festival_Dates   → master list of festival names and dates per year
# ---------------------------------------------------------------------------
GSHEET_P_MASTER = {
    "url": "https://docs.google.com/spreadsheets/d/1TnVwhmJBnVVRGJn0jgQLctJ98P4_EGzC4lXik6s-1mU",
    "worksheet": "P Master",
}

GSHEET_AVL_FLAG = {
    "url": "https://docs.google.com/spreadsheets/d/1vOcxaLtLRJ25ChDjFVXLr18xlTqzqBzZLH6LnSAQgss",
    "worksheet": "Avl_Flag",
}

GSHEET_SUBCAT_TYPE_MAPPING = {
    "url": "https://docs.google.com/spreadsheets/d/1rP2wiye0Dzaw1wznHz8tH_eJMSVRApcw7XiMZb-fMTI",
    "worksheet": "Subcat-Type Mapping",
}

GSHEET_SELL_THROUGH_FACTOR = {
    "url": "https://docs.google.com/spreadsheets/d/1vOcxaLtLRJ25ChDjFVXLr18xlTqzqBzZLH6LnSAQgss",
    "worksheet": "SellThroughFactor",
}

GSHEET_FESTIVAL_DATES = {
    "url": "https://docs.google.com/spreadsheets/d/1XwBZlTjgJgqc4LN4o6Q_54v2BYvu9COL4DQbKSZKouY",
    "worksheet": "List",
}

# Hub mapping – maps old (closed) hubs to their current replacements.
# Columns: city_name, hub_name (current), ref hub (old hub or "old").
GSHEET_HUB_MAPPING = {
    "url": "https://docs.google.com/spreadsheets/d/1RGgvLVXQxKdlftOCyrmOPfObKvJ6Jbakl735p4FqtKU",
    "worksheet": "all cities hub mapping",
}

# Fallback for SKU Class Prod mapping when product_id not in P Master.
# Maps unique product name -> SKU Class Prod.
GSHEET_CC_CAT = {
    "url": "https://docs.google.com/spreadsheets/d/1mgbXJnhxkxjAAeY3ehDEqBvR4RHCsje4-YaV1CRoguM",
    "worksheet": "cc cat",
}

# ---------------------------------------------------------------------------
# Columns to retain from the raw RDS data.
#
# These are the columns used by the availability-correction pipeline.
# Any columns not present in a particular RDS file are silently skipped.
# ---------------------------------------------------------------------------
RDS_COLUMNS_TO_KEEP = [
    "city_name", "product_id", "hub_name", "sku_group", "process_dt",
    "sales", "revenue", "product_discount",
    "group_flag", "group_instances",
    "grp_r7_plan", "grp_r7_inv", "grp_r7_plan_rev", "grp_r7_inv_rev",
    "grp_BasePlan", "grp_BaseRev",
    "r7_plan", "r7_inv", "r7_plan_rev", "r7_inv_rev",
    "BasePlan", "flag", "instances", "sub_category", "product_name",
]

# Total operating hours per day used to estimate the out-of-stock time.
# A product with 100% availability is available for all 12 hours (8am-8pm).
TOTAL_OOS_HOURS = 12

# ---------------------------------------------------------------------------
# Display scaling factor — all revenue/sales values shown in the UI are
# divided by this factor so they appear in Lacs (1 Lac = 100,000).
# ---------------------------------------------------------------------------
LACS = 100_000

# ---------------------------------------------------------------------------
# Major cities — shown by default in all planning views.
# All other cities are considered "minor" and are excluded from the initial
# computation unless the user explicitly requests them (include_minor=True).
# ---------------------------------------------------------------------------
MAJOR_CITIES: list[str] = [
    "Bangalore",
    "Mumbai",
    "NCR",
    "Hyderabad",
    "Kolkata",
    "Pune",
    "Chennai",
]
