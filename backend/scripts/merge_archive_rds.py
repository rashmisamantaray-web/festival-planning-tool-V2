"""
Phase 2: Merge Sales + Forecast + Availability RDS for 2023, 2024, 2025.
Per year:
  1. Read Sales.RDS, Forecast.RDS, SKU_Class_Avl_8am_8pm.RDS
  2. Exclude hubs whose hub_name starts with OFL or PAW
  3. Map product_id -> SKU Class Prod via P Master (primary) + cc cat (fallback)
  4. Merge Sales + Forecast on (city_name, hub_name, process_dt, product_id)
  5. Merge result + Availability on (city_name, hub_name, process_dt, product_id)
  6. Log unmapped SKU Class Prod counts + validation report
  7. If any SKU unmapped: write unmapped_sku_products_{year}.csv/.xlsx and
     unmapped_sku_rows_{year}.csv (or _sample if row count is huge)
  8. Write merged parquet per year
Usage (from backend/, with G: drive accessible):
    py scripts/merge_archive_rds.py
"""
from __future__ import annotations
import sys
from pathlib import Path
_script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_script_dir.parent))
sys.path.insert(0, str(_script_dir))
import pandas as pd
import pyreadr
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from app.config import GSHEET_CREDENTIALS_PATH, GSHEET_SCOPES, GSHEET_P_MASTER, GSHEET_CC_CAT
from merge_validation import (
    MergeIssueCollector,
    inspect_pre_merge,
    inspect_post_sku,
    inspect_post_forecast,
    inspect_post_avl,
    write_debug_report,
)
# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE = Path(
    r"G:/.shortcut-targets-by-id/1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em/"
    r"Planning Team/25. Planning_Database/01_all_day_reporting"
)
ARCHIVE = BASE / "03_archive_data"
OUTPUT_DIR = Path(
    r"G:/.shortcut-targets-by-id/1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em/"
    r"Planning Team/01. Forecasting/Weekly Forecasts/2026/Historicals Festive Tool"
)
FILES = {
    "2025": {
        "sales": ARCHIVE / "Sales.RDS",
        "avl": ARCHIVE / "Availability_8am_8pm.RDS",
        "forecast": ARCHIVE / "Forecast.RDS",
    },
    "2024": {
        "sales": ARCHIVE / "2024" / "Sales.RDS",
        "avl": ARCHIVE / "2024" / "Availability_8am_8pm.RDS",
        "forecast": ARCHIVE / "2024" / "Forecast.RDS",
    },
    "2023": {
        "sales": ARCHIVE / "2023" / "Sales.RDS",
        "avl": ARCHIVE / "2023" / "Availability_8am_8pm.RDS",
        "forecast": ARCHIVE / "2023" / "Forecast.RDS",
    },
}
# Columns to bring from Forecast (only those that exist will be used)
FORECAST_WANT_COLS = [
    "r7_plan", "r7_inv", "r7_plan_rev", "r7_inv_rev", "BasePlan",
    "group_flag", "group_instances",
    "grp_r7_plan", "grp_r7_inv", "grp_r7_plan_rev", "grp_r7_inv_rev",
    "grp_BasePlan", "grp_BaseRev",
]
FORECAST_JOIN_KEYS = ["city_name", "hub_name", "process_dt", "product_id"]
# Unmapped SKU exports (P Master + cc cat both missed)
UNMAPPED_ROW_EXPORT_FULL_MAX = 50_000  # write all row-level rows up to this count
UNMAPPED_ROW_SAMPLE_SIZE = 10_000  # otherwise random sample of this many rows
UNMAPPED_EXCEL_PRODUCT_MAX = 100_000  # cap rows in .xlsx (slow if huge)
HUB_EXCLUDE_PREFIXES = ("OFL", "PAW")
# Missing forecast / avl exports
MISSING_EXPORT_FULL_MAX = 50_000   # write all rows up to this count
MISSING_EXPORT_SAMPLE_SIZE = 10_000  # otherwise random sample of this many rows
MISSING_EXCEL_MAX = 100_000        # cap rows in .xlsx
# ---------------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------------
_gspread_client: gspread.Client | None = None
def _get_gspread_client() -> gspread.Client:
    global _gspread_client
    if _gspread_client is None:
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            GSHEET_CREDENTIALS_PATH, GSHEET_SCOPES
        )
        _gspread_client = gspread.authorize(creds)
    return _gspread_client
def _read_gsheet(url: str, worksheet: str, max_cols: int | None = None) -> pd.DataFrame:
    """Read worksheet; row 0 = header. If max_cols (e.g. 10 for A:J), read only first N columns."""
    client = _get_gspread_client()
    spreadsheet = client.open_by_url(url)
    ws = spreadsheet.worksheet(worksheet)
    data = ws.get_all_values()
    if max_cols is not None:
        data = [row[:max_cols] for row in data]
    return pd.DataFrame(data[1:], columns=data[0])
def _load_p_master_sku() -> pd.DataFrame:
    df = _read_gsheet(GSHEET_P_MASTER["url"], GSHEET_P_MASTER["worksheet"], max_cols=10)
    return df[["Product id", "SKU Class Prod"]].drop_duplicates()
def _load_cc_cat_sku() -> pd.DataFrame:
    df = _read_gsheet(GSHEET_CC_CAT["url"], GSHEET_CC_CAT["worksheet"])
    return df[["unique product name", "SKU Class Prod"]].drop_duplicates()
# ---------------------------------------------------------------------------
# RDS reader
# ---------------------------------------------------------------------------
def _read_rds(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    try:
        result = pyreadr.read_r(str(path))
        return next(iter(result.values()))
    except MemoryError:
        raise MemoryError(
            f"MemoryError reading {path}. File may be too large for available RAM."
        ) from None
    except Exception as e:
        raise RuntimeError(f"Failed to read {path}: {e}") from e
# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------
def _normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Standardize date columns and hub_id type."""
    df = df.copy()
    if "hub_id" in df.columns:
        df["hub_id"] = df["hub_id"].astype(str)
    # Align date column → process_dt
    if source == "forecast" and "process_dt" not in df.columns:
        if "date" in df.columns:
            df["process_dt"] = pd.to_datetime(df["date"], errors="coerce")
        elif "av_dt" in df.columns:
            df["process_dt"] = pd.to_datetime(df["av_dt"], errors="coerce")
    if source == "avl" and "av_dt" in df.columns:
        df["av_dt"] = pd.to_datetime(df["av_dt"], errors="coerce")
    if "process_dt" in df.columns:
        df["process_dt"] = pd.to_datetime(df["process_dt"], errors="coerce")
    # Most downstream joins treat these as day-granularity keys.
    if "process_dt" in df.columns:
        df["process_dt"] = df["process_dt"].dt.normalize()
    if "av_dt" in df.columns:
        df["av_dt"] = df["av_dt"].dt.normalize()
    # Join key helpers: make string matching robust to whitespace/case differences.
    if "city_name" in df.columns:
        df["_city_key"] = df["city_name"].astype("string").str.strip().str.upper()
    if "hub_name" in df.columns:
        df["_hub_key"] = df["hub_name"].astype("string").str.strip().str.upper()
    if "product_id" in df.columns:
        df["_product_id_key"] = df["product_id"].astype("string").str.strip().str.upper()
    if "sku_group" in df.columns:
        df["_sku_group_key"] = df["sku_group"].astype("string").str.strip().str.upper()
    return df
def _exclude_hub_prefixes(df: pd.DataFrame, year: str, source: str) -> pd.DataFrame:
    """Drop rows where hub_name starts with excluded prefixes."""
    if "hub_name" not in df.columns:
        return df
    hub = df["hub_name"].astype(str).str.strip().str.upper()
    drop_mask = hub.str.startswith(HUB_EXCLUDE_PREFIXES)
    removed = int(drop_mask.sum())
    if removed > 0:
        print(
            f"  Filter ({year}, {source}): removed {removed:,} rows "
            f"with hub_name starting {HUB_EXCLUDE_PREFIXES}"
        )
    return df.loc[~drop_mask].copy()


def _exclude_cp_products(df: pd.DataFrame, year: str, source: str) -> pd.DataFrame:
    """Drop rows where product_name ends with (CP) — case-insensitive."""
    if "product_name" not in df.columns:
        return df
    drop_mask = (
        df["product_name"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.endswith("(CP)")
    )
    removed = int(drop_mask.sum())
    if removed > 0:
        print(
            f"  Filter ({year}, {source}): removed {removed:,} rows "
            f"with product_name ending '(CP)'"
        )
    return df.loc[~drop_mask].copy()


# ---------------------------------------------------------------------------
# SKU Class Prod mapping (P Master primary, cc cat fallback)
# ---------------------------------------------------------------------------
def _add_sku_class_prod(
    sales: pd.DataFrame,
    p_master: pd.DataFrame,
    cc_cat: pd.DataFrame,
) -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    # Normalize product_id before joining; many upstream feeds contain
    # whitespace/case differences which would otherwise prevent mapping.
    sales = sales.copy()
    sales["_product_id_norm"] = (
        sales["product_id"].astype(str).str.strip().str.upper()
        if "product_id" in sales.columns
        else ""
    )
    p_master = p_master.copy()
    p_master["_product_id_norm"] = (
        p_master["Product id"].astype(str).str.strip().str.upper()
        if "Product id" in p_master.columns
        else ""
    )
    sales = sales.merge(
        p_master,
        on="_product_id_norm",
        how="left",
        suffixes=("", "_pm"),
    )
    sales["_pm_matched_product_id"] = sales["Product id"].notna()
    sales["_pm_sku_present"] = sales["SKU Class Prod"].notna()
    sales["sku_group"] = sales["SKU Class Prod"].copy()
    sales = sales.drop(columns=["Product id", "SKU Class Prod", "_product_id_norm"], errors="ignore")
    mapped_pm = int(sales["sku_group"].notna().sum())
    if "product_name" in sales.columns:
        sales["_product_name_norm"] = sales["product_name"].astype(str).str.strip().str.lower()
    else:
        sales["_product_name_norm"] = ""
    unmapped_mask = sales["sku_group"].isna()
    cc_lookup = cc_cat.rename(columns={"unique product name": "product_name"}).copy()
    cc_lookup["product_name"] = cc_lookup["product_name"].astype(str)
    cc_lookup["_product_name_norm"] = cc_lookup["product_name"].str.strip().str.lower()
    cc_lookup = cc_lookup.loc[cc_lookup["_product_name_norm"].ne("")]
    cc_name_sku_count = (
        cc_lookup.groupby("_product_name_norm")["SKU Class Prod"]
        .nunique()
        .rename("sku_count")
    )
    ambiguous_cc_names = set(cc_name_sku_count[cc_name_sku_count > 1].index)
    unique_cc_names = set(cc_name_sku_count[cc_name_sku_count == 1].index)
    cc_unique_map = (
        cc_lookup.loc[cc_lookup["_product_name_norm"].isin(unique_cc_names)]
        .drop_duplicates(subset=["_product_name_norm"])
        .set_index("_product_name_norm")["SKU Class Prod"]
    )
    if unmapped_mask.any() and "product_name" in sales.columns:
        fallback_values = sales.loc[unmapped_mask, "_product_name_norm"].map(cc_unique_map)
        sales.loc[unmapped_mask, "sku_group"] = fallback_values.values
    mapped_cc = int(sales["sku_group"].notna().sum()) - mapped_pm
    unmapped = int(sales["sku_group"].isna().sum())
    product_id_norm = (
        sales["product_id"].astype(str).str.strip().str.upper()
        if "product_id" in sales.columns
        else pd.Series("", index=sales.index)
    )
    pm_ids = set(
        p_master["_product_id_norm"].astype(str).str.strip().str.upper()
        if "_product_id_norm" in p_master.columns
        else p_master["Product id"].astype(str).str.strip().str.upper()
    )
    has_product_id = product_id_norm.ne("") & product_id_norm.ne("nan")
    in_p_master = product_id_norm.isin(pm_ids)
    has_product_name = sales["_product_name_norm"].ne("") & sales["_product_name_norm"].ne("nan")
    in_cc_any = sales["_product_name_norm"].isin(set(cc_lookup["_product_name_norm"]))
    in_cc_ambiguous = sales["_product_name_norm"].isin(ambiguous_cc_names)
    unmapped_diag = sales.loc[sales["sku_group"].isna()].copy()
    if not unmapped_diag.empty:
        unmapped_diag["has_product_id"] = has_product_id.loc[unmapped_diag.index]
        unmapped_diag["product_id_in_p_master"] = in_p_master.loc[unmapped_diag.index]
        unmapped_diag["p_master_row_has_blank_sku"] = (
            unmapped_diag["_pm_matched_product_id"] & ~unmapped_diag["_pm_sku_present"]
        )
        unmapped_diag["has_product_name"] = has_product_name.loc[unmapped_diag.index]
        unmapped_diag["product_name_in_cc_cat"] = in_cc_any.loc[unmapped_diag.index]
        unmapped_diag["product_name_ambiguous_in_cc_cat"] = in_cc_ambiguous.loc[unmapped_diag.index]
        unmapped_diag["mapping_issue"] = "product_name_not_in_cc_cat"
        unmapped_diag.loc[~unmapped_diag["has_product_id"], "mapping_issue"] = "missing_product_id"
        unmapped_diag.loc[
            unmapped_diag["has_product_id"] & ~unmapped_diag["product_id_in_p_master"],
            "mapping_issue",
        ] = "product_id_not_in_p_master"
        unmapped_diag.loc[
            unmapped_diag["p_master_row_has_blank_sku"],
            "mapping_issue",
        ] = "p_master_row_has_blank_sku"
        unmapped_diag.loc[
            (~unmapped_diag["p_master_row_has_blank_sku"]) & (~unmapped_diag["has_product_name"]),
            "mapping_issue",
        ] = "missing_product_name_for_cc_fallback"
        unmapped_diag.loc[
            (~unmapped_diag["p_master_row_has_blank_sku"]) & unmapped_diag["product_name_ambiguous_in_cc_cat"],
            "mapping_issue",
        ] = "product_name_ambiguous_in_cc_cat"
    stats = {
        "mapped_p_master": mapped_pm,
        "mapped_cc_cat_fallback": mapped_cc,
        "unmapped_rows": unmapped,
        "total_rows": len(sales),
    }
    sales = sales.drop(columns=["_pm_matched_product_id", "_pm_sku_present", "_product_name_norm"], errors="ignore")
    return sales, stats, unmapped_diag
def _export_unmapped_sku_reports(unmapped_diag: pd.DataFrame, year: str, out_dir: Path) -> None:
    """Write CSV/Excel for rows with no SKU Class Prod (P Master + cc cat miss).
    - *products* file: one row per product_id (deduped) with how often it appears — small, good for fixing master data.
    - *rows* file: row-level (city, hub, date) — full export if not too large, else a random sample.
    """
    if unmapped_diag.empty:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    # --- By product (always written; usually far fewer rows than sales rows)
    group_keys = [c for c in ["product_id", "product_name"] if c in unmapped_diag.columns]
    if group_keys:
        prod_summary = (
            unmapped_diag.groupby(group_keys, dropna=False)
            .size()
            .reset_index(name="row_count_in_sales")
            .sort_values("row_count_in_sales", ascending=False)
        )
        reason_counts = (
            unmapped_diag.groupby(group_keys + ["mapping_issue"], dropna=False)
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        prod_summary = prod_summary.merge(reason_counts, on=group_keys, how="left")
        prod_csv = out_dir / f"unmapped_sku_products_{year}.csv"
        prod_summary.to_csv(prod_csv, index=False)
        print(f"    Wrote unmapped product list: {prod_csv} ({len(prod_summary):,} unique products)")
        xlsx_path = out_dir / f"unmapped_sku_products_{year}.xlsx"
        xlsx_df = prod_summary
        if len(xlsx_df) > UNMAPPED_EXCEL_PRODUCT_MAX:
            xlsx_df = xlsx_df.head(UNMAPPED_EXCEL_PRODUCT_MAX)
            print(
                f"    (Excel capped to top {UNMAPPED_EXCEL_PRODUCT_MAX:,} by row_count; full list is in CSV)"
            )
        try:
            xlsx_df.to_excel(xlsx_path, index=False, engine="openpyxl")
            print(f"    Wrote: {xlsx_path}")
        except Exception as e:
            print(f"    [WARN] Could not write Excel products file: {e}")
    # --- Row-level detail (city / hub / date context)
    row_cols = [
        c for c in [
            "product_id",
            "product_name",
            "city_name",
            "hub_name",
            "process_dt",
            "mapping_issue",
            "has_product_id",
            "product_id_in_p_master",
            "p_master_row_has_blank_sku",
            "has_product_name",
            "product_name_in_cc_cat",
            "product_name_ambiguous_in_cc_cat",
        ] if c in unmapped_diag.columns
    ]
    if not row_cols:
        return
    detail = unmapped_diag[row_cols].copy()
    n = len(detail)
    sampled = False
    if n > UNMAPPED_ROW_EXPORT_FULL_MAX:
        detail = detail.sample(n=UNMAPPED_ROW_SAMPLE_SIZE, random_state=42)
        sampled = True
    stem = f"unmapped_sku_rows_{year}" + ("_sample" if sampled else "")
    row_csv = out_dir / f"{stem}.csv"
    detail.to_csv(row_csv, index=False)
    if sampled:
        print(
            f"    Wrote row-level SAMPLE ({UNMAPPED_ROW_SAMPLE_SIZE:,} of {n:,} rows): {row_csv}"
        )
    else:
        print(f"    Wrote row-level detail: {row_csv} ({n:,} rows)")
    if len(detail) <= 104000:
        try:
            row_xlsx = out_dir / f"{stem}.xlsx"
            detail.to_excel(row_xlsx, index=False, engine="openpyxl")
            print(f"    Wrote: {row_xlsx}")
        except Exception as e:
            print(f"    [WARN] Could not write Excel rows file: {e}")
# ---------------------------------------------------------------------------
# Missing Forecast / Avl export helpers
# ---------------------------------------------------------------------------
def _export_missing_rows(
    merged: pd.DataFrame,
    missing_mask: pd.Series,
    label: str,        # "forecast" or "avl"
    year: str,
    out_dir: Path,
    detail_cols: list[str],
) -> None:
    """Write CSV + Excel of rows that failed to match Forecast or Avl.
    - Full export if row count <= MISSING_EXPORT_FULL_MAX
    - Random sample of MISSING_EXPORT_SAMPLE_SIZE rows otherwise
    - Excel capped at MISSING_EXCEL_MAX rows
    """
    detail = merged.loc[missing_mask, [c for c in detail_cols if c in merged.columns]].copy()
    n = len(detail)
    if n == 0:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    sampled = False
    if n > MISSING_EXPORT_FULL_MAX:
        detail = detail.sample(n=MISSING_EXPORT_SAMPLE_SIZE, random_state=42)
        sampled = True
    stem = f"missing_{label}_{year}" + ("_sample" if sampled else "")
    csv_path = out_dir / f"{stem}.csv"
    detail.to_csv(csv_path, index=False)
    if sampled:
        print(f"    [{label} missing] Wrote SAMPLE ({MISSING_EXPORT_SAMPLE_SIZE:,} of {n:,} rows): {csv_path}")
    else:
        print(f"    [{label} missing] Wrote {n:,} rows: {csv_path}")
    xlsx_rows = detail if len(detail) <= MISSING_EXCEL_MAX else detail.head(MISSING_EXCEL_MAX)
    if len(detail) > MISSING_EXCEL_MAX:
        print(f"    [{label} missing] Excel capped to {MISSING_EXCEL_MAX:,} rows (full list in CSV)")
    try:
        xlsx_path = out_dir / f"{stem}.xlsx"
        xlsx_rows.to_excel(xlsx_path, index=False, engine="openpyxl")
        print(f"    [{label} missing] Wrote: {xlsx_path}")
    except Exception as e:
        print(f"    [WARN] Could not write {label} missing Excel: {e}")
# ---------------------------------------------------------------------------
# Forecast merge
# ---------------------------------------------------------------------------
def _merge_forecast(sales: pd.DataFrame, forecast: pd.DataFrame) -> pd.DataFrame:
    """Merge Sales + Forecast using normalized join keys.
    Adds r7_plan, r7_inv, BasePlan, etc. from Forecast.
    """
    join_keys = ["_city_key", "_hub_key", "process_dt", "_product_id_key"]
    for c in join_keys:
        if c not in sales.columns or c not in forecast.columns:
            raise ValueError(
                f"Forecast missing join key '{c}' (sales or forecast). "
                f"Has sales: {c in sales.columns}, forecast: {c in forecast.columns}"
            )
    fc_cols = [c for c in FORECAST_WANT_COLS if c in forecast.columns and c not in sales.columns]
    if not fc_cols:
        print("    No new columns to add from Forecast")
        return sales
    fc_sub_cols = join_keys + fc_cols
    fc_sub = forecast[fc_sub_cols].drop_duplicates(subset=join_keys)
    merged = sales.merge(fc_sub, on=join_keys, how="left", suffixes=("", "_fc"))
    # Drop _fc duplicates only — keep _ helper keys so _merge_avl can use them.
    return merged.drop(columns=[c for c in merged.columns if c.endswith("_fc")], errors="ignore")
# ---------------------------------------------------------------------------
# Availability merge
# ---------------------------------------------------------------------------
def _merge_avl(sales: pd.DataFrame, avl: pd.DataFrame) -> pd.DataFrame:
    """Merge Sales + Availability on (city, hub, date, product_id).
    Availability_8am_8pm.RDS contains product_id natively, so we join
    directly on product_id — no dependency on SKU mapping.
    """
    left_keys = ["_city_key", "_hub_key", "process_dt", "_product_id_key"]
    right_keys = ["_city_key", "_hub_key", "av_dt", "_product_id_key"]
    for c in left_keys:
        if c not in sales.columns:
            raise ValueError(f"Avl merge missing sales key '{c}'.")
    for c in right_keys:
        if c not in avl.columns:
            raise ValueError(f"Avl merge missing avl key '{c}'.")
    merged = sales.merge(
        avl,
        left_on=left_keys,
        right_on=right_keys,
        how="left",
        suffixes=("", "_avl"),
    )
    merged = merged.drop(columns=["av_dt"], errors="ignore")
    # Drop any _avl duplicates
    merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_avl")], errors="ignore")
    # Remove helper keys so downstream code only sees the original columns.
    merged = merged.drop(columns=[c for c in merged.columns if c.startswith("_")], errors="ignore")
    return merged
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 70)
    print("Phase 2: Merge Archive RDS (Sales + Forecast + Availability)")
    print("=" * 70)
    print("\nLoading reference tables...")
    p_master = _load_p_master_sku()
    cc_cat = _load_cc_cat_sku()
    print(f"  P Master: {len(p_master)} product mappings")
    print(f"  cc cat:   {len(cc_cat)} product-name mappings")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nOutput dir: {OUTPUT_DIR}")
    collector = MergeIssueCollector()
    all_unmapped_log: list[dict] = []
    for year, paths in FILES.items():
        print(f"\n{'='*50}")
        print(f"  {year}")
        print(f"{'='*50}")
        sales_path = paths["sales"]
        avl_path = paths["avl"]
        forecast_path = paths.get("forecast")
        if not sales_path.exists():
            collector.error(year, "pre_merge", "Sales file not found", path=str(sales_path))
            print(f"  [ERROR] Sales not found: {sales_path}")
            continue
        if not avl_path.exists():
            collector.error(year, "pre_merge", "Avl file not found", path=str(avl_path))
            print(f"  [ERROR] Avl not found: {avl_path}")
            continue
        # ── Read all files ────────────────────────────────────────────
        print(f"  Reading Sales ({sales_path.name})...")
        sales = _read_rds(sales_path)
        print(f"    {len(sales):,} rows, {len(sales.columns)} cols")
        print(f"  Reading Avl ({avl_path.name})...")
        avl = _read_rds(avl_path)
        print(f"    {len(avl):,} rows, {len(avl.columns)} cols")
        forecast_df: pd.DataFrame | None = None
        if forecast_path and forecast_path.exists():
            print(f"  Reading Forecast ({forecast_path.name})...")
            forecast_df = _read_rds(forecast_path)
            print(f"    {len(forecast_df):,} rows, {len(forecast_df.columns)} cols")
        else:
            print(f"  [SKIP] Forecast not found: {forecast_path}")
        # ── Pre-merge validation ──────────────────────────────────────
        inspect_pre_merge(collector, year, sales, avl, forecast_df, forecast_path)
        # ── Normalize ─────────────────────────────────────────────────
        sales = _normalize(sales, "sales")
        avl = _normalize(avl, "avl")
        if forecast_df is not None:
            forecast_df = _normalize(forecast_df, "forecast")
        sales = _exclude_hub_prefixes(sales, year, "sales")
        avl = _exclude_hub_prefixes(avl, year, "avl")
        if forecast_df is not None:
            forecast_df = _exclude_hub_prefixes(forecast_df, year, "forecast")
        sales = _exclude_cp_products(sales, year, "sales")
        # ── SKU Class Prod mapping ────────────────────────────────────
        print(f"  Adding SKU Class Prod...")
        sales, stats, unmapped_diag = _add_sku_class_prod(sales, p_master, cc_cat)
        inspect_post_sku(collector, year, sales, stats)
        all_unmapped_log.append({"year": year, **stats})
        print(f"    P Master:  {stats['mapped_p_master']:,}")
        print(f"    cc cat:    {stats['mapped_cc_cat_fallback']:,}")
        print(f"    Unmapped:  {stats['unmapped_rows']:,}")
        if stats["unmapped_rows"] > 0:
            pct = 100 * stats["unmapped_rows"] / stats["total_rows"]
            print(f"    Unmapped%: {pct:.2f}%")
            _export_unmapped_sku_reports(unmapped_diag, year, OUTPUT_DIR)
        # ── Forecast merge ────────────────────────────────────────────
        forecast_merged = False
        if forecast_df is not None:
            try:
                print(f"  Merging Forecast...")
                sales = _merge_forecast(sales, forecast_df)
                forecast_merged = True
                print(f"    Done: {len(sales):,} rows")
            except Exception as e:
                print(f"    [WARN] Forecast merge failed: {e}")
                collector.warn(year, "forecast_merge", f"Merge failed: {e}")
        inspect_post_forecast(collector, year, sales, forecast_merged)
        # Export rows missing all forecast columns
        if forecast_merged:
            from merge_validation import FORECAST_COLS
            fc_cols_present = [c for c in FORECAST_COLS if c in sales.columns]
            if fc_cols_present:
                fc_missing_mask = sales[fc_cols_present[0]].isna()
                for c in fc_cols_present[1:]:
                    fc_missing_mask = fc_missing_mask & sales[c].isna()
                if fc_missing_mask.any():
                    _export_missing_rows(
                        sales, fc_missing_mask, "forecast", year, OUTPUT_DIR,
                        detail_cols=["city_name", "hub_name", "process_dt", "product_id", "product_name"],
                    )
        # ── Avl merge ─────────────────────────────────────────────────
        print(f"  Merging Availability...")
        merged = _merge_avl(sales, avl)
        print(f"    Done: {len(merged):,} rows")
        inspect_post_avl(collector, year, merged)
        # Export rows missing avl (flag or instances is NaN)
        if "flag" in merged.columns and "instances" in merged.columns:
            avl_missing_mask = merged["flag"].isna() | merged["instances"].isna()
            if avl_missing_mask.any():
                _export_missing_rows(
                    merged, avl_missing_mask, "avl", year, OUTPUT_DIR,
                    detail_cols=["city_name", "hub_name", "process_dt", "product_id", "product_name"],
                )
        # ── Write output ──────────────────────────────────────────────
        out_path = OUTPUT_DIR / f"merged_{year}.parquet"
        merged.to_parquet(out_path, index=False)
        print(f"  Wrote: {out_path}")
    # ── Unmapped summary ──────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("UNMAPPED ROWS SUMMARY")
    print("=" * 70)
    log_path = OUTPUT_DIR / "unmapped_rows_log.txt"
    with open(log_path, "w") as f:
        f.write("Year\tMapped_PMaster\tMapped_cc_cat\tUnmapped\tTotal\n")
        for entry in all_unmapped_log:
            f.write(f"{entry['year']}\t{entry['mapped_p_master']}\t{entry['mapped_cc_cat_fallback']}\t{entry['unmapped_rows']}\t{entry['total_rows']}\n")
            print(f"  {entry['year']}: {entry['unmapped_rows']:,} unmapped (of {entry['total_rows']:,})")
        total_unmapped = sum(e["unmapped_rows"] for e in all_unmapped_log)
        total_rows = sum(e["total_rows"] for e in all_unmapped_log)
        f.write(f"TOTAL\t-\t-\t{total_unmapped}\t{total_rows}\n")
        print(f"  TOTAL: {total_unmapped:,} unmapped (of {total_rows:,})")
    print(f"  Log: {log_path}")
    # ── Validation report ─────────────────────────────────────────────
    debug_path = write_debug_report(collector, OUTPUT_DIR)
    print(f"  Debug report: {debug_path}")
    if collector.issues:
        print("\n" + "=" * 70)
        print("VALIDATION ISSUES")
        print("=" * 70)
        collector.print_summary()
    if collector.has_errors():
        print("\n  [FAIL] Critical issues. Review merge_debug_report.txt.")
        return 1
    if collector.has_warnings():
        print("\n  [WARN] Non-critical issues. Review merge_debug_report.txt.")
    return 0
if __name__ == "__main__":
    sys.exit(main())