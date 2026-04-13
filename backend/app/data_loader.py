"""
Data loading pipeline – reads RDS/Parquet files + Google Sheet reference tables,
computes product-level availability, and produces Avl_Corr_Sales.

Layer: Pipeline / Data
Replicates the logic from Opportunity_Loss_Code_v2.ipynb (cells 0-25).

Input: RDS or Parquet files (per year), Google Sheets (P_Master, Avl_Flag, etc.)
Output: DataFrame with Avl_Corr_Sales, Avl_Corr_Revenue, grouping columns
Failure modes: Missing files, GSheet auth failure, schema mismatch

Pipeline summary
================
1. Read RDS files → raw product-hub-day data
2. Merge with P_Master     → get Anchor ID / Anchor Name per product
3. Merge with Avl_Flag     → get per-product availability flag + SKU Class
4. Compute plan_sum        → total r7_inv per (hub, date, anchor)
5. Derive availability flags (simple vs group) based on plan_sum
6. Compute weighted flag / instances using r7_plan_rev as weight
7. Calculate product_level_avl = wgt_flag / wgt_instances
8. Merge with Subcat-Type Mapping → get "Type" per sub-category
9. Compute OOS (out-of-stock) time and hour
10. Merge with SellThroughFactor → hourly sell-through factor
11. Avl_Corr_Sales = ceil(sales / salethroughfactor)
    Avl_Corr_Revenue = revenue / salethroughfactor
"""

from __future__ import annotations

import gc
import logging
import os
import time

import gspread
import numpy as np
import pandas as pd
import pyreadr
from oauth2client.service_account import ServiceAccountCredentials

from app.config import (
    GSHEET_AVL_FLAG,
    GSHEET_CREDENTIALS_PATH,
    GSHEET_HUB_MAPPING,
    GSHEET_P_MASTER,
    GSHEET_SCOPES,
    GSHEET_SELL_THROUGH_FACTOR,
    GSHEET_SUBCAT_TYPE_MAPPING,
    PARQUET_PATHS,
    RDS_COLUMNS_TO_KEEP,
    RDS_PATHS,
    TOTAL_OOS_HOURS,
)

logger = logging.getLogger(__name__)


# =====================================================================
#  Google Sheets client (singleton to avoid re-authenticating)
# =====================================================================

_gspread_client: gspread.Client | None = None


def _get_gspread_client() -> gspread.Client:
    """Return a cached gspread client, creating one on first call."""
    global _gspread_client
    if _gspread_client is None:
        logger.info(f"Authenticating with Google Sheets using: {GSHEET_CREDENTIALS_PATH}")
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            GSHEET_CREDENTIALS_PATH, GSHEET_SCOPES
        )
        _gspread_client = gspread.authorize(creds)
        logger.info("Google Sheets authentication successful")
    return _gspread_client


def _read_gsheet(url: str, worksheet: str) -> pd.DataFrame:
    """Read a Google Sheet worksheet into a DataFrame.

    Uses ``get_all_values()`` which returns all cells as strings.
    Row 0 is treated as the header.  Duplicate column names are
    deduplicated by appending _1, _2, etc.

    Raises
    ------
    RuntimeError
        If GSheet read fails (auth, network, permissions).
    """
    logger.info(f"Reading Google Sheet: {worksheet} from {url[:60]}...")
    try:
        client = _get_gspread_client()
        spreadsheet = client.open_by_url(url)
        ws = spreadsheet.worksheet(worksheet)
        data = ws.get_all_values()
    except Exception as e:
        logger.error(f"GSheet read failed: {worksheet} from {url[:60]}: {e}")
        raise RuntimeError(f"Failed to read Google Sheet '{worksheet}': {e}") from e
    logger.info(f"  -> Got {len(data)} rows from '{worksheet}'")
    headers = data[0]
    seen: dict[str, int] = {}
    deduped: list[str] = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            deduped.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            deduped.append(h)
    return pd.DataFrame(data[1:], columns=deduped)


def _read_gsheet_with_header_row(url: str, worksheet: str, header_row: int = 1) -> pd.DataFrame:
    """Read a Google Sheet where the actual column headers are on a
    specific row (0-indexed).  Rows before the header are skipped.

    Raises
    ------
    RuntimeError
        If GSheet read fails.
    """
    logger.info(f"Reading Google Sheet (header_row={header_row}): {worksheet} from {url[:60]}...")
    try:
        client = _get_gspread_client()
        spreadsheet = client.open_by_url(url)
        ws = spreadsheet.worksheet(worksheet)
        data = ws.get_all_values()
    except Exception as e:
        logger.error(f"GSheet read failed: {worksheet}: {e}")
        raise RuntimeError(f"Failed to read Google Sheet '{worksheet}': {e}") from e
    headers = data[header_row]
    rows = data[header_row + 1:]
    logger.info(f"  -> Got {len(rows)} data rows from '{worksheet}'")
    return pd.DataFrame(rows, columns=headers)


# =====================================================================
#  Reference table loaders (cached after first call for performance)
# =====================================================================

_cache: dict[str, pd.DataFrame] = {}


def _load_p_master() -> pd.DataFrame:
    """Load P_Master sheet → columns: Product id, Anchor ID, Anchor Name,
    Cut Classification.

    This maps every product_id to its Anchor group (used later to
    compute plan_sum at the anchor level for availability logic).
    ``Cut Classification`` is used as the grouping dimension ("cut class")
    in Levels 3 and 5.
    """
    if "p_master" not in _cache:
        df = _read_gsheet(GSHEET_P_MASTER["url"], GSHEET_P_MASTER["worksheet"])
        _cache["p_master"] = df[["Product id", "Anchor ID", "Anchor Name", "Cut Classification"]]
    return _cache["p_master"]


def _load_avl_flag() -> pd.DataFrame:
    """Load Avl_Flag sheet → columns: product_id, Product Name, Avl Flag.

    - ``Avl Flag`` = 1 means the product has individual-level availability
      tracking; 0 means we fall back to group-level flags.
    """
    if "avl_flag" not in _cache:
        df = _read_gsheet(GSHEET_AVL_FLAG["url"], GSHEET_AVL_FLAG["worksheet"])
        _cache["avl_flag"] = df[["product_id", "Product Name", "Avl Flag"]]
    return _cache["avl_flag"]


def load_hub_mapping() -> tuple[dict[str, str], set[str]]:
    """Load the hub mapping sheet and return a remap dict + current hubs set.

    The sheet has columns: city_name, hub_name (current), ref hub (old).
    - ``ref hub = "old"`` means the hub in ``hub_name`` is itself a legacy
      hub that is still active — no remapping needed.
    - Otherwise ``ref hub`` is the name of a closed hub whose historical
      data should be reassigned to the current hub in ``hub_name``.

    Returns
    -------
    (remap, current_hubs)
        remap : {old_hub_name → current_hub_name}  (case-stripped)
        current_hubs : set of all current hub names
    """
    if "hub_mapping" not in _cache:
        df = _read_gsheet(GSHEET_HUB_MAPPING["url"], GSHEET_HUB_MAPPING["worksheet"])
        df.columns = df.columns.str.strip()
        for col in ("city_name", "hub_name", "ref hub"):
            df[col] = df[col].astype(str).str.strip()

        current_hubs: set[str] = set(df["hub_name"].unique())

        remap: dict[str, str] = {}
        for _, row in df.iterrows():
            ref = row["ref hub"]
            if ref.lower() != "old" and ref:
                remap[ref] = row["hub_name"]

        _cache["hub_mapping"] = (remap, current_hubs)
        logger.info(
            f"  Hub mapping loaded: {len(current_hubs)} current hubs, "
            f"{len(remap)} old→current remaps"
        )
    return _cache["hub_mapping"]


def _load_subcat_type_mapping() -> pd.DataFrame:
    """Load Subcat-Type Mapping → columns: Sub-category, Type.

    "Type" (e.g. Perishable, Non-Perishable) determines which sell-through
    curve to use when estimating the time of stock-out.
    """
    if "subcat_type" not in _cache:
        df = _read_gsheet(
            GSHEET_SUBCAT_TYPE_MAPPING["url"],
            GSHEET_SUBCAT_TYPE_MAPPING["worksheet"],
        )
        _cache["subcat_type"] = df[["Sub-category", "Type"]]
    return _cache["subcat_type"]


def _load_sell_through_factor() -> pd.DataFrame:
    """Load SellThroughFactor → factors by (Cat/Type, day, hour, city).

    The sell-through factor tells us what fraction of a full day's sales
    would have occurred by a given hour.  We use it to "correct" the
    observed sales for products that went out of stock partway through
    the day:  Avl_Corr_Sales = ceil(sales / salethroughfactor).
    """
    if "stf" not in _cache:
        df = _read_gsheet(
            GSHEET_SELL_THROUGH_FACTOR["url"],
            GSHEET_SELL_THROUGH_FACTOR["worksheet"],
        )
        df["hour"] = df["hour"].astype(int)
        _cache["stf"] = df
    return _cache["stf"]


# =====================================================================
#  RDS loading
# =====================================================================

def _load_parquet(path: str) -> pd.DataFrame:
    """Load a merged parquet file (archive years or 2026).

    Requires ``pyarrow>=19.0.1`` (see ``requirements.txt``) — older PyArrow can raise
    ``Repetition level histogram size mismatch`` on some parquet files.
    """
    t0 = time.time()
    logger.info(f"  Reading parquet: {path} ...")
    df = pd.read_parquet(path)
    logger.info(f"  Parquet loaded: {len(df)} rows, {len(df.columns)} cols ({time.time()-t0:.1f}s)")
    if "process_dt" in df.columns:
        df["process_dt"] = pd.to_datetime(df["process_dt"])
    return df


def _load_rds_for_year(year_key: str, frames: list[pd.DataFrame]) -> bool:
    """Load RDS file(s) for a year and append to frames. Used as fallback when parquet doesn't exist.
    Returns True if a frame was appended, False otherwise."""
    for path in RDS_PATHS[year_key]:
        if not os.path.exists(path):
            logger.warning(f"  RDS for {year_key} not found ({path}) — skipping")
            continue
        t_rds = time.time()
        logger.info(f"  Reading RDS (fallback): {path} ...")
        result = pyreadr.read_r(path)
        frame = next(iter(result.values()))
        logger.info(f"  RDS loaded: {len(frame)} rows, {len(frame.columns)} cols ({time.time()-t_rds:.1f}s)")
        frames.append(frame)
        return True
    logger.warning(f"  No RDS file found for {year_key} — year will be skipped")
    return False


def _available_year_keys() -> list[str]:
    """Return year keys whose data files actually exist on disk."""
    available: list[str] = []
    for yk, paths in RDS_PATHS.items():
        if any(os.path.exists(p) for p in paths):
            available.append(yk)
    for yk, path in PARQUET_PATHS.items():
        if os.path.exists(path):
            available.append(yk)
    return sorted(set(available))


def load_rds_data(
    year_keys: list[str] | None = None,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Read and concatenate data files for the requested years.

    For years in ``PARQUET_PATHS`` (archive: 2023-2025), reads pre-merged
    parquet files produced by ``scripts/merge_archive_rds.py``.
    For years in ``RDS_PATHS`` (live: 2026), reads RDS files directly.
    Years whose files don't exist on disk are skipped with a warning.

    Parameters
    ----------
    year_keys : list[str] | None
        Which year groups to load (e.g. ["2023", "2024", "2025", "2026"]).
        If None, only years whose files exist on disk are loaded.
    start_date, end_date : Timestamp | None
        Optional date range filter (inclusive on both ends).

    Returns
    -------
    DataFrame with ``process_dt`` as datetime and only the columns listed
    in ``RDS_COLUMNS_TO_KEEP``.  No hub or city filter is applied.
    """
    if year_keys is None:
        year_keys = _available_year_keys()
    logger.info(f"  load_rds_data: year_keys={year_keys}")

    frames: list[pd.DataFrame] = []
    sources_used: list[str] = []

    for yk in year_keys:
        if yk in PARQUET_PATHS:
            pq_path = PARQUET_PATHS[yk]
            if os.path.exists(pq_path):
                frame = _load_parquet(pq_path)
                frames.append(frame)
                sources_used.append(f"{yk}:parquet")
            elif yk in RDS_PATHS:
                # Fallback to RDS when parquet doesn't exist (e.g. 2026 before conversion)
                logger.info(f"  Parquet for {yk} not found — falling back to RDS")
                if _load_rds_for_year(yk, frames):
                    sources_used.append(f"{yk}:rds_fallback")
            else:
                logger.warning(f"  Parquet for {yk} not found ({pq_path}) — skipping. "
                               f"Run scripts/merge_archive_rds.py or convert_6w_to_parquet.py to generate it.")
        elif yk in RDS_PATHS:
            for path in RDS_PATHS[yk]:
                if not os.path.exists(path):
                    logger.warning(f"  RDS for {yk} not found ({path}) — skipping")
                    continue
                t_rds = time.time()
                logger.info(f"  Reading RDS: {path} ...")
                result = pyreadr.read_r(path)
                frame = next(iter(result.values()))
                logger.info(f"  RDS loaded: {len(frame)} rows, {len(frame.columns)} cols ({time.time()-t_rds:.1f}s)")
                frames.append(frame)
                sources_used.append(f"{yk}:rds")
                break  # Only first available RDS per year (matches fallback behavior)
        else:
            logger.warning(f"  Year '{yk}' not configured in RDS_PATHS or PARQUET_PATHS — skipping")

    if not frames:
        raise ValueError(
            f"No data files found for year_keys={year_keys}. "
            f"For archive years (2023-2025), run: py scripts/merge_archive_rds.py. "
            f"For 2026, run: py scripts/convert_6w_to_parquet.py (or ensure 6w_v3.RDS exists)."
        )

    logger.info(f"  Data sources used: {sources_used}")
    logger.info(f"  Concatenating {len(frames)} frames...")
    df = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()
    logger.info(f"  Total rows after concat: {len(df)}")
    df["process_dt"] = pd.to_datetime(df["process_dt"])

    available_cols = [c for c in RDS_COLUMNS_TO_KEEP if c in df.columns]
    dropped_cols = [c for c in RDS_COLUMNS_TO_KEEP if c not in df.columns]
    if dropped_cols:
        logger.warning(f"  Columns not in data (dropped): {dropped_cols}")
    df = df[available_cols]

    rows_before_filter = len(df)
    if start_date is not None:
        df = df[df["process_dt"] >= pd.Timestamp(start_date)]
        logger.info(f"  Date filter start_date>={start_date}: {len(df)} rows (was {rows_before_filter})")
    if end_date is not None:
        rows_before = len(df)
        df = df[df["process_dt"] <= pd.Timestamp(end_date)]
        logger.info(f"  Date filter end_date<={end_date}: {len(df)} rows (was {rows_before})")

    logger.info(f"  load_rds_data complete: {len(df)} rows, {len(df.columns)} cols")
    return df


# =====================================================================
#  Availability-Corrected Sales pipeline
#  (replication of Opportunity_Loss_Code_v2.ipynb, cells 10-25)
#
#  For archive years (2023-2025), group_flag / group_instances / r7_inv
#  may be missing.  In that case we use a simplified path:
#    product_level_avl = flag / instances
# =====================================================================

def compute_avl_corr_sales(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Run the full availability-correction pipeline on raw RDS data.

    This function replicates the exact logic from the notebook:

    Step 1: Merge with P_Master to get Anchor ID per product.
    Step 2: Merge with Avl_Flag to get per-product availability flag
            and SKU Class (cut classification).
    Step 3: Compute plan_sum = total r7_inv per (hub, date, anchor).
            This tells us whether ANY product in the anchor group had
            inventory planned.
    Step 4: Derive simple vs group availability flags.
            - If plan_sum == 0 (no planned inventory for the anchor
              group at this hub on this day), use group-level flags
              regardless of Avl Flag.
            - If plan_sum > 0 and Avl Flag == 1, use individual-level
              flag/instances.
            - If plan_sum > 0 and Avl Flag != 1, use group-level
              flag/instances.
    Step 5: Weight the flags by r7_plan_rev (planned revenue) to create
            wgt_flag and wgt_instances.
    Step 6: product_level_avl = wgt_flag / wgt_instances  (0 if NaN).
            This is a 0-to-1 fraction representing how much of the day
            the product was available.
    Step 7: Merge with Subcat-Type Mapping to get "Type" per sub-category.
    Step 8: Estimate out-of-stock time:
            oos_time = process_dt + 8 hours + (product_level_avl * 12 hours)
            This assumes the store opens at 8am and operates for 12 hours.
            A product with 100% availability goes OOS at 8pm (never OOS).
            A product with 0% availability went OOS at 8am (open).
    Step 9: Merge with SellThroughFactor on (Type, day, oos_hour, city).
    Step 10: Avl_Corr_Sales = ceil(sales / salethroughfactor)
             This "inflates" observed sales to what they would have been
             if the product had been available all day.

    Returns
    -------
    DataFrame at the product-hub-day level with Avl_Corr_Sales and all
    grouping columns needed by downstream level computations.
    """
    df = raw_df
    del raw_df
    t0 = time.time()
    logger.info(f"  compute_avl_corr_sales: starting with {len(df)} rows")

    # Detect whether we have the full set of columns (2026 live data)
    # or the simplified set (archive parquet from 2023-2025).
    has_group_cols = "group_flag" in df.columns and "group_instances" in df.columns
    has_r7 = "r7_inv" in df.columns and "r7_plan_rev" in df.columns
    use_full_pipeline = has_group_cols and has_r7
    if not use_full_pipeline:
        logger.info("  Archive mode: missing group_flag/group_instances/r7 cols -> simplified availability")

    p_master = _load_p_master()
    logger.info(f"  Step 1: Merging P_Master... (GSheet loaded in {time.time()-t0:.1f}s)")
    df = df.merge(
        p_master, left_on="product_id", right_on="Product id", how="left"
    )
    unmapped_pmaster = df["Anchor ID"].isna().sum()
    if unmapped_pmaster > 0:
        logger.warning(f"  Step 1: {unmapped_pmaster} rows have no P_Master match (Anchor ID=NaN)")
    df.rename(columns={"Cut Classification": "SKU Class Prod"}, inplace=True)
    unmapped_cut = df["SKU Class Prod"].isna().sum()
    if unmapped_cut > 0:
        logger.warning(f"  Step 1: {unmapped_cut} rows have no Cut Classification from P_Master")
    logger.info(f"  Step 1 done: {len(df)} rows")

    avl_flag = _load_avl_flag()
    logger.info("  Step 2: Merging Avl_Flag...")
    df = df.merge(avl_flag, on="product_id", how="left")
    unmapped_avl = df["Avl Flag"].isna().sum()
    if unmapped_avl > 0:
        logger.warning(f"  Step 2: {unmapped_avl} rows have no Avl_Flag match (Avl Flag=NaN)")
    logger.info(f"  Step 2 done: {len(df)} rows")

    # ── Steps 3-6: Availability logic (full or simplified) ──────────
    if use_full_pipeline:
        logger.info("  Step 3: Computing plan_sum (full pipeline)...")
        df["plan_sum"] = (
            df.groupby(["hub_name", "process_dt", "Anchor ID"])["r7_inv"]
            .transform("sum")
        )
        logger.info("  Step 3 done")

        df["simple_flag_when_SP_0"] = np.where(
            df["plan_sum"] == 0, df["group_flag"], df["flag"],
        )
        df["simple_instances_when_SP_0"] = np.where(
            df["plan_sum"] == 0, df["group_instances"], df["instances"],
        )
        df["group_flag_when_SP_0"] = df["group_flag"]
        df["group_instances_when_SP_0"] = df["group_instances"]

        avl_flag_numeric = pd.to_numeric(df["Avl Flag"], errors="coerce").fillna(0)

        df["wgt_flag"] = np.where(
            avl_flag_numeric == 1,
            df["simple_flag_when_SP_0"] * df["r7_plan_rev"],
            df["group_flag_when_SP_0"] * df["r7_plan_rev"],
        )
        df["wgt_instances"] = np.where(
            avl_flag_numeric == 1,
            df["simple_instances_when_SP_0"] * df["r7_plan_rev"],
            df["group_instances_when_SP_0"] * df["r7_plan_rev"],
        )

        logger.info("  Step 6: Product-level availability (full)...")
        df["product_level_avl"] = (df["wgt_flag"] / df["wgt_instances"]).fillna(0)
        # If Forecast or Availability mapping failed during archive merging, the
        # weight inputs become NaN which would otherwise force availability to 0.
        # In those cases we assume 100% availability so Avl_Corr_Sales
        # returns the actual sales (no "zeroing" effect).
        forecast_missing = df["r7_plan_rev"].isna() if "r7_plan_rev" in df.columns else pd.Series(False, index=df.index)
        group_missing = (
            (df["group_instances"].isna() if "group_instances" in df.columns else pd.Series(False, index=df.index))
            | (df["group_flag"].isna() if "group_flag" in df.columns else pd.Series(False, index=df.index))
        )
        ind_missing = pd.Series(False, index=df.index)
        if "instances" in df.columns and "flag" in df.columns:
            ind_missing = (avl_flag_numeric == 1) & (df["instances"].isna() | df["flag"].isna())

        mapping_missing = forecast_missing | group_missing | ind_missing
        missing_n = int(mapping_missing.sum())
        if missing_n > 0:
            logger.info(f"  Step 6: {missing_n:,} rows missing forecast/avl inputs -> assuming product_level_avl=1.0")
            df.loc[mapping_missing, "product_level_avl"] = 1.0
        logger.info("  Step 6 done")
    else:
        logger.info("  Steps 3-6 (simplified): product_level_avl = flag / instances")
        if "flag" in df.columns and "instances" in df.columns:
            flag_vals = pd.to_numeric(df["flag"], errors="coerce").fillna(0)
            inst_vals = pd.to_numeric(df["instances"], errors="coerce").fillna(0)
            inst_safe = inst_vals.replace(0, np.nan)
            df["product_level_avl"] = (flag_vals / inst_safe).fillna(0)
            # Same idea: if join inputs are missing (NaN), don't force availability to 0.
            missing_avl_inputs = df["flag"].isna() | df["instances"].isna()
            missing_n = int(missing_avl_inputs.sum())
            if missing_n > 0:
                logger.info(f"  Step 6 (simplified): {missing_n:,} rows missing avl inputs -> assuming product_level_avl=1.0")
                df.loc[missing_avl_inputs, "product_level_avl"] = 1.0
        else:
            logger.warning("  flag/instances columns missing — setting product_level_avl=1 (no correction)")
            df["product_level_avl"] = 1.0
        logger.info("  Simplified availability done")

    # ── Memory cleanup: drop all intermediate columns ─────────────
    # After availability is computed, only product_level_avl is needed
    # going forward.  Drop everything else to free ~60-70% of memory
    # before the remaining merges.
    cols_needed = {
        "city_name", "hub_name", "sub_category", "process_dt",
        "sales", "revenue", "product_id", "product_name",
        "SKU Class Prod", "product_level_avl",
    }
    drop_cols = [c for c in df.columns if c not in cols_needed]
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)
        gc.collect()
        logger.info(f"  Memory cleanup: dropped {len(drop_cols)} intermediate columns, kept {len(df.columns)}")

    # ── Step 7: Merge Subcat-Type Mapping ───────────────────────────
    # Maps sub_category → Type (e.g. "Perishable").  Type is used
    # to look up the correct sell-through curve.
    subcat_type = _load_subcat_type_mapping()
    df = df.merge(
        subcat_type,
        left_on="sub_category",
        right_on="Sub-category",
        how="left",
    )
    unmapped_subcat = df["Type"].isna().sum()
    if unmapped_subcat > 0:
        logger.warning(f"  Step 7: {unmapped_subcat} rows have no Subcat-Type match (Type=NaN)")

    # ── Step 8: Estimate out-of-stock time ──────────────────────────
    # Store opens at 8am, operates for TOTAL_OOS_HOURS (12) hours.
    # oos_time = 8am + (availability_fraction × 12 hours)
    # oos_hour = the hour of day the product went OOS.
    df["day"] = df["process_dt"].dt.strftime("%a")  # Mon, Tue, …
    df["oos_time"] = (
        df["process_dt"]
        + pd.Timedelta(hours=8)
        + pd.to_timedelta(df["product_level_avl"] * TOTAL_OOS_HOURS, unit="h")
    )
    df["oos_hour"] = df["oos_time"].dt.hour

    # ── Step 9: Merge SellThroughFactor ─────────────────────────────
    # The factor table has columns: Cat, day, hour, city_name,
    # salethroughfactor.  We join on (Type↔Cat, day, oos_hour↔hour,
    # city_name).
    stf = _load_sell_through_factor()
    logger.info(f"  Step 9: Merging SellThroughFactor ({len(stf)} rows)...")
    df = df.merge(
        stf,
        left_on=["Type", "day", "oos_hour", "city_name"],
        right_on=["Cat", "day", "hour", "city_name"],
        how="left",
    )
    logger.info(f"  Step 9 done: {len(df)} rows")
    df["salethroughfactor"] = pd.to_numeric(
        df["salethroughfactor"], errors="coerce"
    )
    missing_stf = df["salethroughfactor"].isna() | (df["salethroughfactor"] == 0)
    if missing_stf.sum() > 0:
        logger.warning(f"  Step 9: {missing_stf.sum()} rows have missing/zero SellThroughFactor — using raw sales")

    # ── Step 10: Availability-corrected sales & revenue ─────────────
    # Avl_Corr_Sales  = ceil(sales / salethroughfactor)
    #   → "What would sales have been if the product was available all day?"
    # Avl_Corr_Revenue = revenue / salethroughfactor
    logger.info(f"  Step 10: Computing Avl_Corr_Sales... (elapsed {time.time()-t0:.1f}s)")
    stf_safe = df["salethroughfactor"].replace(0, np.nan)
    df["Avl_Corr_Sales"] = np.ceil(df["sales"] / stf_safe).fillna(df["sales"])
    df["Avl_Corr_Revenue"] = (df["revenue"] / stf_safe).fillna(df["revenue"])
    logger.info(f"  Step 10 done (total pipeline: {time.time()-t0:.1f}s)")

    # Drop rows with missing dates or sales — they can't contribute to analysis
    rows_before_drop = len(df)
    df = df.dropna(subset=["process_dt"])
    dropped = rows_before_drop - len(df)
    if dropped > 0:
        logger.warning(f"  Dropped {dropped} rows with missing process_dt")

    # ISO week number (used for baseline week references)
    df["week"] = df["process_dt"].dt.isocalendar().week.astype("Int64").fillna(0).astype(int)

    # ── Return only the columns needed downstream ───────────────────
    keep = [
        "city_name",       # grouping: city
        "hub_name",        # grouping: hub
        "sub_category",     # grouping: sub-category
        "process_dt",      # date
        "week",            # ISO week number
        "day",             # day-of-week abbreviation (Mon, Tue, …)
        "sales",           # raw sales (before correction)
        "revenue",         # raw revenue
        "Avl_Corr_Sales",  # availability-corrected sales
        "Avl_Corr_Revenue",# availability-corrected revenue
        "product_id",      # product identifier
        "product_name",    # product display name
        "SKU Class Prod",  # cut classification (from P Master)
    ]
    available = [c for c in keep if c in df.columns]
    missing_keep = [c for c in keep if c not in df.columns]
    if missing_keep:
        logger.warning(f"  compute_avl_corr_sales: output missing columns: {missing_keep}")
    return df[available]


# =====================================================================
#  Convenience function: load + compute in one shot
# =====================================================================

_product_cache: dict[str, pd.DataFrame] = {}


def load_and_compute(
    year_keys: list[str] | None = None,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Load RDS data for the given years, run the full availability-
    correction pipeline, and return a product-level DataFrame with
    Avl_Corr_Sales ready for aggregation by the level modules.

    Results are cached in memory so subsequent requests with the same
    year_keys don't re-read the RDS files or re-run the pipeline.

    Parameters
    ----------
    year_keys : list of year strings to load (default: all in config)
    start_date, end_date : optional date filters
    """
    if year_keys is None:
        year_keys = _available_year_keys()
    cache_key = "|".join(year_keys)
    if cache_key in _product_cache:
        logger.info(f"  Using cached product DataFrame for keys={year_keys}")
        df = _product_cache[cache_key]
    else:
        raw = load_rds_data(year_keys, start_date, end_date)
        df = compute_avl_corr_sales(raw)
        _product_cache[cache_key] = df
        logger.info(f"  Cached product DataFrame for keys={year_keys} ({len(df)} rows)")

    if start_date is not None or end_date is not None:
        rows_before = len(df)
        if start_date is not None:
            df = df[df["process_dt"] >= pd.Timestamp(start_date)]
        if end_date is not None:
            df = df[df["process_dt"] <= pd.Timestamp(end_date)]
        logger.info(f"  load_and_compute: date filter applied — {len(df)} rows (was {rows_before})")
    return df
