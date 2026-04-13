"""
Baseline computation engine.

Baseline = median of Avl_Corr_Revenue on the same weekday from the 5 weeks
preceding the festival date, EXCLUDING only candidate dates that fall
exactly on a known festival date.

The engine is grouping-agnostic: the caller passes a pre-aggregated
DataFrame (one row per group x date) and specifies which columns define
the groups.  This allows the same function to work for all 5 levels:

  Level 1 (City):                 group_cols = ["city_name"]
  Level 2 (City-SubCat):          group_cols = ["city_name", "sub_category"]
  Level 3 (City-SubCat-CutClass): group_cols = ["city_name", "sub_category", "SKU Class Prod"]
  Level 4 (City-Hub):             group_cols = ["city_name", "hub_name"]
  Level 5 (City-Hub-SubCat-Cut):  group_cols = ["city_name", "hub_name", "sub_category", "SKU Class Prod"]

Algorithm
---------
1. Normalize all festival dates into a flat set of Timestamps.
2. Walk backwards from the target date, one week at a time.
   - Each candidate is the same weekday N weeks earlier.
   - Skip a candidate ONLY if that exact date is a festival.
   - Collect until we have 5 valid candidates (or exhaust max_lookback).
3. Filter the daily aggregated data to those candidate dates.
4. Group by the requested columns and take the median -> baseline.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pandas as pd

logger = logging.getLogger(__name__)


def compute_baseline(
    daily_agg: pd.DataFrame,
    target_date: pd.Timestamp,
    festival_dates: set[pd.Timestamp],
    value_col: str = "Avl_Corr_Revenue",
    group_cols: list[str] | None = None,
    n_weeks: int = 5,
    max_lookback_weeks: int = 26,
) -> pd.DataFrame:
    """Compute baselines for every group on a single target date.

    Parameters
    ----------
    daily_agg : DataFrame
        Pre-aggregated data.  Must contain ``process_dt``, ``value_col``,
        and all columns listed in ``group_cols``.  One row per (group, date).
    target_date : Timestamp
        The festival date whose baseline we are computing.
    festival_dates : set[Timestamp]
        All known festival dates across all years.  Only the exact date
        is excluded (not the whole week).
    value_col : str
        The column whose median forms the baseline (default "Avl_Corr_Revenue").
    group_cols : list[str] | None
        Grouping columns.  If None, a single global baseline is returned.
    n_weeks : int
        How many valid same-weekday observations to collect (default 5).
    max_lookback_weeks : int
        Maximum number of weeks to look back (default 26).  If we can't
        find 5 non-festival weeks within 26, we use whatever we found.

    Returns
    -------
    DataFrame with ``group_cols`` + ``["baseline"]``.
    """
    # ── Step 1: Build a flat set of normalized festival dates ─────────
    normalized_festival_dates: set[pd.Timestamp] = {
        pd.Timestamp(fd).normalize() for fd in festival_dates
    }

    # Log the data range available for diagnostics
    data_min = daily_agg["process_dt"].min()
    data_max = daily_agg["process_dt"].max()
    logger.info(
        f"  Baseline for {target_date.date()} | "
        f"data range: {data_min.date()} to {data_max.date()} | "
        f"{len(daily_agg)} rows in daily_agg"
    )

    # ── Step 2: Walk backwards collecting candidate same-weekday dates ──
    candidates: list[pd.Timestamp] = []
    skipped_festival: list[pd.Timestamp] = []
    for w in range(1, max_lookback_weeks + 1):
        candidate = pd.Timestamp(target_date - timedelta(weeks=w)).normalize()

        if candidate in normalized_festival_dates:
            skipped_festival.append(candidate)
            continue

        candidates.append(candidate)
        if len(candidates) >= n_weeks:
            break

    logger.info(
        f"  -> {len(candidates)} candidates, "
        f"{len(skipped_festival)} skipped (exact festival day). "
        f"Candidates: {[str(c.date()) for c in candidates]}"
    )

    # ── Step 3: Filter daily data to those candidate dates ──────────
    mask = daily_agg["process_dt"].dt.normalize().isin(candidates)
    subset = daily_agg.loc[mask].copy()

    logger.info(
        f"  -> {len(subset)} rows matched in data"
    )

    if subset.empty:
        logger.warning(
            f"  No baseline data for {target_date.date()}. "
            f"Returning zero baselines."
        )
        if group_cols:
            groups = daily_agg[group_cols].drop_duplicates().copy()
            groups["baseline"] = 0.0
            return groups
        return pd.DataFrame({"baseline": [0.0]})

    # ── Step 4: Median per group -> baseline ─────────────────────────
    if group_cols:
        result = (
            subset.groupby(group_cols, as_index=False)[value_col]
            .median()
            .rename(columns={value_col: "baseline"})
        )
    else:
        result = pd.DataFrame({"baseline": [subset[value_col].median()]})

    return result


def compute_baseline_with_std(
    daily_agg: pd.DataFrame,
    target_date: pd.Timestamp,
    festival_dates: set[pd.Timestamp],
    value_col: str = "Avl_Corr_Sales",
    group_cols: list[str] | None = None,
    n_weeks: int = 5,
    max_lookback_weeks: int = 26,
) -> pd.DataFrame:
    """Compute baseline (median) and std per group for a single target date.
    Used by trends to show % change from baseline and variance band.

    Returns
    -------
    DataFrame with ``group_cols`` + ``["baseline", "std"]``.
    """
    normalized_festival_dates: set[pd.Timestamp] = {
        pd.Timestamp(fd).normalize() for fd in festival_dates
    }

    candidates: list[pd.Timestamp] = []
    for w in range(1, max_lookback_weeks + 1):
        candidate = pd.Timestamp(target_date - timedelta(weeks=w)).normalize()
        if candidate in normalized_festival_dates:
            continue
        candidates.append(candidate)
        if len(candidates) >= n_weeks:
            break

    mask = daily_agg["process_dt"].dt.normalize().isin(candidates)
    subset = daily_agg.loc[mask].copy()

    if len(candidates) < n_weeks:
        logger.warning(
            f"  compute_baseline_with_std: only {len(candidates)} candidates for {target_date.date()} "
            f"(wanted {n_weeks}) — may affect variance estimate"
        )
    if subset.empty:
        logger.warning(
            f"  compute_baseline_with_std: no data for candidates {[str(c.date()) for c in candidates]} "
            f"— returning zero baselines"
        )
        if group_cols:
            groups = daily_agg[group_cols].drop_duplicates().copy()
            groups["baseline"] = 0.0
            groups["std"] = 0.0
            return groups
        return pd.DataFrame({"baseline": [0.0], "std": [0.0]})

    if group_cols:
        agg = subset.groupby(group_cols)[value_col].agg(["median", "std"])
        agg = agg.rename(columns={"median": "baseline"})
        agg["std"] = agg["std"].fillna(0.0)
        return agg.reset_index()
    else:
        return pd.DataFrame({
            "baseline": [subset[value_col].median()],
            "std": [subset[value_col].std() if len(subset) > 1 else 0.0],
        })


def compute_baselines_for_years(
    daily_agg: pd.DataFrame,
    festival_year_dates: dict,
    all_festival_dates: set[pd.Timestamp],
    value_col: str = "Avl_Corr_Revenue",
    group_cols: list[str] | None = None,
) -> dict:
    """Compute baselines for every date key provided.

    Keys can be int (year) or str (label like "current", "ref1").
    Calls ``compute_baseline`` once per key.

    Returns
    -------
    {key: baseline_df} where each baseline_df has ``group_cols`` +
    ``["baseline"]``.
    """
    baselines: dict = {}
    for key, fdate in festival_year_dates.items():
        baselines[key] = compute_baseline(
            daily_agg, fdate, all_festival_dates, value_col, group_cols
        )
    return baselines
