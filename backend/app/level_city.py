"""
Level 1 – City-level festival impact computation.

Computes for each city:
  1. BASELINE — median Avl_Corr_Sales on the same weekday from
     the 5 non-festival weeks before the date.
  2. ACTUAL — total Avl_Corr_Sales on the date itself.
  3. PRISTINE DROP % — (Actual - Baseline) / Baseline * 100
  4. BASE-CORRECTED DROP % — Pristine_Drop * Factor
  5. OVERRIDE ROWS (2) — user-editable blends
  6. FINAL IMPACT % — combined impact from two override rows

Date keys can be strings like "current", "ref1", "ref2", "ref3"
(instead of integer years) to support same-year comparisons.
"""

from __future__ import annotations

import pandas as pd

from app.baseline import compute_baselines_for_years
from app.config import LACS


GROUP_COLS = ["city_name"]
VALUE_COL = "Avl_Corr_Revenue"


def _aggregate_city_day(product_df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        product_df
        .groupby(["city_name", "process_dt"], as_index=False)[VALUE_COL]
        .sum()
    )
    agg["week"] = agg["process_dt"].dt.isocalendar().week.astype(int)
    agg["day_name"] = agg["process_dt"].dt.strftime("%a")
    return agg


def compute_city_level(
    product_df: pd.DataFrame,
    festival_name: str,
    festival_year_dates: dict[str, pd.Timestamp],
    all_festival_dates: set[pd.Timestamp],
    current_key: str = "current",
    override_rows: dict | None = None,
    daily_agg: pd.DataFrame | None = None,
) -> dict:
    """Full Level 1 computation.

    Parameters
    ----------
    festival_year_dates : dict[str, Timestamp]
        Keys are labels like "current", "ref1", "ref2", "ref3".
    current_key : str
        The key in festival_year_dates that represents the planning date.
    daily_agg : DataFrame | None
        Pre-aggregated daily data. If None, aggregates from product_df.
    """
    daily = daily_agg if daily_agg is not None else _aggregate_city_day(product_df)

    all_keys = sorted(festival_year_dates.keys())
    hist_keys = [k for k in all_keys if k != current_key]

    baselines_by_key = compute_baselines_for_years(
        daily, festival_year_dates, all_festival_dates, VALUE_COL, GROUP_COLS
    )

    actuals_by_key: dict[str, pd.DataFrame] = {}
    for key, fdate in festival_year_dates.items():
        mask = daily["process_dt"].dt.normalize() == fdate.normalize()
        actuals_by_key[key] = daily.loc[mask, GROUP_COLS + [VALUE_COL]].rename(
            columns={VALUE_COL: "actual"}
        )

    cities = sorted(daily["city_name"].unique())

    records = []
    for city in cities:
        rec: dict = {"city_name": city, "years": {}}

        for key in all_keys:
            bl_df = baselines_by_key.get(key, pd.DataFrame())
            bl_row = bl_df.loc[bl_df["city_name"] == city, "baseline"]
            baseline = float(bl_row.iloc[0]) if len(bl_row) else 0.0

            act_df = actuals_by_key.get(key, pd.DataFrame())
            act_row = act_df.loc[act_df["city_name"] == city, "actual"]
            actual = float(act_row.iloc[0]) if len(act_row) else 0.0

            fdate = festival_year_dates[key]
            pristine_drop = (
                ((actual - baseline) / baseline * 100) if baseline != 0 else 0.0
            )

            rec["years"][key] = {
                "week": int(fdate.isocalendar()[1]),
                "day_name": fdate.strftime("%a"),
                "date": fdate.isoformat(),
                "baseline": round(baseline / LACS, 4),
                "actual": round(actual / LACS, 4),
                "pristine_drop_pct": round(pristine_drop, 2),
            }

        current_bl = rec["years"].get(current_key, {}).get("baseline", 0.0)
        for key in hist_keys:
            yr_data = rec["years"][key]
            hist_bl = yr_data["baseline"]
            prist = yr_data["pristine_drop_pct"]

            if hist_bl != 0 and current_bl != 0:
                factor = (current_bl / hist_bl) if prist < 0 else (hist_bl / current_bl)
            else:
                factor = 1.0

            yr_data["base_corrected_drop_pct"] = round(prist * factor, 2)

        row1_val, row2_val = _compute_override_rows(
            rec, hist_keys, city, override_rows
        )
        rec["override_row1"] = round(row1_val, 2)
        rec["override_row2"] = round(row2_val, 2)

        vals = [v for v in [row1_val, row2_val] if v != 0]
        if not vals:
            final = 0.0
        elif all(v > 0 for v in vals):
            final = max(vals)   # both positive → take the higher spike
        elif all(v < 0 for v in vals):
            final = min(vals)   # both negative → take the lower (deeper) drop
        else:
            final = min(vals)   # mixed → always take the negative (conservative)
        rec["final_impact_pct"] = round(final, 2)

        records.append(rec)

    return {
        "festival_name": festival_name,
        "current_key": current_key,
        "historical_keys": hist_keys,
        "all_keys": all_keys,
        "cities": cities,
        "data": records,
    }


def _compute_override_rows(
    rec: dict,
    hist_keys: list[str],
    city: str,
    overrides: dict | None,
) -> tuple[float, float]:
    if overrides:
        row1_val = _resolve_override(rec, hist_keys, overrides.get("row1", {}).get(city))
        row2_val = _resolve_override(rec, hist_keys, overrides.get("row2", {}).get(city))
        return row1_val, row2_val

    if hist_keys:
        latest = hist_keys[-1]
        row1 = rec["years"][latest].get("base_corrected_drop_pct", 0.0)
    else:
        row1 = 0.0
    return row1, 0.0


def _resolve_override(rec: dict, hist_keys: list[str], cfg: dict | None) -> float:
    """Resolve an override config to a float value.

    Supports two formats:
      { "direct": -8.5 }           — use the value as-is
      { "weights": { "ref1": 1.0 } } — weighted blend of base-corrected drops
    """
    if not cfg:
        return 0.0
    if "direct" in cfg:
        return float(cfg["direct"])
    if "weights" in cfg:
        total = 0.0
        for key, w in cfg["weights"].items():
            if key in rec["years"]:
                total += rec["years"][key].get("base_corrected_drop_pct", 0.0) * w
        return total
    return 0.0


def recalculate_city_finals(city_data: dict, override_rows: dict) -> dict:
    """Re-run override + final-impact calculation after user edits.

    override_rows format:
      { "row1": { "Mumbai": { "direct": -8.5 } }, "row2": { ... } }
    or the legacy weighted format:
      { "row1": { "Mumbai": { "weights": { "ref1": 1.0 } } } }
    """
    hist_keys = city_data["historical_keys"]

    for rec in city_data["data"]:
        city = rec["city_name"]
        row1, row2 = _compute_override_rows(rec, hist_keys, city, override_rows)
        rec["override_row1"] = round(row1, 2)
        rec["override_row2"] = round(row2, 2)

        vals = [v for v in [row1, row2] if v != 0]
        if not vals:
            final = 0.0
        elif all(v > 0 for v in vals):
            final = max(vals)   # both positive → take the higher spike
        elif all(v < 0 for v in vals):
            final = min(vals)   # both negative → take the lower (deeper) drop
        else:
            final = min(vals)   # mixed → always take the negative (conservative)
        rec["final_impact_pct"] = round(final, 2)

    return city_data
