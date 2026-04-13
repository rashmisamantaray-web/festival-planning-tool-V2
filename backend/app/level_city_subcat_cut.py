"""
Level 3 – City x Sub-Category x Cut Classification.

Indexed against Level 2 (City-SubCat).
Date keys are strings ("current", "ref1", etc.).
"""

from __future__ import annotations

import logging

import pandas as pd

from app.baseline import compute_baselines_for_years

logger = logging.getLogger(__name__)
from app.config import LACS


GROUP_COLS = ["city_name", "sub_category", "SKU Class Prod"]
VALUE_COL = "Avl_Corr_Revenue"


def _aggregate(product_df: pd.DataFrame) -> pd.DataFrame:
    return (
        product_df
        .groupby(
            ["city_name", "sub_category", "SKU Class Prod", "process_dt"],
            as_index=False,
        )[VALUE_COL].sum()
    )


def compute_city_subcat_cut_level(
    product_df: pd.DataFrame,
    festival_name: str,
    festival_year_dates: dict[str, pd.Timestamp],
    all_festival_dates: set[pd.Timestamp],
    subcat_indexed_finals: dict[tuple[str, str], float],
    current_key: str = "current",
    user_finals: dict[tuple[str, str, str], float] | None = None,
    daily_agg: pd.DataFrame | None = None,
) -> dict:
    daily = daily_agg if daily_agg is not None else _aggregate(product_df)
    all_keys = sorted(festival_year_dates.keys())
    hist_keys = [k for k in all_keys if k != current_key]

    baselines = compute_baselines_for_years(
        daily, festival_year_dates, all_festival_dates, VALUE_COL, GROUP_COLS
    )

    actuals: dict[str, pd.DataFrame] = {}
    for key, fdate in festival_year_dates.items():
        mask = daily["process_dt"].dt.normalize() == fdate.normalize()
        actuals[key] = daily.loc[mask, GROUP_COLS + [VALUE_COL]].rename(
            columns={VALUE_COL: "actual"}
        )

    combos = daily[GROUP_COLS].drop_duplicates().sort_values(GROUP_COLS)
    records = []

    for _, combo in combos.iterrows():
        city = combo["city_name"]
        subcat = combo["sub_category"]
        cut_class = combo["SKU Class Prod"]
        rec = {
            "city_name": city,
            "sub_category": subcat,
            "cut_class": cut_class,
            "years": {},
        }

        for key in all_keys:
            bl_df = baselines.get(key, pd.DataFrame())
            bl_mask = (
                (bl_df["city_name"] == city)
                & (bl_df["sub_category"] == subcat)
                & (bl_df["SKU Class Prod"] == cut_class)
            )
            baseline = float(bl_df.loc[bl_mask, "baseline"].iloc[0]) if bl_mask.any() else 0.0

            act_df = actuals.get(key, pd.DataFrame())
            act_mask = (
                (act_df["city_name"] == city)
                & (act_df["sub_category"] == subcat)
                & (act_df["SKU Class Prod"] == cut_class)
            )
            actual = float(act_df.loc[act_mask, "actual"].iloc[0]) if act_mask.any() else 0.0

            pristine = ((actual - baseline) / baseline * 100) if baseline else 0.0
            rec["years"][key] = {
                "baseline": round(baseline / LACS, 4),
                "actual": round(actual / LACS, 4),
                "pristine_drop_pct": round(pristine, 2),
            }

        cur_bl = rec["years"].get(current_key, {}).get("baseline", 0.0)
        for key in hist_keys:
            yd = rec["years"][key]
            hist_bl = yd["baseline"]
            prist = yd["pristine_drop_pct"]
            factor = 1.0
            if hist_bl and cur_bl:
                factor = (cur_bl / hist_bl) if prist < 0 else (hist_bl / cur_bl)
            yd["base_corrected_drop_pct"] = round(prist * factor, 2)

        k = (city, subcat, cut_class)
        if user_finals and k in user_finals:
            rec["final_pct"] = round(user_finals[k], 2)
        elif hist_keys:
            rec["final_pct"] = rec["years"][hist_keys[-1]].get(
                "base_corrected_drop_pct", 0.0
            )
        else:
            rec["final_pct"] = 0.0

        records.append(rec)

    _apply_indexing(records, subcat_indexed_finals, current_key)

    return {
        "festival_name": festival_name,
        "current_key": current_key,
        "historical_keys": hist_keys,
        "all_keys": all_keys,
        "data": records,
    }


def _apply_indexing(
    records: list[dict],
    subcat_indexed_finals: dict[tuple[str, str], float],
    current_key: str,
) -> None:
    groups: dict[tuple[str, str], list[dict]] = {}
    for rec in records:
        gkey = (rec["city_name"], rec["sub_category"])
        groups.setdefault(gkey, []).append(rec)

    for (city, subcat), recs in groups.items():
        baselines = [r["years"].get(current_key, {}).get("baseline", 0.0) for r in recs]
        finals = [r["final_pct"] for r in recs]
        sum_base = sum(baselines)

        # If current-key baselines are all zero (future festival date),
        # fall back to the most recent historical baseline as a size proxy.
        if sum_base == 0:
            all_keys_in_rec = list(recs[0]["years"].keys()) if recs else []
            hist_keys = [k for k in all_keys_in_rec if k != current_key]
            for hk in reversed(hist_keys):
                fallback = [r["years"].get(hk, {}).get("baseline", 0.0) for r in recs]
                if sum(fallback) > 0:
                    baselines = fallback
                    sum_base = sum(baselines)
                    break

        if sum_base:
            drop_with_current = sum(b * f for b, f in zip(baselines, finals)) / sum_base
        else:
            logger.warning(f"L3 indexing ({city},{subcat}): all baselines zero, drop_with_current=0")
            drop_with_current = 0.0

        subcat_drop = subcat_indexed_finals.get((city, subcat), 0.0)

        for rec in recs:
            rec["drop_with_current_pct"] = round(drop_with_current, 2)
            rec["subcat_drop_pct"] = round(subcat_drop, 2)
            if drop_with_current != 0:
                rec["final_after_indexing_pct"] = round(
                    rec["final_pct"] * (subcat_drop / drop_with_current), 2
                )
            else:
                rec["final_after_indexing_pct"] = 0.0


def recalculate_with_new_finals(
    level_data: dict,
    subcat_indexed_finals: dict[tuple[str, str], float],
    user_finals: dict[tuple[str, str, str], float],
) -> dict:
    for rec in level_data["data"]:
        key = (rec["city_name"], rec["sub_category"], rec["cut_class"])
        if key in user_finals:
            rec["final_pct"] = round(user_finals[key], 2)
    _apply_indexing(level_data["data"], subcat_indexed_finals, level_data["current_key"])
    return level_data
