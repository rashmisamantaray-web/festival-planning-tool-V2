"""
Level 5 – City x Hub x Sub-Category x Cut Classification.

Derived (read-only) level. Uses hub drops from Level 4, indexed
against Level 3 (City-SubCat-CutClass).
Date keys are strings ("current", "ref1", etc.).
"""

from __future__ import annotations

import logging

import pandas as pd

from app.baseline import compute_baseline

logger = logging.getLogger(__name__)
from app.config import LACS


GROUP_COLS = ["city_name", "hub_name", "sub_category", "SKU Class Prod"]
VALUE_COL = "Avl_Corr_Revenue"


def _aggregate(product_df: pd.DataFrame) -> pd.DataFrame:
    return (
        product_df
        .groupby(
            ["city_name", "hub_name", "sub_category", "SKU Class Prod", "process_dt"],
            as_index=False,
        )[VALUE_COL].sum()
    )


def compute_city_hub_cut_level(
    product_df: pd.DataFrame,
    festival_name: str,
    festival_year_dates: dict[str, pd.Timestamp],
    all_festival_dates: set[pd.Timestamp],
    hub_indexed_finals: dict[tuple[str, str], float],
    subcat_cut_indexed_finals: dict[tuple[str, str, str], float],
    current_key: str = "current",
    daily_agg: pd.DataFrame | None = None,
) -> dict:
    daily = daily_agg if daily_agg is not None else _aggregate(product_df)
    all_keys = sorted(festival_year_dates.keys())
    hist_keys = [k for k in all_keys if k != current_key]

    # L5 only needs the current-key baseline as a weighting proxy —
    # skip the expensive per-key baseline computation for historical dates.
    cur_baselines = compute_baseline(
        daily,
        festival_year_dates[current_key],
        all_festival_dates,
        VALUE_COL,
        GROUP_COLS,
    )

    # If the current-key baseline is empty or all-zero, try historical dates.
    def _baselines_are_usable(bl_df: pd.DataFrame) -> bool:
        return not bl_df.empty and bl_df["baseline"].sum() > 0

    if not _baselines_are_usable(cur_baselines) and hist_keys:
        for hk in reversed(hist_keys):
            fallback = compute_baseline(
                daily,
                festival_year_dates[hk],
                all_festival_dates,
                VALUE_COL,
                GROUP_COLS,
            )
            if _baselines_are_usable(fallback):
                logger.info(f"L5: current baseline empty/zero, using {hk} as fallback for weighting")
                cur_baselines = fallback
                break

    combos = daily[GROUP_COLS].drop_duplicates().sort_values(GROUP_COLS).reset_index(drop=True)

    if not cur_baselines.empty:
        combos = combos.merge(
            cur_baselines[GROUP_COLS + ["baseline"]],
            on=GROUP_COLS,
            how="left",
        )
        combos["baseline"] = (combos["baseline"].fillna(0.0) / LACS).round(4)
    else:
        combos["baseline"] = 0.0

    hub_drop_df = pd.DataFrame(
        [(c, h, v) for (c, h), v in hub_indexed_finals.items()],
        columns=["city_name", "hub_name", "hub_drop_pct"],
    )
    if not hub_drop_df.empty:
        combos = combos.merge(hub_drop_df, on=["city_name", "hub_name"], how="left")
        combos["hub_drop_pct"] = combos["hub_drop_pct"].fillna(0.0).round(2)
    else:
        combos["hub_drop_pct"] = 0.0

    combos["initial_drop_pct"] = combos["hub_drop_pct"]
    combos = combos.rename(columns={"SKU Class Prod": "cut_class"})

    records = combos.to_dict("records")
    for rec in records:
        rec["baseline"] = round(rec.get("baseline", 0.0), 2)
        rec["hub_drop_pct"] = round(rec.get("hub_drop_pct", 0.0), 2)
        rec["initial_drop_pct"] = round(rec.get("initial_drop_pct", 0.0), 2)

    _apply_indexing(records, subcat_cut_indexed_finals)

    return {
        "festival_name": festival_name,
        "current_key": current_key,
        "all_keys": all_keys,
        "data": records,
    }


def _apply_indexing(
    records: list[dict],
    subcat_cut_indexed_finals: dict[tuple[str, str, str], float],
) -> None:
    groups: dict[tuple[str, str, str], list[dict]] = {}
    for rec in records:
        gkey = (rec["city_name"], rec["sub_category"], rec["cut_class"])
        groups.setdefault(gkey, []).append(rec)

    for (city, subcat, cut_class), recs in groups.items():
        baselines = [r["baseline"] for r in recs]
        initials = [r["initial_drop_pct"] for r in recs]
        sum_base = sum(baselines)

        # If all baselines are zero, use equal weights (1.0 per record)
        # so the weighted average still produces a meaningful result.
        if sum_base == 0:
            baselines = [1.0] * len(recs)
            sum_base = float(len(recs))

        drop_with_current = (
            sum(b * d for b, d in zip(baselines, initials)) / sum_base
            if sum_base else 0.0
        )

        target = subcat_cut_indexed_finals.get((city, subcat, cut_class), 0.0)

        for rec in recs:
            rec["drop_with_current_pct"] = round(drop_with_current, 2)
            rec["target_subcat_cut_drop_pct"] = round(target, 2)

            if drop_with_current != 0:
                indexed = rec["initial_drop_pct"] * (target / drop_with_current)
            else:
                indexed = 0.0
            rec["final_after_indexing_pct"] = round(indexed, 2)
            rec["final_rev"] = round(rec["baseline"] * indexed / 100, 2)


def recalculate(
    level_data: dict,
    hub_indexed_finals: dict[tuple[str, str], float],
    subcat_cut_indexed_finals: dict[tuple[str, str, str], float],
) -> dict:
    for rec in level_data["data"]:
        hub_drop = hub_indexed_finals.get(
            (rec["city_name"], rec["hub_name"]), 0.0
        )
        rec["hub_drop_pct"] = round(hub_drop, 2)
        rec["initial_drop_pct"] = round(hub_drop, 2)

    _apply_indexing(level_data["data"], subcat_cut_indexed_finals)
    return level_data
