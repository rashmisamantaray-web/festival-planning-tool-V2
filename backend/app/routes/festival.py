"""
FastAPI routes for the Festival Planning tool.

All computation results are held in an in-memory store (_store) keyed by
a date-set identifier.  The workflow is:

  1. POST /festivals/compute   → run full computation for all 5 levels
  2. PUT  /festivals/city/overrides  → edit L1 override rows → auto-cascade
  3. PUT  /festivals/city-subcat/finals → edit L2 finals → auto-cascade
  4. PUT  /festivals/city-subcat-cut/finals → edit L3 finals → auto-cascade
  5. PUT  /festivals/city-hub/finals → edit L4 finals → auto-cascade
  6. POST /festivals/trends    → D-5 to D+5 daily trends per city
  7. GET  /festivals/export    → download Excel with all 5 levels

Auto-cascade means: when you edit a higher level, all downstream levels
are automatically re-indexed to stay consistent.
"""

from __future__ import annotations

import hashlib
import logging
import math
import time
import traceback
from datetime import timedelta

import numpy as np
import pandas as pd
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from app.middleware import get_request_id
from pydantic import BaseModel

from app.festival_dates import load_festival_calendar

logger = logging.getLogger(__name__)
from app.data_loader import load_and_compute, load_hub_mapping
from app.level_city import compute_city_level, recalculate_city_finals
from app.level_city_subcat import (
    compute_city_subcat_level,
    recalculate_with_new_finals as reindex_l2,
)
from app.level_city_subcat_cut import (
    compute_city_subcat_cut_level,
    recalculate_with_new_finals as reindex_l3,
)
from app.level_city_hub import (
    compute_city_hub_level,
    recalculate_with_new_finals as reindex_l4,
)
from app.level_city_hub_cut import (
    compute_city_hub_cut_level,
    recalculate as reindex_l5,
)
from app.excel_export import export_all_levels
from app.config import MAJOR_CITIES

router = APIRouter(prefix="/festivals", tags=["festivals"])

_store: dict[str, dict] = {}


# =====================================================================
# JSON sanitisation – replace NaN / Inf with None so FastAPI can serialise
# =====================================================================

def _sanitize(obj):
    """Recursively make an object JSON-safe.

    Handles numpy scalars, NaN/Inf, Timestamps, and nested dicts/lists.
    """
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        v = float(obj)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        return str(obj)
    if isinstance(obj, dict):
        return {str(k): _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return [_sanitize(v) for v in obj.tolist()]
    return obj


def _make_store_key(current_date: str, reference_dates: list[str], include_minor: bool = False) -> str:
    raw = current_date + "|" + "|".join(sorted(reference_dates)) + "|minor=" + str(include_minor)
    return hashlib.md5(raw.encode()).hexdigest()


# =====================================================================
# Request body models (Pydantic)
# =====================================================================

class ComputeRequest(BaseModel):
    """Body for POST /festivals/compute."""
    current_date: str                    # e.g. "2026-03-04"
    reference_dates: list[str]           # e.g. ["2025-03-14", "2024-03-25", "2023-03-08"]
    year_keys: list[str] | None = None
    include_minor: bool = False          # When False, only major cities are computed


class CityOverrideRequest(BaseModel):
    """Body for PUT /festivals/city/overrides."""
    store_key: str
    overrides: dict


class L2FinalsRequest(BaseModel):
    """Body for PUT /festivals/city-subcat/finals."""
    store_key: str
    finals: dict[str, float]


class L3FinalsRequest(BaseModel):
    """Body for PUT /festivals/city-subcat-cut/finals."""
    store_key: str
    finals: dict[str, float]


class L4FinalsRequest(BaseModel):
    """Body for PUT /festivals/city-hub/finals."""
    store_key: str
    finals: dict[str, float]


class TrendRequest(BaseModel):
    """Body for POST /festivals/trends."""
    reference_dates: list[str]
    year_keys: list[str] | None = None


# =====================================================================
# Helper functions
# =====================================================================

def _city_finals_map(city_data: dict) -> dict[str, float]:
    return {r["city_name"]: r["final_impact_pct"] for r in city_data["data"]}


def _l2_indexed_map(l2_data: dict) -> dict[tuple[str, str], float]:
    return {
        (r["city_name"], r["sub_category"]): r["final_after_indexing_pct"]
        for r in l2_data["data"]
    }


def _l3_indexed_map(l3_data: dict) -> dict[tuple[str, str, str], float]:
    return {
        (r["city_name"], r["sub_category"], r["cut_class"]): r["final_after_indexing_pct"]
        for r in l3_data["data"]
    }


def _l4_indexed_map(l4_data: dict) -> dict[tuple[str, str], float]:
    return {
        (r["city_name"], r["hub_name"]): r["final_after_indexing_pct"]
        for r in l4_data["data"]
    }


# =====================================================================
# Cascade functions
# =====================================================================

def _cascade_from_city(store_key: str):
    s = _store[store_key]
    cf = _city_finals_map(s["city"])
    current_key = s["city"]["current_key"]

    from app.level_city_subcat import _apply_indexing as idx_l2
    idx_l2(s["subcat"]["data"], cf, current_key)

    from app.level_city_subcat_cut import _apply_indexing as idx_l3
    idx_l3(s["subcat_cut"]["data"], _l2_indexed_map(s["subcat"]), current_key)

    from app.level_city_hub import _apply_indexing as idx_l4
    idx_l4(s["hub"]["data"], cf, current_key)

    reindex_l5(s["hub_cut"], _l4_indexed_map(s["hub"]), _l3_indexed_map(s["subcat_cut"]))


def _cascade_from_l2(store_key: str):
    s = _store[store_key]
    current_key = s["subcat_cut"]["current_key"]

    from app.level_city_subcat_cut import _apply_indexing as idx_l3
    idx_l3(s["subcat_cut"]["data"], _l2_indexed_map(s["subcat"]), current_key)
    reindex_l5(s["hub_cut"], _l4_indexed_map(s["hub"]), _l3_indexed_map(s["subcat_cut"]))


def _cascade_from_l3(store_key: str):
    s = _store[store_key]
    reindex_l5(s["hub_cut"], _l4_indexed_map(s["hub"]), _l3_indexed_map(s["subcat_cut"]))


def _cascade_from_l4(store_key: str):
    s = _store[store_key]
    reindex_l5(s["hub_cut"], _l4_indexed_map(s["hub"]), _l3_indexed_map(s["subcat_cut"]))


# =====================================================================
# API Endpoints
# =====================================================================

@router.post("/compute")
def compute_festival(request: Request, req: ComputeRequest):
    """
    Run the full computation pipeline using user-specified dates.

    Returns
    -------
    dict
        All 5 levels (city, subcat, subcat_cut, hub, hub_cut) plus store_key.

    Raises
    ------
    JSONResponse (500)
        On pipeline failure.
    """
    rid = get_request_id(request)
    try:
        logger.info(f"[{rid}] Backend received compute request: current_date={req.current_date}, refs={req.reference_dates}, include_minor={req.include_minor}")

        # Validate date format
        try:
            current_ts = pd.Timestamp(req.current_date).normalize()
            ref_timestamps = [pd.Timestamp(d).normalize() for d in req.reference_dates]
        except (ValueError, TypeError) as e:
            logger.warning(f"[{rid}] Invalid date format in request: {e}")
            return JSONResponse(status_code=400, content={"error": f"Invalid date format: {e}"})

        date_keys: dict[str, pd.Timestamp] = {"current": current_ts}
        for i, ts in enumerate(ref_timestamps):
            date_keys[f"ref{i+1}"] = ts

        _, all_festival_dates = load_festival_calendar()
        logger.info(f"Festival calendar: {len(all_festival_dates)} dates loaded")

        # Derive year_keys from dates if not explicitly provided
        year_keys = req.year_keys
        if year_keys is None:
            all_dates = [current_ts] + ref_timestamps
            year_keys = sorted({str(d.year) for d in all_dates})
            logger.info(f"Derived year_keys from dates: {year_keys}")

        logger.info(f"[{rid}] Pipeline started: loading product data")
        product_df = load_and_compute(year_keys)
        logger.info(f"Product data ready: {len(product_df)} rows")

        if not req.include_minor:
            product_df = product_df[product_df["city_name"].isin(MAJOR_CITIES)]
            logger.info(f"Filtered to major cities only: {len(product_df)} rows")
            if product_df.empty:
                logger.warning("No rows remain after filtering to major cities — results may be empty")

        # ── Hub remapping: closed hubs → current replacements ────────
        remap, current_hubs = load_hub_mapping()
        rows_before = len(product_df)
        if remap:
            product_df["hub_name"] = product_df["hub_name"].replace(remap)
        unmapped_mask = ~product_df["hub_name"].isin(current_hubs)
        n_unmapped = int(unmapped_mask.sum())
        if n_unmapped > 0:
            product_df.loc[unmapped_mask, "hub_name"] = (
                "[Unmapped] " + product_df.loc[unmapped_mask, "hub_name"]
            )
            logger.info(
                f"Hub remap: {n_unmapped:,} rows tagged as unmapped "
                f"({product_df.loc[unmapped_mask, 'hub_name'].nunique()} hubs)"
            )
        remapped = rows_before - len(product_df)
        logger.info(
            f"Hub remap done: {len(remap)} old→current rules applied, "
            f"{len(current_hubs)} current hubs, {len(product_df)} rows"
        )

        display_name = f"Dates {req.current_date}"
        current_key = "current"
        val_col = "Avl_Corr_Revenue"

        # ── Pre-aggregate once at the finest granularity (L5) ────────
        # All coarser levels are derived from this single groupby.
        t_agg = time.time()
        logger.info("Pre-aggregating daily data...")
        l5_cols = ["city_name", "hub_name", "sub_category", "SKU Class Prod", "process_dt"]
        agg_l5 = product_df.groupby(l5_cols, as_index=False)[val_col].sum()

        agg_l4 = agg_l5.groupby(
            ["city_name", "hub_name", "process_dt"], as_index=False
        )[val_col].sum()

        agg_l3 = agg_l5.groupby(
            ["city_name", "sub_category", "SKU Class Prod", "process_dt"], as_index=False
        )[val_col].sum()

        agg_l2 = agg_l5.groupby(
            ["city_name", "sub_category", "process_dt"], as_index=False
        )[val_col].sum()

        agg_l1 = agg_l5.groupby(
            ["city_name", "process_dt"], as_index=False
        )[val_col].sum()
        agg_l1["week"] = agg_l1["process_dt"].dt.isocalendar().week.astype(int)
        agg_l1["day_name"] = agg_l1["process_dt"].dt.strftime("%a")

        logger.info(f"Pre-aggregation done in {time.time()-t_agg:.1f}s (L1={len(agg_l1)}, L2={len(agg_l2)}, L3={len(agg_l3)}, L4={len(agg_l4)}, L5={len(agg_l5)})")
        if agg_l1.empty:
            logger.warning("Pre-aggregation L1 is empty — level computations may produce no data")

        t_levels = time.time()
        logger.info("Computing Level 1: City...")
        city_data = compute_city_level(
            product_df, display_name, date_keys, all_festival_dates, current_key,
            daily_agg=agg_l1,
        )
        cf = _city_finals_map(city_data)
        logger.info(f"Level 1 done ({time.time()-t_levels:.1f}s)")

        t2 = time.time()
        logger.info("Computing Level 2: City-SubCategory...")
        subcat_data = compute_city_subcat_level(
            product_df, display_name, date_keys, all_festival_dates, cf, current_key,
            daily_agg=agg_l2,
        )
        logger.info(f"Level 2 done ({time.time()-t2:.1f}s)")

        t3 = time.time()
        logger.info("Computing Level 3: City-SubCategory-CutClass...")
        l2_idx = _l2_indexed_map(subcat_data)
        subcat_cut_data = compute_city_subcat_cut_level(
            product_df, display_name, date_keys, all_festival_dates, l2_idx, current_key,
            daily_agg=agg_l3,
        )
        logger.info(f"Level 3 done ({time.time()-t3:.1f}s)")

        t4 = time.time()
        logger.info("Computing Level 4: City-Hub...")
        hub_data = compute_city_hub_level(
            product_df, display_name, date_keys, all_festival_dates, cf, current_key,
            daily_agg=agg_l4,
        )
        logger.info(f"Level 4 done ({time.time()-t4:.1f}s)")

        t5 = time.time()
        logger.info("Computing Level 5: City-Hub-CutClass...")
        l4_idx = _l4_indexed_map(hub_data)
        l3_idx = _l3_indexed_map(subcat_cut_data)
        hub_cut_data = compute_city_hub_cut_level(
            product_df, display_name, date_keys, all_festival_dates,
            l4_idx, l3_idx, current_key,
            daily_agg=agg_l5,
        )
        logger.info(f"Level 5 done ({time.time()-t5:.1f}s)")
        logger.info(f"All levels computed in {time.time()-t_levels:.1f}s")

        store_key = _make_store_key(req.current_date, req.reference_dates, req.include_minor)
        logger.info(f"[{rid}] Pipeline complete. Response returned to frontend. store_key={store_key}")
        _store[store_key] = {
            "city": city_data,
            "subcat": subcat_data,
            "subcat_cut": subcat_cut_data,
            "hub": hub_data,
            "hub_cut": hub_cut_data,
        }

        return _sanitize({
            "store_key": store_key,
            "include_minor": req.include_minor,
            "city": city_data,
            "subcat": subcat_data,
            "subcat_cut": subcat_cut_data,
            "hub": hub_data,
            "hub_cut": hub_cut_data,
        })
    except Exception as e:
        logger.error(f"[{rid}] Compute failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.put("/city/overrides")
def update_city_overrides(req: CityOverrideRequest):
    """Edit Level 1 override rows and auto-cascade to all downstream levels."""
    try:
        if req.store_key not in _store:
            logger.warning(f"City overrides: store_key {req.store_key} not found. Available keys: {list(_store.keys())}")
            return JSONResponse(status_code=400, content={"error": "Run /compute first"})

        s = _store[req.store_key]
        recalculate_city_finals(s["city"], req.overrides)
        _cascade_from_city(req.store_key)

        return _sanitize({
            "city": s["city"],
            "subcat": s["subcat"],
            "subcat_cut": s["subcat_cut"],
            "hub": s["hub"],
            "hub_cut": s["hub_cut"],
        })
    except Exception as e:
        logger.error(f"City override failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.put("/city-subcat/finals")
def update_l2_finals(req: L2FinalsRequest):
    """Edit Level 2 (City-SubCat) finals and cascade to L3 + L5."""
    try:
        if req.store_key not in _store:
            logger.warning(f"L2 finals: store_key {req.store_key} not found")
            return JSONResponse(status_code=400, content={"error": "Run /compute first"})

        s = _store[req.store_key]
        parsed = {tuple(k.split("||")): v for k, v in req.finals.items()}
        cf = _city_finals_map(s["city"])
        reindex_l2(s["subcat"], cf, parsed)
        _cascade_from_l2(req.store_key)

        return _sanitize({
            "subcat": s["subcat"],
            "subcat_cut": s["subcat_cut"],
            "hub_cut": s["hub_cut"],
        })
    except Exception as e:
        logger.error(f"L2 finals update failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.put("/city-subcat-cut/finals")
def update_l3_finals(req: L3FinalsRequest):
    """Edit Level 3 (City-SubCat-CutClass) finals and cascade to L5."""
    try:
        if req.store_key not in _store:
            logger.warning(f"L3 finals: store_key {req.store_key} not found")
            return JSONResponse(status_code=400, content={"error": "Run /compute first"})

        s = _store[req.store_key]
        parsed = {tuple(k.split("||")): v for k, v in req.finals.items()}
        l2_idx = _l2_indexed_map(s["subcat"])
        reindex_l3(s["subcat_cut"], l2_idx, parsed)
        _cascade_from_l3(req.store_key)

        return _sanitize({
            "subcat_cut": s["subcat_cut"],
            "hub_cut": s["hub_cut"],
        })
    except Exception as e:
        logger.error(f"L3 finals update failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.put("/city-hub/finals")
def update_l4_finals(req: L4FinalsRequest):
    """Edit Level 4 (City-Hub) finals and cascade to L5."""
    try:
        if req.store_key not in _store:
            logger.warning(f"L4 finals: store_key {req.store_key} not found")
            return JSONResponse(status_code=400, content={"error": "Run /compute first"})

        s = _store[req.store_key]
        parsed = {tuple(k.split("||")): v for k, v in req.finals.items()}
        cf = _city_finals_map(s["city"])
        reindex_l4(s["hub"], cf, parsed)
        _cascade_from_l4(req.store_key)

        return _sanitize({
            "hub": s["hub"],
            "hub_cut": s["hub_cut"],
        })
    except Exception as e:
        logger.error(f"L4 finals update failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/trends")
def get_trends(request: Request, req: TrendRequest):
    """
    Return D-5 to D+5 daily city-level % change from baseline for each reference date.
    Also returns baseline and std for variance band.
    """
    rid = get_request_id(request)
    try:
        from app.baseline import compute_baseline_with_std

        logger.info(f"[{rid}] Trends request: ref_dates={req.reference_dates}")
        try:
            ref_timestamps = [pd.Timestamp(d).normalize() for d in req.reference_dates]
        except (ValueError, TypeError) as e:
            logger.warning(f"[{rid}] Invalid date in trends request: {e}")
            return JSONResponse(status_code=400, content={"error": f"Invalid date format: {e}"})

        year_keys = req.year_keys
        if year_keys is None:
            year_keys = sorted({str(d.year) for d in ref_timestamps})
            logger.info(f"Trends: derived year_keys from dates: {year_keys}")

        product_df = load_and_compute(year_keys)
        if product_df.empty:
            logger.warning("Trends: product_df is empty — returning empty trends")

        daily = (
            product_df
            .groupby(["city_name", "process_dt"], as_index=False)["Avl_Corr_Sales"]
            .sum()
        )

        _, all_festival_dates = load_festival_calendar()
        normalized_festival_dates = {pd.Timestamp(fd).normalize() for fd in all_festival_dates}

        result: dict[str, dict[str, list[dict]]] = {}

        for i, ref_date in enumerate(ref_timestamps):
            label = req.reference_dates[i]
            result[label] = {}

            for offset in range(-5, 6):
                target_date = ref_date + timedelta(days=offset)
                mask = daily["process_dt"].dt.normalize() == target_date
                day_data = daily.loc[mask]

                bl_df = compute_baseline_with_std(
                    daily, target_date, normalized_festival_dates,
                    value_col="Avl_Corr_Sales", group_cols=["city_name"],
                )

                for _, row in day_data.iterrows():
                    city = row["city_name"]
                    if city not in result[label]:
                        result[label][city] = []

                    bl_row = bl_df[bl_df["city_name"] == city]
                    baseline = float(bl_row["baseline"].iloc[0]) if len(bl_row) else 0.0
                    std_val = float(bl_row["std"].iloc[0]) if len(bl_row) else 0.0

                    sales = float(row["Avl_Corr_Sales"])
                    pct_change = (
                        ((sales - baseline) / baseline * 100) if baseline > 0 else 0.0
                    )
                    std_pct = (
                        (std_val / baseline * 100) if baseline > 0 else 0.0
                    )

                    result[label][city].append({
                        "day_offset": offset,
                        "date": target_date.strftime("%Y-%m-%d"),
                        "sales": round(sales, 2),
                        "baseline": round(baseline, 2),
                        "pct_change": round(pct_change, 2),
                        "std_pct": round(std_pct, 2),
                    })

        logger.info(f"[{rid}] Trends complete")
        return _sanitize({"trends": result})
    except Exception as e:
        logger.error(f"[{rid}] Trends failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/export")
def export_excel(store_key: str):
    """Download a formatted Excel workbook with all 5 levels."""
    try:
        if store_key not in _store:
            logger.warning(f"Export: store_key {store_key} not found. Available: {list(_store.keys())}")
            return JSONResponse(status_code=400, content={"error": "Run /compute first"})

        s = _store[store_key]
        content = export_all_levels(
            store_key,
            s["city"], s["subcat"], s["subcat_cut"], s["hub"], s["hub_cut"],
        )

        filename = f"Festival_Plan_{store_key[:8]}.xlsx"
        return Response(
            content=content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        logger.error(f"Export failed: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


