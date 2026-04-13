"""
Validation, flagging, and debug utilities for the archive merge pipeline.

Raises flags when mapping issues occur and provides thorough inspection
for debugging.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Thresholds — adjust to control when WARNING vs ERROR is raised
# ---------------------------------------------------------------------------
UNMAPPED_SKU_PCT_WARN = 1.0   # Warn if >1% of rows lack SKU Class Prod
UNMAPPED_SKU_PCT_ERROR = 10.0  # Error if >10%
FORECAST_MISSING_PCT_WARN = 5.0   # Warn if >5% of rows lack any Forecast col
FORECAST_MISSING_PCT_ERROR = 50.0  # Error if >50%
AVL_MISSING_PCT_WARN = 5.0    # Warn if >5% of rows lack flag/instances
# Avl missing: do not ERROR — archive pipeline uses simplified availability
# (product_level_avl=1.0) when flag/instances missing. sku_group mismatch
# between P Master and Avl is a known data quality issue.
AVL_MISSING_PCT_ERROR = 100.0  # Never error; archive handles missing Avl
DEBUG_SAMPLE_SIZE = 20        # Max sample rows to include in debug report


@dataclass
class MergeIssue:
    """Single issue record."""
    severity: str  # "FLAG" | "WARNING" | "ERROR"
    year: str
    stage: str    # "pre_merge" | "sku_mapping" | "forecast_merge" | "avl_merge"
    message: str
    detail: dict[str, Any] = field(default_factory=dict)


class MergeIssueCollector:
    """Collects issues during merge; raises flags for mapping problems."""

    def __init__(self) -> None:
        self.issues: list[MergeIssue] = []
        self.debug_lines: list[str] = []

    def flag(self, year: str, stage: str, message: str, **detail: Any) -> None:
        self.issues.append(MergeIssue("FLAG", year, stage, message, dict(detail)))

    def warn(self, year: str, stage: str, message: str, **detail: Any) -> None:
        self.issues.append(MergeIssue("WARNING", year, stage, message, dict(detail)))

    def error(self, year: str, stage: str, message: str, **detail: Any) -> None:
        self.issues.append(MergeIssue("ERROR", year, stage, message, dict(detail)))

    def add_debug(self, line: str) -> None:
        self.debug_lines.append(line)

    def has_errors(self) -> bool:
        return any(i.severity == "ERROR" for i in self.issues)

    def has_warnings(self) -> bool:
        return any(i.severity == "WARNING" for i in self.issues)

    def print_summary(self) -> None:
        """Print all issues grouped by severity."""
        for sev in ["ERROR", "WARNING", "FLAG"]:
            items = [i for i in self.issues if i.severity == sev]
            if not items:
                continue
            symbol = {"ERROR": "!!", "WARNING": "!!", "FLAG": "--"}[sev]
            print(f"\n  [{sev}] {symbol} {len(items)} issue(s):")
            for i in items:
                print(f"      {i.year} [{i.stage}]: {i.message}")
                if i.detail:
                    for k, v in list(i.detail.items())[:3]:
                        print(f"        {k}: {v}")


# ---------------------------------------------------------------------------
# Pre-merge inspection
# ---------------------------------------------------------------------------

def inspect_pre_merge(
    collector: MergeIssueCollector,
    year: str,
    sales: pd.DataFrame | None,
    avl: pd.DataFrame | None,
    forecast: pd.DataFrame | None,
    forecast_path: Path | None,
) -> None:
    """Validate schemas and join keys before merge."""
    collector.add_debug(f"\n--- Pre-merge inspection: {year} ---")

    # Sales required columns
    sales_req = ["city_name", "hub_name", "process_dt", "product_id", "sales", "revenue"]
    if sales is not None:
        missing = [c for c in sales_req if c not in sales.columns]
        if missing:
            collector.error(year, "pre_merge", f"Sales missing required columns: {missing}",
                           missing_cols=missing, sales_cols=list(sales.columns))
        else:
            collector.add_debug(f"  Sales: {len(sales):,} rows, cols OK")
    else:
        collector.error(year, "pre_merge", "Sales not loaded")

    # Avl required columns
    avl_req = ["city_name", "hub_name", "av_dt", "product_id", "flag", "instances"]
    if avl is not None:
        missing = [c for c in avl_req if c not in avl.columns]
        if missing:
            collector.warn(year, "pre_merge", f"Avl missing columns (merge may fail): {missing}",
                          missing_cols=missing)
        collector.add_debug(f"  Avl: {len(avl):,} rows")
    else:
        collector.error(year, "pre_merge", "Avl not loaded")

    # Forecast optional but check schema if present
    if forecast is not None:
        fc_join = ["city_name", "hub_name", "process_dt", "product_id"]
        fc_date_alt = ["av_dt", "date"]
        has_date = "process_dt" in forecast.columns or any(c in forecast.columns for c in fc_date_alt)
        missing_join = [c for c in fc_join if c not in forecast.columns]
        if not has_date:
            collector.warn(year, "pre_merge", "Forecast has no process_dt/av_dt/date",
                          cols=list(forecast.columns))
        if missing_join and "process_dt" not in missing_join:
            collector.warn(year, "pre_merge", f"Forecast missing join cols: {missing_join}")
        collector.add_debug(f"  Forecast: {len(forecast):,} rows")
    elif forecast_path and forecast_path.exists():
        collector.warn(year, "pre_merge", "Forecast file exists but failed to load")
    else:
        collector.flag(year, "pre_merge", "Forecast file not found (proceeding without)",
                      path=str(forecast_path) if forecast_path else None)


# ---------------------------------------------------------------------------
# Post-SKU mapping inspection
# ---------------------------------------------------------------------------

def inspect_post_sku(
    collector: MergeIssueCollector,
    year: str,
    sales: pd.DataFrame,
    stats: dict,
) -> None:
    """Inspect SKU Class Prod mapping; raise flags for unmapped rows."""
    collector.add_debug(f"\n--- Post-SKU mapping: {year} ---")

    total = stats.get("total_rows", len(sales))
    unmapped = stats.get("unmapped_rows", 0)
    pct = 100 * unmapped / total if total else 0

    collector.add_debug(f"  Mapped P Master: {stats.get('mapped_p_master', 0):,}")
    collector.add_debug(f"  Mapped cc cat:   {stats.get('mapped_cc_cat_fallback', 0):,}")
    collector.add_debug(f"  Unmapped:        {unmapped:,} ({pct:.2f}%)")

    if unmapped > 0:
        # Sample unmapped for debugging
        unmapped_mask = sales["sku_group"].isna()
        sample_df = sales.loc[unmapped_mask]
        sample_ids = sample_df["product_id"].drop_duplicates().head(DEBUG_SAMPLE_SIZE).tolist()
        sample_names = sample_df["product_name"].drop_duplicates().head(DEBUG_SAMPLE_SIZE).tolist() if "product_name" in sample_df.columns else []

        collector.add_debug(f"  Sample unmapped product_ids: {sample_ids}")
        collector.add_debug(f"  Sample unmapped product_names: {sample_names}")

        if pct >= UNMAPPED_SKU_PCT_ERROR:
            collector.error(year, "sku_mapping",
                           f"High unmapped SKU: {unmapped:,} rows ({pct:.1f}%)",
                           unmapped=unmapped, pct=pct,
                           sample_product_ids=sample_ids[:5])
        elif pct >= UNMAPPED_SKU_PCT_WARN:
            collector.warn(year, "sku_mapping",
                          f"Unmapped SKU: {unmapped:,} rows ({pct:.1f}%) — check P Master / cc cat",
                          unmapped=unmapped, pct=pct,
                          sample_product_ids=sample_ids[:5])
        else:
            collector.flag(year, "sku_mapping",
                          f"Unmapped SKU: {unmapped:,} rows ({pct:.2f}%)",
                          unmapped=unmapped, sample_product_ids=sample_ids[:5])


# ---------------------------------------------------------------------------
# Post-Forecast merge inspection
# ---------------------------------------------------------------------------

FORECAST_COLS = [
    "group_flag", "group_instances",
    "grp_r7_plan", "grp_r7_inv", "r7_plan", "r7_inv", "BasePlan",
]


def inspect_post_forecast(
    collector: MergeIssueCollector,
    year: str,
    merged: pd.DataFrame,
    forecast_was_merged: bool,
) -> None:
    """Inspect Forecast merge; flag rows missing Forecast columns."""
    collector.add_debug(f"\n--- Post-Forecast merge: {year} ---")

    if not forecast_was_merged:
        collector.flag(year, "forecast_merge", "Forecast was not merged (skip or fail)")
        collector.add_debug("  Forecast merge skipped")
        return

    fc_cols_present = [c for c in FORECAST_COLS if c in merged.columns]
    if not fc_cols_present:
        collector.warn(year, "forecast_merge", "No Forecast columns in merged output")
        collector.add_debug("  No fc cols in output")
        return

    # Count rows where ALL fc cols are NaN
    missing_mask = merged[fc_cols_present[0]].isna()
    for c in fc_cols_present[1:]:
        missing_mask = missing_mask & merged[c].isna()
    missing_count = int(missing_mask.sum())
    total = len(merged)
    pct = 100 * missing_count / total if total else 0

    collector.add_debug(f"  Rows missing all Forecast cols: {missing_count:,} ({pct:.1f}%)")

    if missing_count > 0:
        sample = merged.loc[missing_mask, ["city_name", "hub_name", "process_dt", "product_id"]].head(DEBUG_SAMPLE_SIZE)
        sample_list = sample.to_dict("records")
        collector.add_debug(f"  Sample missing keys: {sample_list[:3]}")

        if pct >= FORECAST_MISSING_PCT_ERROR:
            collector.error(year, "forecast_merge",
                           f"Many rows missing Forecast: {missing_count:,} ({pct:.1f}%)",
                           missing_count=missing_count, pct=pct)
        elif pct >= FORECAST_MISSING_PCT_WARN:
            collector.warn(year, "forecast_merge",
                          f"Rows missing Forecast: {missing_count:,} ({pct:.1f}%)",
                          missing_count=missing_count, pct=pct)
        else:
            collector.flag(year, "forecast_merge",
                          f"Rows missing Forecast: {missing_count:,} ({pct:.2f}%)",
                          missing_count=missing_count)


# ---------------------------------------------------------------------------
# Post-Avl merge inspection
# ---------------------------------------------------------------------------

def inspect_post_avl(
    collector: MergeIssueCollector,
    year: str,
    merged: pd.DataFrame,
) -> None:
    """Inspect Availability merge; flag rows missing flag/instances."""
    collector.add_debug(f"\n--- Post-Avl merge: {year} ---")

    if "flag" not in merged.columns or "instances" not in merged.columns:
        collector.warn(year, "avl_merge", "flag/instances not in merged output")
        collector.add_debug("  flag/instances missing")
        return

    missing_flag = merged["flag"].isna()
    missing_inst = merged["instances"].isna()
    missing_either = missing_flag | missing_inst
    missing_count = int(missing_either.sum())
    total = len(merged)
    pct = 100 * missing_count / total if total else 0

    collector.add_debug(f"  Rows missing flag or instances: {missing_count:,} ({pct:.1f}%)")

    if missing_count > 0:
        sample = merged.loc[missing_either, ["city_name", "hub_name", "process_dt", "product_id"]].head(DEBUG_SAMPLE_SIZE)
        sample_list = sample.drop_duplicates().to_dict("records")
        collector.add_debug(f"  Sample missing keys: {sample_list[:3]}")

        if pct >= AVL_MISSING_PCT_ERROR:
            collector.error(year, "avl_merge",
                           f"Many rows missing Avl (flag/instances): {missing_count:,} ({pct:.1f}%)",
                           missing_count=missing_count, pct=pct)
        elif pct >= AVL_MISSING_PCT_WARN:
            collector.warn(year, "avl_merge",
                          f"Rows missing Avl: {missing_count:,} ({pct:.1f}%)",
                          missing_count=missing_count, pct=pct)
        else:
            collector.flag(year, "avl_merge",
                          f"Rows missing Avl: {missing_count:,} ({pct:.2f}%)",
                          missing_count=missing_count)


# ---------------------------------------------------------------------------
# Debug report writer
# ---------------------------------------------------------------------------

def write_debug_report(collector: MergeIssueCollector, output_dir: Path) -> Path:
    """Write detailed debug report to file."""
    path = output_dir / "merge_debug_report.txt"
    lines = [
        "=" * 70,
        "MERGE DEBUG REPORT",
        "=" * 70,
        "",
        "ISSUES SUMMARY",
        "-" * 40,
    ]
    for i in collector.issues:
        lines.append(f"  [{i.severity}] {i.year} {i.stage}: {i.message}")
        if i.detail:
            for k, v in i.detail.items():
                lines.append(f"    {k}: {v}")
        lines.append("")

    lines.extend([
        "",
        "DETAILED DEBUG LOG",
        "-" * 40,
    ])
    lines.extend(collector.debug_lines)

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path
