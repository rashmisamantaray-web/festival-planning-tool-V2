"""
Phase 1: Inspect Sales and Availability RDS files for 2023, 2024, 2025.

Reads all 6 files, lists columns, dtypes, row counts, and produces a
comparison report. Run this before the merge script to identify:
- Non-uniform column names
- Data type mismatches
- Missing columns vs RDS_COLUMNS_TO_KEEP

Usage (from backend/):
    python scripts/inspect_archive_rds.py

Or from project root:
    python backend/scripts/inspect_archive_rds.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pyreadr

# Paths from Data Aggregation.docx
BASE = Path(
    r"G:/.shortcut-targets-by-id/1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em/"
    r"Planning Team/25. Planning_Database/01_all_day_reporting"
)
ARCHIVE = BASE / "03_archive_data"

FILES = {
    "2025_sales": ARCHIVE / "Sales.RDS",
    "2025_avl": ARCHIVE / "SKU_Class_Avl_8am_8pm.RDS",
    "2025_forecast": ARCHIVE / "97Forecast.RDS",
    "2024_sales": ARCHIVE / "2024" / "Sales.RDS",
    "2024_avl": ARCHIVE / "2024" / "SKU_Class_Avl_8am_8pm.RDS",
    "2024_forecast": ARCHIVE / "2024" / "Forecast.RDS",
    "2023_sales": ARCHIVE / "2023" / "Sales.RDS",
    "2023_avl": ARCHIVE / "2023" / "SKU_Class_Avl_8am_8pm.RDS",
    "2023_forecast": ARCHIVE / "2023" / "Forecast.RDS",
}

# Join keys per Data Aggregation.docx: hub, date, product_id
JOIN_COLS = ["hub", "date", "product_id"]

# Columns the downstream pipeline expects (from config.RDS_COLUMNS_TO_KEEP)
RDS_COLUMNS_TO_KEEP = [
    "city_name", "product_id", "hub_name", "sku_group", "process_dt",
    "sales", "revenue", "product_discount",
    "group_flag", "group_instances",
    "grp_r7_plan", "grp_r7_inv", "grp_r7_plan_rev", "grp_r7_inv_rev",
    "grp_BasePlan", "grp_BaseRev",
    "r7_plan", "r7_inv", "r7_plan_rev", "r7_inv_rev",
    "BasePlan", "flag", "instances", "sub_category", "product_name",
]


def _read_rds(path: Path) -> pd.DataFrame:
    """Read RDS file. Raises on failure — does not skip; stops and reports."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    try:
        result = pyreadr.read_r(str(path))
        frame = next(iter(result.values()))
        return frame
    except MemoryError:
        raise MemoryError(
            f"MemoryError reading {path}. The file may be too large for available RAM. "
            f"Try running on a machine with more memory, or use a smaller/subset of the data."
        ) from None
    except Exception as e:
        raise RuntimeError(f"Failed to read {path}: {e}") from e


def _inspect_df(df: pd.DataFrame, label: str) -> dict:
    """Inspect a DataFrame and return a summary dict."""
    out = {
        "label": label,
        "rows": len(df),
        "cols": list(df.columns),
        "dtypes": {c: str(df[c].dtype) for c in df.columns},
        "join_cols_found": [],
        "join_cols_missing": [],
        "sample_join": {},
    }
    for jc in JOIN_COLS:
        if jc in df.columns:
            out["join_cols_found"].append(jc)
            out["sample_join"][jc] = df[jc].dropna().head(3).tolist()
        else:
            out["join_cols_missing"].append(jc)

    # Check for approximate column names (e.g. hub_name vs hub)
    alt_names = {
        "hub": ["hub_name", "hub_id", "Hub"],
        "date": ["process_dt", "Process_Dt", "Date"],
        "product_id": ["product_id", "Product_id", "productid"],
    }
    for canonical, aliases in alt_names.items():
        for a in aliases:
            if a in df.columns and canonical not in out["join_cols_found"]:
                out["join_cols_found"].append(f"{canonical} (as '{a}')")

    return out


def main() -> int:
    print("=" * 70)
    print("Phase 1: Inspect Archive RDS Files (Sales + Availability)")
    print("=" * 70)

    summaries: list[dict] = []

    for label, path in FILES.items():
        print(f"\n--- {label}: {path.name} ---")
        df = _read_rds(path)  # Raises on failure — stops and reports, no skip
        s = _inspect_df(df, label)
        summaries.append(s)

        print(f"  Rows: {s['rows']}")
        print(f"  Columns ({len(s['cols'])}): {s['cols'][:15]}...")
        if s["cols"]:
            print(f"  Dtypes (first 5): {dict(list(s['dtypes'].items())[:5])}")
        if s["join_cols_found"]:
            print(f"  Join cols found: {s['join_cols_found']}")
        if s["join_cols_missing"]:
            print(f"  Join cols MISSING: {s['join_cols_missing']}")

    # Comparison report
    print("\n" + "=" * 70)
    print("COMPARISON REPORT")
    print("=" * 70)

    all_cols: dict[str, set[str]] = {}  # col_name -> set of files that have it
    for s in summaries:
        if "cols" in s and s.get("rows", 0) > 0:
            for c in s["cols"]:
                all_cols.setdefault(c, set()).add(s["label"])

    print("\n1. Column presence by file:")
    for label in FILES.keys():
        s = next((x for x in summaries if x["label"] == label), None)
        if s and "cols" in s:
            print(f"   {label}: {len(s['cols'])} cols - {sorted(s['cols'])[:10]}...")

    print("\n2. Non-uniform columns (present in some files but not all):")
    sales_labels = {k for k in FILES if "sales" in k}
    avl_labels = {k for k in FILES if "avl" in k}
    for col, files in sorted(all_cols.items()):
        in_sales = files & sales_labels
        in_avl = files & avl_labels
        if len(in_sales) < len(sales_labels) or len(in_avl) < len(avl_labels):
            print(f"   '{col}': Sales{tuple(in_sales)} Avl{tuple(in_avl)}")

    print("\n3. RDS_COLUMNS_TO_KEEP – presence in any file:")
    for col in RDS_COLUMNS_TO_KEEP:
        files = all_cols.get(col, set())
        if not files:
            print(f"   MISSING everywhere: {col}")
        else:
            print(f"   {col}: in {files}")

    print("\n4. Join key readiness (hub, date, product_id):")
    for s in summaries:
        if s.get("rows", 0) == 0:
            continue
        missing = s.get("join_cols_missing", [])
        found = s.get("join_cols_found", [])
        status = "OK" if not missing else f"MISSING: {missing}"
        print(f"   {s['label']}: {status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
