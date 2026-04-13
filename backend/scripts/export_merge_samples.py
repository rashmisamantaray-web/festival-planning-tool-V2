"""
Export 100 sample rows from each merge source file (Sales, Forecast, Availability)
for each archive year (2023, 2024, 2025).

Use this to inspect join keys, column names, and data types to debug merge issues.

Output: CSV files in backend/scripts/merge_samples/
  - 2023_sales_sample.csv
  - 2023_forecast_sample.csv
  - 2023_avl_sample.csv
  - 2024_sales_sample.csv
  - ... etc.

Usage (from backend/):
    py scripts/export_merge_samples.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_script_dir.parent))

import pandas as pd
import pyreadr

# Same paths as merge_archive_rds.py
BASE = Path(
    r"G:/.shortcut-targets-by-id/1EF0u4bxTzGMLlMY1RfwniRIDikCT29Em/"
    r"Planning Team/25. Planning_Database/01_all_day_reporting"
)
ARCHIVE = BASE / "03_archive_data"

FILES = {
    "2025": {
        "sales": ARCHIVE / "Sales.RDS",
        "avl": ARCHIVE / "SKU_Class_Avl_8am_8pm.RDS",
        "forecast": ARCHIVE / "97Forecast.RDS",
    },
    "2024": {
        "sales": ARCHIVE / "2024" / "Sales.RDS",
        "avl": ARCHIVE / "2024" / "SKU_Class_Avl_8am_8pm.RDS",
        "forecast": ARCHIVE / "2024" / "Forecast.RDS",
    },
    "2023": {
        "sales": ARCHIVE / "2023" / "Sales.RDS",
        "avl": ARCHIVE / "2023" / "SKU_Class_Avl_8am_8pm.RDS",
        "forecast": ARCHIVE / "2023" / "Forecast.RDS",
    },
}

SAMPLE_SIZE = 100
OUTPUT_DIR = _script_dir / "merge_samples"

# Join keys used in merge - for reference
SALES_JOIN = ["city_name", "hub_name", "process_dt", "product_id"]
AVL_JOIN = ["city_name", "hub_name", "av_dt", "product_id"]  # Avl joins on product_id (not sku_group)
FORECAST_JOIN = ["city_name", "hub_name", "process_dt", "product_id"]  # Forecast may use "date" not "process_dt"


def _read_rds(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    result = pyreadr.read_r(str(path))
    return next(iter(result.values()))


def main() -> int:
    print("=" * 70)
    print("Export Merge Samples (100 rows per file per year)")
    print("=" * 70)
    print(f"\nOutput dir: {OUTPUT_DIR}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary_lines: list[str] = [
        "MERGE SAMPLE SUMMARY",
        "=" * 60,
        "",
        "Join keys expected:",
        f"  Sales+Forecast: {SALES_JOIN}",
        f"  Sales+Avl:     city_name, hub_name, process_dt(Sales)=av_dt(Avl), product_id",
        "",
    ]

    for year, paths in FILES.items():
        print(f"\n--- {year} ---")
        summary_lines.append(f"\n--- {year} ---")
        for source, path in paths.items():
            if not path.exists():
                print(f"  [SKIP] {source}: not found ({path})")
                summary_lines.append(f"  {source}: NOT FOUND")
                continue
            try:
                df = _read_rds(path)
                sample = df.sample(n=min(SAMPLE_SIZE, len(df)), random_state=42)
                out_path = OUTPUT_DIR / f"{year}_{source}_sample.csv"
                sample.to_csv(out_path, index=False)
                print(f"  {source}: {len(df):,} rows -> {out_path.name} ({len(sample)} rows)")

                cols = list(df.columns)
                summary_lines.append(f"  {source}: {len(df):,} rows, {len(cols)} cols")
                for jk in (SALES_JOIN if source != "avl" else ["city_name", "hub_name", "av_dt", "product_id"]):
                    status = "OK" if jk in cols else "MISSING"
                    summary_lines.append(f"    {jk}: {status}")
                if source == "forecast" and "process_dt" not in cols:
                    alt = "date" if "date" in cols else ("av_dt" if "av_dt" in cols else "?")
                    summary_lines.append(f"    (Forecast uses '{alt}' -> process_dt)")
            except Exception as e:
                print(f"  [ERROR] {source}: {e}")
                summary_lines.append(f"  {source}: ERROR - {e}")

    summary_path = OUTPUT_DIR / "merge_sample_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))
    print(f"\n  Summary: {summary_path}")

    print("\n" + "=" * 70)
    print("Done. Inspect CSV files to check join keys and column alignment.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
