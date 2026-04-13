"""
Convert 6w_v3.RDS to Parquet for faster loading.

The 6w RDS file contains the merged 6-week rolling data for 2026 (live data).
Converting to Parquet provides:
  - Faster read times (columnar format)
  - Column pruning (read only needed columns)
  - Consistency with archive years (2023-2025) which use parquet

Usage (from backend/, with G: drive accessible):
    py scripts/convert_6w_to_parquet.py

Output: merged_2026.parquet in the Historicals Festive Tool directory
        (same location as merged_2023/2024/2025.parquet)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_script_dir.parent))

import pandas as pd
import pyreadr

from app.config import PARQUET_PATHS, RDS_COLUMNS_TO_KEEP, RDS_PATHS


def main() -> None:
    year_key = "2026"
    if year_key not in RDS_PATHS:
        print(f"ERROR: {year_key} not configured in RDS_PATHS")
        sys.exit(1)

    rds_paths = RDS_PATHS[year_key]
    rds_path = rds_paths[0]
    if not Path(rds_path).exists():
        print(f"ERROR: RDS file not found: {rds_path}")
        print("  Ensure the G: drive (or configured path) is accessible.")
        sys.exit(1)

    output_path = PARQUET_PATHS.get(year_key)
    if not output_path:
        # Fallback: same dir as 2025
        hist_dir = Path(PARQUET_PATHS["2025"]).parent
        output_path = str(hist_dir / "merged_2026.parquet")

    print(f"Reading RDS: {rds_path}")
    t0 = time.time()
    result = pyreadr.read_r(rds_path)
    df = next(iter(result.values()))
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} cols ({time.time()-t0:.1f}s)")

    # Keep only columns needed by the pipeline (copy to avoid SettingWithCopyWarning)
    available = [c for c in RDS_COLUMNS_TO_KEEP if c in df.columns]
    missing = [c for c in RDS_COLUMNS_TO_KEEP if c not in df.columns]
    if missing:
        print(f"  Note: columns not in RDS (will be absent in parquet): {missing}")
    df = df[available].copy()

    # Ensure process_dt is datetime
    if "process_dt" in df.columns:
        df["process_dt"] = pd.to_datetime(df["process_dt"])

    print(f"Writing parquet: {output_path}")
    t1 = time.time()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"  Done ({time.time()-t1:.1f}s)")

    size_mb = Path(output_path).stat().st_size / (1024 * 1024)
    print(f"\nOutput: {output_path} ({size_mb:.1f} MB)")
    print("The data_loader will use this parquet when 2026 is in PARQUET_PATHS.")


if __name__ == "__main__":
    main()
