"""
Bootstrap script — run all data-prep scripts before starting the Festival Planning Tool.

Layer: Scripts / CLI
  1. merge_archive_rds.py  — Merges Sales + Forecast + Availability for 2023–2025
  2. convert_6w_to_parquet.py — Converts 6w_v3.RDS to parquet for 2026 (faster load)

Prerequisites:
  - G: drive (or configured paths) accessible
  - Google Sheets credentials configured

Usage (from backend/):
    py scripts/run_prep.py
    py scripts/run_prep.py --skip-6w          # Skip 6w conversion (archive only)
    py scripts/run_prep.py --skip-archive    # Skip archive merge (6w only)
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_backend_dir = _script_dir.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("run_prep")


def _run_script(name: str, script_path: Path) -> int:
    """Run a script and stream its output. Returns exit code."""
    print(f"\n{'='*60}\n  {name}\n{'='*60}\n")
    logger.info("Pipeline stage started: %s", name)
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(_backend_dir),
    )
    elapsed = time.time() - t0
    print(f"\n  Completed in {elapsed:.1f}s (exit code: {result.returncode})")
    logger.info("Pipeline stage completed: %s in %.1fs (exit_code=%d)", name, elapsed, result.returncode)
    if result.returncode != 0:
        logger.warning("Stage %s failed with exit code %d", name, result.returncode)
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run data-prep scripts before starting the Festival Planning Tool.",
    )
    parser.add_argument(
        "--skip-archive",
        action="store_true",
        help="Skip merge_archive_rds.py (only run 6w conversion)",
    )
    parser.add_argument(
        "--skip-6w",
        action="store_true",
        help="Skip convert_6w_to_parquet.py (only run archive merge)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("Festival Planning Tool — Data Prep")
    print("=" * 70)
    logger.info("Bootstrap started")
    failed = False

    if not args.skip_archive:
        code = _run_script(
            "merge_archive_rds.py (2023, 2024, 2025)",
            _script_dir / "merge_archive_rds.py",
        )
        if code != 0:
            logger.error("merge_archive_rds.py failed. Fix errors and re-run.")
            failed = True
        else:
            logger.info("Archive merge complete.")
    else:
        logger.info("Skipping merge_archive_rds.py (--skip-archive)")

    if not args.skip_6w:
        code = _run_script(
            "convert_6w_to_parquet.py (2026)",
            _script_dir / "convert_6w_to_parquet.py",
        )
        if code != 0:
            logger.warning("convert_6w_to_parquet.py failed. 2026 will use RDS (slower).")
        else:
            logger.info("6w parquet conversion complete.")
    else:
        logger.info("Skipping convert_6w_to_parquet.py (--skip-6w)")

    print("\n" + "=" * 70)
    if failed:
        print("Prep failed. Fix errors above and re-run.")
        logger.error("Bootstrap failed")
        return 1
    print("Prep complete. Start the tool:")
    print("  cd backend")
    print("  uvicorn app.main:app --reload --port 8000")
    print("=" * 70)
    logger.info("Bootstrap complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
