"""
Festival date reader – parses the master festival Google Sheet.

The sheet layout (matching the notebook, cell 27):
  Row 0: title row with "Date" labels (skipped)
  Row 1: column headers — "Significant Event Tag", "Festival", then
         year columns like "2027", "2026", "2025", "2024", "2023", ...
         followed by more year columns for day-of-week, week-number,
         month, etc. and then city-level columns.
  Row 2+: data rows — each row is one festival.

IMPORTANT: The sheet has duplicate column names (e.g. multiple "2027"
columns for date, day, week, month).  The FIRST set of year columns
(columns 2 through 8 in the original sheet) contain the actual dates.
We identify them by checking that the values parse as dates.

Produces:
  - festival_map:  {festival_name: {year_int: Timestamp}}
  - all_festival_dates:  flat set of every known festival Timestamp
    (used to exclude festival weeks from baseline computation)
"""

from __future__ import annotations

import logging

import pandas as pd
import gspread

from app.data_loader import _get_gspread_client
from app.config import GSHEET_FESTIVAL_DATES

logger = logging.getLogger(__name__)

# In-memory cache so we don't re-fetch the sheet on every call.
_festival_cache: tuple[
    dict[str, dict[int, pd.Timestamp]], set[pd.Timestamp]
] | None = None


def load_festival_calendar() -> tuple[
    dict[str, dict[int, pd.Timestamp]], set[pd.Timestamp]
]:
    """Read the festival sheet and return (festival_map, all_festival_dates).

    festival_map
        Maps each festival name to a dict of {year: date}.
        Example: {"Holi": {2023: Timestamp("2023-03-08"), ...}}

    all_festival_dates
        A flat set of every known festival Timestamp.  Used by the
        baseline engine to skip weeks that contain festival dates.
    """
    global _festival_cache
    if _festival_cache is not None:
        return _festival_cache

    logger.info("Loading festival calendar from Google Sheet...")

    # Read raw data from Google Sheet
    client = _get_gspread_client()
    spreadsheet = client.open_by_url(GSHEET_FESTIVAL_DATES["url"])
    ws = spreadsheet.worksheet(GSHEET_FESTIVAL_DATES["worksheet"])
    raw_data = ws.get_all_values()

    # Row 0 is a title row ("Date", "Date", ...); row 1 has actual headers.
    # This matches the notebook: festival_data[1] = headers, festival_data[2:] = data.
    header_row = raw_data[1]  # ['Significant Event Tag', 'Festival', '2027', '2026', ...]
    data_rows = raw_data[2:]

    logger.info(f"  Header: {header_row[:10]}...")
    logger.info(f"  Data rows: {len(data_rows)}")

    # Find the Festival column index
    try:
        festival_col_idx = header_row.index("Festival")
    except ValueError:
        logger.error("Could not find 'Festival' column in the sheet!")
        _festival_cache = ({}, set())
        return _festival_cache

    # The date columns are the FIRST group of year-named columns after
    # "Festival".  They contain values like "12-Jan-2027".  We detect
    # them by checking that the header looks like a 4-digit year and
    # that the first non-empty data value in that column parses as a date.
    date_col_indices: list[int] = []
    for ci in range(festival_col_idx + 1, len(header_row)):
        col_name = header_row[ci].strip()

        # Stop if we hit a non-year column (like "Aug Check", city names, etc.)
        if not col_name.isdigit() or len(col_name) != 4:
            break

        # Verify this column actually contains date-like values
        # by checking the first non-empty cell
        is_date_col = False
        for row in data_rows[:10]:
            if ci < len(row) and row[ci].strip():
                try:
                    pd.to_datetime(row[ci].strip(), dayfirst=True)
                    is_date_col = True
                except (ValueError, TypeError):
                    pass
                break

        if is_date_col:
            date_col_indices.append(ci)
        else:
            # First non-date year column means we've passed the date section
            break

    if not date_col_indices:
        logger.warning("  No date columns found in festival sheet — festival_map will be empty")
    logger.info(f"  Date column indices: {date_col_indices}")
    logger.info(f"  Date column years: {[header_row[i] for i in date_col_indices]}")

    # Parse festival dates
    festival_map: dict[str, dict[int, pd.Timestamp]] = {}
    all_dates: set[pd.Timestamp] = set()

    for row in data_rows:
        # Get festival name
        if festival_col_idx >= len(row):
            continue
        name = str(row[festival_col_idx]).strip()
        if not name:
            continue

        year_dates: dict[int, pd.Timestamp] = {}
        for ci in date_col_indices:
            if ci >= len(row):
                continue
            val = str(row[ci]).strip()
            if not val:
                continue
            try:
                dt = pd.to_datetime(val, dayfirst=True).normalize()
                year_dates[dt.year] = dt
                all_dates.add(dt)
            except (ValueError, TypeError):
                continue

        if year_dates:
            festival_map[name] = year_dates

    logger.info(f"  Parsed {len(festival_map)} festivals, {len(all_dates)} total dates")
    if not festival_map:
        logger.warning("  Festival calendar is empty — no festivals found. Baseline will not exclude any dates.")
    if not all_dates:
        logger.warning("  No festival dates parsed — baseline exclusion set will be empty.")
    _festival_cache = (festival_map, all_dates)
    return festival_map, all_dates


def get_festival_names() -> list[str]:
    """Return a sorted list of all festival names in the sheet."""
    fmap, _ = load_festival_calendar()
    return sorted(fmap.keys())


def dates_for_festival(festival_name: str) -> dict[int, pd.Timestamp]:
    """Return {year: date} for a single festival, or {} if not found."""
    fmap, _ = load_festival_calendar()
    return fmap.get(festival_name, {})
