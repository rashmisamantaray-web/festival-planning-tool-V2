# Festival Impact Planning Tool

Automates the festival impact analysis workflow across 5 hierarchical levels, replacing the manual Excel-based process.

## Levels

1. **City** — overall drop/spike per city (editable override rows) + D-5 to D+5 trend chart
2. **City-Subcategory** — per subcategory, indexed against city
3. **City-Subcategory-CutClass** — per cut class, indexed against city-subcat
4. **City-Hub** — per hub, indexed against city
5. **City-Hub-CutClass** — derived from hub drops, indexed against city-subcat-cutclass

## Setup

### Prerequisites

- Python 3.11+
- Access to the Google Drive RDS files and Google Sheets

### Backend

**Before first run** — prepare merged data (G: drive must be accessible):

```bash
cd backend
py scripts/run_prep.py
```

If you use `start_backend.ps1`, run prep with the same venv: `.\.venv\Scripts\python.exe scripts/run_prep.py`

This runs:
1. `merge_archive_rds.py` — merges Sales + Forecast + Availability for 2023–2025 (required)
2. `convert_6w_to_parquet.py` — converts 6w_v3.RDS to parquet for 2026 (optional, faster)

Then start the server.

**Recommended (Windows):** use the project virtual environment so the backend always has the right packages (including `pyarrow>=19.0.1` for parquet).

From **PowerShell**:

```powershell
cd backend
.\start_backend.ps1
```

From **Command Prompt or Anaconda Prompt** (`.ps1` does not run there — use the batch file or call PowerShell explicitly):

```bat
cd backend
start_backend.bat
```

Or: `powershell -NoProfile -ExecutionPolicy Bypass -File start_backend.ps1`

The first run creates `backend\.venv` and installs `requirements.txt`. After that, always start the API with `.\start_backend.ps1` (not a different `python` or `uvicorn` from elsewhere).

**Manual:** from `backend`, `py -m pip install -r requirements.txt` then `py -m uvicorn app.main:app --reload --reload-dir app --port 8000` — use the same `py` for both. (`--reload-dir app` avoids reload loops from watching `.venv`.)

If Compute fails with a parquet error, check the server log for `pyarrow version:` — it must be **19.0.1 or newer**.

### Frontend (Streamlit)

```bash
pip install -r streamlit_app/requirements.txt
streamlit run streamlit_app/app.py
```

Open http://localhost:8501 in your browser. The backend must be running on port 8000.

### Configuration

Edit `backend/app/config.py` to update:
- RDS file paths (per year) — uncomment/add entries for each year you need (2023, 2024, 2025, 2026)
- Google Sheet URLs
- Service account credentials path

## Usage

1. Enter 4 dates in the date input panel:
   - **Current Date** — the planning date (used as baseline reference for base correction)
   - **Reference Date 1, 2, 3** — historical comparison dates (can be from any year, including the same year)
2. Click **Compute** to run the analysis
3. Review the **City** tab — edit override rows as needed; the D-5 to D+5 trend chart shows daily sales around each reference date, filterable by city
4. Navigate through tabs (City-Subcategory, City-Subcategory-CutClass, City-Hub, City-Hub-CutClass) to review/edit lower levels
5. Edits auto-cascade to downstream levels
6. Click **Export Excel** to download the formatted workbook
