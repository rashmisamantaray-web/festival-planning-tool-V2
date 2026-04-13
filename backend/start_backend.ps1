# Starts the API using a dedicated virtualenv so pip and uvicorn always use the same Python.
# Run from repo root:  .\backend\start_backend.ps1
# Or from backend:      .\start_backend.ps1

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "Creating .venv and installing dependencies (one-time)..."
    py -m venv .venv
    & .\.venv\Scripts\python.exe -m pip install --upgrade pip
    & .\.venv\Scripts\python.exe -m pip install -r requirements.txt
}

# Only watch `app/` — otherwise .venv triggers endless reloads (WatchFiles sees site-packages churn).
Write-Host "Starting server on http://localhost:8000 (pyarrow is logged on startup)"
& .\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --reload-dir app --port 8000
