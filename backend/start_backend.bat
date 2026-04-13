@echo off
REM Run from cmd, Anaconda Prompt, or double-click. Starts API with project .venv.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_backend.ps1"
if errorlevel 1 pause
