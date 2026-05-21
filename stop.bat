@echo off
setlocal

call docker compose down

REM Close log and control terminals opened by start.bat
taskkill /FI "WINDOWTITLE eq dmq-node1-logs*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq dmq-node2-logs*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq dmq-node3-logs*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq dmq-control*" /T /F >nul 2>&1

REM Fallback: force-close any cmd windows with title starting with dmq-
powershell -NoProfile -Command "Get-Process cmd | Where-Object { $_.MainWindowTitle -like 'dmq-*' } | Stop-Process -Force" >nul 2>&1

endlocal
