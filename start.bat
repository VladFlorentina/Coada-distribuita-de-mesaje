@echo off
setlocal

REM Start services in background
call docker compose up -d --build

REM Open log terminals
start "dmq-node1-logs" cmd /k "title dmq-node1-logs & docker compose logs -f node1"
start "dmq-node2-logs" cmd /k "title dmq-node2-logs & docker compose logs -f node2"
start "dmq-node3-logs" cmd /k "title dmq-node3-logs & docker compose logs -f node3"

REM Open a single control terminal with useful commands
start "dmq-control" cmd /k "title dmq-control & control.bat"

endlocal
