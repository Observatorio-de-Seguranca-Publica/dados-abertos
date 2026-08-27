@echo off

chcp 65001 > nul

cd /d "%~dp0"

if not exist logs mkdir logs

set DATA=%date:~6,4%_%date:~3,2%_%date:~0,2%

python main.py >> logs\execucao_%DATA%.log 2>&1