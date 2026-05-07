@echo off

cd /d "C:\Users\x15501492\Downloads\code\observatorio\dados-abertos"

if not exist logs mkdir logs

set DATA=%date:~6,4%_%date:~3,2%_%date:~0,2%

"C:\Program Files\Python313\python.exe" main.py >> logs\execucao_%DATA%.log 2>&1
