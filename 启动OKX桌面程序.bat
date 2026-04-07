@echo off
setlocal
cd /d "%~dp0"
start "" pyw "%~dp0app\desktop_main.pyw"
exit /b 0
