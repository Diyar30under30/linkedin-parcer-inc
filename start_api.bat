@echo off
call venv\Scripts\activate.bat
echo ===========================================
echo LinkedIn Parser API Server
echo ===========================================
echo.
python fastapi_server.py
pause
