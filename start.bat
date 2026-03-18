@echo off
setlocal enabledelayedexpansion

echo ==========================================
echo    AI Trader System Quick Start
echo ==========================================

:: 检查 Python 是否安装
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not in PATH.
    pause
    exit /b 1
)

:: 检查 Node.js 是否安装
node -v >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Node.js is not installed or not in PATH.
    pause
    exit /b 1
)

echo [1/2] Starting Backend Server (FastAPI)...
start "AI Trader Backend" cmd /k "cd backend && python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"

echo Waiting 5 seconds for backend to initialize...
timeout /t 5 /nobreak >nul

echo [2/2] Starting Frontend Server (Vite)...
start "AI Trader Frontend" cmd /k "cd frontend && npm run dev -- --host 0.0.0.0 --port 5173 --strictPort"

echo [3/3] Starting System Monitor...
start "AI Trader Monitor" cmd /k "python backend/scripts/monitor_system.py"

echo.
echo ==========================================
echo    Servers are starting in new windows...
echo    Backend: http://localhost:8000
echo    Frontend: http://localhost:5173
echo ==========================================
echo.
echo Press any key to exit this launcher...
pause >nul
