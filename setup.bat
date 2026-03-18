@echo off
echo ==========================================
echo    AI Trader System Environment Setup
echo ==========================================

echo [1/2] Installing Backend Dependencies...
cd backend
python -m pip install --upgrade pip
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo [ERROR] Backend dependency installation failed.
    pause
    exit /b 1
)
cd ..

echo [2/2] Installing Frontend Dependencies...
cd frontend
npm install
if %errorlevel% neq 0 (
    echo [ERROR] Frontend dependency installation failed.
    pause
    exit /b 1
)
cd ..

echo.
echo ==========================================
echo    Setup Complete!
echo    You can now use start.bat to run the system.
echo ==========================================
pause
