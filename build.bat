@echo off
setlocal enabledelayedexpansion

REM ================================
REM  FSR Backup – ONEDIR build
REM ================================

REM Gå til projektets rodmappe
cd /d "%~dp0"

echo.
echo ===== FSR Backup build starter =====
echo.

REM --- Tjek om venv findes ---
if not exist ".venv\Scripts\python.exe" (
    echo Opretter virtualenv...
    py -m venv .venv
)

REM --- Aktivér venv ---
call ".venv\Scripts\activate.bat"

REM --- Opdater pip ---
echo Opdaterer pip...
python -m pip install --upgrade pip

REM --- Installer dependencies ---
if exist "requirements.txt" (
    echo Installerer requirements...
    python -m pip install -r requirements.txt
)

REM --- Installer build tools ---
echo Installerer PyInstaller...
python -m pip install --upgrade pyinstaller

REM --- Ryd gamle builds ---
if exist "build" (
    echo Sletter build mappe...
    rmdir /s /q "build"
)

if exist "dist" (
    echo Sletter dist mappe...
    rmdir /s /q "dist"
)

REM --- Byg programmet ---
echo.
echo Bygger FSR Backup...
echo.

pyinstaller --clean --noconfirm "FSR-Backup-udkald.spec"

if errorlevel 1 (
    echo.
    echo BUILD FEJLEDE!
    pause
    exit /b 1
)

echo.
echo ===== BUILD FÆRDIG =====
echo.

echo Din færdige build ligger her:
echo.
echo   dist\FSR-Backup-udkald\
echo.
echo EXE:
echo   dist\FSR-Backup-udkald\FSR-Backup-udkald.exe
echo.

pause
endlocal