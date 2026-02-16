@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

REM --- Ensure venv ---
if not exist ".venv\Scripts\python.exe" (
  echo Creating venv...
  py -m venv .venv
)

call ".venv\Scripts\activate.bat"
python -m pip install -U pip

REM --- Install requirements if present ---
if exist "requirements.txt" (
  python -m pip install -r requirements.txt
)

REM --- Build tools ---
python -m pip install -U pyinstaller pillow

REM --- Convert PNG -> ICO (PyInstaller icon works best with .ico) ---
if not exist "assets" mkdir "assets"
if exist "assets\app_icon.png" (
  python - <<PY
from PIL import Image
from pathlib import Path

png = Path("assets/app_icon.png")
ico = Path("assets/app_icon.ico")
img = Image.open(png).convert("RGBA")
# Create multi-size ICO for best Windows display
sizes = [(16,16),(24,24),(32,32),(48,48),(64,64),(128,128),(256,256)]
img.save(ico, format="ICO", sizes=sizes)
print("Wrote", ico)
PY
) else (
  echo [WARN] assets\app_icon.png not found. Build will proceed without icon.
)

REM --- Clean old builds ---
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"

REM --- Build ---
pyinstaller --noconfirm --clean "FSR-Backup-udkald.spec"
if errorlevel 1 (
  echo [ERROR] Build failed.
  exit /b 1
)

echo.
echo Build complete:
echo   dist\FSR-Backup-udkald\FSR-Backup-udkald.exe
echo.
pause
endlocal