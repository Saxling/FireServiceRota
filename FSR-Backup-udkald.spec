# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
from PyInstaller.utils.hooks import collect_all, collect_submodules

block_cipher = None

project_root = Path(__file__).resolve().parent

# ENTRYPOINT (som du ønskede)
script_path = project_root / "src" / "noedudkald" / "main.py"

# App identity
app_name = "FSR-Backup-udkald"  # fil/folder-navn (undgå æ/ø/å og mellemrum)
icon_ico = project_root / "assets" / "app_icon.ico"

# --- PySide6 (incl Qt plugins/resources + QtWebEngineProcess) ---
pyside_datas, pyside_binaries, pyside_hidden = collect_all("PySide6")

hiddenimports = []
hiddenimports += pyside_hidden
hiddenimports += collect_submodules("PySide6.QtWebEngineCore")
hiddenimports += collect_submodules("PySide6.QtWebEngineWidgets")

# --- Bundle your template data folder (read-only in dist) ---
datas = []
data_dir = project_root / "data"
if data_dir.exists():
    datas.append((str(data_dir), "data"))

# --- Analysis ---
a = Analysis(
    [str(script_path)],
    pathex=[str(project_root)],
    binaries=pyside_binaries,
    datas=datas + pyside_datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name=app_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # windowed app
    icon=str(icon_ico) if icon_ico.exists() else None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name=app_name,
)