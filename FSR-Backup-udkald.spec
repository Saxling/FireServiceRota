# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
from PyInstaller.utils.hooks import collect_all, collect_submodules

block_cipher = None

# PyInstaller provides SPECPATH. Use it instead of __file__.
project_root = Path(globals().get("SPECPATH", Path.cwd())).resolve()
src_root = project_root / "src"
script_path = src_root / "noedudkald" / "main.py"

# PySide6 (incl. plugins/resources/QtWebEngineProcess)
pyside_datas, pyside_binaries, pyside_hidden = collect_all("PySide6")

hiddenimports = []
hiddenimports += pyside_hidden
hiddenimports += collect_submodules("PySide6.QtWebEngineCore")
hiddenimports += collect_submodules("PySide6.QtWebEngineWidgets")

# Force include your package (src-layout)
hiddenimports += collect_submodules("noedudkald")

# Bundle default config template (runtime uses APPDATA)
datas = []
cfg_dir = project_root / "data" / "config"
if cfg_dir.exists():
    datas.append((str(cfg_dir), "data/config"))

a = Analysis(
    [str(script_path)],
    pathex=[str(project_root), str(src_root)],   # <-- THIS FIXES src-layout imports
    binaries=pyside_binaries,
    datas=datas + pyside_datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FSR-Backup-udkald",
    console=False,
    icon=str(project_root / "assets" / "app_icon.ico"),
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    name="FSR-Backup-udkald",
)