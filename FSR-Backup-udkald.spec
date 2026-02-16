# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

block_cipher = None

project_root = Path(".").resolve()

datas = [
    ("data", "data"),  # bundle default config and structure
]

hiddenimports = [
    "PySide6.QtWebEngineWidgets",
    "pandas",
    "openpyxl",
]

a = Analysis(
    ["src/noedudkald/main.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FSR-Backup-udkald",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon="assets/app_icon.ico",
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name="FSR-Backup-udkald",
)