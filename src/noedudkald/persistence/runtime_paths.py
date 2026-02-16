from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


APP_DIR_NAME = "FSR-Backup-udkald"  # må gerne være uden æøå i filsystem


def is_frozen() -> bool:
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


def exe_dir() -> Path:
    # Where the exe lives (onedir)
    return Path(sys.executable).resolve().parent if is_frozen() else Path(__file__).resolve().parents[3]


def bundled_data_dir() -> Path:
    # In dev: <repo>/data
    # In frozen: <dist>/FSR-Backup-udkald/data (we bundle it in the spec)
    return exe_dir() / "data"


def appdata_root() -> Path:
    base = os.environ.get("APPDATA")
    if not base:
        # fallback, very rare
        base = str(Path.home() / "AppData" / "Roaming")
    return Path(base) / APP_DIR_NAME


def user_data_dir() -> Path:
    # %APPDATA%\FSR-Backup-udkald\data
    return appdata_root() / "data"


def ensure_user_data_layout() -> Path:
    """
    Ensures %APPDATA% layout exists:
      data/
        input/
        config/
        secrets/
    Copies bundled config defaults (only) if user config missing.
    Returns user data dir.
    """
    udata = user_data_dir()
    (udata / "input").mkdir(parents=True, exist_ok=True)
    (udata / "config").mkdir(parents=True, exist_ok=True)
    (udata / "secrets").mkdir(parents=True, exist_ok=True)

    # Copy default config files from bundled data/config -> user data/config (only if missing)
    bcfg = bundled_data_dir() / "config"
    ucfg = udata / "config"
    if bcfg.exists():
        for p in bcfg.glob("*"):
            if p.is_file():
                target = ucfg / p.name
                if not target.exists():
                    shutil.copy2(p, target)

    return udata
