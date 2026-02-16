from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from noedudkald.ui.qt_app import run_gui


@dataclass(frozen=True)
class AppPaths:
    project_root: Path
    data_dir: Path
    addresses_csv: Path
    aba_xlsx: Path
    pickliste_xlsx: Path
    postnummer_xlsx: Path
    taskids_xlsx: Path


FSR_PRIORITY_MAP = {"Kørsel 1": "prio1", "Kørsel 2": "prio2"}


def fsr_location_from_display(address_display: str) -> str:
    # FSR location should be a clean one-liner. Keep postcode+city.
    return address_display.replace(",", "").strip()


def detect_project_root() -> Path:
    """
    Assumes this file is located at: <root>/src/noedudkald/main.py
    """
    return Path(__file__).resolve().parents[2]


def default_paths() -> AppPaths:
    root = detect_project_root()
    data_dir = root / "data" / "input"
    return AppPaths(
        project_root=root,
        data_dir=data_dir,
        addresses_csv=data_dir / "112 Adresse punkter.csv",
        aba_xlsx=data_dir / "ABA alarmer.xlsx",
        pickliste_xlsx=data_dir / "Pickliste.xlsx",
        postnummer_xlsx=data_dir / "Postnummer.xlsx",
        taskids_xlsx=data_dir / "Task Ids.xlsx",
    )


def ensure_files_exist(paths: AppPaths) -> None:
    missing = [p for p in
               [paths.addresses_csv, paths.aba_xlsx, paths.pickliste_xlsx, paths.postnummer_xlsx, paths.taskids_xlsx] if
               not p.exists()]
    if missing:
        msg = (
                "Missing datasource files:\n"
                + "\n".join(f" - {m}" for m in missing)
                + "\n\nExpected them under:\n"
                  f" {paths.data_dir}\n"
                  "\nFix: create the folder and copy your 3 files there."
        )
        raise FileNotFoundError(msg)


def prompt_priority() -> str:
    """
    Operator must choose:
      1 -> Kørsel 1
      2 -> Kørsel 2
    """
    while True:
        val = input("Priority (1=Kørsel 1, 2=Kørsel 2): ").strip()
        if val == "1":
            return "Kørsel 1"
        if val == "2":
            return "Kørsel 2"
        print("Invalid input. Enter 1 or 2.")


def main() -> int:
    run_gui()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
