import json
import shutil
from pathlib import Path

from noedudkald.persistence.runtime_paths import ensure_user_data_layout


class SourceConfig:
    def __init__(self, project_root: Path):
        self.project_root = project_root

        # 🔹 NEW – use APPDATA
        udata = ensure_user_data_layout()

        self.config_path = udata / "config" / "sources.json"
        self.input_dir = udata / "input"
        self.input_dir.mkdir(parents=True, exist_ok=True)

        self.defaults = {
            "aba": "ABA alarmer.xlsx",
            "addresses": "112 Adresse punkter.csv",
            "incidents": "Pickliste.xlsx",
            "task_ids": "TaskIds.xlsx",
            "postcodes": "Postnummer.xlsx",
        }

    def load(self) -> dict:
        if not self.config_path.exists():
            return self.defaults.copy()

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # ensure missing keys are restored
        for k, v in self.defaults.items():
            data.setdefault(k, v)

        return data

    def save(self, cfg: dict):
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)

    def copy_to_input(self, key: str, source_file: Path) -> Path:
        filename = self.defaults[key]
        target = self.input_dir / filename
        shutil.copy2(source_file, target)
        return target
