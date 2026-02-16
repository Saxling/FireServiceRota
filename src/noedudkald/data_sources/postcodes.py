from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class PostcodeEntry:
    postcode: str
    city: str


class PostcodeDirectory:
    """
    Reads Postnummer.xlsx (sheet 'Data') which contains columns:
      Postnr | By
    """

    def __init__(self, xlsx_path: str | Path):
        self.xlsx_path = Path(xlsx_path)
        self._map: dict[str, str] = {}

    def load(self) -> None:
        try:
            df = pd.read_excel(self.xlsx_path)

            required = ["Postnr", "By"]
            missing = [c for c in required if c not in df.columns]
            if missing:
                raise ValueError(
                    f"Postnummer.xlsx is missing columns {missing}.\n"
                    f"Found columns: {list(df.columns)}"
                )

            df["Postnr"] = df["Postnr"].astype(str).str.strip()
            df["By"] = df["By"].astype(str).str.strip()

            self._map = dict(zip(df["Postnr"], df["By"]))
        except Exception as e:
            raise RuntimeError(
                f"Failed to read Postnummer file: {self.xlsx_path}\n"
                f"Original error: {e}"
            )

    def city_for_postcode(self, postcode: str) -> str:
        return self._map.get(str(postcode).strip(), "")

    def as_dict(self) -> dict[str, str]:
        return dict(self._map)
