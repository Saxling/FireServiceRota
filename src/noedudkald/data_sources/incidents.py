# src/noedudkald/data_sources/incidents.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class IncidentProfile:
    district_no: str
    incident_code: str
    incident_label: str
    units: list[str]


class IncidentMatrix:
    def __init__(self, xlsx_path: str | Path):
        self.xlsx_path = Path(xlsx_path)
        self._by_district: dict[str, dict[str, IncidentProfile]] = {}

    def load(self) -> None:
        xl = pd.ExcelFile(self.xlsx_path)
        self._by_district.clear()

        for sheet in xl.sheet_names:
            district_no = str(sheet).strip()
            df = pd.read_excel(self.xlsx_path, sheet_name=sheet)

            # In your file, incident code column becomes "Unnamed: 0"
            if "Unnamed: 0" not in df.columns:
                raise ValueError(f"Pickliste sheet '{sheet}' missing 'Unnamed: 0' (incident code column).")

            code_col = "Unnamed: 0"

            # The incident label is in the second column; header varies per sheet, so just take col index 1
            if len(df.columns) < 2:
                raise ValueError(f"Pickliste sheet '{sheet}' has too few columns.")
            label_col = df.columns[1]

            # Determine which columns are unit columns: everything from col index 5 onward
            # (based on your file: first columns are metadata flags/notes)
            unit_cols = list(df.columns[5:])

            district_map: dict[str, IncidentProfile] = {}
            for _, r in df.iterrows():
                incident_code = str(r[code_col]).strip()
                if not incident_code or incident_code.lower() == "nan":
                    continue

                incident_label = str(r[label_col]).strip()

                units: list[str] = []
                for uc in unit_cols:
                    val = r.get(uc)
                    if pd.notna(val) and str(val).strip().upper() == "X":
                        units.append(str(uc).strip())

                district_map[incident_code] = IncidentProfile(
                    district_no=district_no,
                    incident_code=incident_code,
                    incident_label=incident_label,
                    units=units,
                )

            self._by_district[district_no] = district_map

    def get_profile(self, district_no: str, incident_code: str) -> Optional[IncidentProfile]:
        district_no = str(district_no).strip()
        incident_code = str(incident_code).strip()
        return self._by_district.get(district_no, {}).get(incident_code)

    def list_incidents(self, district_no: str) -> list[IncidentProfile]:
        district_no = str(district_no).strip()
        return list(self._by_district.get(district_no, {}).values())
