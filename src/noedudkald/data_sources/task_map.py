from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class TaskSelectionResult:
    task_ids: list[int]
    missing_units: list[str]
    assistance_added: bool
    assistance_unit: Optional[str]


class TaskMap:
    """
    Loads mapping unit -> list[int] task_ids from TaskIds.xlsx.
    Supports:
      - normal integers (e.g. 823)
      - "two task ids in one numeric cell" encoded like 3134.1268 -> [3134, 1268]
    """

    # Task IDs that do NOT trigger assistance auto-alert
    ASSIST_EXCLUDE_TASK_IDS = {823, 3134, 7040, 6509, 3176, 7035, 7036, 7037, 5474, 1268}

    def __init__(self, path: str | Path, sheet_name: str | None = None):
        self.path = Path(path)
        self.sheet_name = sheet_name
        self._map: dict[str, list[int]] = {}

    def load(self) -> None:
        if self.sheet_name is None:
            df = pd.read_excel(self.path)
        else:
            df = pd.read_excel(self.path, sheet_name=self.sheet_name)

        # df = pd.read_excel(self.path, sheet_name=self.sheet_name)

        # Expected columns in your file: unit, task_id
        cols = {str(c).strip().lower(): c for c in df.columns}
        unit_col = cols.get("unit")
        task_col = cols.get("task_id")

        if not unit_col or not task_col:
            raise ValueError(f"TaskIds.xlsx must have columns 'unit' and 'task_id'. Found: {list(df.columns)}")

        df = df[[unit_col, task_col]].copy()
        df[unit_col] = df[unit_col].astype(str).str.strip()

        out: dict[str, list[int]] = {}
        for _, r in df.iterrows():
            unit = str(r[unit_col]).strip()
            if not unit:
                continue
            ids = self._parse_task_ids(r[task_col])
            if not ids:
                continue
            out[unit] = ids

        self._map = out

    @staticmethod
    def _parse_task_ids(value) -> list[int]:
        """
        Handles:
          - 823 or 823.0 -> [823]
          - 3134.1268 -> [3134, 1268]
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []

        s = str(value).strip()

        # pandas/openpyxl read numbers as floats; keep the "3134.1268" string
        if "." in s:
            left, right = s.split(".", 1)
            left = left.strip()
            right = right.strip().rstrip("0")

            ids: list[int] = []
            if left and left.isdigit():
                ids.append(int(left))

            if right and right.isdigit():
                ids.append(int(right))

            # de-dup, keep order
            seen = set()
            return [x for x in ids if not (x in seen or seen.add(x))]

        # plain integer-like
        try:
            return [int(float(s))]
        except Exception:
            return []

    def task_ids_for_unit(self, unit: str) -> list[int] | None:
        return self._map.get(unit.strip())

    def select_task_ids_for_units(
            self,
            units: list[str],
            now: Optional[datetime] = None,
            auto_add_assistance: bool = True,
    ) -> TaskSelectionResult:
        """
        Returns task_ids + missing units, and auto-adds Ass.Dag / Ass.Nat when applicable.

        Assistance rule:
          - Only add assistance if ANY selected task_id is NOT in ASSIST_EXCLUDE_TASK_IDS.
          - Ass.Dag is added only Mon–Fri 07:00–17:00
          - Ass.Nat is added nights + weekends
        """
        now = now or datetime.now()
        task_ids: list[int] = []
        missing: list[str] = []

        for u in units:
            ids = self.task_ids_for_unit(u)
            if ids is None:
                missing.append(u)
            else:
                task_ids.extend(ids)

        # de-dup, keep order
        seen = set()
        task_ids = [x for x in task_ids if not (x in seen or seen.add(x))]

        assistance_added = False
        assistance_unit = None

        if auto_add_assistance and task_ids:
            # Trigger assistance if ANY selected task_id is NOT in exclude list
            triggers = any(tid not in self.ASSIST_EXCLUDE_TASK_IDS for tid in task_ids)

            if triggers:
                t = now.time()
                weekday = now.weekday()  # 0=Mon .. 6=Sun
                is_weekday = weekday <= 4
                is_daytime = (t >= time(7, 0)) and (t < time(17, 0))

                # Mon–Fri daytime => Ass.Dag, otherwise Ass.Nat
                assistance_unit = "Ass.Dag" if (is_weekday and is_daytime) else "Ass.Nat"

                ass_ids = self.task_ids_for_unit(assistance_unit) or []

                # Only add if mapping exists
                if ass_ids:
                    for tid in ass_ids:
                        if tid not in seen:
                            task_ids.append(tid)
                            seen.add(tid)
                    assistance_added = True

        return TaskSelectionResult(
            task_ids=task_ids,
            missing_units=missing,
            assistance_added=assistance_added,
            assistance_unit=assistance_unit,
        )
