# src/noedudkald/data_sources/aba.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from .normalize import normalize_text


@dataclass(frozen=True)
class AbaSite:
    doa_no: str
    name: str
    address_display: str  # e.g. "MAGLEHØJEN 10, 4000 ROSKILDE"
    address_norm: str
    primary_response: str  # e.g. "ROIL1,ROM1,ROV1"
    secondary_response: str  # e.g. "ROIL1,ROM2,ROV1"
    status: str


class AbaDirectory:
    def __init__(self, xlsx_path: str | Path):
        self.xlsx_path = Path(xlsx_path)
        self._df: pd.DataFrame | None = None

    def load(self) -> None:
        df = pd.read_excel(self.xlsx_path)
        # Normalize status once
        df["Status"] = df["Status"].fillna("").astype(str).str.strip()
        df["_status_norm"] = df["Status"].str.lower()

        # STRICT: only consider ABA sites that are in drift
        # (covers "Drift", "I drift", etc. — adjust if you truly mean exactly "Drift")
        df = df[df["_status_norm"].str.contains(r"\bdrift\b", na=False)].copy()

        required = ["DOA-nr", "Adresse", "Postnr/bynavn", "Navn", "Primær udrykning", "Sekundær udrykning", "Status"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"ABA Excel missing columns: {missing}")

        df["Adresse"] = df["Adresse"].astype(str).str.strip()
        df["Postnr/bynavn"] = df["Postnr/bynavn"].astype(str).str.strip()
        df["Navn"] = df["Navn"].fillna("").astype(str).str.strip()

        # Normalize response fields (avoid NaN / stray whitespace)
        df["Primær udrykning"] = df["Primær udrykning"].fillna("").astype(str).str.strip()
        df["Sekundær udrykning"] = df["Sekundær udrykning"].fillna("").astype(str).str.strip()
        df["Status"] = df["Status"].fillna("").astype(str).str.strip()

        # Human-readable display
        df["address_display"] = df["Adresse"] + ", " + df["Postnr/bynavn"]

        # Normalized display (useful for debugging / legacy match)
        df["address_norm"] = df["address_display"].map(normalize_text)

        # Key for robust matching: "Adresse" + 4-digit postcode only
        df["postcode4"] = df["Postnr/bynavn"].astype(str).str.extract(r"(\d{4})")[0].fillna("")
        df["key_basic"] = (df["Adresse"].astype(str).str.strip() + " " + df["postcode4"]).map(normalize_text)

        # Prefer the "best" row when multiple ABA sites share the same address key.
        # This prevents *FEJL* rows from "winning" just because they appear first in the Excel file.
        def _aba_row_score(r: pd.Series) -> int:
            prim = str(r.get("Primær udrykning", "")).strip()
            sec = str(r.get("Sekundær udrykning", "")).strip()
            status = str(r.get("Status", "")).strip().lower()
            name = str(r.get("Navn", "")).strip()

            score = 0

            # Primary response quality
            if prim and prim not in ("-", "*FEJL*"):
                score += 100
            elif prim == "*FEJL*":
                score -= 100  # strongly penalize known bad rows

            # Secondary response quality (nice to have, not required)
            if sec and sec not in ("-", "*FEJL*"):
                score += 10

            # Prefer active/in-service statuses
            if any(k in status for k in ("drift", "aktiv", "i drift", "in service")):
                score += 5

            # Tiny preference for named sites
            if name:
                score += 1

            return score

        df["_aba_score"] = df.apply(_aba_row_score, axis=1)

        # Sort so best rows appear first per key, then keep the best.
        df = (
            df.sort_values(["key_basic", "_aba_score"], ascending=[True, False])
              .drop_duplicates(subset=["key_basic"], keep="first")
              .reset_index(drop=True)
        )

        self._df = df

    def match_address(self, address_display: str) -> Optional[AbaSite]:
        if self._df is None:
            raise RuntimeError("AbaDirectory not loaded. Call load().")

        key = normalize_text(address_display)
        hit = self._df[self._df["address_norm"] == key]
        if hit.empty:
            return None

        r = hit.iloc[0]

        # Guard: if the "best" available row is still unusable, treat as no match
        if str(r["Primær udrykning"]).strip() == "*FEJL*":
            return None

        return AbaSite(
            doa_no=str(r["DOA-nr"]),
            name=str(r["Navn"]),
            address_display=str(r["address_display"]),
            address_norm=str(r["address_norm"]),
            primary_response=str(r["Primær udrykning"]),
            secondary_response=str(r["Sekundær udrykning"]),
            status=str(r["Status"]),
        )

    def match_components(self, street: str, house_no: str, house_letter: str, postcode: str):
        if self._df is None:
            raise RuntimeError("AbaDirectory not loaded. Call load().")

        pc = str(postcode).strip()
        hn = str(house_no).strip()
        hl = str(house_letter or "").strip()

        key_with_letter = normalize_text(f"{street} {hn} {hl} {pc}".strip())
        key_no_letter = normalize_text(f"{street} {hn} {pc}".strip())

        hit = self._df[self._df["key_basic"] == key_with_letter]
        if hit.empty:
            hit = self._df[self._df["key_basic"] == key_no_letter]

        if hit.empty:
            must_contain = normalize_text(f"{street} {hn} {pc}")
            hit = self._df[self._df["key_basic"].str.contains(must_contain, na=False)]

        if hit.empty:
            return None

        # If multiple candidates survive fallback contains-match, pick the highest score.
        if "_aba_score" in hit.columns and len(hit) > 1:
            hit = hit.sort_values("_aba_score", ascending=False)

        r = hit.iloc[0]

        if str(r["Primær udrykning"]).strip() == "*FEJL*":
            return None

        return AbaSite(
            doa_no=str(r["DOA-nr"]),
            name=str(r["Navn"]),
            address_display=str(r["address_display"]),
            address_norm=str(r.get("key_basic", "")),
            primary_response=str(r["Primær udrykning"]),
            secondary_response=str(r["Sekundær udrykning"]),
            status=str(r["Status"]),
        )
