# src/noedudkald/data_sources/addresses.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from difflib import SequenceMatcher

import pandas as pd

from .normalize import normalize_address, normalize_text


@dataclass(frozen=True)
class KnownAddress:
    display: str
    norm_key: str
    district_no: str
    street: str
    house_no: str
    house_letter: str
    postcode: str
    city: str = ""


@dataclass(frozen=True)
class ManualAddress:
    display: str
    street: str
    house_no: str
    house_letter: str
    postcode: str
    city: str
    district_no: str  # user-supplied when not known


class AddressDirectory:
    def __init__(self, csv_path: str | Path):
        self.csv_path = Path(csv_path)
        self._df: pd.DataFrame | None = None
        self._known_postcodes: set[str] = set()

    # --------------------------------------------------
    # LOAD
    # --------------------------------------------------
    def load(self, postcode_to_city: dict[str, str] | None = None) -> None:
        df = pd.read_csv(self.csv_path, sep=",", dtype=str).fillna("")

        required = [
            "Distrikt nummer",
            "Vejnavn",
            "Hus nummer",
            "Hus bogstav",
            "Postnummer",
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Address CSV missing columns: {missing}")

        df["Vejnavn"] = df["Vejnavn"].str.strip()
        df["Hus nummer"] = df["Hus nummer"].str.strip()
        df["Hus bogstav"] = df["Hus bogstav"].str.strip()
        df["Postnummer"] = df["Postnummer"].str.strip()

        if postcode_to_city:
            df["city"] = df["Postnummer"].map(postcode_to_city).fillna("")
        else:
            df["city"] = ""

        df["display"] = (
            df["Vejnavn"]
            + " "
            + df["Hus nummer"]
            + df["Hus bogstav"]
            + ", "
            + df["Postnummer"]
            + " "
            + df["city"]
        ).str.strip()

        df["norm_key"] = df["display"].map(normalize_text)

        # ---------- Precompute for fast search ----------
        df["street_norm"] = df["Vejnavn"].map(normalize_text)
        df["house_norm"] = df["Hus nummer"].astype(str).str.strip()
        df["letter_norm"] = df["Hus bogstav"].astype(str).str.strip()
        df["postcode_norm"] = df["Postnummer"].astype(str).str.strip()

        self._known_postcodes = set(df["postcode_norm"].unique())
        df["postcode_in_112"] = df["postcode_norm"].isin(self._known_postcodes).astype(int)

        self._df = df

    # --------------------------------------------------
    # STRICT MATCH
    # --------------------------------------------------
    def find_by_components(
        self, street: str, house: str, extra: str = "", limit: int = 60
    ) -> List[KnownAddress]:
        if self._df is None:
            raise RuntimeError("AddressDirectory not loaded.")

        street_n = normalize_text(street)
        house_n = str(house).strip()
        extra_n = str(extra).strip()

        df = self._df

        hit = df[
            (df["street_norm"] == street_n)
            & (df["house_norm"] == house_n)
        ]

        if extra_n:
            hit = hit[hit["letter_norm"].str.lower() == extra_n.lower()]

        hit = hit.head(limit)

        return [
            KnownAddress(
                display=r["display"],
                norm_key=r["norm_key"],
                district_no=r["Distrikt nummer"],
                street=r["Vejnavn"],
                house_no=r["Hus nummer"],
                house_letter=r["Hus bogstav"],
                postcode=r["Postnummer"],
                city=r["city"],
            )
            for _, r in hit.iterrows()
        ]

    # --------------------------------------------------
    # GOOGLE STYLE FUZZY FALLBACK
    # --------------------------------------------------
    @staticmethod
    def _sim(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()

    def find_fuzzy_street_house(
        self,
        street: str,
        house_no: str,
        house_letter: str = "",
        limit: int = 40,
        min_score: float = 0.72,
    ) -> List[KnownAddress]:

        if self._df is None:
            raise RuntimeError("AddressDirectory not loaded.")

        street_q = normalize_text(street)
        house_q = str(house_no).strip()
        letter_q = str(house_letter or "").strip()

        if not street_q or not house_q:
            return []

        df = self._df

        # 1) strict house filter
        cand = df[df["house_norm"] == house_q]
        if cand.empty:
            return []

        # 2) letter preference
        if letter_q:
            cand = cand.assign(
                _letter_bonus=(cand["letter_norm"].str.lower() == letter_q.lower()).astype(int)
            )
        else:
            cand = cand.assign(_letter_bonus=0)

        # 3) scoring
        def score_row(r) -> float:
            s = self._sim(street_q, r["street_norm"])
            if r["street_norm"] == street_q:
                s += 0.20
            elif r["street_norm"].startswith(street_q):
                s += 0.10
            s += 0.05 * float(r["_letter_bonus"])
            return s

        cand = cand.copy()
        cand["_score"] = cand.apply(score_row, axis=1)

        cand = cand[cand["_score"] >= min_score]
        if cand.empty:
            return []

        # 4) sort: 112 postcodes first, then best similarity
        cand = cand.sort_values(
            by=["postcode_in_112", "_score"],
            ascending=[False, False],
            kind="mergesort",
        ).head(limit)

        return [
            KnownAddress(
                display=r["display"],
                norm_key=r["norm_key"],
                district_no=r["Distrikt nummer"],
                street=r["Vejnavn"],
                house_no=r["Hus nummer"],
                house_letter=r["Hus bogstav"],
                postcode=r["Postnummer"],
                city=r["city"],
            )
            for _, r in cand.iterrows()
        ]


def make_manual_address(
        street: str,
        house_no: str,
        house_extra: str,
        postcode: str,
        city: str,
        district_no: str,
) -> ManualAddress:
    extra = f" {house_extra.strip()}" if house_extra.strip() else ""
    display = f"{street.strip()} {house_no.strip()}{extra}, {postcode.strip()} {city}".strip()
    return ManualAddress(
        display=display,
        street=street.strip(),
        house_no=house_no.strip(),
        house_letter=house_extra.strip(),
        postcode=postcode.strip(),
        city=city.strip(),
        district_no=district_no.strip(),
    )
