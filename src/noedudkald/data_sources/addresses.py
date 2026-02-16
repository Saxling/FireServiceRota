# src/noedudkald/data_sources/addresses.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

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

    def load(self, postcode_to_city: dict[str, str] | None = None) -> None:
        # Try common Danish CSV formats
        last_err = None
        df = None

        for sep in [";", ",", "\t"]:
            try:
                df_try = pd.read_csv(self.csv_path, sep=sep, encoding="utf-8-sig")
                # If it parses into 1 column, it's probably the wrong separator
                if len(df_try.columns) <= 1:
                    continue
                df = df_try
                break
            except Exception as e:
                last_err = e

        if df is None:
            raise RuntimeError(
                f"Failed to read address CSV: {self.csv_path}\n"
                f"Last error: {last_err}"
            )

        # Normalize column names (strip spaces)
        df.columns = [str(c).strip() for c in df.columns]

        required = ["Vejnavn", "Hus nummer", "Hus bogstav", "Postnummer", "Distrikt nummer"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"Address CSV missing columns: {missing}\n"
                f"Found columns: {list(df.columns)}\n"
                f"Hint: if names differ slightly, we can map them."
            )

        # Clean fields
        df["Vejnavn"] = df["Vejnavn"].astype(str).str.strip()
        df["Hus nummer"] = df["Hus nummer"].astype(str).str.strip()
        df["Hus bogstav"] = df["Hus bogstav"].fillna("").astype(str).str.strip()
        df["Postnummer"] = df["Postnummer"].astype(str).str.strip()
        df["Distrikt nummer"] = df["Distrikt nummer"].astype(str).str.strip()

        # Add city from postcode lookup
        df["city"] = ""
        if postcode_to_city:
            df["city"] = df["Postnummer"].map(lambda p: postcode_to_city.get(str(p).strip(), "")).fillna("")

        # Compose a display string and normalized key
        def make_display(row) -> str:
            letter = row["Hus bogstav"]
            hl = f" {letter}" if letter else ""
            city = str(row.get("city", "")).strip()
            city_part = f" {city}" if city else ""
            return f"{row['Vejnavn']} {row['Hus nummer']}{hl}, {row['Postnummer']}{city_part}"

        df["display"] = df.apply(make_display, axis=1)
        df["norm_key"] = df.apply(
            lambda r: normalize_address(r["Vejnavn"], r["Hus nummer"], r["Hus bogstav"], r["Postnummer"]),
            axis=1
        )

        # Drop duplicates on normalized key (keep first)
        df = df.drop_duplicates(subset=["norm_key"], keep="first").reset_index(drop=True)

        self._df = df

    def all_addresses(self) -> Iterable[KnownAddress]:
        if self._df is None:
            raise RuntimeError("AddressDirectory not loaded. Call load().")
        for _, r in self._df.iterrows():
            yield KnownAddress(
                display=r["display"],
                norm_key=r["norm_key"],
                district_no=r["Distrikt nummer"],
                street=r["Vejnavn"],
                house_no=r["Hus nummer"],
                house_letter=r["Hus bogstav"],
                postcode=r["Postnummer"],
                city=str(r.get("city", "")),
            )

    def find_by_display_contains(self, query: str, limit: int = 50) -> list[KnownAddress]:
        """
        Simple operator-friendly search (contains, case-insensitive).
        Use in GUI typeahead.
        """
        if self._df is None:
            raise RuntimeError("AddressDirectory not loaded. Call load().")
        q = normalize_text(query)
        if not q:
            return []

        # Search in normalized display text (cheap and effective)
        temp = self._df.copy()
        temp["display_norm"] = temp["display"].map(normalize_text)
        hit = temp[temp["display_norm"].str.contains(q, na=False)].head(limit)

        out: list[KnownAddress] = []
        for _, r in hit.iterrows():
            out.append(KnownAddress(
                display=r["display"],
                norm_key=r["norm_key"],
                district_no=r["Distrikt nummer"],
                street=r["Vejnavn"],
                house_no=r["Hus nummer"],
                house_letter=r["Hus bogstav"],
                postcode=r["Postnummer"],
                city=str(r.get("city", "")),
            ))
        return out

    def get_district_for_norm_key(self, norm_key: str) -> Optional[str]:
        if self._df is None:
            raise RuntimeError("AddressDirectory not loaded. Call load().")
        hit = self._df[self._df["norm_key"] == norm_key]
        if hit.empty:
            return None
        return str(hit.iloc[0]["Distrikt nummer"])

    def find_by_components(
            self,
            street: str,
            house_no: str,
            house_extra: str = "",
            limit: int = 30,
    ) -> list[KnownAddress]:
        """
        Search by street + house number, then optionally narrow by extra like:
        letter/side/floor ("A", "TH", "1 SAL", etc.)

        Returns candidates; user can select the correct city/postcode from the list.
        """
        if self._df is None:
            raise RuntimeError("AddressDirectory not loaded. Call load().")

        street_q = normalize_text(street)
        no_q = normalize_text(house_no)
        extra_q = normalize_text(house_extra)

        temp = self._df.copy()
        temp["street_norm"] = temp["Vejnavn"].map(normalize_text)
        temp["no_norm"] = temp["Hus nummer"].map(normalize_text)
        temp["letter_norm"] = temp["Hus bogstav"].fillna("").map(normalize_text)
        temp["display_norm"] = temp["display"].map(normalize_text)

        # Must match street and house number
        hit = temp[(temp["street_norm"] == street_q) & (temp["no_norm"] == no_q)]

        # If user supplied extra (letter/side/floor), try to narrow results
        # We only have house letter in data, but users may type "TH", "1", etc.
        # So we do a loose contains search on display.
        if extra_q:
            hit2 = hit[hit["display_norm"].str.contains(extra_q, na=False)]
            if not hit2.empty:
                hit = hit2
        hit = hit.sort_values(by=["Hus bogstav"])
        hit = hit.head(limit)

        out: list[KnownAddress] = []
        for _, r in hit.iterrows():
            out.append(
                KnownAddress(
                    display=r["display"],
                    norm_key=r["norm_key"],
                    district_no=r["Distrikt nummer"],
                    street=r["Vejnavn"],
                    house_no=r["Hus nummer"],
                    house_letter=r["Hus bogstav"],
                    postcode=r["Postnummer"],
                    city=str(r.get("city", "")),
                )
            )
        return out


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
