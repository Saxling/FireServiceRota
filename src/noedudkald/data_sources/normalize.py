# src/noedudkald/data_sources/normalize.py
from __future__ import annotations

import re

import unicodedata


def normalize_text(s: str) -> str:
    """
    Normalize for matching (case-insensitive, whitespace/punctuation tolerant).
    Keeps Danish letters, but normalizes Unicode.
    """
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKC", s)
    s = s.upper()

    # Replace punctuation with spaces (keep letters/numbers)
    s = re.sub(r"[^\wÆØÅ]", " ", s, flags=re.UNICODE)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_address(street: str, house_no: str | int | None, house_letter: str | None,
                      postcode: str | int | None) -> str:
    """
    Builds a normalized key like: 'HOVEDGADEN 12 A 4000'
    """
    parts = [street, str(house_no) if house_no is not None else "", house_letter or "",
             str(postcode) if postcode is not None else ""]
    raw = " ".join(p for p in parts if p and str(p).strip())
    return normalize_text(raw)
