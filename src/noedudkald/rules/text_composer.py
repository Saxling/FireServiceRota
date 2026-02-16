from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CalloutTextInput:
    incident_code: str  # e.g. "BAAl" or other
    incident_text: str  # e.g. "BRANDALARM" or "BYGN.BRAND-BUTIK"
    address_display: str  # e.g. "Maglehøjen 10, 4000"
    city: str = "Roskilde"
    priority: str = "Kørsel 1"
    dispatch_comments: Optional[str] = None  # free text, optional

    # ABA-only fields (ignored for non-ABA)
    aba_site_name: Optional[str] = None  # e.g. '"SILOEN" UNGDOMSBOLIGER'


def _format_address(address_display: str, city: str) -> str:
    # Prefer the full display (usually: "Street X, 4000 City")
    s = (address_display or "").strip()
    if s:
        return s.replace(",", "")
    # Fallback only
    return (city or "").strip()


def _units_to_str(units: list[str]) -> str:
    return " ".join(u.strip() for u in units if u and u.strip())


def compose_alert_text(inp: CalloutTextInput, units: list[str]) -> str:
    """
    ABA:
      ABA <address city> <site> <priority> BRANDALARM <comments?> - <units>

    Non-ABA:
      <incident_text> <address city> <priority> <comments?> - <units>
    """
    address_part = _format_address(inp.address_display, inp.city)
    units_part = _units_to_str(units)
    comments = (inp.dispatch_comments or "").strip()
    if comments:
        comments = "# " + comments

    site = (inp.aba_site_name or "").strip()

    if inp.incident_code == "BAAl":
        # Keep ABA format
        parts = [
            "ABA",
            address_part,
            (inp.aba_site_name or "").strip(),
            inp.priority,
            inp.incident_text,  # typically "BRANDALARM"
        ]
        if comments:
            parts.append(comments)
        parts += ["-", units_part]
        parts = [p for p in parts if p and p.strip()]
        return " ".join(parts)

    # Non-ABA format: incident text first, no prefix
    parts = [
        inp.incident_text,
        address_part,
        inp.priority,
    ]
    if comments:
        parts.append(comments)
    parts += ["-", units_part]
    parts = [p for p in parts if p and p.strip()]
    return " ".join(parts)
