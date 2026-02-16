from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from noedudkald.data_sources.aba import AbaSite


@dataclass(frozen=True)
class AbaRuleResult:
    applied: bool
    reason: str
    units: list[str]


def _parse_units(units_str: str) -> list[str]:
    # ABA file uses comma-separated values e.g. "ROIL1,ROM1,ROV1"
    if not units_str:
        return []
    parts = [p.strip() for p in str(units_str).split(",")]
    return [p for p in parts if p]


def units_from_aba_site(aba_site: AbaSite, use_secondary: bool) -> list[str]:
    chosen = aba_site.secondary_response if use_secondary else aba_site.primary_response
    return _parse_units(chosen)


def apply_aba_rules_case_sensitive(
    incident_code: str,
    aba_site: Optional[AbaSite],
    base_units: list[str],
    use_secondary: bool = False,
) -> AbaRuleResult:
    """
    Case sensitive:
    - For BAAl, units MUST come from ABA site list (Primær/Sekundær).
    - For any other incident, return base_units unchanged.
    """
    code = (incident_code or "").strip()  # case sensitive

    if code != "BAAl":
        return AbaRuleResult(False, "Not BAAl; ABA list not used.", base_units)

    if aba_site is None:
        return AbaRuleResult(False, "BAAl but address not found in ABA list.", [])

    aba_units = units_from_aba_site(aba_site, use_secondary=use_secondary)
    if not aba_units:
        return AbaRuleResult(
            False,
            f"BAAl + ABA match ({aba_site.doa_no} / {aba_site.name}) but response list empty.",
            [],
        )

    return AbaRuleResult(
        True,
        f"BAAl + ABA match ({aba_site.doa_no} / {aba_site.name}) -> "
        f"{'Sekundær' if use_secondary else 'Primær'} units used from ABA list.",
        aba_units,
    )
