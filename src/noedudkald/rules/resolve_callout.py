from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

from noedudkald.data_sources.aba import AbaDirectory, AbaSite
from noedudkald.data_sources.addresses import KnownAddress, ManualAddress
from noedudkald.data_sources.incidents import IncidentMatrix
from noedudkald.rules.aba_rules import apply_aba_rules_case_sensitive, AbaRuleResult

AddressType = Union[KnownAddress, ManualAddress]


@dataclass(frozen=True)
class ResolvedCallout:
    address: KnownAddress
    district_no: str
    incident_code: str
    incident_label: str
    aba_site: Optional[AbaSite]
    base_units: list[str]
    final_units: list[str]
    aba_rule: AbaRuleResult


class CalloutResolver:
    def __init__(self, incidents: IncidentMatrix, aba: AbaDirectory):
        self.incidents = incidents
        self.aba = aba

    def resolve(
            self,
            selected_address: AddressType,
            incident_code: str,
            use_secondary_aba: bool = False,
    ) -> ResolvedCallout:
        district_no = selected_address.district_no
        code = (incident_code or "").strip()  # case sensitive

        # ABA match is needed for BAAl
        aba_site = self.aba.match_components(
            street=selected_address.street,
            house_no=selected_address.house_no,
            house_letter=getattr(selected_address, "house_letter", ""),
            postcode=selected_address.postcode,
        )

        # Default: base units from pickliste (if present)
        base_units: list[str] = []
        incident_label = ""

        if code != "BAAl":
            profile = self.incidents.get_profile(district_no, code)
            if profile is None:
                raise ValueError(f"Incident '{code}' not found for district '{district_no}'.")
            incident_label = profile.incident_label
            base_units = profile.units
        else:
            # For BAAl, units come from ABA list, not pickliste
            incident_label = "Brandalarm (ABA)"  # change text if you prefer

        aba_rule = apply_aba_rules_case_sensitive(
            incident_code=code,
            aba_site=aba_site,
            base_units=base_units,
            use_secondary=use_secondary_aba,
        )

        # For BAAl, require ABA match, otherwise the callout is not resolvable
        if code == "BAAl" and aba_site is None:
            raise ValueError("BAAl selected but address is not found in ABA list (no response can be derived).")

        return ResolvedCallout(
            address=selected_address,
            district_no=district_no,
            incident_code=code,
            incident_label=incident_label,
            aba_site=aba_site,
            base_units=base_units,
            final_units=aba_rule.units if aba_rule.applied or code == "BAAl" else base_units,
            aba_rule=aba_rule,
        )
