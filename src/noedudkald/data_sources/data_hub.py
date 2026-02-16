from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from noedudkald.data_sources.addresses import AddressDirectory
from noedudkald.data_sources.aba import AbaDirectory
from noedudkald.data_sources.incidents import IncidentMatrix
from noedudkald.data_sources.postcodes import PostcodeDirectory


@dataclass
class DataHub:
    postcodes: PostcodeDirectory
    addresses: AddressDirectory
    aba: AbaDirectory
    incidents: IncidentMatrix

    @classmethod
    def from_paths(cls, addresses_csv, aba_xlsx, pickliste_xlsx, postnummer_xlsx):
        return cls(
            postcodes=PostcodeDirectory(postnummer_xlsx),
            addresses=AddressDirectory(addresses_csv),
            aba=AbaDirectory(aba_xlsx),
            incidents=IncidentMatrix(pickliste_xlsx),
        )

    def load_all(self):
        self.addresses.load()
        self.aba.load()
        self.incidents.load()
        self.postcodes.load()

    def reload_all(self) -> None:
        # same as load_all for now; later we can add caching/invalidation
        self.load_all()
