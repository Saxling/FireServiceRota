from __future__ import annotations

import json
from dataclasses import asdict
from json import JSONDecodeError
from pathlib import Path
from typing import Optional

from noedudkald.integrations.fireservicerota_client import TokenInfo


class TokenStore:
    def __init__(self, path: Path):
        self.path = path

    def save(self, token: TokenInfo, username: str | None = None) -> None:
        """
        Save token + optional username.
        If username is None and a username already exists in file, keep it.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)

        existing_user = None
        if username is None and self.path.exists():
            try:
                raw = self.path.read_text(encoding="utf-8").strip()
                if raw:
                    data = json.loads(raw)
                    existing_user = data.get("username")
            except Exception:
                existing_user = None

        data = asdict(token)
        if username:
            data["username"] = username
        elif existing_user:
            data["username"] = existing_user

        self.path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def load(self) -> Optional[TokenInfo]:
        if not self.path.exists():
            return None

        raw = self.path.read_text(encoding="utf-8").strip()
        if not raw:
            return None

        try:
            data = json.loads(raw)
        except JSONDecodeError:
            return None

        # allow optional metadata
        username = data.pop("username", None)

        try:
            token = TokenInfo(**data)
            if username:
                setattr(token, "username", username)
            return token
        except TypeError:
            return None

    def load_username(self) -> Optional[str]:
        if not self.path.exists():
            return None
        try:
            raw = self.path.read_text(encoding="utf-8").strip()
            if not raw:
                return None
            data = json.loads(raw)
            return data.get("username")
        except Exception:
            return None

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
