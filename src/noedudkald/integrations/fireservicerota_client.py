from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass
class TokenInfo:
    access_token: str
    refresh_token: Optional[str]
    token_type: str = "Bearer"
    expires_at: Optional[int] = None  # unix epoch seconds

    def is_expired(self, skew_seconds: int = 30) -> bool:
        if self.expires_at is None:
            return False
        return time.time() >= (self.expires_at - skew_seconds)


class FireServiceRotaError(RuntimeError):
    pass


class FireServiceRotaAuthError(FireServiceRotaError):
    pass


class FireServiceRotaClient:
    """
    Minimal FSR API client:
      - OAuth2 token retrieval + refresh
      - POST incident
    """

    def _safe_json(self, r: requests.Response) -> dict:
        """
        Return JSON if present; otherwise raise a useful error showing status + body.
        """
        text = (r.text or "").strip()
        ctype = (r.headers.get("Content-Type") or "").lower()

        if not text:
            raise FireServiceRotaError(
                f"FSR returned empty response body (status {r.status_code}). "
                f"Content-Type={r.headers.get('Content-Type')!r}"
            )

        # If it's not JSON, show the first part of body for diagnosis
        if "json" not in ctype:
            snippet = text[:500]
            raise FireServiceRotaError(
                f"FSR returned non-JSON response (status {r.status_code}). "
                f"Content-Type={r.headers.get('Content-Type')!r}. "
                f"Body (first 500 chars): {snippet}"
            )

        try:
            return r.json()
        except Exception as e:
            snippet = text[:500]
            raise FireServiceRotaError(
                f"Failed to parse JSON from FSR (status {r.status_code}): {e}. "
                f"Body (first 500 chars): {snippet}"
            )

    def __init__(self, base_url: str = "https://www.fireservicerota.co.uk", timeout_s: int = 20):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.session = requests.Session()

        self._token: Optional[TokenInfo] = None
        self._persist_token_cb = None  # type: ignore

    def set_persist_token_callback(self, cb):
        """
        cb(token: TokenInfo) -> None
        Called after login and after refresh.
        """
        self._persist_token_cb = cb
    # ---- OAuth2 ----
    def login_with_password(self, username: str, password: str, client_id: str | None = None) -> TokenInfo:
        """
        Uses OAuth password grant (FSR uses OAuth2 per docs).
        If FSR requires client_id for your tenant, pass it.
        """
        url = f"{self.base_url}/oauth/token"
        data = {
            "grant_type": "password",
            "username": username,
            "password": password,
        }
        if client_id:
            data["client_id"] = client_id

        r = self.session.post(url, data=data, timeout=self.timeout_s)
        if r.status_code in (401, 403):
            raise FireServiceRotaAuthError(f"FSR login failed ({r.status_code}). Check credentials.")
        if not r.ok:
            raise FireServiceRotaError(f"FSR token request failed ({r.status_code}): {r.text}")

        payload = self._safe_json(r)
        token = self._parse_token_payload(payload)
        self._token = token

        if self._persist_token_cb:
            self._persist_token_cb(token)

        return token

    def refresh_access_token(self) -> TokenInfo:
        if not self._token or not self._token.refresh_token:
            raise FireServiceRotaAuthError("No refresh token available. Please login again.")

        url = f"{self.base_url}/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._token.refresh_token,
        }

        r = self.session.post(url, data=data, timeout=self.timeout_s)
        if r.status_code in (401, 403):
            raise FireServiceRotaAuthError("Refresh token rejected. Please login again.")
        if not r.ok:
            raise FireServiceRotaError(f"FSR token refresh failed ({r.status_code}): {r.text}")

        payload = self._safe_json(r)
        token = self._parse_token_payload(payload)

        # Preserve refresh token if API doesn't send a new one
        if not token.refresh_token:
            token.refresh_token = self._token.refresh_token

        self._token = token

        if self._persist_token_cb:
            self._persist_token_cb(token)

        return token

    def set_token(self, token: TokenInfo) -> None:
        self._token = token

    def get_token(self) -> Optional[TokenInfo]:
        return self._token

    def _ensure_token(self) -> None:
        if not self._token:
            raise FireServiceRotaAuthError("Not authenticated (no token).")
        if self._token.is_expired():
            self.refresh_access_token()

    @staticmethod
    def _parse_token_payload(payload: dict[str, Any]) -> TokenInfo:
        access = payload.get("access_token")
        if not access:
            raise FireServiceRotaError(f"Token response missing access_token: {payload}")

        refresh = payload.get("refresh_token")
        token_type = payload.get("token_type") or "Bearer"

        expires_at = None
        # APIs commonly return expires_in seconds
        if "expires_in" in payload and payload["expires_in"] is not None:
            try:
                expires_at = int(time.time()) + int(payload["expires_in"])
            except Exception:
                expires_at = None

        return TokenInfo(
            access_token=str(access),
            refresh_token=str(refresh) if refresh else None,
            token_type=str(token_type),
            expires_at=expires_at,
        )

    # ---- API calls ----

    def create_incident(
            self,
            body_text: str,
            prio: str,
            location: str,
            task_ids: list[int] | None = None,
            override_responder_membership_ids: list[int] | None = None,
    ) -> dict[str, Any]:

        self._ensure_token()
        assert self._token is not None

        url = f"{self.base_url}/api/v2/incidents/"
        headers = {
            "Authorization": f"{self._token.token_type} {self._token.access_token}",
            "Content-Type": "application/json",
        }

        payload: dict[str, Any] = {
            "body": body_text,
            "prio": prio,
            "location": location,
        }

        # Appliance/task targeting
        if task_ids:
            payload["task_ids"] = task_ids

        # Person targeting (optional, different use-case)
        if override_responder_membership_ids:
            payload["override_responder_membership_ids"] = override_responder_membership_ids

        r = self.session.post(url, json=payload, headers=headers, timeout=self.timeout_s)
        if r.status_code in (401, 403):
            self.refresh_access_token()
            headers["Authorization"] = f"{self._token.token_type} {self._token.access_token}"
            r = self.session.post(url, json=payload, headers=headers, timeout=self.timeout_s)

        if not r.ok:
            raise FireServiceRotaError(f"Create incident failed ({r.status_code}): {r.text}")

        return self._safe_json(r)

    # fireservicerota_client.py (inde i FireServiceRotaClient)

    def _headers(self) -> dict[str, str]:
        self._ensure_token()
        assert self._token is not None
        return {
            "Authorization": f"{self._token.token_type} {self._token.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def test_connection(self) -> tuple[bool, bool]:
        """
        Returns (server_ok, auth_ok)

        server_ok: kan vi nå FSR overhovedet?
        auth_ok: er token accepteret? (ikke 401/403)

        Vi bruger:
          1) GET /api/v2/health  (ingen auth) til reachability
          2) POST users/current/heartbeat (auth) til token-test
        """
        # 1) Reachability
        health_url = f"{self.base_url}/api/v2/health"
        r = self.session.get(health_url, timeout=self.timeout_s)
        if not r.ok:
            return False, False

        # 2) Auth test (heartbeat)
        self._ensure_token()

        candidates = [
            f"{self.base_url}/api/v2/users/current/heartbeat",
            f"{self.base_url}/api/v2/users/current/heartbeat/",
        ]

        last_status = None
        for url in candidates:
            r2 = self.session.post(url, headers=self._headers(), json={}, timeout=self.timeout_s)
            last_status = r2.status_code

            if r2.status_code in (401, 403):
                return True, False

            if r2.status_code == 404:
                continue

            # alt andet end 401/403 og 404 tolker vi som “token accepted”
            # (fx 200/201/204/400/422 etc.)
            return True, True

        # Health var OK, men heartbeat-endpoint fandtes ikke (eller ændret)
        # => server OK, auth uklar
        return True, False
