from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from PySide6.QtCore import Qt, QStringListModel, QUrl, QRunnable, QThreadPool, Signal, QObject, QTimer,QSignalBlocker
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLabel, QLineEdit, QTextEdit, QListWidget, QListWidgetItem,
    QPushButton, QGroupBox, QMessageBox, QRadioButton, QButtonGroup,
    QCheckBox, QSplitter, QInputDialog, QDialog, QDialogButtonBox, QCompleter, QSizePolicy
)

from noedudkald.data_sources.data_hub import DataHub
from noedudkald.data_sources.task_map import TaskMap
from noedudkald.data_sources.addresses import make_manual_address
from noedudkald.rules.resolve_callout import CalloutResolver
from noedudkald.rules.text_composer import CalloutTextInput, compose_alert_text
from noedudkald.integrations.fireservicerota_client import FireServiceRotaClient, FireServiceRotaError, FireServiceRotaAuthError
from noedudkald.integrations.token_store import TokenStore
from noedudkald.ui.settings_dialog import SettingsDialog


import pandas as pd
import re
import urllib.parse
import time
import requests
import json



FSR_PRIORITY_MAP = {"Kørsel 1": "prio1", "Kørsel 2": "prio2"}


@dataclass(frozen=True)
class AppPaths:
    project_root: Path
    data_dir: Path
    addresses_csv: Path
    aba_xlsx: Path
    pickliste_xlsx: Path
    postnummer_xlsx: Path
    taskids_xlsx: Path

class _StartupSignals(QObject):
    done = Signal(bool, bool, str, str)  # sources_ok, fsr_ok, sources_msg, fsr_msg

class _StartupCheckWorker(QRunnable):
    def __init__(self, gui):
        super().__init__()
        self.gui = gui
        self.signals = _StartupSignals()

    def run(self):
        sources_ok, sources_msg = self.gui._check_sources_ready()

        # Default FSR status
        fsr_ok, fsr_msg = False, "FSR ikke testet"

        if sources_ok:
            try:
                fsr_ok, fsr_msg = self.gui._check_fsr_ready()
            except Exception as e:
                fsr_ok, fsr_msg = False, f"FSR fejl: {e}"
        else:
            fsr_ok, fsr_msg = False, "FSR ikke testet (datakilder mangler)"

        self.signals.done.emit(sources_ok, fsr_ok, sources_msg, fsr_msg)


def detect_project_root() -> Path:
    """
    Robust root detection: walk upwards until we find data/input.
    This avoids .venv path quirks on Windows.
    """
    here = Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if (p / "data" / "input").exists():
            return p
    # fallback (shouldn't happen)
    return here.parents[3]


def default_paths() -> AppPaths:
    root = detect_project_root()
    data_dir = root / "data" / "input"
    return AppPaths(
        project_root=root,
        data_dir=data_dir,
        addresses_csv=data_dir / "112 Adresse punkter.csv",
        aba_xlsx=data_dir / "ABA alarmer.xlsx",
        pickliste_xlsx=data_dir / "Pickliste.xlsx",
        postnummer_xlsx=data_dir / "Postnummer.xlsx",
        taskids_xlsx=data_dir / "TaskIds.xlsx",
    )


def ensure_files_exist(paths: AppPaths) -> None:
    required = [
        paths.addresses_csv,
        paths.aba_xlsx,
        paths.pickliste_xlsx,
        paths.postnummer_xlsx,
        paths.taskids_xlsx,
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing datasource files:\n" + "\n".join(f" - {m}" for m in missing)
        )

class NoodudkaldQt(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Nødudkald")
        self.resize(1400, 900)

        # --- Paths ---
        self.paths = default_paths()

        # --- Core runtime state ---
        self.hub = None
        self.task_map = None
        self.resolver = None

        # --- Last resolved data ---
        self.last_alert_text: str | None = None
        self.last_task_ids: list[int] | None = None
        self.last_priority: str | None = None
        self.last_location: str | None = None

        # --- Address selection state ---
        self.selected_address = None

        # --- Build UI ---
        self._build_ui()
        self.thread_pool = QThreadPool.globalInstance()
        # Disable operational buttons until data is ready
        self._set_ready_state(False)
        self._set_header_status("Tjekker…", "info")

        # --- Startup behaviour ---
        missing = self._missing_sources()

        if missing:
            # Do not crash — guide operator
            self._log("Program startet uden datakilder.")
            self._log("Åbner Indstillinger for opsætning...")
            self._set_header_status("Mangler datakilder", "err")

            # Open settings automatically
            self.on_settings()

        else:
            # Normal boot: load sources in UI thread (safe)
            self._reload_sources()

            # Then check FSR in background
            self._run_startup_checks()

    def _run_startup_checks(self):
        worker = _StartupCheckWorker(self)
        worker.signals.done.connect(self._on_startup_checked)
        self.thread_pool.start(worker)

    def _on_startup_checked(self, sources_ok: bool, fsr_ok: bool, sources_msg: str, fsr_msg: str):
        if not sources_ok:
            self._set_header_status("Mangler datakilder", "err")
            self._set_ready_state(False)
            self._log(f"Startup check: IKKE KLAR – {sources_msg}")
            return

        if not fsr_ok:
            # Datakilder OK, men FSR ikke OK
            self._set_header_status(fsr_msg, "warn")  # fx "FSR offline" / "FSR login mangler"
            self._set_ready_state(False)
            self._log(f"Startup check: IKKE KLAR – {fsr_msg}")
            return

        self._set_header_status("Klar", "ok")
        self._set_ready_state(True)
        self._log("Startup check: Klar (Datakilder + FSR OK)")

    def _check_sources_ready(self) -> tuple[bool, str]:
        missing = self._missing_sources()
        if missing:
            return False, "Mangler datakilder"
        return True, "Datakilder OK"

    def _check_fsr_ready(self) -> tuple[bool, str]:
        """
        Returns (ok, message)
        ok=True kun når token findes og heartbeat-test viser auth OK.
        """
        token_path = self.paths.project_root / "data" / "secrets" / "fsr_token.json"
        store = TokenStore(token_path)

        client = FireServiceRotaClient(base_url="https://www.fireservicerota.co.uk")
        token = store.load()

        if token:
            client.set_token(token)

        client.set_persist_token_callback(lambda t: store.save(t))

        if not token:
            # server check uden login (health)
            try:
                r = requests.get("https://www.fireservicerota.co.uk/api/v2/health", timeout=8)
                return (False, "FSR login mangler") if r.ok else (False, "FSR offline")
            except Exception:
                return False, "FSR offline"

        client.set_token(token)

        try:
            server_ok, auth_ok = client.test_connection()
            if not server_ok:
                return False, "FSR offline"
            if not auth_ok:
                return False, "FSR login ugyldigt/udløbet"
            return True, "FSR OK"
        except Exception:
            return False, "FSR offline"

    # ---------- UI building ----------

    def _build_ui(self):
        from PySide6.QtWidgets import QSizePolicy, QFrame

        # ---------- Root ----------
        root = QWidget()
        self.setCentralWidget(root)

        app = QVBoxLayout(root)
        app.setContentsMargins(14, 14, 14, 14)
        app.setSpacing(10)

        # ---------- Header ----------
        header = QWidget()
        h = QHBoxLayout(header)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(10)

        title = QLabel("FireServiceRota Nødudkald")
        f = QFont()
        f.setPointSize(18)
        f.setBold(True)
        title.setFont(f)

        self.header_status = QLabel("Ready")
        self.header_status.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        h.addWidget(title)
        h.addStretch(1)
        h.addWidget(self.header_status)

        app.addWidget(header)

        # ---------- Main Splitter ----------
        main_split = QSplitter(Qt.Horizontal)
        main_split.setChildrenCollapsible(False)
        app.addWidget(main_split, 1)

        # Helpers for "cards"
        def card(title_text: str) -> tuple[QGroupBox, QVBoxLayout]:
            box = QGroupBox(title_text)
            lay = QVBoxLayout(box)
            lay.setContentsMargins(12, 12, 12, 12)
            lay.setSpacing(8)
            return box, lay

        def divider() -> QFrame:
            line = QFrame()
            line.setFrameShape(QFrame.HLine)
            line.setFrameShadow(QFrame.Sunken)
            return line

        # =========================
        # LEFT PANEL: Address
        # =========================
        left = QWidget()
        left_l = QVBoxLayout(left)
        left_l.setContentsMargins(0, 0, 0, 0)
        left_l.setSpacing(10)

        # Address Search card
        addr_box, addr_v = card("Address search")

        row = QWidget()
        r = QHBoxLayout(row)
        r.setContentsMargins(0, 0, 0, 0)
        r.setSpacing(8)

        self.street = QLineEdit()
        self.street.setMinimumWidth(350)
        self.street.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.street.setPlaceholderText("Vej (e.g. Gammel Vindingevej)")

        self.house = QLineEdit()
        self.house.setPlaceholderText("Nr.")
        self.house.setFixedWidth(90)  # 4 digits + padding

        self.extra = QLineEdit()
        self.extra.setPlaceholderText("Ekstra (B / 2. / th)")
        self.extra.setFixedWidth(160)

        self.search_btn = QPushButton("Søg")
        self.search_btn.setFixedWidth(120)
        self.search_btn.clicked.connect(self.on_search)

        r.addWidget(self.street, 1)
        r.addWidget(self.house)
        r.addWidget(self.extra)
        r.addWidget(self.search_btn)

        row.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        addr_v.addWidget(row)

        # Manual fallback card
        manual_box, manual_v = card("Manual adresse")

        row2 = QWidget()
        r2 = QHBoxLayout(row2)
        r2.setContentsMargins(0, 0, 0, 0)
        r2.setSpacing(8)

        self.manual_post = QLineEdit()
        self.manual_post.setPlaceholderText("Postnr")
        self.manual_post.setFixedWidth(90)  # 4 digits + padding

        self.manual_city = QLineEdit()
        self.manual_city.setPlaceholderText("By (auto opslag)")
        self.manual_city.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.manual_post.textChanged.connect(self.on_manual_post_changed)

        self.manual_assist = QCheckBox("Assistance (manual tilføj enheder)")
        # if you already have logic to enable/disable assistance widgets, keep it hooked here:
        # self.manual_assist.stateChanged.connect(self._sync_assistance_mode)

        self.map_btn = QPushButton("Map")
        self.map_btn.setFixedWidth(70)
        self.map_btn.clicked.connect(self.on_manual_map)

        r2.addWidget(self.manual_post)
        r2.addWidget(self.manual_city, 1)
        r2.addWidget(self.manual_assist)
        r2.addWidget(self.map_btn)

        manual_v.addWidget(row2)

        # Candidates list (full-height)
        cand_box, cand_v = card("Kandidater")
        self.candidate_list = QListWidget()
        self.candidate_list.itemSelectionChanged.connect(self.on_candidate_selected)
        cand_v.addWidget(self.candidate_list, 1)

        self.candidate_list.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.candidate_list.setMinimumHeight(200)


        left_l.addWidget(addr_box)
        left_l.addWidget(manual_box)
        left_l.addWidget(cand_box,1)

        main_split.addWidget(left)

        # =========================
        # MIDDLE PANEL: Incident
        # =========================
        mid = QWidget()
        mid_l = QVBoxLayout(mid)
        mid_l.setContentsMargins(0, 0, 0, 0)
        mid_l.setSpacing(10)

        # Incident card
        inc_box, inc_v = card("Hændelse")

        self.incident_code = QLineEdit()
        self.incident_code.setPlaceholderText("Hændelses (e.g. brand) → vælg, el. skriv kode (BAAl)")
        inc_v.addWidget(self.incident_code)

        aba_row = QWidget()
        aba_l = QHBoxLayout(aba_row)
        aba_l.setContentsMargins(0, 0, 0, 0)
        aba_l.setSpacing(10)

        aba_l.addWidget(QLabel("ABA sammensætning:"))
        self.aba_group = QButtonGroup(self)
        self.aba_p = QRadioButton("Primær")
        self.aba_s = QRadioButton("Sekundær")
        self.aba_p.setChecked(True)
        self.aba_group.addButton(self.aba_p)
        self.aba_group.addButton(self.aba_s)
        aba_l.addWidget(self.aba_p)
        aba_l.addWidget(self.aba_s)
        aba_l.addStretch(1)

        inc_v.addWidget(aba_row)

        # Priority card (big + clean)
        prio_box, prio_v = card("Kørsels type")
        prio_row = QWidget()
        prio_l = QHBoxLayout(prio_row)
        prio_l.setContentsMargins(0, 0, 0, 0)
        prio_l.setSpacing(12)

        self.prio_group = QButtonGroup(self)
        self.prio1 = QRadioButton("Kørsel 1")
        self.prio2 = QRadioButton("Kørsel 2")
        self.prio1.setChecked(True)
        self.prio_group.addButton(self.prio1)
        self.prio_group.addButton(self.prio2)

        # Make them feel like “segmented” choices
        self.prio1.setMinimumHeight(34)
        self.prio2.setMinimumHeight(34)

        prio_l.addWidget(self.prio1)
        prio_l.addWidget(self.prio2)
        prio_l.addStretch(1)

        prio_v.addWidget(prio_row)

        # Comments card
        com_box, com_v = card("Kommentare til førstemelding")
        self.comments = QLineEdit()
        self.comments.setPlaceholderText("Kommentare til førstemelding")
        com_v.addWidget(self.comments)

        # Assistance card
        assist_box, assist_v = card("Assistance detaljer")
        self.assist_incident_text = QLineEdit()
        self.assist_incident_text.setPlaceholderText("Hændelse text (e.g. BYGN.BRAND-BUTIK)")
        self.assist_units = QLineEdit()
        self.assist_units.setPlaceholderText("Enheder: ROIL1 ROM1 ROV1 (space/komma opdelt)")
        assist_v.addWidget(self.assist_incident_text)
        assist_v.addWidget(self.assist_units)

        map_box, map_v = card("Map preview")
        self.map_view = QWebEngineView()
        map_v.addWidget(self.map_view)
        self.map_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        mid_l.addWidget(inc_box)
        mid_l.addWidget(prio_box)
        mid_l.addWidget(com_box)
        mid_l.addWidget(assist_box)
        mid_l.addWidget(map_box,1)


        main_split.addWidget(mid)

        # =========================
        # RIGHT PANEL: Preview + Log
        # =========================
        right = QWidget()
        right_l = QVBoxLayout(right)
        right_l.setContentsMargins(0, 0, 0, 0)
        right_l.setSpacing(10)

        right_split = QSplitter(Qt.Vertical)
        right_split.setChildrenCollapsible(False)
        right_l.addWidget(right_split, 1)

        preview_box, preview_v = card("Gennemse")
        self.preview = QTextEdit()
        self.preview.setReadOnly(True)
        preview_v.addWidget(self.preview, 1)

        log_box, log_v = card("Log")
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        log_v.addWidget(self.log, 1)

        right_split.addWidget(preview_box)
        right_split.addWidget(log_box)
        right_split.setSizes([520, 340])

        # Action buttons row
        btn_row = QWidget()
        b = QHBoxLayout(btn_row)
        b.setContentsMargins(0, 0, 0, 0)
        b.setSpacing(10)

        self.resolve_btn = QPushButton("Generer og Gennemse")
        self.send_btn = QPushButton("Send til FireServiceRota")
        self.clear_btn = QPushButton("Ryd")
        self.settings_btn = QPushButton("Indstillinger")

        self.resolve_btn.clicked.connect(self.on_resolve)
        self.send_btn.clicked.connect(self.on_send)
        self.clear_btn.clicked.connect(self.on_clear)
        self.settings_btn.clicked.connect(self.on_settings)

        # Make Send look more “primary”
        self.send_btn.setMinimumHeight(36)
        self.resolve_btn.setMinimumHeight(36)

        b.addWidget(self.resolve_btn)
        b.addWidget(self.send_btn)
        b.addWidget(self.clear_btn)
        b.addWidget(self.settings_btn)
        b.addStretch(1)

        right_l.addWidget(btn_row)

        main_split.addWidget(right)

        # Give panels sane default sizes
        main_split.setStretchFactor(0, 4)  # left
        main_split.setStretchFactor(1, 3)  # mid
        main_split.setStretchFactor(2, 4)  # right
        main_split.setSizes([520, 420, 520])

        # Keyboard shortcuts / flow
        self.street.returnPressed.connect(self.on_search)
        self.house.returnPressed.connect(self.on_search)
        self.extra.returnPressed.connect(self.on_search)
        self.incident_code.returnPressed.connect(self.on_resolve)

        self.street.setFocus()

        # Optional: light styling for a cleaner modern look (safe + subtle)
        self.setStyleSheet("""
            QGroupBox {
                font-weight: 600;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 4px 0 4px;
            }
            QLineEdit, QTextEdit, QListWidget {
                font-size: 12px;
            }
            QPushButton {
                font-weight: 600;
            }
        """)
    # ---------- helpers ----------

    def _format_candidate_label(self, a: Any) -> str:
        # Vis KUN: vej, husnr, husbogstav, postnr, by

        street = str(getattr(a, "street", "")).strip()
        house = str(getattr(a, "house_no", "")).strip()
        letter = str(getattr(a, "house_letter", "")).strip()
        post = str(getattr(a, "postcode", "")).strip()

        # By: fra address hvis den findes, ellers slå op via postnr
        city = str(getattr(a, "city", "")).strip()
        if not city and self.hub is not None and post:
            city = (self.hub.postcodes.city_for_postcode(post) or "").strip()

        line1 = f"{street} {house}".strip()
        if letter:
            line1 = f"{line1} {letter}".strip()

        if city:
            return f"{line1}, {post} {city}".strip()
        return f"{line1}, {post}".strip()

    def _update_map(self, address_display: str) -> None:
        addr = (address_display or "").replace(",", "").strip()
        if not addr:
            return

        coords = self._geocode_nominatim(addr)
        if not coords:
            # Fallback: show OSM search (still ok, but has more UI)
            q = urllib.parse.quote(addr)
            self.map_view.setUrl(QUrl(f"https://www.openstreetmap.org/search?query={q}"))
            return

        lat, lon = coords

        # Clean embedded map view (no side panels)
        # bbox gives a viewport around the marker
        delta = 0.01
        left = lon - delta
        right = lon + delta
        bottom = lat - delta
        top = lat + delta

        embed_url = (
            "https://www.openstreetmap.org/export/embed.html"
            f"?bbox={left}%2C{bottom}%2C{right}%2C{top}"
            f"&layer=mapnik&marker={lat}%2C{lon}"
        )

        self.map_view.setUrl(QUrl(embed_url))

    def _geocode_nominatim(self, address: str) -> tuple[float, float] | None:
        """
        Returns (lat, lon) or None.
        Uses Nominatim (OSM) with basic caching + throttling.
        """
        address = (address or "").strip()
        if not address:
            return None

        if not hasattr(self, "_geo_cache"):
            self._geo_cache = {}
            self._last_geo_t = 0.0

        if address in self._geo_cache:
            return self._geo_cache[address]

        # Polite throttling (Nominatim prefers <= 1 req/sec)
        now = time.time()
        dt = now - self._last_geo_t
        if dt < 1.0:
            time.sleep(1.0 - dt)

        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json", "limit": 1}
        headers = {"User-Agent": "NoedudkaldRB/1.0 (dispatch tool)"}

        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            if not data:
                self._geo_cache[address] = None
                return None

            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            self._geo_cache[address] = (lat, lon)
            self._last_geo_t = time.time()
            return lat, lon
        except Exception:
            self._geo_cache[address] = None
            return None

    def on_manual_post_changed(self, txt: str) -> None:
        post = (txt or "").strip()

        # Only act when it looks like a Danish 4-digit postcode
        if len(post) != 4 or not post.isdigit():
            # allow manual city entry when postcode incomplete/invalid
            self.manual_city.setReadOnly(False)
            return

        city = self.hub.postcodes.city_for_postcode(post) or ""
        if city:
            self.manual_city.setText(city)
            self.manual_city.setReadOnly(True)  # lock it when we have a match
            self._log(f"Postnr {post} -> {city}")
        else:
            # No match: let operator type city manually
            self.manual_city.setReadOnly(False)
            # (optional) clear only if operator hasn't typed something else
            # self.manual_city.clear()

    def on_manual_map(self):
        street = self.street.text().strip()
        house = self.house.text().strip()
        extra = self.extra.text().strip()
        post = self.manual_post.text().strip()
        city = self.manual_city.text().strip()

        # build a display-ish address
        parts = [p for p in [street, house, extra] if p]
        line1 = " ".join(parts)
        line2 = " ".join([p for p in [post, city] if p]).strip()
        addr = (line1 + " " + line2).strip()

        if not addr:
            return
        self._update_map(addr)

    def on_settings(self):
        dlg = SettingsDialog(self.paths.project_root, self)
        if dlg.exec():
            self._set_header_status("Tjekker…", "info")
            self._set_ready_state(False)
            self._reload_sources()
            self._run_startup_checks()

    def _get_incident_pairs(self) -> list[tuple[str, str]]:
        """
        Returns list of (code, label) for incident autocomplete.

        Source of truth: RB Pickliste.xlsx (all sheets).
        This avoids depending on internal structure of hub.incidents.
        """
        def norm(s: str) -> str:
            return re.sub(r"\s+", "", str(s).strip().lower())

        sheets = pd.read_excel(self.paths.pickliste_xlsx, sheet_name=None)

        # Candidate column name patterns (Danish + English)
        code_keys = {"kode", "code", "incidentcode", "haendelsekode", "hændelsekode"}
        label_keys = {"hændelse", "haendelse", "tekst", "text", "label", "beskrivelse", "incident"}

        pairs: list[tuple[str, str]] = []
        seen = set()

        for sheet_name, df in sheets.items():
            if df is None or not hasattr(df, "columns"):
                continue

            cols = list(df.columns)
            ncols = [norm(c) for c in cols]
            colmap = {ncols[i]: cols[i] for i in range(len(cols))}

            # Find best code column
            code_col = None
            for k in code_keys:
                if k in colmap:
                    code_col = colmap[k]
                    break

            # Find best label column
            label_col = None
            for k in label_keys:
                if k in colmap:
                    label_col = colmap[k]
                    break

            # If not found by name, try heuristic:
            # code column often has short values like "BBBu", "BAAl", etc.
            if code_col is None:
                for c in cols:
                    ser = df[c].dropna().astype(str).str.strip()
                    if ser.empty:
                        continue
                    # Check if many values look like 3-5 chars starting with letters (e.g. BAAl/BBBu)
                    sample = ser.head(50)
                    hit = sample.str.match(r"^[A-Za-zÆØÅæøå]{2,4}[A-Za-z0-9ÆØÅæøå]{0,2}$").mean()
                    if hit >= 0.6:
                        code_col = c
                        break

            # label heuristic: longer text column
            if label_col is None:
                # choose a column with longer average length than the code column
                candidate_cols = [c for c in cols if c != code_col]
                best = None
                best_len = 0.0
                for c in candidate_cols:
                    ser = df[c].dropna().astype(str).str.strip()
                    if ser.empty:
                        continue
                    avg_len = ser.head(50).str.len().mean()
                    if avg_len > best_len:
                        best_len = avg_len
                        best = c
                label_col = best

            if code_col is None or label_col is None:
                continue

            sub = df[[code_col, label_col]].dropna()
            for _, r in sub.iterrows():
                code = str(r[code_col]).strip()
                label = str(r[label_col]).strip()
                if not code or not label:
                    continue
                # de-dupe across districts/sheets
                key = (code, label)
                if key in seen:
                    continue
                seen.add(key)
                pairs.append((code, label))

        if not pairs:
            raise RuntimeError("No incident code/label pairs could be extracted from Pickliste.xlsx.")

        # Sort by label for nicer browsing
        pairs.sort(key=lambda x: x[1].lower())
        return pairs

    def _install_incident_completer(self) -> None:
        """
        Fast incident search:
        - Debounce filtering (prevents UI freeze)
        - Only shows suggestions after 2 chars
        - Limits list to top N matches
        """
        # Cache all pairs once (expensive excel parsing happens here only)
        pairs = self._get_incident_pairs()
        self._log(f"Hændelser indlæst til søgning: {len(pairs)}")

        # Precompute searchable strings
        self._incident_all = []
        self._incident_display_to_code = {}
        for code, label in pairs:
            display = f"{label} — {code}"
            self._incident_all.append((code, label, display, label.lower(), code.lower()))
            self._incident_display_to_code[display] = code

        # Model that we will update dynamically (small list)
        self._incident_model = QStringListModel([], self)

        self._incident_completer = QCompleter(self._incident_model, self)
        self._incident_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._incident_completer.setCompletionMode(QCompleter.UnfilteredPopupCompletion)  # <-- VIGTIG
        self._incident_completer.setFilterMode(Qt.MatchContains)

        self.incident_code.setCompleter(self._incident_completer)
        self._incident_completer.activated.connect(self._on_incident_chosen)

        # Debounce timer
        self._incident_timer = QTimer(self)
        self._incident_timer.setSingleShot(True)
        self._incident_timer.setInterval(120)  # ms

        # When text edited: restart timer
        self.incident_code.textEdited.connect(self._on_incident_text_edited)
        self._incident_timer.timeout.connect(self._update_incident_suggestions)

        # Start empty
        self._incident_model.setStringList([])

    def _on_incident_text_edited(self, txt: str) -> None:
        # Restart debounce timer on each key
        self._incident_timer.stop()
        self._incident_timer.start()

    def _update_incident_suggestions(self) -> None:
        q = (self.incident_code.text() or "").strip().lower()

        # Don’t suggest for 0–1 chars (prevents huge match set)
        if len(q) < 2:
            self._incident_model.setStringList([])
            if self.incident_code.completer():
                self.incident_code.completer().popup().hide()
            return

        raw = (self.incident_code.text() or "").strip()

        # Kun suppress hvis det ligner en rigtig incident-kode (typisk 3-5 tegn, mange store bogstaver)
        if 3 <= len(raw) <= 5 and raw.replace("-", "").isalnum():
            upper_count = sum(1 for ch in raw if ch.isalpha() and ch.isupper())
            # BAAl, BBBu osv. har typisk 2+ store bogstaver tidligt
            if upper_count >= 2 and " " not in raw:
                self._incident_model.setStringList([])
                comp = self.incident_code.completer()
                if comp:
                    comp.popup().hide()
                return

        # Compute top matches (limit!)
        matches = []
        limit = 40

        for code, label, display, label_l, code_l in self._incident_all:
            if q in label_l or q in code_l:
                matches.append(display)
                if len(matches) >= limit:
                    break

        self._incident_model.setStringList(matches)

        comp = self.incident_code.completer()
        if comp and matches:
            comp.setCompletionPrefix("")  # <-- VIGTIG: viser hele model-listen uden ekstra filtrering
            comp.complete()

    def _on_incident_chosen(self, text: str) -> None:
        code = self._incident_display_to_code.get(text)
        if not code:
            return
        blocker = QSignalBlocker(self.incident_code)
        try:
            self.incident_code.setText(code)
        finally:
            del blocker

    def _extract_incident_code(self, raw: str) -> str:
        """
        Accepts:
          - "BBBu"
          - "Bygn.brand-Butik — BBBu"
          - "Bygn.brand-Butik - BBBu"
        Returns only the code part.
        """
        s = (raw or "").strip()
        if not s:
            return ""

        # Most common: label — CODE  (em dash)
        if "—" in s:
            s = s.split("—")[-1].strip()

        # Also allow: label - CODE
        if " - " in s:
            s = s.split(" - ")[-1].strip()

        # Final safety: take a code-like token at end
        m = re.search(r"([A-Za-zÆØÅæøå]{2,4}[A-Za-z0-9ÆØÅæøå]{0,3})\s*$", s)
        return m.group(1) if m else s

    def _log(self, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log.append(f"[{ts}] {msg}")

    def _error(self, title: str, msg: str):
        QMessageBox.critical(self, title, msg)
        self._log(f"ERROR: {msg}")

    def _info(self, title: str, msg: str):
        QMessageBox.information(self, title, msg)
        self._log(msg)

    def _fsr_location(self, address_display: str) -> str:
        return (address_display or "").replace(",", "").strip()

    def _priority_text(self, incident_code: str) -> str:
        # ABA always Kørsel 1
        if incident_code == "BAAl":
            return "Kørsel 1"

        if self.prio1.isChecked():
            return "Kørsel 1"
        if self.prio2.isChecked():
            return "Kørsel 2"
        raise ValueError("Priority must be selected (1 or 2).")

    def _ensure_address(self) -> Any:
        # selected candidate?
        if self.selected_address is not None:
            return self.selected_address

        street = self.street.text().strip()
        house = self.house.text().strip()
        extra = self.extra.text().strip()
        post = self.manual_post.text().strip()
        city = self.manual_city.text().strip()
        assist = self.manual_assist.isChecked()

        if not street or not house:
            raise ValueError("Street and house number are required.")

        if not post:
            raise ValueError("No candidate selected. Enter manual postal code.")

        if not city:
            city = self.hub.postcodes.city_for_postcode(post) or ""
        if not city:
            raise ValueError("City not found for postcode. Enter city manually.")

        # District is not used in GUI manual mode.
        # If the address is unknown and NOT assistance, we cannot safely derive units from matrix.
        if not assist:
            raise ValueError(
                "Unknown address without Assistance: cannot derive units (district not available). "
                "Tick 'Assistance (manual units)'."
            )

        return make_manual_address(
            street=street,
            house_no=house,
            house_extra=extra,
            postcode=post,
            city=city,
            district_no="",  # intentionally blank
        )

    # ---------- events ----------

    def on_search(self):
        self.candidate_list.clear()
        self.selected_address = None
        self._candidates = []

        street = self.street.text().strip()
        house = self.house.text().strip()
        extra = self.extra.text().strip()

        if self.hub is None:
            QMessageBox.warning(self, "Ikke klar", "Datakilder er ikke indlæst endnu.")
            return

        if not street or not house:
            self._error("Missing input", "Street and house number are required.")
            return

        candidates = self.hub.addresses.find_by_components(street, house, extra, limit=60)
        if not candidates:
            self._info("No matches", "No known address found. Use manual postnr/city fields.")
            return

        self._candidates = candidates
        for a in candidates:
            label = self._format_candidate_label(a)
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, label)  # gem label til map/log
            self.candidate_list.addItem(item)

        self._log(f"Found {len(candidates)} candidate(s). Select one.")

    def on_candidate_selected(self):
        items = self.candidate_list.selectedItems()

        if not items:
            return
        idx = self.candidate_list.row(items[0])
        self.selected_address = self._candidates[idx]

        item = items[0]
        label = item.data(Qt.UserRole) or self.selected_address.display

        self._update_map(label)
        self._log(f"Selected: {label} (district {self.selected_address.district_no})")

    def on_resolve(self):
        if not self.hub or not self.resolver or not self.task_map:
            raise ValueError("Datakilder er ikke indlæst. Brug Indstillinger.")
        try:
            addr = self._ensure_address()
            assist = self.manual_assist.isChecked()
            comments = self.comments.text().strip() or None

            if assist:
                # manual incident + units
                priority = self._priority_text("ASSIST")
                incident_text = self.assist_incident_text.text().strip()
                units_raw = self.assist_units.text().strip()
                units = [u for u in units_raw.replace(",", " ").split() if u.strip()]
                if not incident_text:
                    raise ValueError("Mangler hændelsestekst til Assistance.")
                if not units:
                    raise ValueError("Assistance manger enheder.")

                alert = compose_alert_text(
                    CalloutTextInput(
                        incident_code="ASSIST",
                        incident_text=incident_text,
                        address_display=addr.display,
                        city=getattr(addr, "city", "") or "",
                        priority=priority,
                        dispatch_comments=comments,
                    ),
                    units=units,
                )
                sel = self.task_map.select_task_ids_for_units(units, now=datetime.now(), auto_add_assistance=True)
                if sel.missing_units:
                    raise ValueError("Missing task_id mapping for: " + ", ".join(sel.missing_units))

                self.last_priority = priority
                self.last_alert_text = alert
                self.last_task_ids = sel.task_ids
                self.last_location = self._fsr_location(addr.display)

                self.preview.setPlainText(alert + "\n\n" + f"Task IDs: {sel.task_ids}" +
                                          (f"\nAssistance auto-added: {sel.assistance_unit}" if sel.assistance_added else ""))
                self._log("Assistance preview ready.")
                return

            incident_code = self._extract_incident_code(self.incident_code.text())
            if not incident_code:
                raise ValueError("Incident code is required.")

            use_secondary = (incident_code == "BAAl" and self.aba_s.isChecked())
            resolved = self.resolver.resolve(addr, incident_code, use_secondary_aba=use_secondary)

            if resolved.incident_code == "BAAl":
                priority = "Kørsel 1"
                incident_text = "BRANDALARM"
                aba_site_name = resolved.aba_site.name if resolved.aba_site else None
            else:
                priority = self._priority_text(resolved.incident_code)
                incident_text = resolved.incident_label or resolved.incident_code
                aba_site_name = None

            alert = compose_alert_text(
                CalloutTextInput(
                    incident_code=resolved.incident_code,
                    incident_text=incident_text,
                    address_display=resolved.address.display,
                    city=resolved.address.city or "",
                    priority=priority,
                    dispatch_comments=comments,
                    aba_site_name=aba_site_name,
                ),
                units=resolved.final_units,
            )

            sel = self.task_map.select_task_ids_for_units(resolved.final_units, now=datetime.now(), auto_add_assistance=True)
            if sel.missing_units:
                raise ValueError("Missing task_id mapping for: " + ", ".join(sel.missing_units))

            self.last_priority = priority
            self.last_alert_text = alert
            self.last_task_ids = sel.task_ids
            self.last_location = self._fsr_location(resolved.address.display)

            extra_lines = [
                f"Units: {' '.join(resolved.final_units)}",
                f"Task IDs: {sel.task_ids}",
            ]
            if sel.assistance_added:
                extra_lines.append(f"Assistance auto-added: {sel.assistance_unit}")
            self.preview.setPlainText(alert + "\n\n" + "\n".join(extra_lines))
            self._log("Resolved preview ready.")

        except Exception as e:
            self._error("Resolve error", str(e))

    def on_send(self):

        def _extract_http_code(msg: str) -> str | None:
            # Matches: "... (400): ..." or "... (401) ..." etc.
            m = re.search(r"\((\d{3})\)", msg)
            return m.group(1) if m else None

        try:
            # 1) Always re-generate before sending to ensure latest inputs are used
            self._log("Opdaterer preview før afsendelse...")
            self.on_resolve()

            # If resolve failed, on_resolve() shows error dialog and will not set last_*.
            if not self.last_alert_text or not self.last_task_ids or not self.last_priority or not self.last_location:
                raise ValueError("Kunne ikke generere opdateret preview. Ret input og prøv igen.")

            fsr_prio = FSR_PRIORITY_MAP[self.last_priority]

            token_path = self.paths.project_root / "data" / "secrets" / "fsr_token.json"
            store = TokenStore(token_path)
            client = FireServiceRotaClient(base_url="https://www.fireservicerota.co.uk")

            token = store.load()
            if token:
                client.set_token(token)
            else:
                dlg = LoginDialog(self)
                if dlg.exec() != QDialog.Accepted:
                    return
                username, password = dlg.creds()
                if not username or not password:
                    raise ValueError("Brugernavn/adgangskode mangler.")
                token = client.login_with_password(username, password)
                store.save(token)

            self._log("Sender til FireServiceRota...")

            result = client.create_incident(
                body_text=self.last_alert_text,
                prio=fsr_prio,
                location=self.last_location,
                task_ids=self.last_task_ids,
            )

            inc_id = result.get("id") or result.get("incidentId")
            if inc_id:
                self._log(f"FSR OK (HTTP 200) – Hændelse oprettet (ID: {inc_id})")
                self._info("Sendt", f"Hændelse oprettet i FireServiceRota.\nID: {inc_id}")
            else:
                self._log("FSR OK (HTTP 200) – Hændelse oprettet")
                self._info("Sendt", "Hændelse oprettet i FireServiceRota.")

        except FireServiceRotaAuthError as e:
            code = _extract_http_code(str(e))
            if code:
                self._log(f"FSR AUTH FEJL (HTTP {code})")
            else:
                self._log("FSR AUTH FEJL")
            self._error("FSR login fejlede", "Adgang nægtet/ugyldig login (tjek credentials).")

        except FireServiceRotaError as e:
            code = _extract_http_code(str(e))
            if code:
                self._log(f"FSR FEJL (HTTP {code})")
                self._error("FSR fejl", f"Server returnerede fejl (HTTP {code}).")
            else:
                self._log("FSR FEJL")
                self._error("FSR fejl", "Server returnerede en fejl (ukendt HTTP kode).")

        except Exception as e:
            self._log("NETVÆRKSFEJL/TIMEOUT")
            self._log(str(e))
            self._error("Fejl", str(e))

    def _set_ready_state(self, ready: bool):
        self.resolve_btn.setEnabled(ready)
        self.send_btn.setEnabled(ready)
        self.search_btn.setEnabled(ready)

    def _missing_sources(self) -> list[str]:
        req = [
            self.paths.addresses_csv,
            self.paths.aba_xlsx,
            self.paths.pickliste_xlsx,
            self.paths.postnummer_xlsx,
            self.paths.taskids_xlsx,
        ]
        return [str(p) for p in req if not p.exists()]

    def _reload_sources(self):
        try:
            self._log("Indlæser datakilder...")

            # Load core data (hub owns these)
            self.hub = DataHub.from_paths(
                self.paths.addresses_csv,
                self.paths.aba_xlsx,
                self.paths.pickliste_xlsx,
                self.paths.postnummer_xlsx,
            )
            self.hub.load_all()

            # Task IDs are separate (not in DataHub)
            self.task_map = TaskMap(self.paths.taskids_xlsx)
            self.task_map.load()

            # Build resolver
            self.resolver = CalloutResolver(
                incidents=self.hub.incidents,
                aba=self.hub.aba,
            )

            self._set_ready_state(True)
            self._log("Datakilder indlæst.")
            self._install_incident_completer()

        except Exception as e:
            self._set_ready_state(False)
            self._log(f"FEJL ved indlæsning af datakilder: {e}")

    def _set_header_status(self, text: str, level: str = "info"):
        """
        level: 'ok' (green), 'warn' (yellow), 'err' (red), 'info' (grey)
        """
        dots = {"ok": "●", "warn": "●", "err": "●", "info": "●"}
        colors = {
            "ok": "#1b5e20",  # dark green
            "warn": "#8a6d00",  # amber
            "err": "#b71c1c",  # dark red
            "info": "#37474f",  # blue-grey
        }

        dot = dots.get(level, "●")
        color = colors.get(level, colors["info"])

        self.header_status.setText(f"{dot} {text}")
        self.header_status.setStyleSheet(f"font-weight: 900; color: {color};")

        f = self.header_status.font()
        f.setPointSize(16)  # tydeligere
        f.setBold(True)
        self.header_status.setFont(f)

    def on_clear(self):
        self.street.clear()
        self.house.clear()
        self.extra.clear()

        self.manual_post.clear()
        self.manual_city.clear()
        self.manual_assist.setChecked(False)

        self.candidate_list.clear()
        self._candidates = []
        self.selected_address = None

        self.incident_code.clear()
        self.aba_p.setChecked(True)
        self.prio1.setChecked(True)

        self.assist_incident_text.clear()
        self.assist_units.clear()
        self.comments.clear()

        self.last_alert_text = None
        self.last_task_ids = None
        self.last_priority = None
        self.last_location = None

        self.preview.clear()
        self._log("Cleared.")

    def _pretty(self, obj) -> str:
        try:
            return json.dumps(obj, ensure_ascii=False, indent=2)
        except Exception:
            return str(obj)


def run_gui():
    app = QApplication([])
    w = NoodudkaldQt()
    w.show()
    app.exec()
