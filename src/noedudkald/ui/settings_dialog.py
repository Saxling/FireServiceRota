from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import requests

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit,
    QPushButton, QFileDialog, QHBoxLayout, QMessageBox,
    QProgressDialog, QApplication, QDialogButtonBox
)

from noedudkald.core.source_config import SourceConfig

# Validators (import your real loaders)
from noedudkald.data_sources.addresses import AddressDirectory
from noedudkald.data_sources.aba import AbaDirectory
from noedudkald.data_sources.incidents import IncidentMatrix
from noedudkald.data_sources.postcodes import PostcodeDirectory
from noedudkald.data_sources.task_map import TaskMap
from noedudkald.integrations.fireservicerota_client import FireServiceRotaClient, FireServiceRotaAuthError, FireServiceRotaError
from noedudkald.integrations.token_store import TokenStore
from noedudkald.persistence.runtime_paths import ensure_user_data_layout


@dataclass(frozen=True)
class SourceRow:
    label: str
    key: str


ROWS = [
    SourceRow("ABA liste", "aba"),
    SourceRow("112 adresser", "addresses"),
    SourceRow("Pickliste", "incidents"),
    SourceRow("Postnumre", "postcodes"),
    SourceRow("Enheder / TaskID", "task_ids"),
]


class SettingsDialog(QDialog):
    def __init__(self, project_root: Path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Indstillinger")
        self.resize(720, 340)

        self.cfg_mgr = SourceConfig(project_root)
        self.cfg = self.cfg_mgr.load()

        layout = QVBoxLayout(self)

        self.form = QFormLayout()
        layout.addLayout(self.form)

        self.edits: dict[str, QLineEdit] = {}
        for row in ROWS:
            self._add_row(row.label, row.key)

        # Buttons
        btn_row = QHBoxLayout()
        self.test_btn = QPushButton("Test datakilder")
        self.test_btn.clicked.connect(self.on_test_all)

        self.save_btn = QPushButton("Gem")
        self.save_btn.clicked.connect(self.on_save)

        self.close_btn = QPushButton("Luk")
        self.close_btn.clicked.connect(self.reject)

        btn_row.addWidget(self.test_btn)
        btn_row.addStretch(1)
        btn_row.addWidget(self.save_btn)
        btn_row.addWidget(self.close_btn)
        layout.addLayout(btn_row)

        fsr_row = QHBoxLayout()

        self.fsr_status = QLineEdit()
        self.fsr_status.setReadOnly(True)
        self.fsr_status.setPlaceholderText("FSR: ingen token")

        self.fsr_login_btn = QPushButton("Log ind på FSR…")
        self.fsr_login_btn.clicked.connect(self.on_fsr_login)

        self.fsr_test_btn = QPushButton("Test FSR")
        self.fsr_test_btn.clicked.connect(self.on_fsr_test)

        fsr_row.addWidget(self.fsr_status, 1)
        fsr_row.addWidget(self.fsr_login_btn)
        fsr_row.addWidget(self.fsr_test_btn)

        layout.addLayout(fsr_row)

        self._refresh_fsr_status()

    def _add_row(self, label: str, key: str):
        row = QHBoxLayout()

        edit = QLineEdit()
        edit.setReadOnly(True)

        # Show actual selected file if it exists, otherwise blank + placeholder
        expected = self.cfg_mgr.input_dir / self.cfg_mgr.defaults[key]
        if expected.exists():
            edit.setText(str(expected))
        else:
            edit.setText("")
            edit.setPlaceholderText("Ingen fil valgt…")

        btn = QPushButton("Vælg…")
        btn.clicked.connect(lambda: self.on_select_file(key))

        row.addWidget(edit, 1)
        row.addWidget(btn)

        self.form.addRow(label, row)
        self.edits[key] = edit

    def _progress(self, title: str, text: str) -> QProgressDialog:
        dlg = QProgressDialog(text, None, 0, 0, self)
        dlg.setWindowTitle(title)
        dlg.setWindowModality(Qt.ApplicationModal)
        dlg.setCancelButton(None)
        dlg.setMinimumDuration(0)
        dlg.setAutoClose(False)
        dlg.setAutoReset(False)
        dlg.show()

        # Force it to render before we do blocking work
        QApplication.processEvents()
        return dlg

    def _copy_with_backup(self, key: str, src: Path) -> tuple[Path, Path | None]:
        """
        Copy src into data/input under standard internal filename.
        Returns (target, backup_path)
        """
        target = self.cfg_mgr.input_dir / self.cfg_mgr.defaults[key]
        backup = None

        self.cfg_mgr.input_dir.mkdir(parents=True, exist_ok=True)

        if target.exists():
            backup = target.with_suffix(target.suffix + ".bak")
            if backup.exists():
                backup.unlink()
            shutil.copy2(target, backup)

        shutil.copy2(src, target)
        return target, backup

    def _rollback(self, target: Path, backup: Path | None):
        try:
            if target.exists():
                target.unlink()
        except Exception:
            pass
        if backup and backup.exists():
            shutil.copy2(backup, target)

    # ---- validation: only changed source ----
    def _validate_one(self, key: str, path: Path):
        if key == "addresses":
            d = AddressDirectory(path)
            d.load()
            return
        if key == "aba":
            a = AbaDirectory(path)
            a.load()
            return
        if key == "incidents":
            m = IncidentMatrix(path)
            m.load()
            return
        if key == "postcodes":
            p = PostcodeDirectory(path)
            p.load()
            return
        if key == "task_ids":
            t = TaskMap(path)
            t.load()
            return
        raise ValueError(f"Ukendt datakilde key: {key}")

    def on_select_file(self, key: str):
        file, _ = QFileDialog.getOpenFileName(self, "Vælg fil")
        if not file:
            return

        src = Path(file)

        prog = self._progress("Validerer", "Kopierer og validerer fil...")
        QApplication.processEvents()
        target = None
        backup = None
        try:
            target, backup = self._copy_with_backup(key, src)

            # validate only this source
            self._validate_one(key, target)

            # update config (stores the internal filename)
            self.cfg[key] = target.name
            self.edits[key].setPlaceholderText("Fil ok - tryk gem for at opdatere ")

            QMessageBox.information(self, "OK", "Fil valideret og gemt.")
        except Exception as e:
            if target is not None:
                self._rollback(target, backup)
            QMessageBox.critical(self, "Valideringsfejl", f"Filen kunne ikke bruges:\n\n{e}")
        finally:
            prog.close()

    def on_test_all(self):
        prog = self._progress("Tester", "Tester alle datakilder...")
        QApplication.processEvents()
        try:
            # Validate each required source (fast, but complete)
            for row in ROWS:
                path = self.cfg_mgr.input_dir / self.cfg_mgr.defaults[row.key]
                if not path.exists():
                    raise FileNotFoundError(f"Mangler fil: {path.name}")
                self._validate_one(row.key, path)

            QMessageBox.information(self, "OK", "Alle datakilder er OK.")
        except Exception as e:
            QMessageBox.critical(self, "Test fejlede", str(e))
        finally:
            prog.close()

    def on_save(self):
        self.cfg_mgr.save(self.cfg)
        self.accept()

    def _token_store(self) -> TokenStore:
        udata = ensure_user_data_layout()
        token_path = udata / "secrets" / "fsr_token.json"
        return TokenStore(token_path)

    def _refresh_fsr_status(self):
        store = self._token_store()
        token = store.load()

        if token:
            user = getattr(token, "username", None)
            if user:
                self.fsr_status.setText(f"Logget ind som: {user}")
            else:
                self.fsr_status.setText("Token gemt")
        else:
            self.fsr_status.setText("")
            self.fsr_status.setPlaceholderText("FSR: ingen bruger logget ind")

    def on_fsr_login(self):
        dlg = LoginDialog(self)
        if dlg.exec() != QDialog.Accepted:
            return

        username, password = dlg.creds()
        if not username or not password:
            QMessageBox.critical(self, "FSR login", "Brugernavn/adgangskode mangler.")
            return

        client = FireServiceRotaClient(base_url="https://www.fireservicerota.co.uk")

        try:
            token = client.login_with_password(username, password)
            self._token_store().save(token, username=username)
            client.set_persist_token_callback(lambda t: store.save(t))
            QMessageBox.information(self, "FSR login", "Login OK. Token gemt.")
            parent = self.parent()
            if parent and hasattr(parent, "_run_startup_checks"):
                parent._run_startup_checks()
        except Exception as e:
            QMessageBox.critical(self, "FSR login fejlede", str(e))

    def on_fsr_test(self):
        # Test reachability + token validity (samme logik som startup)
        client = FireServiceRotaClient(base_url="https://www.fireservicerota.co.uk")
        store = self._token_store()
        token = store.load()

        try:
            r = requests.get("https://www.fireservicerota.co.uk/api/v2/health", timeout=6)
            if not r.ok:
                QMessageBox.critical(self, "FSR test", "FSR offline (health fejlede).")
                return
        except Exception:
            QMessageBox.critical(self, "FSR test", "FSR offline (ingen forbindelse).")
            return

        if not token:
            QMessageBox.warning(self, "FSR test", "FSR er online, men der er ingen token gemt.")
            return

        client.set_token(token)
        server_ok, auth_ok = client.test_connection()
        if server_ok and auth_ok:
            QMessageBox.information(self, "FSR test", "FSR OK (token gyldig).")
            self.fsr_status.setText("Token OK")
        elif server_ok and not auth_ok:
            QMessageBox.warning(self, "FSR test", "FSR online, men token er ugyldig/udløbet.")
            self.fsr_status.setText("Token ugyldig/udløbet")
        else:
            QMessageBox.critical(self, "FSR test", "FSR offline.")
            self.fsr_status.setText("FSR offline")


class LoginDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("FireServiceRota login")
        self.resize(340, 150)

        self.setModal(True)

        layout = QVBoxLayout(self)

        form = QFormLayout()
        self.user = QLineEdit()
        self.pw = QLineEdit()
        self.pw.setEchoMode(QLineEdit.Password)
        form.addRow("Username / email", self.user)
        form.addRow("Password", self.pw)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.user.setFocus()

        self.pw.returnPressed.connect(self.accept)

    def creds(self) -> tuple[str, str]:
        return self.user.text().strip(), self.pw.text()
