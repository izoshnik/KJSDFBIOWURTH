"""Пути к данным, конфигурация, журнал и защита от второго экземпляра программы."""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

APP_NAME = "TgBroadcast"


def data_dir() -> Path:
    """%APPDATA%\\TgBroadcast на Windows, ~/.tgbroadcast в остальных системах.

    Можно переопределить переменной окружения TGBROADCAST_HOME.
    """
    override = os.environ.get("TGBROADCAST_HOME")
    if override:
        base = Path(override)
    elif sys.platform == "win32" and os.environ.get("APPDATA"):
        base = Path(os.environ["APPDATA"]) / APP_NAME
    else:
        base = Path.home() / ".tgbroadcast"
    base.mkdir(parents=True, exist_ok=True)
    return base


class Config:
    """config.json: API ID/Hash и параметры. Хранится только локально, не в коде."""

    DEFAULTS = {
        "api_id": None,
        "api_hash": None,
        "delay_between_sends": 3.0,
        "max_flood_wait": 300,
        "auto_show_confirmation": True,
    }

    def __init__(self, path: Path):
        self.path = path
        self.values = dict(self.DEFAULTS)
        if path.exists():
            try:
                self.values.update(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                logging.getLogger(__name__).warning("config.json повреждён — используются значения по умолчанию")

    def __getitem__(self, key):
        return self.values.get(key, self.DEFAULTS.get(key))

    def __setitem__(self, key, value):
        self.values[key] = value

    def has_api(self) -> bool:
        return bool(self["api_id"]) and bool(self["api_hash"])

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.values, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)


class RedactingFilter(logging.Filter):
    """Маскирует в журнале номера телефонов, хэши API и коды подтверждения."""

    PATTERNS = [
        (re.compile(r"\+?\d[\d\s\-()]{8,}\d"), "<скрыто>"),
        (re.compile(r"\b[0-9a-f]{32}\b", re.I), "<скрыто>"),
        (re.compile(r"(?i)(code|код|password|пароль|hash)(\s*[=:]\s*)\S+"), r"\1\2<скрыто>"),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for rx, repl in self.PATTERNS:
            msg = rx.sub(repl, msg)
        record.msg, record.args = msg, ()
        return True


def setup_logging(directory: Path) -> Path:
    log_path = directory / "tgbroadcast.log"
    handler = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    handler.addFilter(RedactingFilter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    logging.getLogger("telethon").setLevel(logging.WARNING)
    return log_path


class SingleInstance:
    """Блокировка файла: не даёт запустить две копии программы с одной базой."""

    def __init__(self, path: Path):
        self.path = path
        self._fh: Optional[object] = None

    def acquire(self) -> bool:
        fh = open(self.path, "a+")
        try:
            if sys.platform == "win32":
                import msvcrt
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self._fh = fh
        return True
