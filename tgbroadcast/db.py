"""Хранилище SQLite: чаты, обнаруженные сообщения, задания и отправки.

Ключевая идея защиты от дублей:
  * у задания есть уникальный ``job_key`` (аккаунт + источник + id сообщений + номер запуска);
  * у каждой отправки есть уникальный ``delivery_key`` (job_key + целевой чат);
  * перед сетевой операцией отправка атомарно переводится из ``pending`` в ``sending``
    (UPDATE ... WHERE state='pending'), и это фиксируется в базе ДО вызова Telegram;
  * после сбоя все ``sending`` превращаются в ``unknown`` и никогда не отправляются
    автоматически повторно.
"""
from __future__ import annotations

import json
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

# Состояния отправки в конкретный чат
PENDING = "pending"        # ожидает отправки
SENDING = "sending"        # зарезервирована, сетевой запрос выполняется
SENT = "sent"              # Telegram подтвердил отправку
FAILED = "failed"          # Telegram однозначно отклонил отправку (сообщение НЕ отправлено)
UNKNOWN = "unknown"        # результат неизвестен (обрыв связи, сбой) — требуется проверка
CANCELLED = "cancelled"    # отменена пользователем до начала отправки
SKIPPED = "skipped"        # пропущена: уже отправлялась ранее в этот чат

DELIVERY_STATES_RU = {
    PENDING: "Ожидает",
    SENDING: "Отправляется",
    SENT: "Успешно",
    FAILED: "Ошибка",
    UNKNOWN: "Результат неизвестен",
    CANCELLED: "Отменено",
    SKIPPED: "Пропущено (уже отправлялось)",
}

# Состояния задания
JOB_READY = "ready"
JOB_RUNNING = "running"
JOB_PAUSED = "paused"
JOB_COMPLETED = "completed"
JOB_CANCELLED = "cancelled"

JOB_STATES_RU = {
    JOB_READY: "Готово к запуску",
    JOB_RUNNING: "Выполняется",
    JOB_PAUSED: "Пауза",
    JOB_COMPLETED: "Завершено",
    JOB_CANCELLED: "Отменено",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chats (
    peer_id    INTEGER PRIMARY KEY,
    title      TEXT NOT NULL,
    kind       TEXT NOT NULL,
    username   TEXT,
    can_send   INTEGER NOT NULL DEFAULT 1,
    selected   INTEGER NOT NULL DEFAULT 0,
    excluded   INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS detected_messages (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id     INTEGER NOT NULL,
    source_peer_id INTEGER NOT NULL,
    message_ids    TEXT NOT NULL,
    preview_text   TEXT,
    detected_at    REAL NOT NULL,
    status         TEXT NOT NULL DEFAULT 'new',
    UNIQUE(account_id, source_peer_id, message_ids)
);

CREATE TABLE IF NOT EXISTS jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_key        TEXT NOT NULL UNIQUE,
    source_key     TEXT NOT NULL,
    account_id     INTEGER NOT NULL,
    source_peer_id INTEGER NOT NULL,
    message_ids    TEXT NOT NULL,
    run_no         INTEGER NOT NULL,
    allow_resend   INTEGER NOT NULL DEFAULT 0,
    state          TEXT NOT NULL,
    preview_text   TEXT,
    created_at     REAL NOT NULL,
    started_at     REAL,
    finished_at    REAL,
    UNIQUE(source_key, run_no)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_key     TEXT NOT NULL UNIQUE,
    job_id           INTEGER NOT NULL REFERENCES jobs(id),
    source_key       TEXT NOT NULL,
    target_peer_id   INTEGER NOT NULL,
    target_title     TEXT NOT NULL,
    state            TEXT NOT NULL,
    random_ids       TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    sent_message_ids TEXT,
    error            TEXT,
    started_at       REAL,
    finished_at      REAL,
    updated_at       REAL NOT NULL,
    UNIQUE(job_id, target_peer_id)
);
CREATE INDEX IF NOT EXISTS ix_deliveries_source_target
    ON deliveries(source_key, target_peer_id, state);

CREATE TABLE IF NOT EXISTS delivery_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id INTEGER NOT NULL,
    ts          REAL NOT NULL,
    from_state  TEXT,
    to_state    TEXT NOT NULL,
    note        TEXT
);
"""


class JobAlreadyExists(Exception):
    """Для этого сообщения уже есть рассылка. Повторный запуск требует подтверждения."""

    def __init__(self, job: "Job"):
        super().__init__(f"Для этого сообщения уже есть рассылка #{job.id}")
        self.job = job


@dataclass
class Job:
    id: int
    job_key: str
    source_key: str
    account_id: int
    source_peer_id: int
    message_ids: list[int]
    run_no: int
    allow_resend: bool
    state: str
    preview_text: str
    created_at: float
    started_at: Optional[float]
    finished_at: Optional[float]


@dataclass
class Delivery:
    id: int
    delivery_key: str
    job_id: int
    source_key: str
    target_peer_id: int
    target_title: str
    state: str
    random_ids: Optional[list[int]]
    attempts: int
    sent_message_ids: Optional[list[int]]
    error: Optional[str]
    started_at: Optional[float]
    finished_at: Optional[float]
    updated_at: float


def make_source_key(account_id: int, source_peer_id: int, message_ids: Iterable[int]) -> str:
    ids = ",".join(str(i) for i in sorted(set(message_ids)))
    return f"{account_id}:{source_peer_id}:{ids}"


def make_job_key(source_key: str, run_no: int) -> str:
    return f"{source_key}#run{run_no}"


def make_delivery_key(job_key: str, target_peer_id: int) -> str:
    return f"{job_key}->{target_peer_id}"


def new_random_ids(count: int) -> list[int]:
    # random_id в MTProto — знаковое 64-битное число; Telegram использует его
    # для распознавания повторной доставки одного и того же запроса.
    return [random.randint(-(2 ** 63), 2 ** 63 - 1) or 1 for _ in range(count)]


def _job(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"], job_key=row["job_key"], source_key=row["source_key"],
        account_id=row["account_id"], source_peer_id=row["source_peer_id"],
        message_ids=json.loads(row["message_ids"]), run_no=row["run_no"],
        allow_resend=bool(row["allow_resend"]), state=row["state"],
        preview_text=row["preview_text"] or "", created_at=row["created_at"],
        started_at=row["started_at"], finished_at=row["finished_at"],
    )


def _delivery(row: sqlite3.Row) -> Delivery:
    return Delivery(
        id=row["id"], delivery_key=row["delivery_key"], job_id=row["job_id"],
        source_key=row["source_key"], target_peer_id=row["target_peer_id"],
        target_title=row["target_title"], state=row["state"],
        random_ids=json.loads(row["random_ids"]) if row["random_ids"] else None,
        attempts=row["attempts"],
        sent_message_ids=json.loads(row["sent_message_ids"]) if row["sent_message_ids"] else None,
        error=row["error"], started_at=row["started_at"], finished_at=row["finished_at"],
        updated_at=row["updated_at"],
    )


class Database:
    """Потокобезопасная обёртка над SQLite (одно соединение + блокировка)."""

    def __init__(self, path: str):
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            # FULL: каждая фиксированная транзакция переживает отключение питания.
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """Транзакция BEGIN IMMEDIATE: запись блокируется сразу, исключая гонки."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def _query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    # ------------------------------------------------------------------ settings
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        rows = self._query("SELECT value FROM settings WHERE key=?", (key,))
        return rows[0]["value"] if rows else default

    def set_setting(self, key: str, value: str) -> None:
        with self.tx() as c:
            c.execute("INSERT INTO settings(key, value) VALUES(?, ?) "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    # --------------------------------------------------------------------- chats
    def upsert_chats(self, chats: Iterable[dict]) -> None:
        """Обновляет список диалогов, сохраняя выбор пользователя."""
        now = time.time()
        with self.tx() as c:
            for ch in chats:
                c.execute(
                    "INSERT INTO chats(peer_id, title, kind, username, can_send, updated_at) "
                    "VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT(peer_id) DO UPDATE SET "
                    "title=excluded.title, kind=excluded.kind, username=excluded.username, "
                    "can_send=excluded.can_send, updated_at=excluded.updated_at",
                    (ch["peer_id"], ch["title"], ch["kind"], ch.get("username"),
                     1 if ch.get("can_send", True) else 0, now),
                )

    def list_chats(self) -> list[sqlite3.Row]:
        return self._query("SELECT * FROM chats ORDER BY can_send DESC, title COLLATE NOCASE")

    def set_chat_flags(self, peer_ids: Iterable[int], *, selected: Optional[bool] = None,
                       excluded: Optional[bool] = None) -> None:
        with self.tx() as c:
            for pid in peer_ids:
                if selected is not None:
                    c.execute("UPDATE chats SET selected=? WHERE peer_id=?", (int(selected), pid))
                if excluded is not None:
                    c.execute("UPDATE chats SET excluded=? WHERE peer_id=?", (int(excluded), pid))

    def selected_targets(self, exclude_peer_ids: Iterable[int] = ()) -> list[sqlite3.Row]:
        skip = set(exclude_peer_ids)
        rows = self._query("SELECT * FROM chats WHERE selected=1 AND excluded=0 AND can_send=1 "
                           "ORDER BY title COLLATE NOCASE")
        return [r for r in rows if r["peer_id"] not in skip]

    # --------------------------------------------------------- detected messages
    def record_detected(self, account_id: int, source_peer_id: int, message_ids: list[int],
                        preview_text: str) -> Optional[int]:
        """Возвращает id новой записи или None, если это сообщение уже обнаруживалось."""
        ids = json.dumps(sorted(set(message_ids)))
        with self.tx() as c:
            cur = c.execute(
                "INSERT OR IGNORE INTO detected_messages(account_id, source_peer_id, message_ids, "
                "preview_text, detected_at) VALUES(?, ?, ?, ?, ?)",
                (account_id, source_peer_id, ids, preview_text, time.time()))
            return cur.lastrowid if cur.rowcount == 1 else None

    def set_detected_status(self, detected_id: int, status: str) -> None:
        with self.tx() as c:
            c.execute("UPDATE detected_messages SET status=? WHERE id=?", (status, detected_id))

    # ---------------------------------------------------------------------- jobs
    def find_jobs_for_source(self, source_key: str) -> list[Job]:
        return [_job(r) for r in self._query(
            "SELECT * FROM jobs WHERE source_key=? ORDER BY run_no", (source_key,))]

    def create_job(self, account_id: int, source_peer_id: int, message_ids: list[int],
                   targets: list[tuple[int, str]], preview_text: str = "",
                   confirm_rerun: bool = False) -> Job:
        """Создаёт задание и строки отправок в одной транзакции.

        Если для этого сообщения уже существует задание, выбрасывается JobAlreadyExists,
        если только пользователь явно не подтвердил повторный запуск (confirm_rerun).
        Цели, совпадающие с исходным чатом («Избранное»), отбрасываются.
        """
        if not message_ids:
            raise ValueError("Нет сообщений для рассылки")
        source_key = make_source_key(account_id, source_peer_id, message_ids)
        seen: set[int] = set()
        clean_targets = []
        for pid, title in targets:
            if pid == source_peer_id or pid == account_id or pid in seen:
                continue
            seen.add(pid)
            clean_targets.append((pid, title))
        if not clean_targets:
            raise ValueError("Не выбрано ни одного чата для рассылки")
        now = time.time()
        with self.tx() as c:
            existing = c.execute("SELECT * FROM jobs WHERE source_key=? ORDER BY run_no DESC LIMIT 1",
                                 (source_key,)).fetchone()
            if existing is not None and not confirm_rerun:
                raise JobAlreadyExists(_job(existing))
            run_no = (existing["run_no"] + 1) if existing is not None else 1
            job_key = make_job_key(source_key, run_no)
            cur = c.execute(
                "INSERT INTO jobs(job_key, source_key, account_id, source_peer_id, message_ids, run_no, "
                "allow_resend, state, preview_text, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (job_key, source_key, account_id, source_peer_id,
                 json.dumps(sorted(set(message_ids))), run_no, int(run_no > 1), JOB_READY,
                 preview_text, now))
            job_id = cur.lastrowid
            for pid, title in clean_targets:
                c.execute(
                    "INSERT INTO deliveries(delivery_key, job_id, source_key, target_peer_id, target_title, "
                    "state, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (make_delivery_key(job_key, pid), job_id, source_key, pid, title, PENDING, now))
            row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return _job(row)

    def get_job(self, job_id: int) -> Optional[Job]:
        rows = self._query("SELECT * FROM jobs WHERE id=?", (job_id,))
        return _job(rows[0]) if rows else None

    def list_jobs(self, limit: int = 200) -> list[Job]:
        return [_job(r) for r in self._query("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,))]

    def try_start_job(self, job_id: int) -> bool:
        """Атомарно переводит задание в running. False — уже выполняется/завершено."""
        with self.tx() as c:
            cur = c.execute(
                "UPDATE jobs SET state=?, started_at=COALESCE(started_at, ?) "
                "WHERE id=? AND state IN (?, ?)",
                (JOB_RUNNING, time.time(), job_id, JOB_READY, JOB_PAUSED))
            return cur.rowcount == 1

    def set_job_state(self, job_id: int, state: str) -> None:
        with self.tx() as c:
            fin = time.time() if state in (JOB_COMPLETED, JOB_CANCELLED) else None
            c.execute("UPDATE jobs SET state=?, finished_at=COALESCE(?, finished_at) WHERE id=?",
                      (state, fin, job_id))

    def reopen_job_if_finished(self, job_id: int) -> None:
        """Делает задание снова запускаемым (после ручного разрешения повторов/ошибок)."""
        with self.tx() as c:
            c.execute("UPDATE jobs SET state=?, finished_at=NULL WHERE id=? AND state IN (?, ?)",
                      (JOB_PAUSED, job_id, JOB_COMPLETED, JOB_CANCELLED))

    def cancel_remaining(self, job_id: int) -> int:
        """Отменяет только ещё не начатые отправки. Отправленные/неизвестные не трогаются."""
        with self.tx() as c:
            rows = c.execute("SELECT id FROM deliveries WHERE job_id=? AND state=?",
                             (job_id, PENDING)).fetchall()
            for r in rows:
                self._transition(c, r["id"], PENDING, CANCELLED, "Отменено пользователем")
            c.execute("UPDATE jobs SET state=?, finished_at=? WHERE id=? AND state!=?",
                      (JOB_CANCELLED, time.time(), job_id, JOB_RUNNING))
            return len(rows)

    # ---------------------------------------------------------------- deliveries
    def list_deliveries(self, job_id: Optional[int] = None) -> list[Delivery]:
        if job_id is None:
            rows = self._query("SELECT * FROM deliveries ORDER BY id DESC LIMIT 2000")
        else:
            rows = self._query("SELECT * FROM deliveries WHERE job_id=? ORDER BY id", (job_id,))
        return [_delivery(r) for r in rows]

    def get_delivery(self, delivery_id: int) -> Optional[Delivery]:
        rows = self._query("SELECT * FROM deliveries WHERE id=?", (delivery_id,))
        return _delivery(rows[0]) if rows else None

    def counts(self, job_id: int) -> dict[str, int]:
        result = {s: 0 for s in DELIVERY_STATES_RU}
        for r in self._query("SELECT state, COUNT(*) AS n FROM deliveries WHERE job_id=? GROUP BY state",
                             (job_id,)):
            result[r["state"]] = r["n"]
        return result

    def next_pending(self, job_id: int) -> Optional[Delivery]:
        rows = self._query("SELECT * FROM deliveries WHERE job_id=? AND state=? ORDER BY id LIMIT 1",
                           (job_id, PENDING))
        return _delivery(rows[0]) if rows else None

    @staticmethod
    def _transition(c: sqlite3.Connection, delivery_id: int, from_state: str, to_state: str,
                    note: Optional[str] = None, **fields) -> bool:
        sets = ["state=?", "updated_at=?"]
        params: list = [to_state, time.time()]
        for k, v in fields.items():
            sets.append(f"{k}=?")
            params.append(v)
        params += [delivery_id, from_state]
        cur = c.execute(f"UPDATE deliveries SET {', '.join(sets)} WHERE id=? AND state=?", params)
        if cur.rowcount == 1:
            c.execute("INSERT INTO delivery_events(delivery_id, ts, from_state, to_state, note) "
                      "VALUES(?, ?, ?, ?, ?)", (delivery_id, time.time(), from_state, to_state, note))
            return True
        return False

    def reserve(self, delivery_id: int, message_count: int) -> tuple[Optional[Delivery], Optional[str]]:
        """Атомарно резервирует отправку перед сетевым запросом.

        Возвращает (delivery, None) при успешной резервации или (None, причина), если
        отправлять нельзя. Проверки выполняются в одной транзакции BEGIN IMMEDIATE:
          1) строка всё ещё в состоянии pending;
          2) это же сообщение не отправлено/не отправляется/не «неизвестно» в этот чат
             в рамках другого задания (если повтор явно не разрешён);
          3) random_id генерируются один раз и сохраняются — при ручном повторе
             используются те же, чтобы Telegram мог распознать дубликат.
        """
        with self.tx() as c:
            row = c.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
            if row is None or row["state"] != PENDING:
                return None, "Отправка уже не в состоянии ожидания"
            job = c.execute("SELECT * FROM jobs WHERE id=?", (row["job_id"],)).fetchone()
            busy_states = (SENT, SENDING, UNKNOWN) if not job["allow_resend"] else (SENDING, UNKNOWN)
            other = c.execute(
                f"SELECT d.id, d.state, d.job_id FROM deliveries d WHERE d.source_key=? AND d.target_peer_id=? "
                f"AND d.id!=? AND d.state IN ({','.join('?' * len(busy_states))}) LIMIT 1",
                (row["source_key"], row["target_peer_id"], delivery_id, *busy_states)).fetchone()
            if other is not None:
                reason = (f"Уже {DELIVERY_STATES_RU[other['state']].lower()} в рассылке #{other['job_id']}")
                self._transition(c, delivery_id, PENDING, SKIPPED, reason, error=reason,
                                 finished_at=time.time())
                return None, reason
            random_ids = json.loads(row["random_ids"]) if row["random_ids"] else None
            if not random_ids or len(random_ids) != message_count:
                random_ids = new_random_ids(message_count)
            ok = self._transition(c, delivery_id, PENDING, SENDING, "Зарезервировано перед отправкой",
                                  random_ids=json.dumps(random_ids), attempts=row["attempts"] + 1,
                                  started_at=time.time(), error=None)
            if not ok:
                return None, "Не удалось зарезервировать отправку"
            new_row = c.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
        return _delivery(new_row), None

    def mark_sent(self, delivery_id: int, sent_ids: list[int], note: str = "Подтверждено Telegram") -> bool:
        with self.tx() as c:
            return self._transition(c, delivery_id, SENDING, SENT, note,
                                    sent_message_ids=json.dumps(sent_ids), finished_at=time.time(), error=None)

    def mark_failed(self, delivery_id: int, error: str) -> bool:
        with self.tx() as c:
            return self._transition(c, delivery_id, SENDING, FAILED, error, error=error,
                                    finished_at=time.time())

    def mark_unknown(self, delivery_id: int, error: str) -> bool:
        with self.tx() as c:
            return self._transition(c, delivery_id, SENDING, UNKNOWN, error, error=error)

    def release_to_pending(self, delivery_id: int, note: str) -> bool:
        """Только для случаев, когда Telegram ОДНОЗНАЧНО не принял запрос (FloodWait)."""
        with self.tx() as c:
            return self._transition(c, delivery_id, SENDING, PENDING, note, error=note)

    def resolve_unknown(self, delivery_id: int, *, sent: bool, note: str,
                        sent_ids: Optional[list[int]] = None) -> bool:
        """Ручное/проверочное разрешение «неизвестного» результата.

        sent=True — считать отправленным; sent=False — разрешить повторную отправку
        (вернуть в pending; сохранённые random_id будут использованы повторно).
        """
        with self.tx() as c:
            if sent:
                return self._transition(c, delivery_id, UNKNOWN, SENT, note,
                                        sent_message_ids=json.dumps(sent_ids or []),
                                        finished_at=time.time(), error=None)
            return self._transition(c, delivery_id, UNKNOWN, PENDING, note, error=None)

    def retry_failed(self, job_id: int) -> int:
        with self.tx() as c:
            rows = c.execute("SELECT id FROM deliveries WHERE job_id=? AND state=?",
                             (job_id, FAILED)).fetchall()
            n = sum(self._transition(c, r["id"], FAILED, PENDING, "Повтор после ошибки по запросу пользователя")
                    for r in rows)
        if n:
            self.reopen_job_if_finished(job_id)
        return n

    def events(self, delivery_id: int) -> list[sqlite3.Row]:
        return self._query("SELECT * FROM delivery_events WHERE delivery_id=? ORDER BY id", (delivery_id,))

    # ------------------------------------------------------------------ recovery
    def recover_after_crash(self) -> tuple[int, int]:
        """Вызывается при запуске. Прерванные отправки -> unknown, задания -> пауза."""
        with self.tx() as c:
            rows = c.execute("SELECT id FROM deliveries WHERE state=?", (SENDING,)).fetchall()
            for r in rows:
                self._transition(c, r["id"], SENDING, UNKNOWN,
                                 "Программа была прервана во время отправки: результат неизвестен",
                                 error="Программа была прервана во время отправки")
            cur = c.execute("UPDATE jobs SET state=? WHERE state=?", (JOB_PAUSED, JOB_RUNNING))
            return len(rows), cur.rowcount
