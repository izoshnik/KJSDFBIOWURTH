"""Движок рассылки: последовательная отправка с резервированием в SQLite.

Движок не зависит от Telethon: сетевой слой передаётся как ``Transport`` и
сообщает результат через исключения ниже. Это позволяет тестировать логику
защиты от дублей без доступа к Telegram.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Protocol

from . import db as D

log = logging.getLogger(__name__)


class SendRejected(Exception):
    """Telegram однозначно отклонил запрос: сообщение НЕ было отправлено."""


class SendBlocked(SendRejected):
    """Отказ, после которого продолжать рассылку бессмысленно (ограничение аккаунта,
    потеря авторизации). Сообщение НЕ отправлено, задание ставится на паузу."""


class SendFloodWait(Exception):
    """Telegram попросил подождать (FloodWait/SlowMode). Запрос не был выполнен."""

    def __init__(self, seconds: int, message: str = ""):
        super().__init__(message or f"Требуется подождать {seconds} с")
        self.seconds = seconds


class SendAlreadyDelivered(Exception):
    """Telegram сообщил, что запрос с этими random_id уже был выполнен."""


class SendUnknown(Exception):
    """Результат неизвестен (обрыв связи, таймаут, непредвиденная ошибка)."""


class Transport(Protocol):
    async def send(self, source_peer_id: int, message_ids: list[int], target_peer_id: int,
                   random_ids: list[int]) -> list[int]:
        """Копирует сообщения в целевой чат и возвращает id новых сообщений."""


@dataclass
class EngineSettings:
    delay_between_sends: float = 3.0      # пауза между чатами для соблюдения лимитов
    max_flood_wait: int = 300             # дольше — ставим задание на паузу
    max_flood_retries: int = 3            # сколько раз ждать FloodWait для одного чата


@dataclass
class _Control:
    pause: asyncio.Event = field(default_factory=asyncio.Event)
    wake: asyncio.Event = field(default_factory=asyncio.Event)


ProgressCallback = Callable[[int, str], None]  # (job_id, текстовое событие)


class BroadcastEngine:
    """Выполняет задания строго по одному (глобальная блокировка)."""

    def __init__(self, database: D.Database, transport: Transport,
                 settings: Optional[EngineSettings] = None,
                 on_progress: Optional[ProgressCallback] = None,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self.db = database
        self.transport = transport
        self.settings = settings or EngineSettings()
        self.on_progress = on_progress or (lambda job_id, text: None)
        self._sleep = sleep
        self._run_lock: Optional[asyncio.Lock] = None
        self._controls: dict[int, _Control] = {}
        self.current_job_id: Optional[int] = None

    def _lock(self) -> asyncio.Lock:
        if self._run_lock is None:
            self._run_lock = asyncio.Lock()
        return self._run_lock

    def _emit(self, job_id: int, text: str) -> None:
        log.info("Рассылка #%s: %s", job_id, text)
        try:
            self.on_progress(job_id, text)
        except Exception:  # интерфейс не должен ломать отправку
            log.exception("Ошибка обработчика прогресса")

    # ---------------------------------------------------------------- controls
    def request_pause(self, job_id: int) -> None:
        ctl = self._controls.get(job_id)
        if ctl:
            ctl.pause.set()
            ctl.wake.set()

    def is_running(self, job_id: Optional[int] = None) -> bool:
        if job_id is None:
            return self.current_job_id is not None
        return self.current_job_id == job_id

    def cancel_remaining(self, job_id: int) -> int:
        """Отменяет ещё не начатые отправки; текущая (уже начатая) завершится штатно."""
        n = self.db.cancel_remaining(job_id)
        ctl = self._controls.get(job_id)
        if ctl:
            ctl.wake.set()
        self._emit(job_id, f"Отменено ожидающих отправок: {n}")
        return n

    async def _interruptible_sleep(self, ctl: _Control, seconds: float) -> None:
        if seconds <= 0:
            return
        ctl.wake.clear()
        sleeper = asyncio.ensure_future(self._sleep(seconds))
        waker = asyncio.ensure_future(ctl.wake.wait())
        try:
            await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (sleeper, waker):
                if not t.done():
                    t.cancel()

    # --------------------------------------------------------------------- run
    async def run_job(self, job_id: int) -> str:
        """Запускает/продолжает задание. Возвращает итоговое состояние задания."""
        lock = self._lock()
        if lock.locked():
            self._emit(job_id, "Другая рассылка уже выполняется — запуск отклонён")
            return "busy"
        async with lock:
            if not self.db.try_start_job(job_id):
                job = self.db.get_job(job_id)
                self._emit(job_id, "Задание нельзя запустить в текущем состоянии")
                return job.state if job else "missing"
            ctl = _Control()
            self._controls[job_id] = ctl
            self.current_job_id = job_id
            try:
                final = await self._loop(job_id, ctl)
            except Exception as e:  # защитная сетка: задание не должно «зависнуть» в running
                log.exception("Сбой выполнения рассылки #%s", job_id)
                self.db.set_job_state(job_id, D.JOB_PAUSED)
                self._emit(job_id, f"Внутренняя ошибка, задание поставлено на паузу: {e}")
                final = D.JOB_PAUSED
            finally:
                self._controls.pop(job_id, None)
                self.current_job_id = None
            return final

    async def _loop(self, job_id: int, ctl: _Control) -> str:
        job = self.db.get_job(job_id)
        assert job is not None
        flood_retries: dict[int, int] = {}
        first = True
        while True:
            if ctl.pause.is_set():
                self.db.set_job_state(job_id, D.JOB_PAUSED)
                self._emit(job_id, "Пауза")
                return D.JOB_PAUSED
            delivery = self.db.next_pending(job_id)
            if delivery is None:
                break
            if not first:
                await self._interruptible_sleep(ctl, self.settings.delay_between_sends)
                if ctl.pause.is_set():
                    continue
                # за время ожидания отправку могли отменить
                delivery = self.db.next_pending(job_id)
                if delivery is None:
                    break
            first = False

            reserved, reason = self.db.reserve(delivery.id, len(job.message_ids))
            if reserved is None:
                self._emit(job_id, f"«{delivery.target_title}»: пропуск — {reason}")
                first = True
                continue
            assert reserved.random_ids is not None
            try:
                sent_ids = await self.transport.send(job.source_peer_id, job.message_ids,
                                                     reserved.target_peer_id, reserved.random_ids)
            except SendAlreadyDelivered as e:
                self.db.mark_sent(reserved.id, [], f"Telegram сообщил о уже выполненном запросе: {e}")
                self._emit(job_id, f"«{reserved.target_title}»: уже было доставлено ранее (повтор не выполнен)")
            except SendFloodWait as e:
                # Запрос отклонён сервером до выполнения — сообщение не отправлено.
                self.db.release_to_pending(reserved.id, f"Ограничение Telegram: подождать {e.seconds} с")
                n = flood_retries.get(reserved.id, 0) + 1
                flood_retries[reserved.id] = n
                if e.seconds > self.settings.max_flood_wait or n > self.settings.max_flood_retries:
                    self.db.set_job_state(job_id, D.JOB_PAUSED)
                    self._emit(job_id, f"Telegram требует подождать {e.seconds} с. Задание поставлено "
                                       f"на паузу — продолжите позже вручную.")
                    return D.JOB_PAUSED
                self._emit(job_id, f"Ограничение частоты Telegram: ожидание {e.seconds} с")
                await self._interruptible_sleep(ctl, e.seconds + 1)
                first = True
            except SendBlocked as e:
                self.db.mark_failed(reserved.id, str(e))
                self.db.set_job_state(job_id, D.JOB_PAUSED)
                self._emit(job_id, f"«{reserved.target_title}»: {e}. Задание поставлено на паузу.")
                return D.JOB_PAUSED
            except SendRejected as e:
                self.db.mark_failed(reserved.id, str(e))
                self._emit(job_id, f"«{reserved.target_title}»: ошибка — {e}")
            except asyncio.CancelledError:
                self.db.mark_unknown(reserved.id, "Отправка прервана: результат неизвестен")
                raise
            except Exception as e:  # SendUnknown и всё непредвиденное
                self.db.mark_unknown(reserved.id, f"Результат неизвестен: {e}")
                self._emit(job_id, f"«{reserved.target_title}»: результат неизвестен — {e}")
            else:
                self.db.mark_sent(reserved.id, sent_ids)
                self._emit(job_id, f"«{reserved.target_title}»: отправлено")

        counts = self.db.counts(job_id)
        final = D.JOB_CANCELLED if counts[D.CANCELLED] else D.JOB_COMPLETED
        self.db.set_job_state(job_id, final)
        self._emit(job_id, "Рассылка завершена" if final == D.JOB_COMPLETED else "Рассылка отменена")
        return final
