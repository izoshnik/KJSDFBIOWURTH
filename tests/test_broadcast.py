import asyncio
import json
import logging

import pytest

from tgbroadcast import db as D
from tgbroadcast.engine import (BroadcastEngine, EngineSettings, SendAlreadyDelivered, SendBlocked,
                                SendFloodWait, SendRejected, SendUnknown)
from tgbroadcast.infra import RedactingFilter

ME = 100
TARGETS = [(1, "Чат A"), (2, "Чат B"), (3, "Чат C")]


async def no_sleep(_seconds):
    await asyncio.sleep(0)


class FakeTransport:
    """Имитирует Telegram. outcomes: target -> список исходов по попыткам."""

    def __init__(self, outcomes=None):
        self.outcomes = outcomes or {}
        self.calls = []
        self.delivered = []  # (target, tuple(random_ids))

    async def send(self, source_peer_id, message_ids, target_peer_id, random_ids):
        self.calls.append((target_peer_id, tuple(random_ids)))
        seq = self.outcomes.get(target_peer_id, [])
        outcome = seq.pop(0) if seq else "ok"
        if outcome == "ok":
            self.delivered.append((target_peer_id, tuple(random_ids)))
            return [1000 + target_peer_id]
        if outcome == "deliver_then_drop":  # сервер принял, ответ потерян
            self.delivered.append((target_peer_id, tuple(random_ids)))
            raise SendUnknown("соединение разорвано")
        if isinstance(outcome, Exception):
            raise outcome
        raise AssertionError(outcome)


@pytest.fixture
def database(tmp_path):
    d = D.Database(str(tmp_path / "t.sqlite3"))
    yield d
    d.close()


def make_engine(database, transport, **kw):
    return BroadcastEngine(database, transport, EngineSettings(delay_between_sends=0, **kw), sleep=no_sleep)


def run(coro):
    return asyncio.run(coro)


def test_happy_path_sends_once_per_chat(database):
    job = database.create_job(ME, ME, [5], TARGETS)
    t = FakeTransport()
    assert run(make_engine(database, t).run_job(job.id)) == D.JOB_COMPLETED
    assert sorted(c[0] for c in t.calls) == [1, 2, 3]
    assert database.counts(job.id)[D.SENT] == 3
    # повторный запуск завершённого задания ничего не отправляет
    assert run(make_engine(database, t).run_job(job.id)) == D.JOB_COMPLETED
    assert len(t.calls) == 3


def test_saved_messages_never_target(database):
    job = database.create_job(ME, ME, [5], [(ME, "Избранное"), (1, "A"), (1, "A дубль")])
    assert [d.target_peer_id for d in database.list_deliveries(job.id)] == [1]


def test_double_click_creates_no_second_job(database):
    database.create_job(ME, ME, [5, 6], TARGETS)
    with pytest.raises(D.JobAlreadyExists):
        database.create_job(ME, ME, [6, 5], TARGETS)  # тот же альбом в другом порядке
    assert len(database.list_jobs()) == 1


def test_rerun_requires_confirmation_and_is_new_job(database):
    j1 = database.create_job(ME, ME, [5], TARGETS)
    run(make_engine(database, FakeTransport()).run_job(j1.id))
    j2 = database.create_job(ME, ME, [5], TARGETS, confirm_rerun=True)
    assert j2.run_no == 2 and j2.allow_resend and j2.job_key != j1.job_key
    t = FakeTransport()
    run(make_engine(database, t).run_job(j2.id))
    assert len(t.calls) == 3


def test_cross_job_protection_skips_already_sent(database):
    j1 = database.create_job(ME, ME, [5], TARGETS[:1])
    run(make_engine(database, FakeTransport()).run_job(j1.id))
    # искусственно создаём второе задание без флага allow_resend (например, гонка/сбой в UI)
    with database.tx() as c:
        c.execute("UPDATE jobs SET run_no=99 WHERE id=?", (j1.id,))
    j2 = database.create_job(ME, ME, [5], TARGETS[:2], confirm_rerun=True)
    with database.tx() as c:
        c.execute("UPDATE jobs SET allow_resend=0 WHERE id=?", (j2.id,))
    t = FakeTransport()
    run(make_engine(database, t).run_job(j2.id))
    assert [c[0] for c in t.calls] == [2]
    assert database.counts(j2.id)[D.SKIPPED] == 1


def test_reserve_is_atomic(database):
    job = database.create_job(ME, ME, [5], TARGETS[:1])
    d = database.list_deliveries(job.id)[0]
    first, _ = database.reserve(d.id, 1)
    second, reason = database.reserve(d.id, 1)
    assert first is not None and second is None and reason


def test_unknown_result_is_never_auto_resent(database):
    job = database.create_job(ME, ME, [5], TARGETS)
    t = FakeTransport({2: ["deliver_then_drop"]})
    run(make_engine(database, t).run_job(job.id))
    c = database.counts(job.id)
    assert c[D.SENT] == 2 and c[D.UNKNOWN] == 1
    run(make_engine(database, t).run_job(job.id))  # продолжить — ничего не меняет
    assert [x[0] for x in t.calls].count(2) == 1


def test_manual_retry_of_unknown_reuses_random_ids(database):
    job = database.create_job(ME, ME, [5, 6], TARGETS[:1])
    t = FakeTransport({1: ["deliver_then_drop", SendAlreadyDelivered("dup")]})
    run(make_engine(database, t).run_job(job.id))
    d = database.list_deliveries(job.id)[0]
    assert d.state == D.UNKNOWN
    assert database.resolve_unknown(d.id, sent=False, note="ручной повтор")
    database.reopen_job_if_finished(job.id)
    run(make_engine(database, t).run_job(job.id))
    assert t.calls[0][1] == t.calls[1][1]  # те же random_id -> Telegram распознал дубль
    assert database.get_delivery(d.id).state == D.SENT
    assert len(t.delivered) == 1


def test_crash_recovery_marks_sending_as_unknown(database):
    job = database.create_job(ME, ME, [5], TARGETS)
    database.try_start_job(job.id)
    d = database.list_deliveries(job.id)[0]
    database.reserve(d.id, 1)  # «отключили питание» сразу после резервации
    assert database.recover_after_crash() == (1, 1)
    assert database.get_delivery(d.id).state == D.UNKNOWN
    assert database.get_job(job.id).state == D.JOB_PAUSED
    t = FakeTransport()
    run(make_engine(database, t).run_job(job.id))
    assert sorted(x[0] for x in t.calls) == [2, 3]


def test_rejected_and_flood_wait(database):
    job = database.create_job(ME, ME, [5], TARGETS)
    t = FakeTransport({1: [SendRejected("нет прав")], 2: [SendFloodWait(5)]})
    assert run(make_engine(database, t).run_job(job.id)) == D.JOB_COMPLETED
    states = {d.target_peer_id: d.state for d in database.list_deliveries(job.id)}
    assert states == {1: D.FAILED, 2: D.SENT, 3: D.SENT}
    assert [x[0] for x in t.calls].count(2) == 2  # после FloodWait — повтор (запрос не выполнялся)
    assert database.retry_failed(job.id) == 1
    run(make_engine(database, t).run_job(job.id))
    assert database.counts(job.id)[D.SENT] == 3


def test_long_flood_wait_pauses_job(database):
    job = database.create_job(ME, ME, [5], TARGETS)
    t = FakeTransport({1: [SendFloodWait(10_000)]})
    assert run(make_engine(database, t).run_job(job.id)) == D.JOB_PAUSED
    assert database.counts(job.id)[D.PENDING] == 3


def test_blocked_pauses_job(database):
    job = database.create_job(ME, ME, [5], TARGETS)
    t = FakeTransport({1: [SendBlocked("PEER_FLOOD")]})
    assert run(make_engine(database, t).run_job(job.id)) == D.JOB_PAUSED
    assert len(t.calls) == 1


def test_cancel_only_affects_pending(database):
    job = database.create_job(ME, ME, [5], TARGETS)
    db_ = database

    class CancelAfterFirst(FakeTransport):
        async def send(self, *a):
            r = await super().send(*a)
            db_.cancel_remaining(job.id)
            return r

    t = CancelAfterFirst()
    assert run(make_engine(database, t).run_job(job.id)) == D.JOB_CANCELLED
    c = database.counts(job.id)
    assert c[D.SENT] == 1 and c[D.CANCELLED] == 2 and len(t.calls) == 1


def test_pause_and_resume(database):
    job = database.create_job(ME, ME, [5], TARGETS)

    async def scenario():
        holder = {}

        class PauseAfterFirst(FakeTransport):
            async def send(self, *a):
                r = await super().send(*a)
                holder["engine"].request_pause(job.id)
                return r

        t = PauseAfterFirst()
        eng = make_engine(database, t)
        holder["engine"] = eng
        assert await eng.run_job(job.id) == D.JOB_PAUSED
        assert database.counts(job.id)[D.SENT] == 1
        eng2 = make_engine(database, FakeTransport())
        assert await eng2.run_job(job.id) == D.JOB_COMPLETED
    run(scenario())
    assert database.counts(job.id)[D.SENT] == 3


def test_concurrent_run_is_rejected(database):
    j1 = database.create_job(ME, ME, [5], TARGETS)

    async def scenario():
        gate = asyncio.Event()

        class Slow(FakeTransport):
            async def send(self, *a):
                await gate.wait()
                return await super().send(*a)

        t = Slow()
        eng = make_engine(database, t)
        first = asyncio.ensure_future(eng.run_job(j1.id))
        await asyncio.sleep(0.01)
        second = await eng.run_job(j1.id)  # двойной клик «Продолжить»
        gate.set()
        return await first, second, t
    first, second, t = run(scenario())
    assert first == D.JOB_COMPLETED and second == "busy"
    assert len(t.calls) == 3


def test_unexpected_exception_becomes_unknown(database):
    job = database.create_job(ME, ME, [5], TARGETS[:1])
    t = FakeTransport({1: [RuntimeError("boom")]})
    run(make_engine(database, t).run_job(job.id))
    assert database.list_deliveries(job.id)[0].state == D.UNKNOWN


def test_chat_selection_persists(tmp_path):
    path = str(tmp_path / "p.sqlite3")
    d1 = D.Database(path)
    d1.upsert_chats([{"peer_id": 1, "title": "A", "kind": "Группа", "can_send": True},
                     {"peer_id": 2, "title": "B", "kind": "Канал", "can_send": False}])
    d1.set_chat_flags([1, 2], selected=True)
    d1.close()
    d2 = D.Database(path)
    d2.upsert_chats([{"peer_id": 1, "title": "A2", "kind": "Группа", "can_send": True}])
    assert [r["peer_id"] for r in d2.selected_targets()] == [1]  # без права отправки — не цель
    d2.set_chat_flags([1], excluded=True)
    assert d2.selected_targets() == []
    d2.close()


def test_detected_dedup(database):
    assert database.record_detected(ME, ME, [7, 8], "x") is not None
    assert database.record_detected(ME, ME, [8, 7], "x") is None


def test_log_redaction():
    rec = logging.LogRecord("x", logging.INFO, "", 0, "phone +7 999 123-45-67 code=12345 hash %s",
                            ("0123456789abcdef0123456789abcdef",), None)
    RedactingFilter().filter(rec)
    msg = rec.getMessage()
    assert "999" not in msg and "12345" not in msg and "0123456789abcdef" not in msg


def test_random_ids_persisted_before_send(database):
    job = database.create_job(ME, ME, [5, 6, 7], TARGETS[:1])
    d = database.list_deliveries(job.id)[0]
    r, _ = database.reserve(d.id, 3)
    stored = json.loads(database._query("SELECT random_ids FROM deliveries WHERE id=?", (d.id,))[0][0])
    assert stored == r.random_ids and len(set(stored)) == 3
