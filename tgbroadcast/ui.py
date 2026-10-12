"""Графический интерфейс на Tkinter.

Telethon работает в отдельном потоке со своим циклом asyncio, поэтому окно
никогда не «зависает» во время рассылки. Результаты возвращаются в главный
поток через очередь, которую интерфейс опрашивает каждые 100 мс.
"""
from __future__ import annotations

import asyncio
import io
import logging
import os
import platform
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from concurrent.futures import Future
from tkinter import messagebox, simpledialog, ttk
from typing import Any, Callable, Optional

from . import __version__
from . import db as D
from .engine import BroadcastEngine, EngineSettings
from .infra import Config

log = logging.getLogger(__name__)

try:
    from PIL import Image, ImageTk
except ImportError:  # предпросмотр изображений просто не показывается
    Image = ImageTk = None


def fmt_time(ts: Optional[float]) -> str:
    return time.strftime("%d.%m.%Y %H:%M:%S", time.localtime(ts)) if ts else ""


def open_path(path: str) -> None:
    if sys.platform == "win32":
        os.startfile(path)  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])


class AsyncRunner:
    """Цикл asyncio в фоновом потоке."""

    def __init__(self, ui_queue: "queue.Queue"):
        self.loop = asyncio.new_event_loop()
        self.ui_queue = ui_queue
        self.thread = threading.Thread(target=self._run, name="telegram-loop", daemon=True)
        self.thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def call(self, coro, on_ok: Optional[Callable[[Any], None]] = None,
             on_err: Optional[Callable[[BaseException], None]] = None) -> Future:
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        fut.add_done_callback(lambda f: self.ui_queue.put(("callback", f, on_ok, on_err)))
        return fut


def make_photo(data: Optional[bytes], max_size=(320, 320)):
    if not data or Image is None:
        return None
    try:
        img = Image.open(io.BytesIO(data))
        img.thumbnail(max_size)
        return ImageTk.PhotoImage(img)
    except Exception:
        log.warning("Не удалось показать изображение", exc_info=True)
        return None


class ConfirmDialog(tk.Toplevel):
    """Предпросмотр сообщения и список получателей перед рассылкой."""

    def __init__(self, master: "App", preview, targets: list, rerun: bool = False):
        super().__init__(master)
        self.result = False
        self.title("Подтверждение рассылки")
        self.transient(master)
        self.attributes("-topmost", True)
        self.resizable(True, True)
        pad = {"padx": 10, "pady": 4}

        head = "ПОВТОРНАЯ рассылка уже отправленного сообщения" if rerun else "Новое сообщение для рассылки"
        ttk.Label(self, text=head, font=("Segoe UI", 12, "bold"),
                  foreground="#b00020" if rerun else "").pack(anchor="w", **pad)

        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, **pad)
        left = ttk.Frame(body)
        left.pack(side="left", fill="both", expand=True)
        self._img = make_photo(preview.thumb)
        if self._img:
            ttk.Label(left, image=self._img).pack(anchor="w")
        ttk.Label(left, text="Тип: " + ", ".join(preview.kinds) +
                  ("  •  с форматированием" if preview.has_formatting else "")).pack(anchor="w")
        txt = tk.Text(left, height=8, width=50, wrap="word")
        txt.insert("1.0", preview.text or "(без текста)")
        txt.configure(state="disabled")
        txt.pack(fill="both", expand=True, pady=4)
        for w in preview.warnings:
            ttk.Label(left, text="⚠ " + w, foreground="#9a6700", wraplength=420).pack(anchor="w")
        for b in preview.blockers:
            ttk.Label(left, text="⛔ " + b, foreground="#b00020", wraplength=420).pack(anchor="w")

        right = ttk.Frame(body)
        right.pack(side="left", fill="both", expand=True, padx=(10, 0))
        ttk.Label(right, text=f"Получатели: {len(targets)}", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        lb = tk.Listbox(right, height=16, width=40)
        for t in targets:
            lb.insert("end", f"{t['title']}  ({t['kind']})")
        lb.pack(fill="both", expand=True)

        btns = ttk.Frame(self)
        btns.pack(fill="x", **pad)
        self.start_btn = ttk.Button(btns, text="Начать рассылку", command=self._start)
        self.start_btn.pack(side="right", padx=4)
        ttk.Button(btns, text="Отмена", command=self.destroy).pack(side="right")
        if preview.blockers or not targets:
            self.start_btn.state(["disabled"])
            if not targets:
                ttk.Label(btns, text="Выберите чаты на вкладке «Чаты».", foreground="#b00020").pack(side="left")
        self.bind("<Escape>", lambda e: self.destroy())
        self.grab_set()
        self.focus_force()

    def _start(self):
        self.start_btn.state(["disabled"])  # защита от двойного нажатия
        self.result = True
        self.destroy()


class App(tk.Tk):
    def __init__(self, data_path, database: D.Database, config: Config, log_path,
                 recovered: tuple[int, int]):
        super().__init__()
        self.data_path = data_path
        self.db = database
        self.config_store = config
        self.log_path = log_path
        self.ui_queue: "queue.Queue" = queue.Queue()
        self.runner = AsyncRunner(self.ui_queue)
        self.service = None
        self.engine: Optional[BroadcastEngine] = None
        self.last_preview = None
        self._confirm_open = False
        self._photo_ref = None

        self.title(f"TG Broadcast {__version__} — рассылка из «Избранного»")
        self.geometry("1100x720")
        self.minsize(900, 600)
        try:
            ttk.Style(self).theme_use("vista" if sys.platform == "win32" else "clam")
        except tk.TclError:
            pass

        self.status_var = tk.StringVar(value="Не подключено")
        self.account_var = tk.StringVar(value="Аккаунт: —")
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True)
        self._build_home()
        self._build_chats()
        self._build_jobs()
        self._build_history()
        self._build_settings()
        bar = ttk.Frame(self)
        bar.pack(fill="x")
        ttk.Label(bar, textvariable=self.status_var).pack(side="left", padx=8, pady=2)

        if recovered[0] or recovered[1]:
            self.after(500, lambda: messagebox.showwarning(
                "Восстановление после сбоя",
                f"Программа была закрыта во время рассылки.\n\n"
                f"Отправок с неизвестным результатом: {recovered[0]}\n"
                f"Заданий поставлено на паузу: {recovered[1]}\n\n"
                "Они НЕ будут отправлены повторно автоматически. Откройте вкладку «Рассылки», "
                "чтобы проверить их или вручную разрешить повторную отправку.", parent=self))

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(100, self._poll_queue)
        self.after(1500, self._periodic_refresh)
        self.refresh_chats()
        self.refresh_jobs()
        self.after(200, self.start_telegram)

    # ================================================================ общая часть
    def _poll_queue(self):
        try:
            while True:
                item = self.ui_queue.get_nowait()
                self._handle(item)
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)

    def _handle(self, item):
        kind = item[0]
        if kind == "callback":
            _, fut, on_ok, on_err = item
            exc = fut.exception()
            if exc is None:
                if on_ok:
                    on_ok(fut.result())
            elif on_err:
                on_err(exc)
            else:
                log.error("Ошибка фоновой операции: %s", exc)
                messagebox.showerror("Ошибка", str(exc), parent=self)
        elif kind == "detected":
            self._on_detected(item[1])
        elif kind == "job":
            _, job_id, text = item
            self._append_job_log(job_id, text)
            self.refresh_jobs()

    def _service_event(self, name: str, payload: dict):
        """Вызывается из потока Telegram."""
        self.ui_queue.put((name, payload))

    def _engine_progress(self, job_id: int, text: str):
        """Вызывается из потока Telegram."""
        self.ui_queue.put(("job", job_id, text))

    def set_status(self, text: str):
        self.status_var.set(text)
        self.home_status.configure(text=text)

    # ================================================================= Telegram
    def start_telegram(self):
        if not self.config_store.has_api():
            self.nb.select(self.tab_settings)
            if not self.ask_api_credentials():
                self.set_status("Укажите API ID и API Hash на вкладке «Настройки»")
                return
        from .telegram_service import TelegramService
        if self.service is not None:
            self.runner.call(self.service.disconnect())
        self.service = TelegramService(str(self.data_path / "account"), self.config_store["api_id"],
                                       self.config_store["api_hash"], self.db, self._service_event)
        self.engine = BroadcastEngine(self.db, self.service, self._engine_settings(),
                                      on_progress=self._engine_progress)
        self.set_status("Подключение к Telegram…")
        self.runner.call(self.service.connect(), self._on_connected, self._on_connect_error)

    def _engine_settings(self) -> EngineSettings:
        return EngineSettings(delay_between_sends=float(self.config_store["delay_between_sends"]),
                              max_flood_wait=int(self.config_store["max_flood_wait"]))

    def _on_connect_error(self, exc):
        self.set_status(f"Нет подключения: {exc}")
        log.warning("Ошибка подключения: %s", exc)
        self.after(15000, self._reconnect_if_needed)

    def _reconnect_if_needed(self):
        if self.service is not None and not self.service.is_connected():
            self.runner.call(self.service.connect(), self._on_connected, self._on_connect_error)

    def _on_connected(self, authorized: bool):
        if authorized:
            self.runner.call(self.service.after_login(), self._on_logged_in)
        else:
            self.set_status("Требуется вход в аккаунт Telegram")
            self.login_flow()

    def login_flow(self):
        phone = simpledialog.askstring("Вход в Telegram",
                                       "Номер телефона в международном формате (например, +79991234567):",
                                       parent=self)
        if not phone:
            self.set_status("Вход отменён. Нажмите «Войти» на вкладке «Настройки».")
            return
        self.set_status("Отправка кода…")
        self.runner.call(self.service.send_code(phone), lambda _: self._ask_code(),
                         lambda e: self._login_error(e))

    def _ask_code(self):
        code = simpledialog.askstring("Код подтверждения",
                                      "Введите код, который пришёл в Telegram (или по SMS):", parent=self)
        if not code:
            self.set_status("Вход отменён")
            return
        self.runner.call(self.service.sign_in_code(code), self._after_code, self._login_error)

    def _after_code(self, result: str):
        if result == "password":
            pw = simpledialog.askstring("Двухэтапная аутентификация", "Пароль облачной защиты Telegram:",
                                        show="•", parent=self)
            if not pw:
                self.set_status("Вход отменён")
                return
            self.runner.call(self.service.sign_in_password(pw),
                             lambda _: self.runner.call(self.service.after_login(), self._on_logged_in),
                             self._login_error)
        else:
            self.runner.call(self.service.after_login(), self._on_logged_in)

    def _login_error(self, exc):
        log.warning("Ошибка входа: %s", type(exc).__name__)  # без кода и пароля
        messagebox.showerror("Ошибка входа", f"Не удалось войти: {exc}", parent=self)
        self.set_status("Ошибка входа. Нажмите «Войти» на вкладке «Настройки».")

    def _on_logged_in(self, me):
        name = " ".join(filter(None, [me.first_name, me.last_name])) or me.username or str(me.id)
        self.account_var.set(f"Аккаунт: {name}")
        self.set_status("Подключено. Ожидание новых сообщений в «Избранном»…")
        self.reload_dialogs()
        self.refresh_latest()
        self.runner.call(self.service.check_missed_on_start())

    def logout(self):
        if self.service is None:
            return
        if self.engine and self.engine.is_running():
            messagebox.showwarning("Выход", "Сначала дождитесь завершения или поставьте рассылку на паузу.",
                                   parent=self)
            return
        if not messagebox.askyesno(
                "Выход из аккаунта",
                "Выйти из аккаунта Telegram?\n\nЛокальная сессия будет удалена, для следующего запуска "
                "потребуется снова войти по коду. История рассылок сохранится.", parent=self):
            return

        def done(_):
            self.account_var.set("Аккаунт: —")
            self.set_status("Вы вышли из аккаунта")
        self.runner.call(self.service.logout(), done)

    # ==================================================================== Главная
    def _build_home(self):
        tab = ttk.Frame(self.nb, padding=12)
        self.nb.add(tab, text="Главная")
        ttk.Label(tab, textvariable=self.account_var, font=("Segoe UI", 11, "bold")).pack(anchor="w")
        self.home_status = ttk.Label(tab, text="Не подключено")
        self.home_status.pack(anchor="w", pady=(0, 10))
        ttk.Label(tab, text=("Как пользоваться: отправьте сообщение в «Избранное» — программа покажет "
                             "предпросмотр. Рассылка начнётся только после нажатия «Начать рассылку»."),
                  wraplength=900).pack(anchor="w", pady=(0, 10))
        box = ttk.LabelFrame(tab, text="Последнее сообщение в «Избранном»", padding=8)
        box.pack(fill="both", expand=True)
        self.latest_img = ttk.Label(box)
        self.latest_img.pack(side="left", anchor="n", padx=(0, 10))
        right = ttk.Frame(box)
        right.pack(side="left", fill="both", expand=True)
        self.latest_kind = ttk.Label(right, text="—")
        self.latest_kind.pack(anchor="w")
        self.latest_text = tk.Text(right, height=10, wrap="word", state="disabled")
        self.latest_text.pack(fill="both", expand=True, pady=4)
        row = ttk.Frame(right)
        row.pack(fill="x")
        ttk.Button(row, text="Обновить", command=self.refresh_latest).pack(side="left")
        ttk.Button(row, text="Разослать это сообщение…",
                   command=lambda: self.open_confirm(self.last_preview)).pack(side="left", padx=6)

    def refresh_latest(self):
        if self.service is None or self.service.me is None:
            return

        async def job():
            msgs = await self.service.latest_saved()
            return await self.service.build_preview(msgs) if msgs else None
        self.runner.call(job(), self._show_latest)

    def _show_latest(self, preview):
        self.last_preview = preview
        self.latest_text.configure(state="normal")
        self.latest_text.delete("1.0", "end")
        if preview is None:
            self.latest_kind.configure(text="«Избранное» пусто")
        else:
            self.latest_kind.configure(text="Тип: " + ", ".join(preview.kinds))
            self.latest_text.insert("1.0", preview.text or "(без текста)")
        self.latest_text.configure(state="disabled")
        self._photo_ref = make_photo(preview.thumb, (240, 240)) if preview else None
        self.latest_img.configure(image=self._photo_ref or "")

    def _on_detected(self, payload: dict):
        preview = payload["preview"]
        self._show_latest(preview)
        self.bell()
        if self.state() == "iconic":
            self.deiconify()
        self.lift()
        if self.config_store["auto_show_confirmation"]:
            self.open_confirm(preview, detected_id=payload["detected_id"])

    # ------------------------------------------------------------ подтверждение
    def current_targets(self) -> list:
        me = self.service.account_id if self.service and self.service.me else None
        return [dict(r) for r in self.db.selected_targets([me] if me else [])]

    def open_confirm(self, preview, detected_id: Optional[int] = None, rerun: bool = False):
        if preview is None:
            messagebox.showinfo("Нет сообщения", "В «Избранном» нет сообщения для рассылки.", parent=self)
            return
        if self.service is None or self.service.me is None:
            messagebox.showwarning("Нет подключения", "Сначала войдите в аккаунт Telegram.", parent=self)
            return
        if self._confirm_open:
            return
        self._confirm_open = True
        targets = self.current_targets()
        dlg = ConfirmDialog(self, preview, targets, rerun=rerun)
        self.wait_window(dlg)
        self._confirm_open = False
        if not dlg.result:
            if detected_id:
                self.db.set_detected_status(detected_id, "dismissed")
            return
        self.create_and_run(preview, targets, confirm_rerun=rerun, detected_id=detected_id)

    def create_and_run(self, preview, targets, confirm_rerun=False, detected_id=None):
        acc = self.service.account_id
        try:
            job = self.db.create_job(acc, acc, preview.message_ids,
                                     [(t["peer_id"], t["title"]) for t in targets],
                                     preview_text=preview.summary, confirm_rerun=confirm_rerun)
        except D.JobAlreadyExists as e:
            old = e.job
            counts = self.db.counts(old.id)
            if old.state in (D.JOB_RUNNING, D.JOB_READY, D.JOB_PAUSED):
                messagebox.showinfo("Рассылка уже существует",
                                    f"Для этого сообщения уже есть рассылка #{old.id} "
                                    f"({D.JOB_STATES_RU[old.state]}). Управляйте ею на вкладке «Рассылки».",
                                    parent=self)
                self.select_job(old.id)
                return
            if not messagebox.askyesno(
                    "Сообщение уже рассылалось",
                    f"Это сообщение уже рассылалось (рассылка #{old.id}, "
                    f"успешно: {counts[D.SENT]}, результат неизвестен: {counts[D.UNKNOWN]}).\n\n"
                    "Отправить его ПОВТОРНО во все выбранные чаты, включая те, куда оно уже "
                    "доставлено?\n(Чаты с неизвестным результатом всё равно будут пропущены.)",
                    icon="warning", parent=self):
                return
            return self.create_and_run(preview, targets, confirm_rerun=True, detected_id=detected_id)
        except ValueError as e:
            messagebox.showwarning("Рассылка", str(e), parent=self)
            return
        if detected_id:
            self.db.set_detected_status(detected_id, "queued")
        self.nb.select(self.tab_jobs)
        self.refresh_jobs()
        self.select_job(job.id)
        self.run_job(job.id)

    def run_job(self, job_id: int):
        if self.engine is None:
            return
        if self.engine.is_running():
            messagebox.showinfo("Рассылка", "Сейчас выполняется другая рассылка. Дождитесь её завершения "
                                "или поставьте на паузу.", parent=self)
            return
        self.engine.settings = self._engine_settings()
        self.runner.call(self.engine.run_job(job_id), lambda state: self.refresh_jobs())
        self.refresh_jobs()

    # ====================================================================== Чаты
    def _build_chats(self):
        tab = ttk.Frame(self.nb, padding=8)
        self.nb.add(tab, text="Чаты")
        top = ttk.Frame(tab)
        top.pack(fill="x")
        ttk.Label(top, text="Поиск:").pack(side="left")
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *a: self.refresh_chats())
        ttk.Entry(top, textvariable=self.search_var, width=30).pack(side="left", padx=4)
        self.show_readonly = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="Показывать чаты без права отправки", variable=self.show_readonly,
                        command=self.refresh_chats).pack(side="left", padx=8)
        ttk.Button(top, text="Обновить список", command=self.reload_dialogs).pack(side="right")

        cols = ("sel", "title", "kind", "state")
        self.chat_tree = ttk.Treeview(tab, columns=cols, show="headings", selectmode="extended")
        for c, t, w in (("sel", "✓", 40), ("title", "Название", 420), ("kind", "Тип", 140),
                        ("state", "Статус", 220)):
            self.chat_tree.heading(c, text=t)
            self.chat_tree.column(c, width=w, anchor="center" if c == "sel" else "w")
        sb = ttk.Scrollbar(tab, orient="vertical", command=self.chat_tree.yview)
        self.chat_tree.configure(yscrollcommand=sb.set)
        self.chat_tree.pack(side="left", fill="both", expand=True, pady=6)
        sb.pack(side="left", fill="y", pady=6)
        self.chat_tree.bind("<Button-1>", self._chat_click)
        self.chat_tree.bind("<space>", lambda e: self._toggle_selected())

        side = ttk.Frame(tab, padding=(8, 6))
        side.pack(side="left", fill="y")
        self.chat_count = ttk.Label(side, text="Выбрано: 0")
        self.chat_count.pack(anchor="w", pady=(0, 8))
        for text, cmd in (("Выбрать все (видимые)", lambda: self._bulk_select(True)),
                          ("Снять выделение", lambda: self._bulk_select(False)),
                          ("Отметить/снять выделенные", self._toggle_selected),
                          ("Исключить выделенные", lambda: self._set_excluded(True)),
                          ("Вернуть исключённые", lambda: self._set_excluded(False))):
            ttk.Button(side, text=text, command=cmd, width=28).pack(anchor="w", pady=2)
        ttk.Label(side, text=("Щелчок по колонке ✓ отмечает чат.\nИсключённые чаты не получат "
                              "рассылку, даже если отмечены.\nВыбор сохраняется автоматически."),
                  wraplength=220, foreground="#555").pack(anchor="w", pady=10)

    def reload_dialogs(self):
        if self.service is None or self.service.me is None:
            return
        self.set_status("Загрузка списка чатов…")

        def done(_):
            self.refresh_chats()
            self.set_status("Подключено. Ожидание новых сообщений в «Избранном»…")
        self.runner.call(self.service.load_dialogs(), done)

    def refresh_chats(self):
        q = self.search_var.get().strip().lower() if hasattr(self, "search_var") else ""
        tree = self.chat_tree
        tree.delete(*tree.get_children())
        selected = 0
        for r in self.db.list_chats():
            if r["selected"] and not r["excluded"] and r["can_send"]:
                selected += 1
            if not r["can_send"] and not self.show_readonly.get():
                continue
            if q and q not in r["title"].lower() and q not in (r["username"] or "").lower():
                continue
            state = "нет права отправки" if not r["can_send"] else ("исключён" if r["excluded"] else "")
            tree.insert("", "end", iid=str(r["peer_id"]),
                        values=("☑" if r["selected"] else "☐", r["title"], r["kind"], state))
        self.chat_count.configure(text=f"Выбрано для рассылки: {selected}")

    def _chat_click(self, event):
        if self.chat_tree.identify_column(event.x) == "#1" and self.chat_tree.identify_region(
                event.x, event.y) == "cell":
            iid = self.chat_tree.identify_row(event.y)
            if iid:
                cur = self.chat_tree.set(iid, "sel") == "☑"
                self.db.set_chat_flags([int(iid)], selected=not cur)
                self.refresh_chats()
                return "break"

    def _toggle_selected(self):
        for iid in self.chat_tree.selection():
            cur = self.chat_tree.set(iid, "sel") == "☑"
            self.db.set_chat_flags([int(iid)], selected=not cur)
        self.refresh_chats()

    def _bulk_select(self, value: bool):
        ids = [int(i) for i in self.chat_tree.get_children()]
        if value:
            ids = [i for i in ids if self.chat_tree.set(str(i), "state") == ""]
        else:
            ids = [r["peer_id"] for r in self.db.list_chats()]
        self.db.set_chat_flags(ids, selected=value)
        self.refresh_chats()

    def _set_excluded(self, value: bool):
        ids = [int(i) for i in self.chat_tree.selection()]
        if not value and not ids:
            ids = [r["peer_id"] for r in self.db.list_chats() if r["excluded"]]
        self.db.set_chat_flags(ids, excluded=value)
        self.refresh_chats()

    # ================================================================= Рассылки
    def _build_jobs(self):
        tab = ttk.Frame(self.nb, padding=8)
        self.tab_jobs = tab
        self.nb.add(tab, text="Рассылки")
        cols = ("id", "created", "msg", "state", "progress")
        self.job_tree = ttk.Treeview(tab, columns=cols, show="headings", height=8, selectmode="browse")
        for c, t, w in (("id", "№", 50), ("created", "Создано", 150), ("msg", "Сообщение", 480),
                        ("state", "Состояние", 140), ("progress", "Успешно/Всего", 120)):
            self.job_tree.heading(c, text=t)
            self.job_tree.column(c, width=w)
        self.job_tree.pack(fill="x")
        self.job_tree.bind("<<TreeviewSelect>>", lambda e: self.refresh_job_detail())

        det = ttk.LabelFrame(tab, text="Выбранная рассылка", padding=8)
        det.pack(fill="both", expand=True, pady=8)
        self.progress = ttk.Progressbar(det, mode="determinate")
        self.progress.pack(fill="x")
        cnt = ttk.Frame(det)
        cnt.pack(fill="x", pady=6)
        self.counter_labels = {}
        for key, title, color in ((D.SENT, "Успешно", "#1a7f37"), (D.FAILED, "Ошибка", "#b00020"),
                                  (D.PENDING, "Ожидают отправки", "#555"),
                                  (D.UNKNOWN, "Результат неизвестен", "#9a6700"),
                                  (D.SKIPPED, "Пропущено", "#555"), (D.CANCELLED, "Отменено", "#555")):
            lbl = ttk.Label(cnt, text=f"{title}: 0", foreground=color, font=("Segoe UI", 10, "bold"))
            lbl.pack(side="left", padx=(0, 18))
            self.counter_labels[key] = (lbl, title)

        btns = ttk.Frame(det)
        btns.pack(fill="x")
        self.job_buttons = {}
        for key, text, cmd in (("pause", "Пауза", self._job_pause),
                               ("resume", "Продолжить", self._job_resume),
                               ("cancel", "Отменить оставшиеся отправки", self._job_cancel),
                               ("retry", "Повторить ошибочные", self._job_retry_failed),
                               ("verify", "Проверить «неизвестные»", self._job_verify_unknown),
                               ("allow", "Разрешить повтор «неизвестных»…", self._job_allow_unknown),
                               ("rerun", "Запустить повторно…", self._job_rerun)):
            b = ttk.Button(btns, text=text, command=cmd)
            b.pack(side="left", padx=2, pady=2)
            self.job_buttons[key] = b
        self.job_log = tk.Text(det, height=10, wrap="word", state="disabled")
        self.job_log.pack(fill="both", expand=True, pady=6)

    def selected_job_id(self) -> Optional[int]:
        sel = self.job_tree.selection()
        return int(sel[0]) if sel else None

    def select_job(self, job_id: int):
        if self.job_tree.exists(str(job_id)):
            self.job_tree.selection_set(str(job_id))
            self.job_tree.see(str(job_id))

    def refresh_jobs(self):
        keep = self.selected_job_id()
        self.job_tree.delete(*self.job_tree.get_children())
        for job in self.db.list_jobs():
            c = self.db.counts(job.id)
            total = sum(c.values())
            prefix = f"[повтор {job.run_no}] " if job.run_no > 1 else ""
            self.job_tree.insert("", "end", iid=str(job.id), values=(
                job.id, fmt_time(job.created_at), prefix + job.preview_text,
                D.JOB_STATES_RU.get(job.state, job.state), f"{c[D.SENT]}/{total}"))
        if keep and self.job_tree.exists(str(keep)):
            self.job_tree.selection_set(str(keep))
        self.refresh_job_detail()
        self.refresh_history()

    def refresh_job_detail(self):
        job_id = self.selected_job_id()
        if job_id is None:
            for b in self.job_buttons.values():
                b.state(["disabled"])
            return
        job = self.db.get_job(job_id)
        c = self.db.counts(job_id)
        total = sum(c.values()) or 1
        done = total - c[D.PENDING] - c[D.SENDING]
        self.progress.configure(maximum=total, value=done)
        for key, (lbl, title) in self.counter_labels.items():
            lbl.configure(text=f"{title}: {c[key] + (c[D.SENDING] if key == D.PENDING else 0)}")
        running = self.engine is not None and self.engine.is_running(job_id)
        enable = {
            "pause": running,
            "resume": not running and job.state in (D.JOB_PAUSED, D.JOB_READY) and c[D.PENDING] > 0,
            "cancel": c[D.PENDING] > 0,
            "retry": not running and c[D.FAILED] > 0,
            "verify": not running and c[D.UNKNOWN] > 0,
            "allow": not running and c[D.UNKNOWN] > 0,
            "rerun": not running and job.state in (D.JOB_COMPLETED, D.JOB_CANCELLED),
        }
        for k, b in self.job_buttons.items():
            b.state(["!disabled"] if enable[k] else ["disabled"])

    def _append_job_log(self, job_id: int, text: str):
        self.job_log.configure(state="normal")
        self.job_log.insert("end", f"{time.strftime('%H:%M:%S')}  #{job_id}: {text}\n")
        self.job_log.see("end")
        self.job_log.configure(state="disabled")

    def _job_pause(self):
        job_id = self.selected_job_id()
        if job_id and self.engine:
            self.engine.request_pause(job_id)
            self._append_job_log(job_id, "Пауза запрошена (текущая отправка завершится)")

    def _job_resume(self):
        job_id = self.selected_job_id()
        if job_id:
            self.run_job(job_id)

    def _job_cancel(self):
        job_id = self.selected_job_id()
        if job_id and messagebox.askyesno("Отмена", "Отменить все ещё не начатые отправки этой рассылки?\n"
                                          "Уже отправленные сообщения останутся.", parent=self):
            if self.engine:
                self.engine.cancel_remaining(job_id)
            else:
                self.db.cancel_remaining(job_id)
            self.refresh_jobs()

    def _job_retry_failed(self):
        job_id = self.selected_job_id()
        if job_id and messagebox.askyesno("Повтор", "Повторить отправку в чаты, где Telegram отклонил "
                                          "сообщение? (В эти чаты сообщение точно не было доставлено.)",
                                          parent=self):
            self.db.retry_failed(job_id)
            self.run_job(job_id)

    def _job_verify_unknown(self):
        job_id = self.selected_job_id()
        if not job_id or self.service is None:
            return
        job = self.db.get_job(job_id)
        unknown = [d for d in self.db.list_deliveries(job_id) if d.state == D.UNKNOWN]

        async def verify_all():
            report = []
            for d in unknown:
                try:
                    found = await self.service.verify_delivery(d, job)
                except Exception as e:
                    report.append(f"«{d.target_title}»: проверить не удалось ({e})")
                    continue
                if found:
                    self.db.resolve_unknown(d.id, sent=True, sent_ids=found,
                                            note="Найдено в чате при проверке")
                    report.append(f"«{d.target_title}»: найдено в чате — отмечено как отправленное")
                else:
                    report.append(f"«{d.target_title}»: копия не найдена. Можно вручную разрешить повтор.")
            return report

        def done(report):
            for line in report:
                self._append_job_log(job_id, line)
            self.refresh_jobs()
            messagebox.showinfo("Проверка", "\n".join(report) or "Нечего проверять", parent=self)
        self.runner.call(verify_all(), done)

    def _job_allow_unknown(self):
        job_id = self.selected_job_id()
        if not job_id:
            return
        unknown = [d for d in self.db.list_deliveries(job_id) if d.state == D.UNKNOWN]
        names = "\n".join(f"• {d.target_title}" for d in unknown[:20])
        if not messagebox.askyesno(
                "Разрешить повторную отправку",
                "Для этих чатов неизвестно, дошло ли сообщение:\n" + names +
                "\n\nСначала рекомендуется нажать «Проверить «неизвестные»». Если вы подтвердите повтор, "
                "сообщение может появиться в чате ДВАЖДЫ (Telegram может распознать повтор, но это "
                "не гарантируется).\n\nРазрешить повторную отправку?", icon="warning", parent=self):
            return
        for d in unknown:
            self.db.resolve_unknown(d.id, sent=False, note="Повтор разрешён пользователем вручную")
        self.db.reopen_job_if_finished(job_id)
        self.run_job(job_id)

    def _job_rerun(self):
        job_id = self.selected_job_id()
        if not job_id or self.service is None:
            return
        job = self.db.get_job(job_id)
        if not messagebox.askyesno("Повторный запуск",
                                   "Эта рассылка уже выполнена. Запустить её ещё раз для текущего списка "
                                   "выбранных чатов? В чатах, где сообщение уже есть, оно появится повторно.",
                                   icon="warning", parent=self):
            return
        self.runner.call(self.service.preview_for_ids(job.message_ids),
                         lambda p: self.open_confirm(p, rerun=True))

    # ================================================================== История
    def _build_history(self):
        tab = ttk.Frame(self.nb, padding=8)
        self.nb.add(tab, text="История")
        top = ttk.Frame(tab)
        top.pack(fill="x")
        ttk.Label(top, text="Фильтр по состоянию:").pack(side="left")
        self.hist_state = ttk.Combobox(top, state="readonly", width=28,
                                       values=["Все"] + list(D.DELIVERY_STATES_RU.values()))
        self.hist_state.current(0)
        self.hist_state.bind("<<ComboboxSelected>>", lambda e: self.refresh_history())
        self.hist_state.pack(side="left", padx=4)
        ttk.Button(top, text="Обновить", command=self.refresh_history).pack(side="left", padx=4)
        ttk.Label(top, text="Двойной щелчок — подробности по отправке", foreground="#555").pack(side="right")
        cols = ("job", "chat", "state", "attempts", "time", "error")
        self.hist_tree = ttk.Treeview(tab, columns=cols, show="headings")
        for c, t, w in (("job", "Рассылка", 70), ("chat", "Чат", 260), ("state", "Результат", 190),
                        ("attempts", "Попыток", 70), ("time", "Время", 150), ("error", "Комментарий", 330)):
            self.hist_tree.heading(c, text=t)
            self.hist_tree.column(c, width=w)
        sb = ttk.Scrollbar(tab, orient="vertical", command=self.hist_tree.yview)
        self.hist_tree.configure(yscrollcommand=sb.set)
        self.hist_tree.pack(side="left", fill="both", expand=True, pady=6)
        sb.pack(side="left", fill="y", pady=6)
        self.hist_tree.bind("<Double-1>", self._history_details)

    def refresh_history(self):
        if not hasattr(self, "hist_tree"):
            return
        want = self.hist_state.get()
        self.hist_tree.delete(*self.hist_tree.get_children())
        for d in self.db.list_deliveries():
            label = D.DELIVERY_STATES_RU.get(d.state, d.state)
            if want != "Все" and label != want:
                continue
            self.hist_tree.insert("", "end", iid=str(d.id), values=(
                f"#{d.job_id}", d.target_title, label, d.attempts,
                fmt_time(d.finished_at or d.started_at or d.updated_at), d.error or ""))

    def _history_details(self, _event):
        sel = self.hist_tree.selection()
        if not sel:
            return
        d = self.db.get_delivery(int(sel[0]))
        lines = [f"Чат: {d.target_title} (id {d.target_peer_id})", f"Ключ отправки: {d.delivery_key}",
                 f"Состояние: {D.DELIVERY_STATES_RU.get(d.state, d.state)}",
                 f"ID отправленных сообщений: {d.sent_message_ids or '—'}", "", "Журнал:"]
        for e in self.db.events(d.id):
            lines.append(f"{fmt_time(e['ts'])}  {e['from_state'] or ''} → {e['to_state']}  {e['note'] or ''}")
        messagebox.showinfo("Подробности отправки", "\n".join(lines), parent=self)

    # ================================================================= Настройки
    def _build_settings(self):
        tab = ttk.Frame(self.nb, padding=12)
        self.tab_settings = tab
        self.nb.add(tab, text="Настройки")
        auth = ttk.LabelFrame(tab, text="Авторизация", padding=8)
        auth.pack(fill="x")
        ttk.Label(auth, text=("API ID и API Hash выдаются на https://my.telegram.org → API development tools. "
                              "Они хранятся только на этом компьютере."), wraplength=900).grid(
            row=0, column=0, columnspan=4, sticky="w")
        ttk.Button(auth, text="Указать API ID / API Hash…", command=self._change_api).grid(
            row=1, column=0, pady=6, sticky="w")
        ttk.Button(auth, text="Войти / переподключиться", command=self.start_telegram).grid(
            row=1, column=1, padx=6)
        ttk.Button(auth, text="Выйти из аккаунта", command=self.logout).grid(row=1, column=2)

        par = ttk.LabelFrame(tab, text="Параметры", padding=8)
        par.pack(fill="x", pady=8)
        self.delay_var = tk.DoubleVar(value=float(self.config_store["delay_between_sends"]))
        self.flood_var = tk.IntVar(value=int(self.config_store["max_flood_wait"]))
        self.auto_var = tk.BooleanVar(value=bool(self.config_store["auto_show_confirmation"]))
        ttk.Label(par, text="Интервал между отправками в разные чаты, с:").grid(row=0, column=0, sticky="w")
        ttk.Spinbox(par, from_=1, to=120, increment=1, textvariable=self.delay_var, width=8).grid(
            row=0, column=1, sticky="w", padx=6)
        ttk.Label(par, text="Максимальное ожидание FloodWait (дольше — пауза), с:").grid(
            row=1, column=0, sticky="w")
        ttk.Spinbox(par, from_=10, to=3600, increment=10, textvariable=self.flood_var, width=8).grid(
            row=1, column=1, sticky="w", padx=6)
        ttk.Checkbutton(par, text="Сразу открывать окно подтверждения при новом сообщении",
                        variable=self.auto_var).grid(row=2, column=0, columnspan=2, sticky="w")
        ttk.Button(par, text="Сохранить параметры", command=self._save_params).grid(
            row=3, column=0, sticky="w", pady=6)

        diag = ttk.LabelFrame(tab, text="Диагностика", padding=8)
        diag.pack(fill="both", expand=True)
        row = ttk.Frame(diag)
        row.pack(fill="x")
        ttk.Button(row, text="Открыть папку данных", command=lambda: open_path(str(self.data_path))).pack(
            side="left")
        ttk.Button(row, text="Открыть журнал", command=lambda: open_path(str(self.log_path))).pack(
            side="left", padx=6)
        ttk.Button(row, text="Обновить", command=self._refresh_diag).pack(side="left")
        self.diag_text = tk.Text(diag, height=10, state="disabled")
        self.diag_text.pack(fill="both", expand=True, pady=6)
        self._refresh_diag()

    def _refresh_diag(self):
        try:
            import telethon
            tv = telethon.__version__
        except ImportError:
            tv = "не установлен"
        lines = [f"Версия программы: {__version__}", f"Python: {platform.python_version()}",
                 f"Telethon: {tv}", f"ОС: {platform.platform()}", f"Папка данных: {self.data_path}",
                 f"Журнал: {self.log_path}",
                 f"Подключение: {'есть' if self.service and self.service.is_connected() else 'нет'}",
                 f"Активная рассылка: {self.engine.current_job_id if self.engine else '—'}"]
        self.diag_text.configure(state="normal")
        self.diag_text.delete("1.0", "end")
        self.diag_text.insert("1.0", "\n".join(lines))
        self.diag_text.configure(state="disabled")

    def _save_params(self):
        try:
            delay = max(1.0, float(self.delay_var.get()))
            flood = max(10, int(self.flood_var.get()))
        except (tk.TclError, ValueError):
            messagebox.showerror("Параметры", "Введите числовые значения", parent=self)
            return
        self.config_store["delay_between_sends"] = delay
        self.config_store["max_flood_wait"] = flood
        self.config_store["auto_show_confirmation"] = bool(self.auto_var.get())
        self.config_store.save()
        messagebox.showinfo("Параметры", "Сохранено", parent=self)

    def ask_api_credentials(self) -> bool:
        api_id = simpledialog.askstring("API ID", "Введите API ID (число с my.telegram.org):", parent=self)
        if not api_id:
            return False
        if not api_id.strip().isdigit():
            messagebox.showerror("API ID", "API ID должен состоять только из цифр", parent=self)
            return False
        api_hash = simpledialog.askstring("API Hash", "Введите API Hash:", show="•", parent=self)
        if not api_hash or len(api_hash.strip()) < 16:
            messagebox.showerror("API Hash", "Некорректный API Hash", parent=self)
            return False
        self.config_store["api_id"] = int(api_id.strip())
        self.config_store["api_hash"] = api_hash.strip()
        self.config_store.save()
        return True

    def _change_api(self):
        if self.engine and self.engine.is_running():
            messagebox.showwarning("API", "Дождитесь окончания рассылки", parent=self)
            return
        if self.ask_api_credentials():
            self.start_telegram()

    # ===================================================================== прочее
    def _periodic_refresh(self):
        if self.engine and self.engine.is_running():
            self.refresh_job_detail()
        self.after(1500, self._periodic_refresh)

    def _on_close(self):
        if self.engine and self.engine.is_running():
            if not messagebox.askyesno(
                    "Выход", "Идёт рассылка. Закрыть программу?\n\nНе начатые отправки останутся в очереди "
                    "(задание будет на паузе), текущая может получить статус «Результат неизвестен».",
                    parent=self):
                return
            self.engine.request_pause(self.engine.current_job_id)
            time.sleep(0.3)
        if self.service is not None:
            try:
                self.runner.call(self.service.disconnect()).result(timeout=5)
            except Exception:
                pass
        self.runner.loop.call_soon_threadsafe(self.runner.loop.stop)
        self.destroy()
