"""Точка входа: python main.py (или собранный TgBroadcast.exe)."""
import logging
import sys
import tkinter as tk
from tkinter import messagebox

from tgbroadcast.db import Database
from tgbroadcast.infra import Config, SingleInstance, data_dir, setup_logging


def main() -> int:
    base = data_dir()
    log_path = setup_logging(base)
    log = logging.getLogger("main")

    lock = SingleInstance(base / "app.lock")
    if not lock.acquire():
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("TG Broadcast", "Программа уже запущена. Вторая копия не может работать "
                                             "с той же базой — это защищает от повторных отправок.")
        return 1

    database = Database(str(base / "broadcast.sqlite3"))
    recovered = database.recover_after_crash()
    if any(recovered):
        log.warning("Восстановление после сбоя: неизвестных отправок=%s, заданий на паузе=%s", *recovered)

    from tgbroadcast.ui import App
    app = App(base, database, Config(base / "config.json"), log_path, recovered)
    try:
        app.mainloop()
    finally:
        database.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
