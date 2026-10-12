"""Работа с Telegram через Telethon (MTProto): вход, диалоги, «Избранное», отправка."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable, Optional

from telethon import TelegramClient, errors, events, types, utils
from telethon.tl.functions.messages import ForwardMessagesRequest

from . import db as D
from .engine import SendAlreadyDelivered, SendBlocked, SendFloodWait, SendRejected, SendUnknown

log = logging.getLogger(__name__)

ALBUM_SETTLE_SECONDS = 1.5

EventCallback = Callable[[str, dict], None]


@dataclass
class Preview:
    message_ids: list[int]
    text: str
    has_formatting: bool
    kinds: list[str]
    thumb: Optional[bytes] = None
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)  # если не пусто — рассылать нельзя

    @property
    def summary(self) -> str:
        head = ", ".join(self.kinds) or "текст"
        body = self.text.strip().replace("\n", " ")
        return f"[{head}] {body[:120]}" if body else f"[{head}]"


def media_kind(msg: types.Message) -> str:
    m = msg.media
    if m is None:
        return "текст"
    if isinstance(m, types.MessageMediaWebPage):
        return "текст со ссылкой"
    if isinstance(m, types.MessageMediaPhoto):
        return "фото"
    if isinstance(m, types.MessageMediaDocument):
        if msg.video_note:
            return "видеосообщение"
        if msg.voice:
            return "голосовое"
        if msg.gif:
            return "GIF"
        if msg.video:
            return "видео"
        if msg.audio:
            return "аудио"
        if msg.sticker:
            return "стикер"
        return "файл"
    if isinstance(m, types.MessageMediaPoll):
        return "опрос"
    if isinstance(m, (types.MessageMediaGeo, types.MessageMediaGeoLive, types.MessageMediaVenue)):
        return "геопозиция"
    if isinstance(m, types.MessageMediaContact):
        return "контакт"
    if isinstance(m, types.MessageMediaDice):
        return "кубик"
    if isinstance(m, types.MessageMediaUnsupported):
        return "неподдерживаемый тип"
    return type(m).__name__


def analyse_messages(msgs: list[types.Message]) -> tuple[list[str], list[str]]:
    """Возвращает (предупреждения, блокирующие причины) для набора сообщений."""
    warnings: list[str] = []
    blockers: list[str] = []
    for msg in msgs:
        kind = media_kind(msg)
        if getattr(msg, "noforwards", False):
            blockers.append("Сообщение защищено от копирования и пересылки (noforwards).")
        if isinstance(msg.media, types.MessageMediaUnsupported):
            blockers.append("Тип сообщения не поддерживается этой версией Telegram API.")
        ttl = getattr(msg.media, "ttl_seconds", None) if msg.media else None
        if ttl:
            blockers.append("Самоуничтожающееся медиа нельзя переслать.")
        if isinstance(msg.media, types.MessageMediaGeoLive):
            blockers.append("Трансляцию геопозиции нельзя скопировать.")
        if isinstance(msg.media, types.MessageMediaDice):
            warnings.append("Кубик/анимация будет отправлен заново, результат может отличаться.")
        if kind == "опрос":
            warnings.append("Опрос будет создан как копия: голоса не переносятся.")
        if kind in ("голосовое", "видеосообщение"):
            warnings.append("В некоторых чатах голосовые/видеосообщения запрещены — там будет ошибка.")
    if any(media_kind(m) not in ("текст", "текст со ссылкой") for m in msgs):
        warnings.append("Если в чате запрещены медиа, отправка туда завершится ошибкой "
                        "(текст отдельно НЕ отправляется).")
    return list(dict.fromkeys(warnings)), list(dict.fromkeys(blockers))


def can_send_to(entity) -> bool:
    """Оценивает право писать в чат по данным диалога (без лишних запросов)."""
    if isinstance(entity, types.User):
        return not entity.deleted
    if isinstance(entity, types.Chat):
        if entity.left or entity.deactivated or entity.migrated_to:
            return False
        if entity.creator or entity.admin_rights:
            return True
        rights = entity.default_banned_rights
        return not (rights and rights.send_messages)
    if isinstance(entity, types.Channel):
        if entity.left:
            return False
        if entity.broadcast:
            return bool(entity.creator or (entity.admin_rights and entity.admin_rights.post_messages))
        if entity.creator or entity.admin_rights:
            return True
        if entity.banned_rights and entity.banned_rights.send_messages:
            return False
        rights = entity.default_banned_rights
        return not (rights and rights.send_messages)
    return False


def entity_kind(entity) -> str:
    if isinstance(entity, types.User):
        return "Бот" if entity.bot else "Личный чат"
    if isinstance(entity, types.Chat):
        return "Группа"
    if isinstance(entity, types.Channel):
        return "Канал" if entity.broadcast else "Супергруппа"
    return "Другое"


REJECT_MESSAGES = {
    errors.ChatWriteForbiddenError: "Нет права писать в этот чат",
    errors.ChatSendMediaForbiddenError: "В этом чате запрещено отправлять медиа",
    errors.ChatSendPhotosForbiddenError: "В этом чате запрещено отправлять фото",
    errors.ChatSendVideosForbiddenError: "В этом чате запрещено отправлять видео",
    errors.ChatSendStickersForbiddenError: "В этом чате запрещены стикеры",
    errors.ChatSendGifsForbiddenError: "В этом чате запрещены GIF",
    errors.ChatSendPollForbiddenError: "В этом чате запрещены опросы",
    errors.ChatSendVoicesForbiddenError: "В этом чате запрещены голосовые сообщения",
    errors.ChatSendPlainForbiddenError: "В этом чате запрещён обычный текст",
    errors.VoiceMessagesForbiddenError: "Пользователь запретил голосовые сообщения",
    errors.UserBannedInChannelError: "Аккаунт ограничен в этом чате",
    errors.ChannelPrivateError: "Нет доступа к чату (приватный или вы исключены)",
    errors.ChatAdminRequiredError: "Требуются права администратора",
    errors.UserIsBlockedError: "Пользователь заблокировал вас",
    errors.YouBlockedUserError: "Вы заблокировали этого пользователя",
    errors.PeerIdInvalidError: "Чат не найден",
    errors.MessageIdInvalidError: "Исходное сообщение удалено или недоступно",
}


class TelegramService:
    def __init__(self, session_path: str, api_id: int, api_hash: str, database: D.Database,
                 on_event: EventCallback):
        self.db = database
        self.on_event = on_event
        # flood_sleep_threshold=0: FloodWait всегда возвращается нам, а не «засыпает» молча.
        self.client = TelegramClient(session_path, int(api_id), api_hash,
                                     flood_sleep_threshold=0, request_retries=3,
                                     connection_retries=5, retry_delay=2, auto_reconnect=True,
                                     device_model="Windows PC", app_version="TgBroadcast 1.0")
        self.me: Optional[types.User] = None
        self._phone: Optional[str] = None
        self._phone_code_hash: Optional[str] = None
        self._album_buffers: dict[int, list[types.Message]] = {}
        self._album_timers: dict[int, asyncio.TimerHandle] = {}
        self._handlers_registered = False

    # ------------------------------------------------------------- авторизация
    async def connect(self) -> bool:
        await self.client.connect()
        return await self.client.is_user_authorized()

    async def send_code(self, phone: str) -> None:
        self._phone = phone.strip()
        sent = await self.client.send_code_request(self._phone)
        self._phone_code_hash = sent.phone_code_hash

    async def sign_in_code(self, code: str) -> str:
        """'ok' — вход выполнен; 'password' — требуется пароль двухэтапной аутентификации."""
        try:
            await self.client.sign_in(self._phone, code.strip(), phone_code_hash=self._phone_code_hash)
        except errors.SessionPasswordNeededError:
            return "password"
        return "ok"

    async def sign_in_password(self, password: str) -> None:
        await self.client.sign_in(password=password)

    async def logout(self) -> None:
        # log_out завершает сессию на серверах Telegram и удаляет локальный файл сессии.
        await self.client.log_out()
        self.me = None

    async def disconnect(self) -> None:
        await self.client.disconnect()

    def is_connected(self) -> bool:
        return self.client.is_connected()

    async def after_login(self) -> types.User:
        self.me = await self.client.get_me()
        if not self._handlers_registered:
            self.client.add_event_handler(self._on_new_message, events.NewMessage(
                func=lambda e: self.me is not None and e.chat_id == self.me.id))
            self._handlers_registered = True
        log.info("Вход выполнен, аккаунт id=%s", self.me.id)
        return self.me

    @property
    def account_id(self) -> int:
        assert self.me is not None
        return self.me.id

    # ------------------------------------------------------------------ диалоги
    async def load_dialogs(self) -> list[dict]:
        result = []
        async for d in self.client.iter_dialogs():
            ent = d.entity
            if isinstance(ent, types.User) and self.me and ent.id == self.me.id:
                continue  # «Избранное» никогда не является целью рассылки
            result.append({
                "peer_id": d.id,
                "title": d.name or "(без названия)",
                "kind": entity_kind(ent),
                "username": getattr(ent, "username", None),
                "can_send": can_send_to(ent),
            })
        self.db.upsert_chats(result)
        return result

    # --------------------------------------------------------- «Избранное»
    async def _collect_album(self, msg: types.Message) -> list[types.Message]:
        if not msg.grouped_id:
            return [msg]
        around = await self.client.get_messages("me", min_id=msg.id - 12, max_id=msg.id + 12, limit=30)
        album = [m for m in around if m and m.grouped_id == msg.grouped_id]
        return sorted(album or [msg], key=lambda m: m.id)

    async def latest_saved(self) -> Optional[list[types.Message]]:
        msgs = await self.client.get_messages("me", limit=1)
        if not msgs:
            return None
        return await self._collect_album(msgs[0])

    async def check_missed_on_start(self) -> None:
        """Показывает сообщение, появившееся в «Избранном», пока программа была закрыта."""
        latest = await self.latest_saved()
        if not latest:
            return
        last_id = max(m.id for m in latest)
        known = self.db.get_setting("last_saved_msg_id")
        self.db.set_setting("last_saved_msg_id", str(last_id))
        if known is None or last_id <= int(known):
            return
        await self._announce(latest)

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        msg = event.message
        if msg.grouped_id:
            gid = msg.grouped_id
            self._album_buffers.setdefault(gid, []).append(msg)
            timer = self._album_timers.pop(gid, None)
            if timer:
                timer.cancel()
            loop = asyncio.get_running_loop()
            self._album_timers[gid] = loop.call_later(
                ALBUM_SETTLE_SECONDS, lambda: asyncio.ensure_future(self._flush_album(gid)))
            return
        await self._announce([msg])

    async def _flush_album(self, gid: int) -> None:
        self._album_timers.pop(gid, None)
        msgs = sorted(self._album_buffers.pop(gid, []), key=lambda m: m.id)
        if msgs:
            await self._announce(msgs)

    async def _announce(self, msgs: list[types.Message]) -> None:
        ids = [m.id for m in msgs]
        current = int(self.db.get_setting("last_saved_msg_id", "0") or 0)
        if max(ids) > current:
            self.db.set_setting("last_saved_msg_id", str(max(ids)))
        preview = await self.build_preview(msgs)
        detected_id = self.db.record_detected(self.account_id, self.account_id, ids, preview.summary)
        if detected_id is None:
            log.info("Повторное событие для уже обнаруженного сообщения %s — пропущено", ids)
            return
        log.info("Обнаружено новое сообщение в «Избранном»: %s", ids)
        self.on_event("detected", {"detected_id": detected_id, "preview": preview})

    async def build_preview(self, msgs: list[types.Message]) -> Preview:
        text = "\n".join(m.message for m in msgs if m.message)
        has_fmt = any(m.entities for m in msgs)
        kinds = [media_kind(m) for m in msgs]
        if len(msgs) > 1:
            kinds = [f"альбом из {len(msgs)}: " + ", ".join(dict.fromkeys(kinds))]
        warnings, blockers = analyse_messages(msgs)
        thumb = None
        for m in msgs:
            if m.photo or m.document:
                try:
                    # -1 — самая крупная миниатюра; оригинал файла не скачивается
                    thumb = await self.client.download_media(m, file=bytes, thumb=-1)
                except Exception:
                    log.warning("Не удалось загрузить миниатюру для предпросмотра", exc_info=True)
                if thumb:
                    break
        return Preview([m.id for m in msgs], text, has_fmt, kinds, thumb, warnings, blockers)

    async def preview_for_ids(self, message_ids: list[int]) -> Preview:
        msgs = [m for m in await self.client.get_messages("me", ids=message_ids) if m]
        if not msgs:
            return Preview(message_ids, "", False, [], None, [],
                           ["Исходное сообщение удалено из «Избранного»."])
        return await self.build_preview(msgs)

    # ---------------------------------------------------------------- отправка
    async def send(self, source_peer_id: int, message_ids: list[int], target_peer_id: int,
                   random_ids: list[int]) -> list[int]:
        """Копирует сообщение(я) без подписи «Переслано от» (drop_author).

        Медиа не скачиваются и не загружаются повторно; альбом передаётся одним
        запросом и сохраняет группировку; форматирование сохраняется. random_id
        сохранены в базе до запроса, что позволяет Telegram распознать повтор.
        """
        # Ошибки этой части возникают до отправки — сообщение точно не ушло.
        try:
            from_peer = await self.client.get_input_entity(
                types.InputPeerSelf() if source_peer_id == self.account_id else source_peer_id)
            to_peer = await self.client.get_input_entity(target_peer_id)
        except (ValueError, TypeError) as e:
            raise SendRejected(f"Чат не найден в списке диалогов: {e}") from e

        request = ForwardMessagesRequest(from_peer=from_peer, id=list(message_ids), to_peer=to_peer,
                                         random_id=list(random_ids), drop_author=True)
        try:
            result = await self.client(request)
        except errors.FloodWaitError as e:
            raise SendFloodWait(e.seconds) from e
        except errors.SlowModeWaitError as e:
            raise SendFloodWait(e.seconds, f"Медленный режим в чате: {e.seconds} с") from e
        except errors.RandomIdDuplicateError as e:
            raise SendAlreadyDelivered("RANDOM_ID_DUPLICATE") from e
        except errors.PeerFloodError as e:
            raise SendBlocked("Telegram временно ограничил отправку сообщений этим аккаунтом") from e
        except (errors.UnauthorizedError, errors.AuthKeyError) as e:
            raise SendBlocked(f"Потеряна авторизация: {e}") from e
        except (errors.BadRequestError, errors.ForbiddenError) as e:
            raise SendRejected(self._describe(e)) from e
        except errors.RPCError as e:
            # 5xx и прочие ответы сервера не дают уверенности, что сообщение не ушло
            raise SendUnknown(f"Ответ сервера: {e}") from e
        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            raise SendUnknown(f"Связь потеряна: {e!r}") from e
        return self._extract_ids(result, random_ids)

    @staticmethod
    def _describe(e: Exception) -> str:
        for cls, text in REJECT_MESSAGES.items():
            if isinstance(e, cls):
                return text
        return f"Telegram отклонил отправку: {e}"

    @staticmethod
    def _extract_ids(result, random_ids: list[int]) -> list[int]:
        by_random = {}
        for upd in getattr(result, "updates", []) or []:
            if isinstance(upd, types.UpdateMessageID):
                by_random[upd.random_id] = upd.id
        return [by_random[r] for r in random_ids if r in by_random]

    # -------------------------------------------- проверка неизвестного результата
    async def verify_delivery(self, delivery: D.Delivery, job: D.Job) -> Optional[list[int]]:
        """Ищет в целевом чате копию сообщения, отправленную после начала попытки.

        Сравниваются текст и id медиа (при копировании Telegram сохраняет те же
        photo.id/document.id). Возвращает id найденных сообщений либо None.
        """
        originals = [m for m in await self.client.get_messages("me", ids=job.message_ids) if m]
        if not originals:
            return None
        since = (delivery.started_at or job.created_at) - 120

        def signature(m: types.Message):
            media_id = None
            if m.photo:
                media_id = ("photo", m.photo.id)
            elif m.document:
                media_id = ("doc", m.document.id)
            return (m.message or "", media_id)

        wanted = [signature(m) for m in originals]
        found: list[int] = []
        async for m in self.client.iter_messages(delivery.target_peer_id, limit=200):
            if m.date and m.date.timestamp() < since:
                break
            if m.out and signature(m) in wanted and m.id not in found:
                found.append(m.id)
        return sorted(found) if len(found) >= len(wanted) else None


def peer_title(entity) -> str:
    return utils.get_display_name(entity)
