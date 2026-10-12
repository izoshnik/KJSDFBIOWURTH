from telethon import types

from tgbroadcast.telegram_service import TelegramService, analyse_messages, can_send_to, media_kind


def msg(**kw):
    return types.Message(id=kw.pop("id", 1), peer_id=types.PeerUser(1), date=None,
                         message=kw.pop("message", "hi"), **kw)


def test_media_kinds_and_blockers():
    assert media_kind(msg()) == "текст"
    photo = msg(media=types.MessageMediaPhoto(photo=types.PhotoEmpty(id=1)))
    assert media_kind(photo) == "фото"
    w, b = analyse_messages([photo])
    assert not b and any("медиа" in x for x in w)
    _, b = analyse_messages([msg(media=types.MessageMediaUnsupported())])
    assert b
    _, b = analyse_messages([msg(noforwards=True)])
    assert b
    _, b = analyse_messages([msg(media=types.MessageMediaPhoto(photo=types.PhotoEmpty(id=1), ttl_seconds=10))])
    assert b


def test_can_send_rules():
    rights = types.ChatBannedRights(until_date=None, send_messages=True)
    assert can_send_to(types.User(id=5))
    assert not can_send_to(types.User(id=5, deleted=True))
    chan = dict(id=1, title="c", photo=types.ChatPhotoEmpty(), date=None)
    assert not can_send_to(types.Channel(broadcast=True, **chan))
    assert can_send_to(types.Channel(broadcast=True, creator=True, **chan))
    assert can_send_to(types.Channel(megagroup=True, **chan))
    assert not can_send_to(types.Channel(megagroup=True, default_banned_rights=rights, **chan))
    grp = dict(id=1, title="g", photo=types.ChatPhotoEmpty(), participants_count=2, date=None, version=1)
    assert can_send_to(types.Chat(**grp))
    assert not can_send_to(types.Chat(left=True, **grp))


def test_extract_ids_maps_random_ids():
    upd = types.Updates(updates=[types.UpdateMessageID(id=11, random_id=-5),
                                 types.UpdateMessageID(id=10, random_id=7)],
                        users=[], chats=[], date=None, seq=0)
    assert TelegramService._extract_ids(upd, [7, -5]) == [10, 11]
