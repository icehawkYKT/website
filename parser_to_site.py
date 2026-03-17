import socks
import os
import time
import requests
import asyncio
import tempfile
from pathlib import Path

from telethon.sync import TelegramClient

AD_KEYWORDS = [
    "реклама",
    "промокод",
    "анонс",
    "стрим",
    "донат",
    "youtube",
    "vk.com",
    "trovo",
    "ozon",
]


# ====== НАСТРОЙКИ TELEGRAM (как в win_fixed.py) ======
API_ID = int(os.environ.get("TG_API_ID", "38657726"))
API_HASH = os.environ.get("TG_API_HASH", "9e23f9e71122fabe0435e9769b5a5bc2")
CHANNELS = [
    "rf4map",
    #"rr4pepper"
    #"NiOoooN_UL"
    ]
LIMIT = int(os.environ.get("TG_LIMIT", "100"))
TMP_DIR = os.path.join(tempfile.gettempdir(), "tg_widget_images")
Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

# ====== НАСТРОЙКИ САЙТА (ВАЖНО) ======
# ==== SITE_IMPORT_URL = "http://localhost:8080/backend/api/import_post.php" ====
SITE_IMPORT_URL = "https://запискирыбака.рф/backend/api/import_post.php"


SITE_KEY = "MY_SUPER_SECRET_KEY"  # это значение проверяется в import_post.php

# ======изменения ======
def extract_caption_from_group(group_msgs):
    """
    Возвращает ЕДИНЫЙ caption для поста:
    - сначала ищем caption
    - потом message
    - raw_text используем только как fallback
    """
    for m in group_msgs:
        if getattr(m, "caption", None):
            return str(m.caption).strip()

    for m in group_msgs:
        if getattr(m, "message", None):
            return str(m.message).strip()

    for m in group_msgs:
        if getattr(m, "raw_text", None):
            text = str(m.raw_text).strip()
            if text:
                return text

    return ""


def fetch_items_sync():
    """Возвращает список постов: {paths: [...], caption: '...', msg_id: 123}"""
    items = []
    loop = None

    try:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        proxy = (socks.SOCKS5, "198.105.121.200", 6462, True, "hpnokppe", "bu31v1b4hlht")
        
        with TelegramClient("tg_widget_session", API_ID, API_HASH, proxy=proxy) as client:

            groups = {}

            for channel in CHANNELS:
                print(f"Fetching from channel: {channel}")
                msgs = list(client.iter_messages(channel, limit=LIMIT))

                for msg in msgs:
                    has_photo = getattr(msg, "photo", None) is not None
                    has_text = bool(getattr(msg, "caption", None) or getattr(msg, "message", None))

# пропускаем ТОЛЬКО полностью пустые
                    if not has_photo and not has_text:
                        continue

                    base_key = getattr(msg, "grouped_id", None) or msg.id
                    key = f"{channel}_{base_key}"

                    groups.setdefault(key, []).append(msg)

                                # --- ОБРАБОТКА СОБРАННЫХ ГРУПП (после сбора всех сообщений) ---
            for key in sorted(groups.keys(), key=lambda k: min(m.id for m in groups[k])):
                group_msgs = groups[key]
                group_msgs.sort(key=lambda m: getattr(m, "id", 0))

                # канал берём из ключа, чтобы он НЕ "плыл"
                group_channel = key.rsplit("_", 1)[0]

                # --- главное сообщение (ищем текст) ---
                main_msg = None
                for m in group_msgs:
                    text = (
                        (getattr(m, "caption", None) or "").strip() or
                        (getattr(m, "message", None) or "").strip()
                    )
                    if text:
                        main_msg = m
                        break
                if main_msg is None:
                    main_msg = group_msgs[0]

                # --- определяем grouped_id альбома фото ---
                main_gid = getattr(main_msg, "grouped_id", None)
                if main_gid is None:
                    for m in group_msgs:
                        gid = getattr(m, "grouped_id", None)
                        if gid is not None:
                            main_gid = gid
                            break

                # --- скачиваем ТОЛЬКО фото этого альбома ---
                paths = []
                for msg in group_msgs:
                    if getattr(msg, "photo", None) is None:
                        continue
                    if getattr(msg, "grouped_id", None) != main_gid:
                        continue

                    fname = f"{msg.id}.jpg"
                    path = os.path.join(TMP_DIR, fname)
                    try:
                        client.download_media(msg, file=path)
                        paths.append(path)
                    except Exception as e:
                        print(f"Warning: failed download msg {msg.id}: {e}")

                if not paths:
                    continue

                # --- caption из сообщений ЭТОГО ЖЕ альбома ---
                caption = ""
                for m in group_msgs:
                    if getattr(m, "grouped_id", None) != main_gid:
                        continue
                    if getattr(m, "caption", None):
                        caption = str(m.caption).strip()
                        break
                    if getattr(m, "message", None):
                        caption = str(m.message).strip()
                        break

                if not caption and getattr(main_msg, "raw_text", None):
                    caption = str(main_msg.raw_text).strip()

                rep_id = min(getattr(m, "id", 0) for m in group_msgs)

                if not caption:
                    print(f"SKIP msg_id={rep_id} — empty caption")
                    continue

                caption_lc = caption.lower()
                if any(word in caption_lc for word in AD_KEYWORDS):
                    print(f"SKIP msg_id={rep_id} — ad/announcement")
                    continue

                items.append({
                    "paths": paths,
                    "caption": caption,
                    "msg_id": rep_id,
                    "channel": group_channel,
                })




    finally:
        try:
            if loop is not None and not loop.is_closed():
                asyncio.set_event_loop(None)
                loop.close()
        except Exception:
            pass

    return items



def send_to_site(item: dict):
    """Шлёт caption + photos[] в import_post.php (multipart/form-data)."""
    data = {
    "key": SITE_KEY,
    "text": item.get("caption", "") or "",
    "telegram_msg_id": f"{item.get('channel')}_{item.get('msg_id')}",
    }

    files = []
    opened = []
    try:
        for p in item.get("paths", []):
            f = open(p, "rb")
            opened.append(f)
            files.append(("photos[]", (os.path.basename(p), f, "image/jpeg")))

        r = requests.post(SITE_IMPORT_URL, data=data, files=files, timeout=180)
        print("SITE RESPONSE:", r.status_code, r.text)
        return r.status_code, r.text

    finally:
        for f in opened:
            try:
                f.close()
            except Exception:
                pass


def main():
    items = fetch_items_sync()
    print(f"Fetched items: {len(items)}")

    for it in items:
        print(f"Sending msg_id={it['msg_id']} photos={len(it['paths'])}")
        send_to_site(it)
        time.sleep(0.3)


if __name__ == "__main__":
    main()
