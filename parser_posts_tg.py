import os
import time
import requests
import tempfile
from pathlib import Path

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
TG_SESSION = os.environ["TG_SESSION"]

SITE_IMPORT_URL = os.environ["SITE_IMPORT_URL"]
SITE_KEY = os.environ["SITE_KEY"]

CHANNELS = [
    ch.strip()
    for ch in os.environ.get("TG_CHANNELS", "rf4map").split(",")
    if ch.strip()
]

LIMIT = int(os.environ.get("TG_LIMIT", "100"))

TMP_DIR = os.path.join(tempfile.gettempdir(), "tg_widget_images")
Path(TMP_DIR).mkdir(parents=True, exist_ok=True)


def cleanup_paths(paths):
    for p in paths:
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception as e:
            print(f"Warning: failed to remove temp file {p}: {e}")


def should_skip_post(caption: str) -> bool:
    text = (caption or "").lower()

    skip_keywords = [
        "реклама",
        "промокод",
        "анонс",
        "стрим",
        "донат",
        "youtube",
        "youtu.be",
        "vk.com",
        "trovo",
        "ozon",
        "проверка точки",
        "проверка точки.",
        "проверка точк",
        "а у вас клюёт",
        "у вас клюёт",
        
    ]

    return any(word in text for word in skip_keywords)


def fetch_items_sync():
    items = []

    with TelegramClient(StringSession(TG_SESSION), API_ID, API_HASH) as client:
        groups = {}

        for channel in CHANNELS:
            print(f"Fetching from channel: {channel}")
            msgs = list(client.iter_messages(channel, limit=LIMIT))

            for msg in msgs:
                has_photo = getattr(msg, "photo", None) is not None
                has_text = bool(
                    getattr(msg, "caption", None) or getattr(msg, "message", None)
                )

                if not has_photo and not has_text:
                    continue

                base_key = getattr(msg, "grouped_id", None) or msg.id
                key = f"{channel}_{base_key}"
                groups.setdefault(key, []).append(msg)

        for key in sorted(groups.keys(), key=lambda k: min(m.id for m in groups[k])):
            group_msgs = groups[key]
            group_msgs.sort(key=lambda m: getattr(m, "id", 0))

            group_channel = key.rsplit("_", 1)[0]

            main_msg = None
            for m in group_msgs:
                text = (
                    (getattr(m, "caption", None) or "").strip()
                    or (getattr(m, "message", None) or "").strip()
                )
                if text:
                    main_msg = m
                    break

            if main_msg is None:
                main_msg = group_msgs[0]

            main_gid = getattr(main_msg, "grouped_id", None)
            if main_gid is None:
                for m in group_msgs:
                    gid = getattr(m, "grouped_id", None)
                    if gid is not None:
                        main_gid = gid
                        break

            paths = []
            for msg in group_msgs:
                if getattr(msg, "photo", None) is None:
                    continue

                msg_gid = getattr(msg, "grouped_id", None)

                if main_gid is not None and msg_gid != main_gid:
                    continue

                if main_gid is None and msg.id != main_msg.id and msg_gid is not None:
                    continue

                fname = f"{group_channel}_{msg.id}.jpg"
                path = os.path.join(TMP_DIR, fname)

                try:
                    client.download_media(msg, file=path)
                    if os.path.exists(path):
                        paths.append(path)
                except Exception as e:
                    print(f"Warning: failed download msg {msg.id}: {e}")

            if not paths:
                continue

            caption = ""
            for m in group_msgs:
                msg_gid = getattr(m, "grouped_id", None)

                if main_gid is not None and msg_gid != main_gid:
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
                cleanup_paths(paths)
                continue

            if should_skip_post(caption):
                print(f"SKIP msg_id={rep_id} — unwanted content")
                cleanup_paths(paths)
                continue

            items.append({
                "paths": paths,
                "caption": caption,
                "msg_id": rep_id,
                "channel": group_channel,
            })

    return items


def send_to_site(item: dict):
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

        response = requests.post(
            SITE_IMPORT_URL,
            data=data,
            files=files,
            timeout=180,
        )

        print("SITE RESPONSE:", response.status_code, response.text)
        return response.status_code, response.text

    finally:
        for f in opened:
            try:
                f.close()
            except Exception:
                pass

        cleanup_paths(item.get("paths", []))


def main():
    items = fetch_items_sync()
    print(f"Fetched items: {len(items)}")

    for it in items:
        print(
            f"Sending channel={it['channel']} msg_id={it['msg_id']} photos={len(it['paths'])}"
        )
        send_to_site(it)
        time.sleep(0.3)


if __name__ == "__main__":
    main()
