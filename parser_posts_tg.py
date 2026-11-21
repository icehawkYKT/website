import os
import re
import random
import sys
import time
import requests
import tempfile
from pathlib import Path

from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError


# ----------------------------- настройки ----------------------------- #

def env_str(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and (value is None or not str(value).strip()):
        raise SystemExit(f"Не задана обязательная переменная окружения: {name}")
    return str(value) if value is not None else ""


def env_int(name, default):
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        raise SystemExit(f"{name}: ожидалось целое число, получено {raw!r}")


def env_float(name, default):
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip().replace(",", "."))
    except ValueError:
        raise SystemExit(f"{name}: ожидалось число, получено {raw!r}")


API_ID = env_int("TG_API_ID", 0)
API_HASH = env_str("TG_API_HASH", required=True)
TG_SESSION = env_str("TG_SESSION", required=True)

SITE_IMPORT_URL = env_str("SITE_IMPORT_URL", required=True)
SITE_KEY = env_str("SITE_KEY", required=True)

CHANNELS = [
    ch.strip().lstrip("@")
    for ch in os.environ.get("TG_CHANNELS", "rf4map").split(",")
    if ch.strip()
]

LIMIT = env_int("TG_LIMIT", 100)
MIN_ID = env_int("TG_MIN_ID", 0)
DELAY = env_float("TG_DELAY", 10.0)
TIMEOUT = env_float("TG_TIMEOUT", 180.0)
MAX_ATTEMPTS = max(1, env_int("TG_MAX_ATTEMPTS", 5))
TMP_CLEANUP_HOURS = env_int("TG_TMP_CLEANUP_HOURS", 24)

TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504, 522, 524}

TMP_DIR = env_str("TG_TMP_DIR") or os.path.join(tempfile.gettempdir(), "tg_widget_images")
Path(TMP_DIR).mkdir(parents=True, exist_ok=True)


# ------------------------- HTTP-сессия и утилиты ------------------------- #

def new_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "tg-widget-importer/1.2",
        # Шаред-хостинг рвёт keep-alive раньше, чем мы успеваем его переиспользовать,
        # и urllib3 получает SSLEOFError на мёртвом сокете. Свежее соединение на каждый пост.
        "Connection": "close",
    })
    return session


SESSION = new_session()


def reset_session():
    """После сетевой/SSL-ошибки выбрасываем пул соединений целиком."""
    global SESSION
    try:
        SESSION.close()
    except Exception:
        pass
    SESSION = new_session()


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def cleanup_paths(paths):
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception as e:
            log(f"Warning: failed to remove temp file {p}: {e}")


def cleanup_stale(tmp_dir, hours):
    if hours <= 0:
        return
    cutoff = time.time() - hours * 3600
    removed = 0
    try:
        with os.scandir(tmp_dir) as entries:
            for entry in entries:
                try:
                    if entry.is_file() and entry.stat().st_mtime < cutoff:
                        os.remove(entry.path)
                        removed += 1
                except OSError:
                    continue
    except FileNotFoundError:
        return
    if removed:
        log(f"Удалено старых временных файлов: {removed}")


def safe_name(value):
    return re.sub(r"[^\w.-]", "_", value) or "channel"


def detect_mime(path):
    try:
        with open(path, "rb") as f:
            head = f.read(12)
    except OSError:
        return "image/jpeg", ".jpg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp", ".webp"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif", ".gif"
    return "image/jpeg", ".jpg"


def backoff(attempt):
    return min(120.0, 10.0 * (2 ** (attempt - 1))) + random.uniform(0.0, 2.0)


def retry_after(resp):
    header = (resp.headers.get("Retry-After") or "").strip()
    if header.isdigit():
        return min(float(header), 300.0)
    return None


# --------------------------- фильтр постов --------------------------- #

def should_skip_post(caption):
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
        "- скидки рыбака",
        "- условия",
        "быть подписанным",
        "новый видеоролик",
        "чтобы принять участие"
    ]

    return any(word in text for word in skip_keywords)


# ----------------------------- Telegram ----------------------------- #

DOWNLOAD_FAILURES = 0


def fetch_items_sync():
    global DOWNLOAD_FAILURES

    items = []
    groups = {}
    group_counts = {}

    try:
        with TelegramClient(StringSession(TG_SESSION), API_ID, API_HASH) as client:
            # ---------- 1. собираем сообщения ----------
            for channel in CHANNELS:
                log(f"Канал {channel}: запрашиваю до {LIMIT} сообщений (min_id={MIN_ID})")

                msgs = []
                try:
                    msgs = list(client.iter_messages(channel, limit=LIMIT, min_id=MIN_ID))
                except FloodWaitError as e:
                    wait = int(getattr(e, "seconds", 60)) + 5
                    log(f"  FloodWait {wait} c — жду и повторяю запрос")
                    time.sleep(wait)
                    try:
                        msgs = list(client.iter_messages(channel, limit=LIMIT, min_id=MIN_ID))
                    except Exception as e2:
                        log(f"  ОШИБКА повторного запроса {channel}: {type(e2).__name__}: {e2}")
                        continue
                except Exception as e:
                    log(f"  ОШИБКА запроса {channel}: {type(e).__name__}: {e}")
                    continue

                if not msgs:
                    log(f"  {channel}: новых сообщений нет")
                    continue

                if len(msgs) >= LIMIT:
                    log(
                        f"  ВНИМАНИЕ: {channel}: получено ровно {LIMIT} сообщений (лимит). "
                        f"Возможно, есть более старые посты — увеличьте TG_LIMIT. "
                        f"Помните: альбом из 10 фото = 10 сообщений = 1 пост."
                    )

                for msg in msgs:
                    has_photo = getattr(msg, "photo", None) is not None
                    has_text = bool(
                        getattr(msg, "caption", None) or getattr(msg, "message", None)
                    )

                    if not has_photo and not has_text:
                        continue

                    base_key = getattr(msg, "grouped_id", None) or msg.id
                    key = f"{channel}_{base_key}"

                    if key not in groups:
                        group_counts[channel] = group_counts.get(channel, 0) + 1
                    groups.setdefault(key, []).append(msg)

                log(
                    f"  {channel}: сообщений={len(msgs)}, "
                    f"постов(групп)={group_counts.get(channel, 0)}"
                )

            # ---------- 2. скачиваем фото ----------
            # ВАЖНО: этот блок должен оставаться ВНУТРИ with, иначе клиент уже
            # отключён и download_media падает с ConnectionError.
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

                    fname = f"{safe_name(group_channel)}_{msg.id}.jpg"
                    path = os.path.join(TMP_DIR, fname)

                    try:
                        if not client.is_connected():
                            log("  клиент отключился — переподключаюсь")
                            client.connect()

                        client.download_media(msg, file=path)
                        if os.path.exists(path):
                            paths.append(path)
                        else:
                            DOWNLOAD_FAILURES += 1
                            log(f"Warning: download_media не создал файл для msg {msg.id}")
                    except Exception as e:
                        DOWNLOAD_FAILURES += 1
                        log(f"Warning: failed download msg {msg.id}: {type(e).__name__}: {e}")

                if not paths:
                    log(f"SKIP msg_id={min(m.id for m in group_msgs)} — фото не скачались")
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
                    log(f"SKIP msg_id={rep_id} — empty caption")
                    cleanup_paths(paths)
                    continue

                if should_skip_post(caption):
                    log(f"SKIP msg_id={rep_id} — unwanted content")
                    cleanup_paths(paths)
                    continue

                items.append({
                    "paths": paths,
                    "caption": caption,
                    "msg_id": rep_id,
                    "channel": group_channel,
                })

    except Exception as e:
        log(f"КРИТИЧНО: ошибка Telegram: {type(e).__name__}: {e}")
        log("Отправлю то, что успел собрать.")

    return items


# ------------------------------ отправка ------------------------------ #

def post_once(url, data, paths):
    files = []
    opened = []
    try:
        for p in paths:
            f = open(p, "rb")
            opened.append(f)
            mime, ext = detect_mime(p)
            name = os.path.splitext(os.path.basename(p))[0] + ext
            files.append(("photos[]", (name, f, mime)))

        return SESSION.post(url, data=data, files=files, timeout=(20.0, TIMEOUT))
    finally:
        for f in opened:
            try:
                f.close()
            except Exception:
                pass


def send_to_site(item):
    """Возвращает (ok, status, text). Сетевые исключения наружу не выпускает."""
    paths = item.get("paths", [])
    data = {
        "key": SITE_KEY,
        "text": item.get("caption", "") or "",
        "telegram_msg_id": f"{item.get('channel')}_{item.get('msg_id')}",
    }

    total = sum(os.path.getsize(p) for p in paths if os.path.exists(p))
    if total > 8 * 1024 * 1024:
        log(f"  ВНИМАНИЕ: суммарно {total / 1048576:.1f} МБ — сайт может ответить 413")

    ok = False
    status = None
    text = ""

    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                resp = post_once(SITE_IMPORT_URL, data, paths)
            except requests.RequestException as e:
                # SSLError / ConnectionError / Timeout / ChunkedEncodingError и прочее.
                # Падение на рукопожатии означает, что тело не ушло — повтор безопасен.
                log(
                    f"  сеть/SSL (попытка {attempt}/{MAX_ATTEMPTS}): "
                    f"{type(e).__name__}: {e}"
                )
                reset_session()
                if attempt < MAX_ATTEMPTS:
                    time.sleep(backoff(attempt))
                continue

            status = resp.status_code
            text = resp.text or ""

            if 200 <= status < 300:
                ok = True
                break

            if status in TRANSIENT_STATUS:
                log(f"  HTTP {status} (попытка {attempt}/{MAX_ATTEMPTS}), повторю")
                if attempt < MAX_ATTEMPTS:
                    time.sleep(retry_after(resp) or backoff(attempt))
                continue

            log(f"  HTTP {status} — постоянная ошибка, повторять не буду")
            break
    finally:
        cleanup_paths(paths)

    print("SITE RESPONSE:", status, text[:2000], flush=True)
    return ok, status, text


# -------------------------------- main -------------------------------- #

def main():
    if not CHANNELS:
        log("КРИТИЧНО: список каналов пуст (TG_CHANNELS)")
        return 1

    cleanup_stale(TMP_DIR, TMP_CLEANUP_HOURS)

    items = fetch_items_sync()
    log(f"Постов к отправке: {len(items)}")

    if not items:
        if DOWNLOAD_FAILURES:
            log(f"ОШИБКА: ни одного поста не собрано, сбоев скачивания={DOWNLOAD_FAILURES}")
            return 1
        log("ВНИМАНИЕ: отправлять нечего (нет новых постов или всё отсеял фильтр)")
        return 0

    sent = 0
    failed = 0

    for index, item in enumerate(items, 1):
        log(
            f"[{index}/{len(items)}] channel={item['channel']} "
            f"msg_id={item['msg_id']} photos={len(item['paths'])}"
        )
        try:
            ok, _status, _text = send_to_site(item)
        except Exception as e:
            log(f"  ОШИБКА отправки: {type(e).__name__}: {e}")
            cleanup_paths(item.get("paths", []))
            ok = False

        if ok:
            sent += 1
        else:
            failed += 1

        if index < len(items):
            time.sleep(max(0.0, DELAY))

    log(f"ИТОГ: отправлено={sent}, с ошибками={failed}, всего={len(items)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
