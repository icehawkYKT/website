#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram -> site importer (widget feed).

Зависимости:
    pip install telethon requests
    pip install pillow        # опционально: ресайз и перекодирование фото

Что изменено относительно первой версии:
  * идемпотентность: state-файл + min_id, повторный запуск не шлёт старые посты;
  * фильтр по тексту ДО скачивания фото (экономия трафика и FloodWait);
  * альбом на границе лимита дочитывается целиком (не режется пополам);
  * ретраи с экспоненциальной задержкой, разделение постоянных и временных ошибок;
  * один сбойный пост не роняет весь прогон;
  * временные файлы удаляются только после того, как пост разрешён (отправлен/пропущен);
  * MIME определяется по сигнатуре файла, а не хардкодом image/jpeg;
  * опциональный ресайз (Pillow) и контроль суммарного размера отправки;
  * logging вместо print, валидация env, requests.Session, --dry-run.

Переменные окружения:
    Обязательные:
        TG_API_ID, TG_API_HASH, TG_SESSION
        SITE_IMPORT_URL, SITE_KEY
    Опциональные:
        TG_CHANNELS=rf4map,other_channel     каналы через запятую
        TG_LIMIT=100                         сообщений на канал за прогон
        TG_DELAY=10                          пауза между отправками, сек
        TG_MAX_ATTEMPTS=5                    попыток на один пост
        TG_TIMEOUT=180                       таймаут запроса, сек
        TG_MAX_TOTAL_MB=40                   лимит суммарного размера фото
        TG_MAX_IMAGE_SIDE=1600               0 = не ресайзить
        TG_TMP_DIR=...                       каталог для временных файлов
        TG_STATE_FILE=state.json             где хранить прогресс
        TG_TMP_CLEANUP_HOURS=24              чистить сироты старше N часов (0 = не чистить)
        TG_SKIP_KEYWORDS_FILE=...            свой список стоп-слов (по одному в строке)
        SITE_EXPECT_JSON_OK=0                1 = требовать JSON-ответ с ok/success
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import requests
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.sync import TelegramClient

try:
    from PIL import Image
    HAVE_PIL = True
except ImportError:
    Image = None  # type: ignore[assignment]
    HAVE_PIL = False


LOG = logging.getLogger("tg_import")

# --------------------------------------------------------------------------- #
# Конфигурация
# --------------------------------------------------------------------------- #

DEFAULT_SKIP_KEYWORDS = [
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
    "а у вас клюёт",
    "у вас клюёт",
    "скидки рыбака",
    "быть подписанным",
    "новый видеоролик",
    "чтобы принять участие",
]

DASHES = "\u2010\u2011\u2012\u2013\u2014\u2015-"


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and (value is None or not str(value).strip()):
        raise SystemExit(f"Не задана обязательная переменная окружения: {name}")
    return str(value) if value is not None else ""


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        raise SystemExit(f"{name}: ожидалось целое число, получено {raw!r}")


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip().replace(",", "."))
    except ValueError:
        raise SystemExit(f"{name}: ожидалось число, получено {raw!r}")


def normalize_channel(raw: str) -> str:
    """Приводит '@name', 'https://t.me/name', 'name/' к единому виду 'name'."""
    value = raw.strip()
    value = re.sub(r"^https?://t\.me/", "", value, flags=re.IGNORECASE)
    value = value.lstrip("@").rstrip("/")
    return value


def entity_arg(channel: str) -> Any:
    """Telethon: числовые id передаём как int, остальное — как username."""
    return int(channel) if re.fullmatch(r"-?\d+", channel) else channel


def normalize_text(text: str) -> str:
    text = (text or "").replace("\u00a0", " ").replace("\u202f", " ")
    text = text.replace("ё", "е").replace("Ё", "Е")
    text = re.sub(f"[{re.escape(DASHES)}]", "-", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def compile_skip_patterns(keywords: Iterable[str]) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for raw in keywords:
        keyword = normalize_text(raw)
        if not keyword:
            continue
        body = r"\s+".join(re.escape(part) for part in keyword.split())
        # (?<!\\w) — не матчим внутри других слов, но ловим словоформы:
        # "стрим" -> "стример", "стримы"; "донат" -> "донатить".
        patterns.append(re.compile(rf"(?<!\w){body}"))
    return patterns


def load_skip_patterns() -> list[re.Pattern[str]]:
    path = os.environ.get("TG_SKIP_KEYWORDS_FILE", "").strip()
    if not path:
        return compile_skip_patterns(DEFAULT_SKIP_KEYWORDS)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            keywords = [line.strip() for line in fh if line.strip() and not line.startswith("#")]
        LOG.info("Стоп-слова загружены из %s (%d шт.)", path, len(keywords))
        return compile_skip_patterns(keywords)
    except OSError:
        LOG.exception("Не удалось прочитать %s, использую встроенный список", path)
        return compile_skip_patterns(DEFAULT_SKIP_KEYWORDS)


def skip_reason(caption: str, patterns: Sequence[re.Pattern[str]]) -> str | None:
    text = normalize_text(caption)
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return match.group(0)
    return None


@dataclass
class Config:
    api_id: int
    api_hash: str
    tg_session: str
    site_url: str
    site_key: str
    channels: list[str]
    limit: int
    delay: float
    timeout: float
    max_attempts: int
    max_total_bytes: int
    max_image_side: int
    tmp_dir: str
    state_file: str
    skip_patterns: list[re.Pattern[str]]
    expect_json_ok: bool
    dry_run: bool


def load_config(args: argparse.Namespace) -> Config:
    channels_raw = args.channels or os.environ.get("TG_CHANNELS", "rf4map")
    channels = [normalize_channel(ch) for ch in channels_raw.split(",") if ch.strip()]
    if not channels:
        raise SystemExit("Список каналов пуст (TG_CHANNELS / --channels)")

    tmp_dir = os.environ.get("TG_TMP_DIR", "").strip() or os.path.join(
        tempfile.gettempdir(), "tg_widget_images"
    )
    state_file = args.state_file or os.environ.get("TG_STATE_FILE", "").strip() or "state.json"

    return Config(
        api_id=env_int("TG_API_ID", 0),
        api_hash=env("TG_API_HASH", required=True),
        tg_session=env("TG_SESSION", required=True),
        site_url=env("SITE_IMPORT_URL", required=True),
        site_key=env("SITE_KEY", required=True),
        channels=channels,
        limit=args.limit if args.limit is not None else env_int("TG_LIMIT", 100),
        delay=env_float("TG_DELAY", 10.0),
        timeout=env_float("TG_TIMEOUT", 180.0),
        max_attempts=max(1, env_int("TG_MAX_ATTEMPTS", 5)),
        max_total_bytes=int(env_float("TG_MAX_TOTAL_MB", 40.0) * 1024 * 1024),
        max_image_side=env_int("TG_MAX_IMAGE_SIDE", 1600),
        tmp_dir=tmp_dir,
        state_file=state_file,
        skip_patterns=load_skip_patterns(),
        expect_json_ok=env("SITE_EXPECT_JSON_OK", "0").strip() == "1",
        dry_run=bool(args.dry_run),
    )


# --------------------------------------------------------------------------- #
# State (прогресс по каналам)
# --------------------------------------------------------------------------- #

def load_state(path: str) -> dict[str, int]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return {str(k): int(v) for k, v in data.items()}
    except FileNotFoundError:
        return {}
    except Exception:
        LOG.exception("Не удалось прочитать state-файл %s — начинаю с нуля", path)
        return {}


def save_state(path: str, state: dict[str, int]) -> None:
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        LOG.exception("Не удалось сохранить state-файл %s", path)


# --------------------------------------------------------------------------- #
# Модели данных
# --------------------------------------------------------------------------- #

@dataclass
class Candidate:
    channel: str
    messages: list[Any] = field(default_factory=list)
    rep_id: int = 0          # ключ дедупликации на сайте (min id в альбоме)
    last_id: int = 0         # до какого id двигать state (max id в альбоме)
    caption: str = ""


@dataclass
class MediaFile:
    path: str
    msg_id: int


@dataclass
class PreparedFile:
    path: str
    filename: str
    mime: str
    size: int


# --------------------------------------------------------------------------- #
# Временные файлы
# --------------------------------------------------------------------------- #

def cleanup_paths(paths: Iterable[str]) -> None:
    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except OSError as exc:
            LOG.warning("Не удалось удалить временный файл %s: %s", path, exc)


def cleanup_stale(tmp_dir: str, hours: int) -> None:
    """Убирает сироты от прошлых упавших прогонов (только свой tmp-каталог)."""
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
        LOG.info("Удалено старых временных файлов: %d", removed)


# --------------------------------------------------------------------------- #
# Изображения
# --------------------------------------------------------------------------- #

def guess_image_mime(path: str) -> tuple[str, str]:
    with open(path, "rb") as fh:
        head = fh.read(16)
    if head.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", ".jpg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp", ".webp"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif", ".gif"
    return "application/octet-stream", ""


def optimize_image(path: str, max_side: int) -> str:
    """Уменьшает и/или перекодирует фото. Возвращает путь к актуальному файлу."""
    if not HAVE_PIL or max_side <= 0:
        return path
    try:
        resample = getattr(getattr(Image, "Resampling", None), "LANCZOS", None)
        if resample is None:
            resample = getattr(Image, "LANCZOS", 1)

        with Image.open(path) as img:
            img.load()
            width, height = img.size
            too_big = max(width, height) > max_side or os.path.getsize(path) > 2 * 1024 * 1024
            if not too_big:
                return path

            converted = img.convert("RGB") if img.mode not in ("RGB", "L") else img
            converted.thumbnail((max_side, max_side), resample)

            out_path = os.path.splitext(path)[0] + ".jpg"
            converted.save(out_path, "JPEG", quality=85, optimize=True)

        if out_path != path:
            try:
                os.remove(path)
            except OSError:
                pass
        return out_path
    except Exception:
        LOG.exception("Не удалось оптимизировать %s — отправляю как есть", path)
        return path


def prepare_files(files: Sequence[MediaFile], channel: str, max_side: int) -> list[PreparedFile]:
    safe_channel = re.sub(r"[^\w.-]", "_", channel) or "channel"
    prepared: list[PreparedFile] = []
    for media in files:
        path = optimize_image(media.path, max_side)
        media.path = path
        mime, ext = guess_image_mime(path)
        prepared.append(
            PreparedFile(
                path=path,
                filename=f"{safe_channel}_{media.msg_id}{ext}",
                mime=mime,
                size=os.path.getsize(path),
            )
        )
    return prepared


# --------------------------------------------------------------------------- #
# Telegram: сбор и скачивание
# --------------------------------------------------------------------------- #

def build_candidates(channel: str, messages: Sequence[Any]) -> list[Candidate]:
    buckets: dict[Any, list[Any]] = {}
    for msg in messages:
        if getattr(msg, "photo", None) is None:
            continue
        gid = getattr(msg, "grouped_id", None)
        key = gid if gid is not None else f"single:{msg.id}"
        buckets.setdefault(key, []).append(msg)

    candidates: list[Candidate] = []
    for group in buckets.values():
        group.sort(key=lambda m: m.id)
        caption = ""
        for msg in group:
            text = (getattr(msg, "caption", None) or getattr(msg, "message", None) or "").strip()
            if text:
                caption = str(text)
                break
        candidates.append(
            Candidate(
                channel=channel,
                messages=list(group),
                rep_id=group[0].id,
                last_id=group[-1].id,
                caption=caption,
            )
        )
    return candidates


def complete_oldest_album(
    client: TelegramClient, channel: str, messages: list[Any], limit: int
) -> list[Any]:
    """
    Лимит iter_messages может разрезать альбом на границе окна.
    Если самый старый полученный пост — часть альбома, дочитываем остальные части
    (в Telegram альбом не длиннее 10 элементов и идёт подряд по id).
    """
    if len(messages) < limit or not messages:
        return messages

    oldest = min(messages, key=lambda m: m.id)
    gid = getattr(oldest, "grouped_id", None)
    if gid is None:
        return messages

    have = {m.id for m in messages}
    extra_ids = [oldest.id + offset for offset in range(1, 10)]
    try:
        extra = client.get_messages(entity_arg(channel), ids=extra_ids)
    except FloodWaitError as exc:
        LOG.warning("FloodWait %s c при дочитывании альбома в %s", exc.seconds, channel)
        return messages
    except Exception:
        LOG.exception("Не удалось дочитать альбом в %s", channel)
        return messages

    added = 0
    for msg in extra or []:
        if msg is None or msg.id in have:
            continue
        if getattr(msg, "grouped_id", None) == gid and getattr(msg, "photo", None) is not None:
            messages.append(msg)
            have.add(msg.id)
            added += 1
    if added:
        LOG.debug("Альбом в %s дочитан: +%d фото", channel, added)
    return messages


def download_group(client: TelegramClient, cfg: Config, cand: Candidate) -> list[MediaFile]:
    safe_channel = re.sub(r"[^\w.-]", "_", cand.channel) or "channel"
    files: list[MediaFile] = []
    for msg in cand.messages:
        path = os.path.join(cfg.tmp_dir, f"{safe_channel}_{msg.id}.bin")
        try:
            result = client.download_media(msg, file=path)
        except FloodWaitError as exc:
            LOG.warning("FloodWait %s c на скачивании id=%s", exc.seconds, msg.id)
            time.sleep(exc.seconds + 1)
            try:
                result = client.download_media(msg, file=path)
            except Exception:
                LOG.exception("Повторное скачивание не удалось: id=%s", msg.id)
                continue
        except Exception:
            LOG.exception("Не удалось скачать фото id=%s (%s)", msg.id, cand.channel)
            continue

        if result and os.path.exists(result):
            files.append(MediaFile(path=result, msg_id=msg.id))
        else:
            LOG.warning("download_media вернул %r для id=%s", result, msg.id)
    return files


# --------------------------------------------------------------------------- #
# Отправка на сайт
# --------------------------------------------------------------------------- #

def response_is_ok(resp: requests.Response, expect_json_ok: bool) -> tuple[bool, str]:
    body = (resp.text or "")[:1000]
    if not 200 <= resp.status_code < 300:
        return False, body
    if not expect_json_ok:
        return True, body
    try:
        payload = resp.json()
    except ValueError:
        return False, f"ожидался JSON, получено: {body}"
    if isinstance(payload, dict):
        for key in ("ok", "success"):
            if key in payload:
                return bool(payload[key]), body
        for key in ("status", "result"):
            if key in payload:
                return str(payload[key]).lower() in ("ok", "success", "true"), body
    return False, f"в JSON нет ok/success: {body}"


def sleep_backoff(attempt: int, retry_after: float | None = None) -> None:
    if retry_after is not None:
        delay = min(retry_after, 300.0)
    else:
        delay = min(60.0, 2.0 ** attempt) + random.uniform(0.0, 1.5)
    LOG.debug("Пауза перед повтором: %.1f c", delay)
    time.sleep(delay)


def send_post(
    session: requests.Session, cfg: Config, cand: Candidate, files: Sequence[MediaFile]
) -> str:
    """
    Возвращает:
        "ok"        — принято сайтом;
        "permanent" — постоянная ошибка, повторять бессмысленно;
        "temporary" — временная ошибка, надо повторить в следующем прогоне.
    """
    prepared = prepare_files(files, cand.channel, cfg.max_image_side)
    if not prepared:
        LOG.warning("[%s] id=%s: не удалось подготовить ни одного файла", cand.channel, cand.rep_id)
        return "permanent"

    total = sum(item.size for item in prepared)
    if total > cfg.max_total_bytes:
        LOG.error(
            "[%s] id=%s: суммарный размер %.1f МБ превышает лимит %.1f МБ — пропуск",
            cand.channel,
            cand.rep_id,
            total / 1024 / 1024,
            cfg.max_total_bytes / 1024 / 1024,
        )
        return "permanent"

    data = {
        "key": cfg.site_key,
        "text": cand.caption,
        "telegram_msg_id": f"{cand.channel}_{cand.rep_id}",
    }

    for attempt in range(1, cfg.max_attempts + 1):
        try:
            handles: list[Any] = []
            multipart: list[tuple[str, tuple[str, Any, str]]] = []
            try:
                for item in prepared:
                    handle = open(item.path, "rb")
                    handles.append(handle)
                    multipart.append(("photos[]", (item.filename, handle, item.mime)))
                resp = session.post(
                    cfg.site_url,
                    data=data,
                    files=multipart,
                    timeout=(15.0, cfg.timeout),
                )
            finally:
                for handle in handles:
                    try:
                        handle.close()
                    except Exception:
                        pass
        except requests.RequestException as exc:
            LOG.warning(
                "[%s] id=%s: сеть/таймаут (попытка %s/%s): %s",
                cand.channel,
                cand.rep_id,
                attempt,
                cfg.max_attempts,
                exc,
            )
            if attempt < cfg.max_attempts:
                sleep_backoff(attempt)
            continue

        ok, body = response_is_ok(resp, cfg.expect_json_ok)
        if ok:
            LOG.info(
                "[%s] id=%s: отправлено, фото=%d, %.1f МБ, HTTP %s, ответ: %s",
                cand.channel,
                cand.rep_id,
                len(prepared),
                total / 1024 / 1024,
                resp.status_code,
                body,
            )
            return "ok"

        if resp.status_code in (400, 401, 403, 404, 405, 409, 413, 422):
            LOG.error(
                "[%s] id=%s: постоянная ошибка HTTP %s: %s",
                cand.channel,
                cand.rep_id,
                resp.status_code,
                body,
            )
            return "permanent"

        LOG.warning(
            "[%s] id=%s: временная ошибка HTTP %s (попытка %s/%s): %s",
            cand.channel,
            cand.rep_id,
            resp.status_code,
            attempt,
            cfg.max_attempts,
            body,
        )
        if attempt < cfg.max_attempts:
            retry_after: float | None = None
            header = resp.headers.get("Retry-After", "")
            if header.strip().isdigit():
                retry_after = float(header.strip())
            sleep_backoff(attempt, retry_after)

    return "temporary"


# --------------------------------------------------------------------------- #
# Основной цикл
# --------------------------------------------------------------------------- #

def advance(state: dict[str, int], cfg: Config, cand: Candidate) -> None:
    if cand.last_id > state.get(cand.channel, 0):
        state[cand.channel] = cand.last_id
        save_state(cfg.state_file, state)


def handle_candidate(
    client: TelegramClient,
    session: requests.Session,
    cfg: Config,
    state: dict[str, int],
    cand: Candidate,
    stats: Counter,
) -> str:
    """Возвращает 'continue' или 'break' (прервать канал из-за временной ошибки)."""
    prefix = f"[{cand.channel}] id={cand.rep_id}"

    if not cand.caption:
        LOG.info("%s: пустая подпись — пропуск", prefix)
        stats["skipped_empty"] += 1
        if not cfg.dry_run:
            advance(state, cfg, cand)
        return "continue"

    reason = skip_reason(cand.caption, cfg.skip_patterns)
    if reason:
        LOG.info("%s: стоп-слово %r — пропуск", prefix, reason)
        stats["skipped_filter"] += 1
        if not cfg.dry_run:
            advance(state, cfg, cand)
        return "continue"

    if cfg.dry_run:
        LOG.info("%s: [DRY-RUN] отправил бы фото=%d, текст=%r",
                 prefix, len(cand.messages), cand.caption[:120])
        stats["dry_run"] += 1
        return "continue"

    files = download_group(client, cfg, cand)
    if not files:
        LOG.warning("%s: ни одного фото не скачалось — пропуск", prefix)
        stats["skipped_no_files"] += 1
        advance(state, cfg, cand)
        return "continue"

    try:
        result = send_post(session, cfg, cand, files)
    finally:
        cleanup_paths(media.path for media in files)

    if result == "ok":
        stats["sent"] += 1
        advance(state, cfg, cand)
        return "continue"

    if result == "permanent":
        stats["failed_permanent"] += 1
        advance(state, cfg, cand)  # повторять бессмысленно — не блокируем канал
        return "continue"

    stats["failed_temporary"] += 1
    LOG.error("%s: временная ошибка — останавливаю канал до следующего прогона", prefix)
    return "break"


def process_channel(
    client: TelegramClient,
    session: requests.Session,
    cfg: Config,
    state: dict[str, int],
    channel: str,
    stats: Counter,
) -> None:
    min_id = int(state.get(channel, 0))
    if cfg.limit <= 0:
        LOG.warning("Канал %s: лимит <= 0, пропуск", channel)
        return

    LOG.info("Канал %s: сообщения новее id=%s, лимит=%s", channel, min_id, cfg.limit)
    try:
        messages = list(client.iter_messages(entity_arg(channel), limit=cfg.limit, min_id=min_id))
    except FloodWaitError as exc:
        LOG.warning("Канал %s: FloodWait %s c — пропуск до следующего прогона", channel, exc.seconds)
        stats["floodwait"] += 1
        return
    except Exception:
        LOG.exception("Канал %s: не удалось получить сообщения", channel)
        stats["channel_errors"] += 1
        return

    if not messages:
        LOG.info("Канал %s: новых сообщений нет", channel)
        return

    messages = complete_oldest_album(client, channel, messages, cfg.limit)
    candidates = sorted(build_candidates(channel, messages), key=lambda c: c.rep_id)
    LOG.info("Канал %s: к обработке %d пост(ов)", channel, len(candidates))

    for index, cand in enumerate(candidates):
        outcome = handle_candidate(client, session, cfg, state, cand, stats)
        if outcome == "break":
            return
        if not cfg.dry_run and index < len(candidates) - 1:
            time.sleep(max(0.0, cfg.delay) + random.uniform(0.0, 1.0))


def run(cfg: Config) -> int:
    os.makedirs(cfg.tmp_dir, exist_ok=True)
    cleanup_stale(cfg.tmp_dir, env_int("TG_TMP_CLEANUP_HOURS", 24))

    state = load_state(cfg.state_file)
    stats: Counter = Counter()

    LOG.info(
        "Старт: каналов=%d, лимит=%d, задержка=%.1f c, ресайз=%s, dry-run=%s",
        len(cfg.channels),
        cfg.limit,
        cfg.delay,
        cfg.max_image_side if HAVE_PIL else "нет Pillow",
        cfg.dry_run,
    )

    session = requests.Session()
    session.headers.update({"User-Agent": "tg-widget-importer/2.0"})

    try:
        with TelegramClient(StringSession(cfg.tg_session), cfg.api_id, cfg.api_hash) as client:
            for channel in cfg.channels:
                process_channel(client, session, cfg, state, channel, stats)
    except Exception:
        LOG.exception("Фатальная ошибка при работе с Telegram")
        return 1
    finally:
        session.close()

    LOG.info(
        "Итог: отправлено=%d, пропущено(фильтр)=%d, пропущено(пусто)=%d, "
        "пропущено(нет фото)=%d, ошибки(постоянные)=%d, ошибки(временные)=%d, "
        "dry-run=%d, каналов с ошибками=%d",
        stats["sent"],
        stats["skipped_filter"],
        stats["skipped_empty"],
        stats["skipped_no_files"],
        stats["failed_permanent"],
        stats["failed_temporary"],
        stats["dry_run"],
        stats["channel_errors"],
    )
    return 1 if (stats["failed_temporary"] or stats["channel_errors"]) else 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Импорт постов из Telegram на сайт")
    parser.add_argument("--dry-run", action="store_true", help="ничего не отправлять и не двигать state")
    parser.add_argument("--limit", type=int, default=None, help="сообщений на канал (перебивает TG_LIMIT)")
    parser.add_argument("--channels", default=None, help="каналы через запятую (перебивает TG_CHANNELS)")
    parser.add_argument("--state-file", default=None, help="путь к state-файлу (перебивает TG_STATE_FILE)")
    parser.add_argument("-v", "--verbose", action="store_true", help="подробный лог")
    return parser.parse_args(argv)


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)
    cfg = load_config(args)
    return run(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
