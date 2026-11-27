#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер наживок rf4-stat -> таблица baits_records.

Что поменялось на rf4-stat (проверено 27.11.2026) и почему старый код встал:

  1) Общий список /baits/?days=1&hours=0 — это ТОП-100 строк на весь сайт
     (100 строк, ~17 водоёмов, низ таблицы обрезан). Для наживок по водоёмам он не годится:
     например, у Комарного там 6 строк, а на странице водоёма — 45.

  2) Страница водоёма отдаётся обычным GET:
         /baits/?days=1&hours=0&location=оз.%20Комариное
     Раньше работал POST ajax=1&page=N — сейчас он отдаёт пустой ответ,
     а page=2 и дальше тоже пусто: у водоёма ровно одна страница (до 100 строк).

  3) Названия водоёмов больше не надо угадывать («р. X» / «оз. X»):
     они лежат в <select id="finderShareLocation"> на самой странице /baits/.
     Берём оттуда — это официальный список сайта, включая «Ладожское оз.» и «Штрафной пруд».

  4) Параметры периода (days/hours) сервер понимает только на www-домене:
     rf4-stat.ru/?days=1 -> 302 -> www.rf4-stat.ru/... и параметры теряются.
     Поэтому ходим сразу на https://www.rf4-stat.ru.

  5) Картинки теперь абсолютные (cdn.rf4spot.com / cdn2.rf4spot.com) —
     проверка «ссылка начинается с /images/rf4game/» больше не срабатывает.

Запуск:
    python parser_baits_rf4.py                  # период 1 день, пишет в БД
    python parser_baits_rf4.py --days 7
    python parser_baits_rf4.py --dry-run        # только спарсить и показать, БД не трогать

Переменные окружения: DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME.
Опционально: BAITS_DAYS, BAITS_HOURS, MIN_ROWS (порог защиты), DEBUG_RF4_STAT=1.
"""

import argparse
import os
import re
import sys
import time
from collections import Counter
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

try:
    import pymysql
except ImportError:
    pymysql = None


WWW = "https://www.rf4-stat.ru"
BAITS_URL = WWW + "/baits/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0 Safari/537.36 NotesFisherBot/1.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

DEBUG = os.environ.get("DEBUG_RF4_STAT", "0") == "1"
DEFAULT_MIN_ROWS = int(os.environ.get("MIN_ROWS", 100))


# ------------------------------------------------------------------ утилиты

_PREFIXES = ("оз.", "р.", "г.", "п.", "вдхр.", "водохр.")
_SUFFIXES = ("оз.", "р.", "г.", "вдхр.")


def norm_space(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def norm_name(s):
    """'оз. Комариное' == 'Комариное' == 'Комариное оз.'"""
    s = norm_space(s).lower().replace("ё", "е")
    s = re.sub(r"[«»\"'`]", "", s)
    changed = True
    while changed:
        changed = False
        for p in _PREFIXES:
            if s.startswith(p):
                s = s[len(p):].strip()
                changed = True
        for p in _SUFFIXES:
            if s.endswith(p):
                s = s[: -len(p)].strip()
                changed = True
    return norm_space(s)


def to_int(s):
    d = re.sub(r"\D+", "", s or "")
    return int(d) if d else 0


def abs_img(src):
    src = (src or "").strip()
    if not src or src.startswith("data:"):
        return ""
    if src.startswith("//"):
        src = "https:" + src
    elif not src.startswith("http"):
        src = WWW + "/" + src.lstrip("/")
    return src.replace("//img.rf4spot.com/", "//cdn.rf4spot.com/")


# ------------------------------------------------------------------ парсинг

def parse_rows(html, expected_location=None):
    """
    Разбирает таблицу страницы водоёма.
    Текущая вёрстка строки:
        <td class="mobile-location"><a>оз. Комариное</a></td>
        <td class="bait-icon"><img src="..."></td>
        <td>Выползок</td>
        <td>Улов: 30</td>
        <td>Посты: 3</td>
        ...
    """
    soup = BeautifulSoup(html, "html.parser")
    out = []
    skipped = Counter()

    for tr in soup.select("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        loc_cell = tr.select_one(".mobile-location") or tds[0]
        loc = norm_space(loc_cell.get_text(" ", strip=True))
        if not loc or loc.lower() == "водоем":
            continue

        img_tag = tds[1].find("img")
        img = abs_img(img_tag.get("src") if img_tag else "")

        bait = norm_space(tds[2].get_text(" ", strip=True))
        if not bait:
            skipped["нет наживки"] += 1
            continue

        records = to_int(tds[3].get_text(" ", strip=True))
        if records <= 0:
            m = re.search(r"Улов\s*:?\s*([\d\s]+)", norm_space(tr.get_text(" ", strip=True)), re.I)
            records = to_int(m.group(1)) if m else 0
        if records <= 0:
            skipped["нет улова"] += 1
            continue

        out.append((expected_location or loc, bait, img, records))

    if skipped:
        print(f"      пропущено: {dict(skipped)}")
    return out


def get_location_names(session, days, hours):
    """Официальный список водоёмов из селекта на /baits/."""
    r = session.get(BAITS_URL, params={"days": days, "hours": hours},
                    headers=HEADERS, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    sel = soup.select_one("#finderShareLocation")
    names = []
    if sel:
        names = [norm_space(o.get_text(strip=True)) for o in sel.select("option") if o.get("value")]

    print(f"  водоёмов на сайте: {len(names)}")
    if not names:
        raise RuntimeError("не нашёл селект водоёмов (#finderShareLocation) — вёрстка снова поменялась")
    return names


def fetch_location(session, name, days, hours, retries=3):
    params = {"days": int(days), "hours": int(hours), "location": name}
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(BAITS_URL, params=params, headers=HEADERS, timeout=40)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            rows = parse_rows(r.text, expected_location=name)
            return rows
        except Exception as e:
            last = e
            print(f"      попытка {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(2 * attempt)
    print(f"      не удалось: {last}")
    return []


def fetch_all(session, days, hours, sleep_between=0.3):
    names = get_location_names(session, days, hours)
    all_rows = []
    per_loc = {}

    print(f"\n  обход водоёмов (период {days} д. {hours} ч.):")
    for name in names:
        rows = fetch_location(session, name, days, hours)
        # на всякий случай убираем дубли по наживке внутри водоёма
        uniq = {}
        for _, bait, img, rec in rows:
            key = norm_name(bait)
            if key not in uniq or rec > uniq[key][3]:
                uniq[key] = (name, bait, img, rec)
        rows = list(uniq.values())
        per_loc[name] = len(rows)
        all_rows.extend(rows)
        time.sleep(sleep_between)
        print(f"    {name:24} {len(rows):3}")

    print(f"\n  всего строк: {len(all_rows)} | водоёмов с данными: "
          f"{sum(1 for v in per_loc.values() if v)} из {len(names)}")
    return all_rows, per_loc, names


# ------------------------------------------------------------------ БД

def db_connect():
    if pymysql is None:
        raise RuntimeError("pymysql не установлен: pip install pymysql")
    cfg = {k: os.environ.get(k) for k in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME")}
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError("не заданы переменные окружения: " + ", ".join(missing))
    return pymysql.connect(
        host=cfg["DB_HOST"], port=int(cfg["DB_PORT"]), user=cfg["DB_USER"],
        password=cfg["DB_PASS"], database=cfg["DB_NAME"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor, autocommit=False,
    )


def match_location(db_name, rf4_names_by_norm):
    n = norm_name(db_name)
    if n in rf4_names_by_norm:
        return rf4_names_by_norm[n]
    for k, v in rf4_names_by_norm.items():
        if k.startswith(n + " ") or n.startswith(k + " ") or n in k or k in n:
            return v
    return None


def main():
    ap = argparse.ArgumentParser(description="Наживки rf4-stat -> baits_records")
    ap.add_argument("--days", type=int, default=int(os.environ.get("BAITS_DAYS", 1)))
    ap.add_argument("--hours", type=int, default=int(os.environ.get("BAITS_HOURS", 0)))
    ap.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS,
                    help="если строк меньше — не трогаем таблицу")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    session = requests.Session()
    print(f"[1/4] rf4-stat, период {args.days} д. {args.hours} ч.")
    rows, per_loc, names = fetch_all(session, args.days, args.hours)

    if args.dry_run:
        print("\n--dry-run, примеры:")
        for r in rows[:8]:
            print("   ", r[0], "|", r[1], "|", r[3], "|", r[2][-40:])
        return

    if len(rows) < args.min_rows:
        raise RuntimeError(
            f"строк {len(rows)} < порога {args.min_rows}: похоже на смену вёрстки. "
            "Таблицу не трогаю."
        )

    print("\n[2/4] БД")
    conn = db_connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, name, rf4_stat_name FROM locations ORDER BY id")
        locs = cur.fetchall()
        print(f"  локаций в БД: {len(locs)}")

        rf4_by_norm = {norm_name(n): n for n in names}
        to_insert = []
        unresolved = []

        for r in locs:
            loc_id = int(r["id"])
            db_name = norm_space(r["name"])
            target = match_location(db_name, rf4_by_norm)

            if not target:
                unresolved.append(db_name)
                continue

            if norm_space(r.get("rf4_stat_name") or "") != target:
                cur.execute("UPDATE locations SET rf4_stat_name = %s WHERE id = %s", (target, loc_id))

            got = [x for x in rows if x[0] == target]
            for _, bait, img, rec in got:
                to_insert.append((loc_id, bait, img, rec))

        conn.commit()

        if unresolved:
            print("  без пары на rf4-stat:", ", ".join(unresolved))

        print(f"\n[3/4] строк к записи: {len(to_insert)}")
        if not to_insert:
            raise RuntimeError("нечего писать — таблицу не трогаю")

        cur.execute("DROP TABLE IF EXISTS baits_records_tmp")
        cur.execute("CREATE TABLE baits_records_tmp LIKE baits_records")
        conn.commit()

        cur.executemany(
            "INSERT INTO baits_records_tmp (location_id, bait_name, image_url, records, date_created) "
            "VALUES (%s, %s, %s, %s, NOW()) "
            "ON DUPLICATE KEY UPDATE image_url = VALUES(image_url), records = VALUES(records)",
            to_insert,
        )
        conn.commit()

        cur.execute("SELECT COUNT(*) AS n FROM baits_records_tmp")
        tmp_n = int(cur.fetchone()["n"])
        print(f"  во временной таблице: {tmp_n}")
        if tmp_n <= 0:
            raise RuntimeError("временная таблица пустая — основную не трогаю")

        cur.execute("DROP TABLE IF EXISTS baits_records_old")
        conn.commit()
        cur.execute("RENAME TABLE baits_records TO baits_records_old, "
                    "baits_records_tmp TO baits_records")
        conn.commit()
        cur.execute("DROP TABLE IF EXISTS baits_records_old")
        conn.commit()

        cur.execute("SELECT COUNT(*) AS n FROM baits_records")
        print(f"\n[4/4] готово, в baits_records {int(cur.fetchone()['n'])} строк")

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nОШИБКА:", e)
        sys.exit(1)
