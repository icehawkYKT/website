#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер наживок rf4-stat -> таблица baits_records.

Что поменялось на rf4-stat (проверено 27.11.2026):
  1) Общий список /baits/?days=1&hours=0 — это ТОП-100 строк на весь сайт
     (у Комарного там 6 строк, а на странице водоёма — 45). Для наживок по
     водоёмам он не годится.
  2) Страница водоёма отдаётся обычным GET:
         /baits/?days=1&hours=0&location=оз.%20Комариное
     Старый POST ajax=1&page=N сейчас возвращает пустой ответ, page=2 тоже пусто:
     у водоёма ровно одна страница (до 100 строк).
  3) Периода (days/hours) понимает только www-домен: rf4-stat.ru/?days=1 -> 302
     на www и параметры теряются. Ходим сразу на https://www.rf4-stat.ru.
  4) Картинки теперь абсолютные (cdn.rf4spot.com / cdn2.rf4spot.com).

Отдельно: с датацентровых IP (GitHub Actions) сайт может отдавать страницу без
селекта водоёмов, поэтому список имён берётся по цепочке:
    селект на сайте -> locations.rf4_stat_name из БД -> встроенный список.

Запуск:
    python parser_baits_rf4.py                    # период 1 день, пишет в БД
    python parser_baits_rf4.py --days 7
    python parser_baits_rf4.py --dry-run          # только спарсить, БД не трогать
    python parser_baits_rf4.py --names-source builtin   # не ходить за списком на сайт

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

# Список снят с сайта 27.11.2026 (#finderShareLocation).
DEFAULT_LOCATIONS = [
    "оз. Комариное", "оз. Лосиное", "р. Вьюнок", "оз. Старый Острог", "р. Белая",
    "оз. Куори", "оз. Медвежье", "р. Волхов", "р. Северский Донец", "р. Сура",
    "Ладожское оз.", "оз. Янтарное", "Ладожский архипелаг", "р. Ахтуба",
    "оз. Медное", "р. Нижняя Тунгуска", "р. Яма", "Норвежское море",
    "Штрафной пруд",
]

# Поправки на случай, если в БД сохранено короткое имя.
NAME_ALIASES = {
    "ладожское": "Ладожское оз.",
    "ладожское озеро": "Ладожское оз.",
}


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


def page_info(resp, html):
    """Короткая сводка ответа — чтобы в логе было видно, что именно пришло."""
    low = html.lower()
    marks = []
    for m in ("captcha", "cloudflare", "access denied", "too many requests",
              "please wait", "проверка браузера", "не робот", "заблокирован",
              "forbidden", "temporarily unavailable"):
        if m in low:
            marks.append(m)
    return {
        "status": resp.status_code,
        "url": resp.url,
        "len": len(html),
        "select": 'id="findersharelocation"' in low,
        "table": "водоем" in low,
        "marks": marks,
        "snippet": norm_space(html[:200]),
    }


# ------------------------------------------------------------------ парсинг

def parse_rows(html, expected_location=None):
    """
    Текущая вёрстка строки:
        <td class="mobile-location"><a>оз. Комариное</a></td>
        <td class="bait-icon"><img src="..."></td>
        <td>Выползок</td>
        <td>Улов: 30</td>
        <td>Посты: 3</td>
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

    if skipped and DEBUG:
        print(f"      пропущено: {dict(skipped)}")
    return out


# ------------------------------------------------------------------ имена водоёмов

def names_from_select(session, days, hours):
    r = session.get(BAITS_URL, params={"days": days, "hours": hours},
                    headers=HEADERS, timeout=40)
    html = r.text or ""
    info = page_info(r, html)
    print(f"    страница списка: HTTP {info['status']}, {info['len']} симв., "
          f"селект={'да' if info['select'] else 'НЕТ'}"
          + (f", признаки: {', '.join(info['marks'])}" if info["marks"] else ""))
    if not info["select"]:
        print(f"    начало ответа: {info['snippet'][:160]}")

    soup = BeautifulSoup(html, "html.parser")
    sel = soup.select_one("#finderShareLocation")
    if not sel:
        return [], info
    names = [norm_space(o.get_text(strip=True)) for o in sel.select("option") if o.get("value")]
    return names, info


def names_from_db(cur):
    cur.execute("SELECT DISTINCT rf4_stat_name FROM locations "
                "WHERE rf4_stat_name IS NOT NULL AND rf4_stat_name <> ''")
    raw = [norm_space(r["rf4_stat_name"]) for r in cur.fetchall()]
    names = []
    for n in raw:
        n = NAME_ALIASES.get(n.lower(), n)
        if n not in names:
            names.append(n)
    return names


def resolve_names(session, days, hours, source, cur):
    """Возвращает (names, где_взяли)."""
    if source in ("auto", "select"):
        names, _ = names_from_select(session, days, hours)
        if names:
            return names, "селект на сайте"
        if source == "select":
            raise RuntimeError("селект не найден, а --names-source select запрещает фолбэк")

    if source in ("auto", "db") and cur is not None:
        names = names_from_db(cur)
        if names:
            return names, f"БД ({len(names)} шт.)"

    if source == "db":
        raise RuntimeError("в locations.rf4_stat_name пусто, а --names-source db запрещает фолбэк")

    return list(DEFAULT_LOCATIONS), "встроенный список"


# ------------------------------------------------------------------ загрузка

def fetch_location(session, name, days, hours, retries=3):
    params = {"days": int(days), "hours": int(hours), "location": name}
    for attempt in range(1, retries + 1):
        try:
            r = session.get(BAITS_URL, params=params, headers=HEADERS, timeout=40)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            return parse_rows(r.text, expected_location=name)
        except Exception as e:
            if attempt < retries:
                print(f"      попытка {attempt}/{retries}: {e}")
                time.sleep(2 * attempt)
            else:
                print(f"      не удалось: {e}")
    return []


def fetch_all(session, days, hours, names, sleep_between=0.3):
    all_rows = []
    per_loc = {}
    print(f"\n  обход водоёмов (период {days} д. {hours} ч.):")

    for name in names:
        rows = fetch_location(session, name, days, hours)

        if not rows:
            # пробуем без префикса: «оз. Комариное» -> «Комариное»
            alt = norm_name(name)
            if alt and alt != name.lower():
                probe = name
                # восстановим привычный вид: если в имени был префикс — пробуем другой
                if name.lower().startswith(("оз.", "р.")):
                    other = ("р. " if name.lower().startswith("оз.") else "оз. ") + alt
                    rows = fetch_location(session, other, days, hours)
                    probe = other
                if rows:
                    print(f"    {name:24} сработало как '{probe}'")

        uniq = {}
        for _, bait, img, rec in rows:
            key = norm_name(bait)
            if key not in uniq or rec > uniq[key][3]:
                uniq[key] = (name, bait, img, rec)
        rows = list(uniq.values())

        per_loc[name] = len(rows)
        all_rows.extend(rows)
        print(f"    {name:24} {len(rows):3}")
        time.sleep(sleep_between)

    print(f"\n  всего строк: {len(all_rows)} | водоёмов с данными: "
          f"{sum(1 for v in per_loc.values() if v)} из {len(names)}")
    return all_rows, per_loc


# ------------------------------------------------------------------ БД

def db_connect():
    if pymysql is None:
        raise RuntimeError("pymysql не установлен: pip install pymysql")
    cfg = {k: os.environ.get(k) for k in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME")}
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError("не заданы переменные окружения: " + ", ".join(missing))

    print(f"  цель: {cfg['DB_USER']}@{cfg['DB_HOST']}:{cfg['DB_PORT']}/{cfg['DB_NAME']}")
    return pymysql.connect(
        host=cfg["DB_HOST"], port=int(cfg["DB_PORT"]), user=cfg["DB_USER"],
        password=cfg["DB_PASS"], database=cfg["DB_NAME"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor, autocommit=False,
        connect_timeout=15,
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
    ap.add_argument("--names-source", choices=["auto", "select", "db", "builtin"],
                    default="auto", help="откуда брать список водоёмов")
    ap.add_argument("--dry-run", action="store_true", help="только спарсить, БД не трогать")
    args = ap.parse_args()

    session = requests.Session()

    print(f"[1/4] rf4-stat, период {args.days} д. {args.hours} ч.")
    print("  список водоёмов:")

    conn = None
    cur = None
    if not args.dry_run or args.names_source in ("db",):
        print("  подключаюсь к БД (нужна для записи/списка)")
        conn = db_connect()
        cur = conn.cursor()

    try:
        names, source = resolve_names(session, args.days, args.hours, args.names_source, cur)
        print(f"  взято из: {source} — {len(names)} водоёмов")

        rows, per_loc = fetch_all(session, args.days, args.hours, names)

        if not rows:
            raise RuntimeError(
                "не получено ни одной строки: похоже, rf4-stat блокирует IP "
                "или снова сменил вёрстку. Таблицу не трогаю (см. диагностику выше)."
            )

        if args.dry_run:
            print("\n--dry-run, примеры:")
            for r in rows[:8]:
                print("   ", r[0], "|", r[1], "|", r[3], "|", r[2][-40:])
            return

        if len(rows) < args.min_rows:
            raise RuntimeError(
                f"строк {len(rows)} < порога {args.min_rows}: похоже на смену вёрстки "
                "или неполную выдачу. Таблицу не трогаю."
            )

        print("\n[2/4] локации в БД")
        if cur is None:
            conn = db_connect()
            cur = conn.cursor()

        cur.execute("SELECT id, name, rf4_stat_name FROM locations ORDER BY id")
        locs = cur.fetchall()
        print(f"  локаций: {len(locs)}")

        rf4_by_norm = {norm_name(n): n for n in names}
        to_insert = []
        unresolved = []
        empty = []

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
            if not got:
                empty.append(db_name)
            for _, bait, img, rec in got:
                to_insert.append((loc_id, bait, img, rec))

        conn.commit()

        if unresolved:
            print("  без пары на rf4-stat:", ", ".join(unresolved))
        if empty:
            print("  без данных за период:", ", ".join(empty))

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

        cur.execute("SELECT COUNT(*) AS n, MIN(date_created) AS mn, MAX(date_created) AS mx "
                    "FROM baits_records")
        chk = cur.fetchone()
        print(f"\n[4/4] готово: baits_records = {int(chk['n'])} строк, "
              f"date_created {chk['mn']} .. {chk['mx']}")

    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        if conn:
            conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nОШИБКА:", e)
        sys.exit(1)
