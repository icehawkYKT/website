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
