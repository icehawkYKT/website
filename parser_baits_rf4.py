import os
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pymysql


BASE_URL = "https://rf4-stat.ru"
BAITS_URL = f"{BASE_URL}/baits/"

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]

DEBUG_RF4_STAT = os.environ.get("DEBUG_RF4_STAT", "0") == "1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0 Safari/537.36 NotesFisherBot/1.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

_RF4_ALL_ROWS_CACHE = None


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def normalize_name(s: str) -> str:
    return normalize_space(s).lower().replace("ё", "е")


def get_records_int(s: str) -> int:
    digits = re.sub(r"\D+", "", s or "")
    return int(digits) if digits else 0


def get_img_url(tag):
    if not tag:
        return ""

    possible_attrs = [
        "src",
        "href",
        "data-src",
        "data-original",
        "data-lazy-src",
        "data-url",
        "data-original-src",
    ]

    src = ""

    for attr in possible_attrs:
        value = (tag.get(attr) or "").strip()
        if value:
            src = value
            break

    if not src:
        srcset = (tag.get("srcset") or "").strip()
        if srcset:
            src = srcset.split(",")[0].strip().split(" ")[0].strip()

    if not src:
        style = tag.get("style") or ""
        m = re.search(r"url\(['\"]?([^'\")]+)['\"]?\)", style)
        if m:
            src = m.group(1).strip()

    if not src:
        return ""

    if src.startswith("data:"):
        return ""

    if src.startswith("//"):
        return "https:" + src

    if src.startswith("http://") or src.startswith("https://"):
        return src

    return urljoin(BASE_URL, src)


def is_game_image_url(url: str) -> bool:
    if not url:
        return False

    u = url.lower()

    return (
        "img.rf4spot.com" in u
        or "/images/rf4game/" in u
        or "/i/images/rf4game/" in u
        or u.endswith((".png", ".jpg", ".jpeg", ".webp"))
    )


def find_image_url(container):
    if not container:
        return ""

    for tag in container.find_all(["img", "a"]):
        candidate_url = get_img_url(tag)

        if is_game_image_url(candidate_url):
            return candidate_url

    for tag in container.find_all(style=True):
        candidate_url = get_img_url(tag)

        if is_game_image_url(candidate_url):
            return candidate_url

    return ""


def parse_rows_from_html(html: str):
    """
    Возвращает список кортежей:
    (location_name, bait_name, image_url, records)

    Под текущую верстку rf4-stat:
    0 = водоем
    1 = колонка с картинками наживки
    2 = название наживки
    3 = улов

    Также оставлена поддержка запасного варианта.
    """
    soup = BeautifulSoup(html, "html.parser")
    parsed = []

    rows = soup.select("tr")

    for row in rows:
        cols = row.find_all("td")

        if len(cols) < 3:
            continue

        location_name = normalize_space(cols[0].get_text(" ", strip=True))

        if not location_name:
            continue

        image_url = ""
        bait_name = ""
        records = 0

        # Основная текущая схема rf4-stat:
        # Водоем | картинки | наживка | улов | посты | ...
        if len(cols) >= 4:
            image_col = cols[1]
            bait_col = cols[2]
            records_col_index = 3

            bait_name = normalize_space(bait_col.get_text(" ", strip=True))
            image_url = find_image_url(image_col)

            records = extract_records(row, cols, records_col_index)

        # Запасной вариант, если верстка снова станет другой:
        # Водоем | наживка с картинкой | улов | ...
        if (not bait_name or records <= 0) and len(cols) >= 3:
            bait_col = cols[1]
            records_col_index = 2

            fallback_bait_name = normalize_space(bait_col.get_text(" ", strip=True))
            fallback_image_url = find_image_url(bait_col) or find_image_url(row)
            fallback_records = extract_records(row, cols, records_col_index)

            if fallback_bait_name and fallback_records > 0:
                bait_name = fallback_bait_name
                image_url = fallback_image_url
                records = fallback_records

        if not bait_name:
            continue

        if records <= 0:
            continue

        # Если картинку не нашли в нужной колонке, пробуем по всей строке.
        if not image_url:
            image_url = find_image_url(row)

        parsed.append((location_name, bait_name, image_url, records))

    print("DEBUG IMAGE URLS:")
    for item in parsed[:10]:
        print(item[0], "|", item[1], "=>", item[2])

    return parsed


def extract_records(row, cols, records_col_index: int) -> int:
    """
    Пытаемся вытащить количество улова.
    В новой верстке обычно:
    0 = водоем
    1 = наживка
    2 = улов
    3 = посты
    """
    if len(cols) > records_col_index:
        records_text = normalize_space(cols[records_col_index].get_text(" ", strip=True))
        records = get_records_int(records_text)
        if records:
            return records

    row_text = normalize_space(row.get_text(" ", strip=True))
    m = re.search(r"Улов\s*:?\s*([\d\s]+)", row_text, re.IGNORECASE)
    if m:
        return get_records_int(m.group(1))

    return 0


def parse_rows_from_html(html: str):
    """
    Возвращает список кортежей:
    (location_name, bait_name, image_url, records)

    Устойчивая версия:
    - ищет картинку не только в колонке наживки, а во всей строке;
    - поддерживает картинки с img.rf4spot.com;
    - поддерживает старую и новую верстку rf4-stat.
    """
    soup = BeautifulSoup(html, "html.parser")
    parsed = []

    rows = soup.select("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        location_name = normalize_space(cols[0].get_text(" ", strip=True))

        if not location_name:
            continue

        # Сначала пробуем новую верстку:
        # cols[0] = водоем
        # cols[1] = наживка
        # cols[2] = улов
        bait_col = cols[1]
        records_col_index = 2

        bait_name = normalize_space(bait_col.get_text(" ", strip=True))

        # Если во второй колонке только картинка, а название лежит дальше —
        # используем старую схему.
        if not bait_name and len(cols) >= 4:
            bait_col = cols[2]
            records_col_index = 3
            bait_name = normalize_space(bait_col.get_text(" ", strip=True))

        if not bait_name:
            continue

        # ВАЖНО:
        # картинка может быть не внутри bait_col, а просто где-то в этой строке.
        image_url = ""

        for img in row.find_all("img"):
            candidate_url = get_img_url(img)

            if not candidate_url:
                continue

            # Берем только игровые картинки, а не флаги/иконки сайта.
            if (
                "/images/rf4game/" in candidate_url
                or "img.rf4spot.com" in candidate_url
            ):
                image_url = candidate_url
                break

        records = extract_records(row, cols, records_col_index)

        if records <= 0:
            continue

        parsed.append((location_name, bait_name, image_url, records))

    return parsed


def fetch_all_baits_rows():
    """
    rf4-stat сейчас отдает данные наживок прямо HTML-страницей.
    Поэтому тянем страницу один раз и потом фильтруем по водоему локально.
    """
    global _RF4_ALL_ROWS_CACHE

    if _RF4_ALL_ROWS_CACHE is not None:
        return _RF4_ALL_ROWS_CACHE

    print("Загружаю страницу rf4-stat /baits/ ...")

    session = requests.Session()

    resp = session.get(
        BAITS_URL,
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()

    html = resp.text or ""

    print(f"  URL ответа: {resp.url}")
    print(f"  HTTP статус: {resp.status_code}")
    print(f"  Размер HTML: {len(html)} символов")
    print(f"  Есть слово 'Комариное': {'Комариное' in html}")
    print(f"  Количество <tr>: {html.count('<tr')}")

    if DEBUG_RF4_STAT:
        print("===== HTML START DEBUG =====")
        print(html[:4000])
        print("===== HTML END DEBUG =====")

    rows = parse_rows_from_html(html)

    print(f"  Всего строк распознано на странице: {len(rows)}")

    if not rows:
        raise RuntimeError(
            "Не удалось распознать ни одной строки на rf4-stat. "
            "Вероятно, изменилась верстка или GitHub Actions получает не ту страницу."
        )

    _RF4_ALL_ROWS_CACHE = rows
    return rows


def fetch_baits_for_location(location_name: str, max_pages: int = 200):
    """
    Оставил имя функции старым, чтобы остальной код не ломать.
    Но теперь max_pages не используется: данные берутся из общей HTML-страницы.
    """
    wanted = normalize_name(location_name)
    all_rows = fetch_all_baits_rows()

    matched = [
        row for row in all_rows
        if normalize_name(row[0]) == wanted
    ]

    return matched


def save_rf4_stat_name(conn, loc_id: int, rf4_name: str):
    with conn.cursor() as upd:
        upd.execute(
            "UPDATE locations SET rf4_stat_name = %s WHERE id = %s",
            (rf4_name, loc_id),
        )
    conn.commit()


def pick_best_rf4_name(db_name: str):
    """
    Подбираем имя, которое понимает rf4-stat:
    пробуем 'р. X', потом 'оз. X', потом 'X'.
    """
    candidates = []

    clean_name = normalize_space(db_name)

    if not clean_name.startswith(("р.", "оз.")):
        candidates.append(f"р. {clean_name}")
        candidates.append(f"оз. {clean_name}")

    candidates.append(clean_name)

    best_name = None
    best_rows = []

    for cand in candidates:
        print(f"  пробуем '{cand}' ...")

        try:
            rows = fetch_baits_for_location(cand)
        except Exception as e:
            print("    ошибка запроса/парсинга:", e)
            continue

        print(f"    строк получено: {len(rows)}")

        if len(rows) > len(best_rows):
            best_rows = rows
            best_name = cand

        if rows and cand.startswith(("р. ", "оз. ")):
            break

    return best_name, best_rows


def main():
    conn = None
    cur = None

    try:
        print("Подключаемся к БД...")

        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

        cur = conn.cursor()

        cur.execute("SELECT id, name, rf4_stat_name FROM locations ORDER BY id ASC")
        loc_rows = cur.fetchall()

        print(f"Найдено локаций в БД: {len(loc_rows)}")

        # Сразу грузим rf4-stat один раз.
        # Если тут проблема — таблицу в БД не трогаем.
        fetch_all_baits_rows()

        all_to_insert = []
        total_parsed = 0

        for r in loc_rows:
            loc_id = int(r["id"])
            db_name = normalize_space(r["name"])
            rf4_name_saved = normalize_space(r.get("rf4_stat_name") or "")

            print(f"\nЛокация: {db_name}")

            if rf4_name_saved:
                print(f"  использую rf4_stat_name из БД: '{rf4_name_saved}'")

                try:
                    rows = fetch_baits_for_location(rf4_name_saved)
                except Exception as e:
                    print("  ошибка запроса/парсинга:", e)
                    rows = []

                print(f"  строк получено: {len(rows)}")

                if not rows:
                    print("  сохранённое имя не дало строк, подбираю заново...")

                    best_name, best_rows = pick_best_rf4_name(db_name)

                    if best_name and best_rows:
                        save_rf4_stat_name(conn, loc_id, best_name)
                        rows = best_rows
                        print(f"  ✅ сохранил новое rf4_stat_name: '{best_name}'")
                    else:
                        print("  ❌ не нашёл данных на rf4-stat")
                        continue
            else:
                best_name, rows = pick_best_rf4_name(db_name)

                if best_name and rows:
                    save_rf4_stat_name(conn, loc_id, best_name)
                    print(f"  ✅ сохранил rf4_stat_name: '{best_name}'")
                else:
                    print("  ❌ не нашёл данных на rf4-stat")
                    continue

            total_parsed += len(rows)

            for _, bait_name, image_url, records in rows:
                all_to_insert.append((loc_id, bait_name, image_url, records))

        print("\n===== ИТОГ =====")
        print(f"Всего строк спарсено (сырых): {total_parsed}")
        print(f"Строк готово к записи: {len(all_to_insert)}")

        # ВАЖНАЯ защита:
        # если парсер ничего не собрал — не заменяем рабочую таблицу пустой.
        if not all_to_insert:
            raise RuntimeError(
                "Парсер не собрал ни одной строки. "
                "Останавливаюсь, чтобы не заменить baits_records пустой таблицей."
            )

        print("\nГотовлю временную таблицу baits_records_tmp...")

        cur.execute("DROP TABLE IF EXISTS baits_records_tmp")
        cur.execute("CREATE TABLE baits_records_tmp LIKE baits_records")
        conn.commit()

        sql_tmp = """
        INSERT INTO baits_records_tmp (location_id, bait_name, image_url, records, date_created)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            image_url = VALUES(image_url),
            records   = VALUES(records)
        """

        print(f"Пишу {len(all_to_insert)} строк во временную таблицу...")

        cur.executemany(sql_tmp, all_to_insert)
        conn.commit()

        cur.execute("SELECT COUNT(*) AS total_rows FROM baits_records_tmp")
        tmp_total = int(cur.fetchone()["total_rows"])

        print(f"Во временной таблице строк: {tmp_total}")

        if tmp_total <= 0:
            raise RuntimeError(
                "Временная таблица пустая. "
                "Останавливаюсь, чтобы не заменить рабочую baits_records пустой таблицей."
            )

        print("Атомарно подменяю таблицы через RENAME TABLE...")

        try:
            cur.execute("DROP TABLE IF EXISTS baits_records_old")
            conn.commit()

            cur.execute("""
                RENAME TABLE
                    baits_records TO baits_records_old,
                    baits_records_tmp TO baits_records
            """)
            conn.commit()

            print("Основная таблица успешно обновлена.")

            cur.execute("DROP TABLE IF EXISTS baits_records_old")
            conn.commit()

        except Exception as e:
            print("ОШИБКА при замене таблиц:", e)

            try:
                cur.execute("DROP TABLE IF EXISTS baits_records_tmp")
                conn.commit()
            except Exception:
                pass

            raise

        cur.execute("SELECT COUNT(*) AS total_rows FROM baits_records")
        total = int(cur.fetchone()["total_rows"])

        print(f"Записано в baits_records (факт): {total}")
        print("Готово.")

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass

        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
