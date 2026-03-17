import os
import re
import time
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NotesFisherBot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.8",
    "Connection": "keep-alive",
}


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def get_records_int(s: str) -> int:
    digits = re.sub(r"\D+", "", s or "")
    return int(digits) if digits else 0


def parse_rows_from_html(html: str):
    """
    Возвращает список кортежей:
    (location_name, bait_name, image_url, records)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr")

    parsed = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        img_tag = cols[1].find("img") if len(cols) > 1 else None
        if not img_tag:
            continue

        img = (img_tag.get("src") or "").strip()
        if not img.startswith("/images/rf4game/"):
            continue

        location_name = normalize_space(cols[0].get_text(strip=True))
        bait_name = normalize_space(cols[2].get_text(strip=True))
        records_text = normalize_space(cols[3].get_text(strip=True))
        records = get_records_int(records_text)

        if not location_name or not bait_name:
            continue

        image_url = BASE_URL + img
        parsed.append((location_name, bait_name, image_url, records))

    return parsed


def fetch_baits_for_location(location_name: str, max_pages: int = 200):
    """
    Тянем все страницы для указанной локации.
    """
    all_rows = []
    seen = set()

    for page in range(1, max_pages + 1):
        resp = requests.post(
            BAITS_URL,
            params={"location": location_name},
            data={"ajax": 1, "page": page},
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()

        rows = parse_rows_from_html(resp.text)
        if not rows:
            break

        new_count = 0
        for r in rows:
            key = (r[0], r[1], r[2], r[3])
            if key not in seen:
                seen.add(key)
                all_rows.append(r)
                new_count += 1

        if new_count == 0:
            break

        time.sleep(0.2)

    return all_rows


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
    пробуем 'р. X', потом 'оз. X', потом 'X'
    """
    candidates = []
    if not db_name.startswith(("р.", "оз.")):
        candidates.append(f"р. {db_name}")
        candidates.append(f"оз. {db_name}")
    candidates.append(db_name)

    best_name = None
    best_rows = []

    for cand in candidates:
        print(f"  пробуем '{cand}' ...")
        try:
            rows = fetch_baits_for_location(cand)
        except Exception as e:
            print("    ошибка запроса:", e)
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

        cur.execute("SELECT id, name, rf4_stat_name FROM locations")
        loc_rows = cur.fetchall()
        print(f"Найдено локаций в БД: {len(loc_rows)}")

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
                    print("  ошибка запроса:", e)
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
        tmp_total = cur.fetchone()["total_rows"]
        print(f"Во временной таблице строк: {tmp_total}")

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
        total = cur.fetchone()["total_rows"]

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
