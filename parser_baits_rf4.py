import re
import time
import requests
from bs4 import BeautifulSoup
import pymysql

BASE_URL = "https://rf4-stat.ru"
BAITS_URL = f"{BASE_URL}/baits/"

# ====== НАСТРОЙКИ БД (как у тебя в Docker на Windows) ======
import os

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]
# ===========================================================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NotesFisherBot/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.8",
    "Connection": "keep-alive",
}

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def get_records_int(s: str) -> int:
    # "Количество рекордов:351" -> 351  |  "351" -> 351
    digits = re.sub(r"\D+", "", s or "")
    return int(digits) if digits else 0

def make_location_aliases(db_name: str) -> list[str]:
    """
    Твоя таблица locations хранит имена типа "Вьюнок", "Комариное".
    А rf4-stat часто отдаёт "р. Вьюнок", "оз. Комариное", "Ладожское оз."
    Поэтому делаем алиасы, чтобы матчить корректно.
    """
    n = normalize_space(db_name)
    aliases = {n}

    # Если в БД нет "р." / "оз.", добавим варианты
    if not n.startswith(("р.", "оз.", "г.", "п.")):
        aliases.add(f"р. {n}")
        aliases.add(f"оз. {n}")

    # Частные случаи
    if n.lower() == "ладожское":
        aliases.add("Ладожское оз.")
    if n.lower() == "нижняя тунгуска":
        aliases.add("р. Нижняя Тунгуска")
    if n.lower() == "северский донец":
        aliases.add("р. Северский Донец")
    if n.lower() == "белая":
        aliases.add("р. Белая")
    if n.lower() == "ахтуба":
        aliases.add("р. Ахтуба")

    return list(aliases)

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

        # col[0] - локация, col[1] - иконка, col[2] - наживка, col[3] - рекорды
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
    Тянем ВСЕ страницы для указанной локации.
    Судя по твоему скрину: QueryString location=..., FormData ajax=1&page=N
    """
    all_rows = []
    seen = set()

    for page in range(1, max_pages + 1):
        # важный момент: location идёт как query string,
        # а ajax/page — в form data (как у тебя в DevTools)
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
            # страница пустая => конец
            break

        # защита от “зацикливания”, если сайт начнёт возвращать одно и то же
        new_count = 0
        for r in rows:
            key = (r[0], r[1], r[2], r[3])
            if key not in seen:
                seen.add(key)
                all_rows.append(r)
                new_count += 1

        if new_count == 0:
            break

        # маленькая пауза, чтобы не долбить сайт
        time.sleep(0.2)

    return all_rows

print("Подключаемся к БД...")
conn = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)
cur = conn.cursor()

# 1) Берём список локаций из твоей таблицы locations
cur.execute("SELECT id, name, rf4_stat_name FROM locations")
loc_rows = cur.fetchall()
print(f"Найдено локаций в БД: {len(loc_rows)}")

# 2) Делаем map алиасов -> location_id
loc_map = {}
for r in loc_rows:
    loc_id = int(r["id"])
    db_name = normalize_space(r["name"])
    for alias in make_location_aliases(db_name):
        loc_map[normalize_space(alias)] = loc_id

# 3) Парсим по всем локациям из БД (берём “чистое” имя из БД и пробуем его вариантами)
# 3) Парсим по всем локациям из БД, используя rf4_stat_name если он уже сохранён
all_to_insert = []
unknown_remote_locations = set()
total_parsed = 0

def save_rf4_stat_name(loc_id: int, rf4_name: str):
    upd = conn.cursor()
    upd.execute("UPDATE locations SET rf4_stat_name = %s WHERE id = %s", (rf4_name, loc_id))
    conn.commit()
    upd.close()

def pick_best_rf4_name(db_name: str):
    """
    Подбираем какое имя rf4-stat понимает:
    пробуем 'р. X', потом 'оз. X', потом 'X'
    возвращаем (best_name, best_rows_count)
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

        # если нашли что-то нормальное — можно остановиться пораньше
        if rows and cand.startswith(("р. ", "оз. ")):
            break

    return best_name, best_rows

for r in loc_rows:
    loc_id = int(r["id"])
    db_name = normalize_space(r["name"])
    rf4_name_saved = normalize_space(r.get("rf4_stat_name") or "")

    print(f"\nЛокация: {db_name}")

    # если уже сохранено — используем сразу
    if rf4_name_saved:
        print(f"  использую rf4_stat_name из БД: '{rf4_name_saved}'")
        try:
            rows = fetch_baits_for_location(rf4_name_saved)
        except Exception as e:
            print("  ошибка запроса:", e)
            rows = []

        print(f"  строк получено: {len(rows)}")

        # если внезапно стало 0 (сайт поменялся) — подберём заново
        if not rows:
            print("  сохранённое имя не дало строк, подбираю заново...")
            best_name, best_rows = pick_best_rf4_name(db_name)
            if best_name and best_rows:
                save_rf4_stat_name(loc_id, best_name)
                rows = best_rows
                print(f"  ✅ сохранил новое rf4_stat_name: '{best_name}'")
            else:
                print("  ❌ не нашёл данных на rf4-stat")
                continue
    else:
        # если не сохранено — подбираем
        best_name, rows = pick_best_rf4_name(db_name)
        if best_name and rows:
            save_rf4_stat_name(loc_id, best_name)
            print(f"  ✅ сохранил rf4_stat_name: '{best_name}'")
        else:
            print("  ❌ не нашёл данных на rf4-stat")
            continue

    total_parsed += len(rows)

    for location_name, bait_name, image_url, records in rows:
        # пишем строго в текущую локацию (loc_id из БД), не пытаясь матчить по строке
        all_to_insert.append((loc_id, bait_name, image_url, records))


print("\n===== ИТОГ =====")
print(f"Всего строк спарсено (сырых): {total_parsed}")
print(f"Строк готово к записи (смэтчились с locations): {len(all_to_insert)}")

if unknown_remote_locations:
    print("Локации, которые пришли с rf4-stat, но не смэтчились с твоими locations (первые 20):")
    for x in list(sorted(unknown_remote_locations))[:20]:
        print(" -", x)

# 4) Перезаливаем таблицу
print("\nЧищу таблицу baits_records...")
cur.execute("TRUNCATE TABLE baits_records")
conn.commit()

cur.execute("SELECT COUNT(*) AS cnt FROM baits_records")
print("После TRUNCATE строк:", cur.fetchone()["cnt"])

sql = """
INSERT INTO baits_records (location_id, bait_name, image_url, records, date_created)
VALUES (%s, %s, %s, %s, NOW())
ON DUPLICATE KEY UPDATE
    image_url = VALUES(image_url),
    records   = VALUES(records)
"""
cur.executemany(sql, all_to_insert)
conn.commit()


cur.execute("SELECT COUNT(*) AS total_rows FROM baits_records")
total = cur.fetchone()["total_rows"]

print(f"Записано в baits_records (факт): {total}")
print("Готово.")

cur.close()
conn.close()
print("Готово.")
