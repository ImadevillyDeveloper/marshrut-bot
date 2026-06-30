#!/usr/bin/env python3
"""
Telegram bot: мониторинг появления ТС на маршрутах (bus-55.ru / Navitrans / ГЛОНАСС).

Запуск:
    set TELEGRAM_BOT_TOKEN=<токен>
    python bot.py
"""

import os
import sqlite3
import hashlib
import time
import math
import logging
import asyncio
import threading
from datetime import datetime
from collections import defaultdict
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

DB_PATH = os.path.join(os.path.dirname(__file__), "marshrut.db")

import requests as http
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
    KeyboardButton, ReplyKeyboardMarkup as RKMarkup, ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Navitrans / bus-55.ru API ──────────────────────────────────────

ETK55_BALANCE_URL = "https://etk55.ru/local/templates/main/ajax/makeRefillRequest.php"
ETK55_HEADERS     = {
    "Referer": "https://etk55.ru/balance/",
    "Origin":  "https://etk55.ru",
}

BUS55_BASE    = "https://bus-55.ru/api/rpc.php"
BUS55_RPC     = "2․2"          # специальная точка (U+2024) — как в оригинале
BUS55_SYS_ID  = "omsk"
BUS55_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin":       "https://bus-55.ru",
    "Referer":      "https://bus-55.ru/",
}

_session: dict = {"sid": None, "exp": 0.0}
_req_id: list  = [0]


def _ts() -> int:
    t = int(time.time())
    while t % 10 in (0, 3, 7):
        t += 1
    return t


def _next_id() -> int:
    while True:
        _req_id[0] += 1
        if _req_id[0] % 7 != 0:
            return _req_id[0]


def _sign(method: str, req_id: int, sid: str) -> tuple[str, str]:
    raw  = hashlib.sha1(f"{method}~{BUS55_SYS_ID}~{req_id}~{sid}".encode()).hexdigest()
    guid = f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[24:28]}-{raw[28:40]}"
    return f"{BUS55_BASE}?m={guid}", raw[16:24]


def _start_session() -> Optional[str]:
    try:
        r = http.post(BUS55_BASE, headers=BUS55_HEADERS, json={
            "jsonrpc": BUS55_RPC, "method": "startSession",
            "ts": _ts(), "params": {}, "id": 1,
        }, timeout=6)
        log.info("startSession HTTP %d: %s", r.status_code, r.text[:300])
        data = r.json()
        if "error" in data:
            log.warning("startSession ошибка API: %s", data["error"])
            return None
        sid = data["result"]["sid"]
        _session["sid"] = sid
        _session["exp"] = time.time() + 3500
        log.info("startSession OK, sid=%s", sid[:8])
        return sid
    except Exception as e:
        log.warning("startSession исключение: %s", e)
        return None


def _get_sid() -> Optional[str]:
    if not _session["sid"] or time.time() > _session["exp"]:
        return _start_session()
    return _session["sid"]


def _rpc(method: str, params: dict) -> dict:
    sid = _get_sid()
    if not sid:
        return {}
    rid = _next_id()
    url, magic = _sign(method, rid, sid)
    r = http.post(url, headers=BUS55_HEADERS, json={
        "jsonrpc": BUS55_RPC, "method": method,
        "ts": _ts(), "id": rid,
        "params": {"sid": sid, "magic": magic, **params},
    }, timeout=8)
    return r.json()


def fetch_vehicles() -> list[dict]:
    """Все активные ТС в границах Омска."""
    for attempt in range(2):
        try:
            sid = _get_sid()
            if not sid:
                return []
            rid = _next_id()
            url, magic = _sign("getUnitsInRect", rid, sid)
            r = http.post(url, headers=BUS55_HEADERS, json={
                "jsonrpc": BUS55_RPC, "method": "getUnitsInRect",
                "ts": _ts(), "id": rid,
                "params": {
                    "sid": sid, "magic": magic,
                    "minlat": 54.80, "maxlat": 55.15,
                    "minlong": 73.10, "maxlong": 73.70,
                },
            }, timeout=10)
            data = r.json()
            if "error" in data:
                code = data["error"].get("code", 0)
                if code == -33100 and attempt == 0:
                    _session["sid"] = None
                    continue
                log.warning("fetch_vehicles error: %s", data["error"])
                return []
            result = data.get("result", [])
            return result if isinstance(result, list) else []
        except Exception as e:
            if attempt == 0:
                _session["sid"] = None
            else:
                log.warning("fetch_vehicles: %s", e)
    return []


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER NOT NULL,
            route   TEXT    NOT NULL,
            PRIMARY KEY (user_id, route)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_cards (
            user_id     INTEGER NOT NULL,
            card_number TEXT    NOT NULL,
            name        TEXT    NOT NULL DEFAULT '',
            color       TEXT    NOT NULL DEFAULT '💙',
            PRIMARY KEY (user_id, card_number)
        )
    """)
    conn.commit()
    conn.close()


def db_load_subscriptions() -> dict:
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT user_id, route FROM subscriptions").fetchall()
        conn.close()
        result: dict[int, set[str]] = defaultdict(set)
        for uid, route in rows:
            result[uid].add(route)
        return result
    except Exception as e:
        log.warning("db_load_subscriptions: %s", e)
        return defaultdict(set)


def db_add_sub(uid: int, route: str) -> None:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT OR IGNORE INTO subscriptions (user_id, route) VALUES (?, ?)", (uid, route))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("db_add_sub: %s", e)


def db_remove_sub(uid: int, route: str) -> None:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM subscriptions WHERE user_id = ? AND route = ?", (uid, route))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("db_remove_sub: %s", e)


def db_remove_all_subs(uid: int) -> None:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM subscriptions WHERE user_id = ?", (uid,))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("db_remove_all_subs: %s", e)


def fetch_route_stops(mr_id: str) -> list[dict]:
    """Список остановок маршрута: [{name, lat, lng, st_id}, ...]. Также кеширует рейсы."""
    try:
        data = _rpc("getRoute", {"mr_id": mr_id})
        races = data.get("result", {}).get("races", [])
        races_cache[mr_id] = races
        stops: list[dict] = []
        seen: set[str] = set()
        for race in races:
            for s in race.get("stopList", []):
                lat  = s.get("st_lat")
                lng  = s.get("st_long")
                name = (s.get("st_title") or s.get("st_name") or "").strip()
                if lat and lng and name and name not in seen:
                    stops.append({
                        "name":  name,
                        "lat":   float(lat),
                        "lng":   float(lng),
                        "st_id": str(s.get("st_id") or ""),
                    })
                    seen.add(name)
        return stops
    except Exception as e:
        log.warning("fetch_route_stops mr_id=%s: %s", mr_id, e)
        return []


def fetch_stop_arrivals(st_id: str) -> list[dict]:
    """Прогноз прибытия на остановку: [{mr_num, tc_arrivetime, laststation_title}, ...]."""
    try:
        data = _rpc("getStopArrive", {"st_id": st_id})
        result = data.get("result")
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        log.warning("fetch_stop_arrivals st_id=%s: %s", st_id, e)
        return []


def fetch_stops_by_name_api(query: str) -> list[dict]:
    """Возвращает [{name, st_id}, ...] из API getStopsByName.
    Используется когда stops_cache не содержит нужной остановки."""
    try:
        data = _rpc("getStopsByName", {"str": query})
        result = data.get("result", [])
        log.info("getStopsByName(%r) → %d results: %s", query, len(result) if isinstance(result, list) else -1, str(result)[:300])
        if not isinstance(result, list):
            return []
        out = []
        for s in result:
            name  = str(s.get("st_title") or s.get("st_name") or "").strip()
            st_id = str(s.get("st_id") or "").strip()
            if name and st_id:
                out.append({"name": name, "st_id": st_id})
        return out
    except Exception as e:
        log.warning("fetch_stops_by_name_api query=%s: %s", query, e)
        return []


def fetch_stop_id_by_name(query: str) -> Optional[str]:
    """Ищет st_id остановки через API getStopsByName (не зависит от кеша)."""
    matches = fetch_stops_by_name_api(query)
    # Предпочитаем точное совпадение имени
    for m in matches:
        if _stop_name_matches(query, m["name"]):
            return m["st_id"]
    # Иначе первый результат
    return matches[0]["st_id"] if matches else None


def _get_stop_id(stop_name: str) -> Optional[str]:
    """Ищет st_id остановки по имени в stops_cache."""
    for stops in stops_cache.values():
        for s in stops:
            if _stop_name_matches(stop_name, s["name"]):
                st_id = s.get("st_id")
                if st_id:
                    return st_id
    return None


def _stop_name_matches(query: str, stop_name: str) -> bool:
    """Надёжное сравнение введённого названия остановки с именем из базы.
    - Числа сравниваются точно: '4-я' ≠ '14-я'
    - Остальные слова: точно или с допуском на опечатку (SequenceMatcher ≥ 0.82)
    - Все слова запроса должны найтись в названии (или наоборот для коротких имён)
    """
    import re as _re
    from difflib import SequenceMatcher as _SM
    q = query.lower().strip()
    n = stop_name.lower().strip()
    if q == n:
        return True
    q_words = _re.findall(r'[а-яёa-z0-9]+', q)
    n_words = _re.findall(r'[а-яёa-z0-9]+', n)
    if not q_words:
        return False

    def _wm(a: str, b: str) -> bool:
        if a == b:
            return True
        if a.isdigit() or b.isdigit():
            return False  # числа только точно
        # SequenceMatcher только если слова близки по длине (≤30% разница)
        la, lb = len(a), len(b)
        if la < 3 or lb < 3:
            return False
        if abs(la - lb) > max(la, lb) * 0.3:
            return False
        return _SM(None, a, b).ratio() >= 0.82

    if all(any(_wm(qw, nw) for nw in n_words) for qw in q_words):
        return True
    if n_words and all(any(_wm(nw, qw) for qw in q_words) for nw in n_words):
        return True
    return False


def _find_stop_idx(query: str, names: list) -> int:
    """Первый индекс в списке names, совпадающий с query, или -1."""
    for i, n in enumerate(names):
        if _stop_name_matches(query, n):
            return i
    return -1


def _canonical_stop_name(query: str) -> str:
    """Возвращает каноническое название остановки из stops_cache (как на сайте).
    Если не найдено — возвращает исходный запрос."""
    q = query.lower()
    best: Optional[str] = None
    best_len = 0
    for stops in stops_cache.values():
        for s in stops:
            n = s["name"]
            nl = n.lower()
            if _stop_name_matches(q, n):
                if nl == q:
                    return n
                if len(n) > best_len:
                    best = n
                    best_len = len(n)
    return best if best else query


def db_add_card(uid: int, card_number: str, name: str, color: str) -> None:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT OR REPLACE INTO user_cards (user_id, card_number, name, color) VALUES (?, ?, ?, ?)",
            (uid, card_number, name, color),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("db_add_card: %s", e)


def db_get_cards(uid: int) -> list[dict]:
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT card_number, name, color FROM user_cards WHERE user_id = ? ORDER BY rowid",
            (uid,),
        ).fetchall()
        conn.close()
        return [{"card_number": r[0], "name": r[1], "color": r[2]} for r in rows]
    except Exception as e:
        log.warning("db_get_cards: %s", e)
        return []


def db_remove_card(uid: int, card_number: str) -> None:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "DELETE FROM user_cards WHERE user_id = ? AND card_number = ?",
            (uid, card_number),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("db_remove_card: %s", e)


def fetch_card_balance(card_number: str) -> dict:
    """Запрашивает баланс карты ОМКА через etk55.ru."""
    try:
        r = http.post(
            ETK55_BALANCE_URL,
            headers=ETK55_HEADERS,
            data={
                "balance-card": card_number,
                "submitType":   "check",
                "finalStep":    "no",
            },
            timeout=10,
        )
        return r.json()
    except Exception as e:
        log.warning("fetch_card_balance: %s", e)
        return {}


# Полный справочник маршрутов Омска: номер -> описание
OMSK_ROUTES: dict[str, str] = {
    "1": "ул. Бархатовой — Омский нефтеперерабатывающий завод",
    "3": "ул. Бархатовой — микрорайон «Входной»",
    "6Н": "ул. 3-я Железнодорожная — завод им. Попова",
    "8Н": "ЗАО МПК Компур (СНТ Медик) — МСЧ-9",
    "12": "пос. Ермак — пос. Большие Поля",
    "13": "пос. Чкаловский — микрорайон «Осташково»",
    "14": "пос. Мелиораторов — пос. Николаевка (пос. Юбилейный)",
    "16": "Трикотажная фабрика — пос. Рыбачий",
    "17": "Железнодорожный вокзал — Ново-Кировское кладбище (пос. Новостройка)",
    "20": "Омский нефтеперерабатывающий завод (пос. Ермак) — ул. Гашека",
    "21": "ДСК-2 — Кожевенный Завод",
    "22": "ул. Бархатовой — МСЧ-9 (ул. Индустриальная)",
    "23": "пос. Чкаловский — 12-й микрорайон ул. Ватутина",
    "24": "Железнодорожный вокзал — пос. Солнечный",
    "25": "Дом Туриста — микрорайон «Новоалександровский»",
    "26": "Микрорайон «Булатово» — пос. Чкаловский",
    "28": "Площадь Победы — Поворотная",
    "29": "Микрорайон «Первокирпичный» (ул. 21-я Амурская) — Омский НПЗ (пос. Ермак)",
    "30": "ул. Лобкова — пос. Армейский",
    "31Н": "пос. Светлый — ТК Лента",
    "32": "ул. Бархатовой — Железнодорожный вокзал",
    "33": "ул. 3-я Железнодорожная — ул. Бархатовой",
    "34": "Площадь Победы — пос. Большие Поля Северо-Восточное кладбище",
    "37": "пос. Солнечный — микрорайон «Входной»",
    "39": "пос. Чкаловский — пос. Степной (СНТ «Ивушка»)",
    "41": "пос. Светлый — ул. Л. Чайкиной",
    "42": "ПО Иртыш — ул. Бархатовой",
    "45": "Ясная Поляна — пос. Амурский-2",
    "46": "СНТ Заря-2 — ул. Облепиховая",
    "47Н": "ул. Гашека — Онкологический диспансер",
    "49": "ул. 21-я Амурская — ПО «Иртыш»",
    "51": "Речной порт — СНТ Березка",
    "54": "Микрорайон «Рябиновка» — пос. Юбилейный",
    "55": "ул. Лобкова — микрорайон «Булатово»",
    "58": "пос. Чкаловский — ПО «Иртыш»",
    "59": "Биофабрика — Омский нефтеперерабатывающий завод (пос. Ермак)",
    "61": "пос. Солнечный — ул. Гашека",
    "62": "пос. Большая Островка — ул. Партизанская — пос. Большая Островка",
    "63": "МСЧ-9 — гараж ЦС",
    "64": "мкр. Зеленая река — пос. Дальний",
    "66": "ул. 1-я Учхозная — Омский нефтеперерабатывающий завод",
    "67": "пос. Солнечный — Омский нефтеперерабатывающий завод пос. Ермак",
    "68": "МСЧ-9 — СНТ «Заря-2»",
    "69": "ул. Стрельникова — ПО Иртыш",
    "71": "ул. Лобкова — СНТ «Тепличный-3»",
    "72": "пос. Чкаловский — пос. Большие поля",
    "73": "пос. Чкаловский — ул. Стрельникова",
    "77": "пос. Солнечный — пл. Победы",
    "78": "пос. Солнечный — пос. Биофабрика",
    "79": "ул. Бархатовой — ул. Володарского",
    "83": "ул. Стрельникова — Кирпичный завод",
    "83Н": "ул. Стрельникова — Кирпичный завод",
    "87": "РЭБ — завод СК",
    "88": "пос. Рыбачий — пос. Чукреевка",
    "89": "ул. Лобкова — пос. Дальний",
    "90": "ул. Бархатовой — пос. Солнечный (СНТ «Медик»)",
    "94": "ул. Крупской — микрорайон «Первокирпичный» (микрорайон Загородный)",
    "95": "Строительный рынок «Южный» — 12-й микрорайон (СТЦ «МЕГА»)",
    "96": "ООО «Лента» — ул. Крупской",
    "97": "Микрорайон «Амурский-2» — МСЧ-9",
    "98": "ул. Нефтезаводская — ул. Попова — ул. Студенческая",
    "103": "пос. Солнечный — Онкодиспансер",
    "109": "пос. Солнечный — Речной порт",
    "110": "ул. Бархатовой — Железнодорожный вокзал",
    "112": "ул. Гашека — СНТ «Осташково»",
    "117Н": "ул. Лобкова — пос. Новая Станица",
    "119": "пос. Чкаловский — микрорайон «Осташково» СНТ «Осташково»",
    "122": "ул. Лобкова — СНТ 33 км Русско-Полянского тракта",
    "125": "Железнодорожный вокзал — микрорайон «Входной» (пос. Северный)",
    "126": "Омск — п. Ростовка (с. Новомосковка)",
    "131": "ул. 25-я Линия — СНТ «Золотое Руно»",
    "138": "ул. Партизанская — пос. Ростовка (с. Новомосковка)",
    "139": "пос. Ермак — микрорайон «Входной»",
    "141": "пос. Чкаловский — СНТ «Золотое Руно»",
    "144П": "пос. Солнечный — СНТ Автомобилист-2 (Переезд)",
    "145П": "ул. Нефтезаводская — СНТ Росинка",
    "156П": "ПО «Иртыш» — СНТ «Кварц»",
    "171": "пос. Чкаловский — СНТ «Осташково»",
    "173П": "ул. Ватутина — СНТ «Заозерный»",
    "174П": "ул. Дружбы — СНТ «Кедр»",
    "178": "ПО «Иртыш» — СНТ «Осташково»",
    "190П": "пос. Солнечный — СНТ «Авиатор»",
    "200": "ул. Бархатовой — ЗАО «ТЦ «Континент»",
    "203": "ул. Бархатовой — пос. Юбилейный",
    "212": "СТЦ «МЕГА» — Бауцентр",
    "222": "МСЧ-9 — Онкодиспансер",
    "272": "СТЦ «МЕГА» — ул. Малиновского",
    "303": "ул. Лобкова — ПО Иртыш",
    "305": "ул. Стрельникова — Аэропорт",
    "323": "ул. 3-я Железнодорожная — ул. Бархатовой",
    "331": "ПО «Иртыш» — СТЦ «МЕГА»",
    "335": "проспект Губкина — ул. 1 Мая",
    "343Н": "ДСК-2 — Микрорайон «Амурский-2»",
    "344": "ООО «Лента» — пос. Новостройка",
    "346": "ул. 1-я Красной Звезды — ул. 50 лет Октября",
    "350": "пос. Степной СНТ «Ивушка» — пос. Карьер «СНТ «Маяк-2»",
    "353": "пос. Дальний — Гараж ЦС",
    "359": "Кирпичный завод — пос. Чкаловский",
    "385": "ул. Стрельникова — ПО «Иртыш»",
    "386": "Микрорайон «Амурский-2» — ул. 1-я Учхозная",
    "392": "ПО «Иртыш» — Гараж ЦС",
    "394": "пос. Мелиораторов — Красноярский тракт",
    "399": "СНТ «Молния-5» — Оптовый рынок Черлакский тракт",
    "409": "Микрорайон «Рябиновка» — ул. Труда",
    "410": "МСЧ-9 — ООО «ОБИ»",
    "414": "Микрорайон «Ясная Поляна» — Онкодиспансер Микрорайон «Первокирпичный»",
    "415": "ЗАО ТЦ «Континент» — Омский нефтеперерабатывающий завод пос. Ермак",
    "418": "ул. Крымская — ул. Дергачева",
    "421": "пос. Юбилейный — завод СК",
    "424": "СНТ «Заря-2» — пос. Юбилейный",
    "425": "Красноярский тракт — Омский нефтеперерабатывающий завод пос. Ермак",
    "470Н": "пос. Чкаловский — ДСК-2",
    "500": "ООО «Лента» — ДСК-2",
    "501А": "ул. Бархатовой — Арена-Омск",
    "502А": "ул. Бархатовой — Арена-Омск",
    "503": "СТЦ «МЕГА» — микрорайон «Загородный»",
    "503А": "пос. Ермак — Арена-Омск",
    "504А": "ул. Стрельникова — Арена-Омск",
    "505А": "пос. Ермак — Арена-Омск",
    "506А": "пос. Николаевка — Арена-Омск",
    "507А": "Первокирпичный — Арена-Омск",
    "508А": "микр. Амурский-2 — Арена-Омск",
    "509А": "ул. 21-я Амурская — Арена-Омск",
    "510А": "пос. Чкаловский — Арена-Омск",
    "511А": "пос. Биофабрика — Арена-Омск",
    "512А": "пос. Чкаловский — Арена-Омск",
    "513А": "ул. Гашека — Арена-Омск",
    "514": "ул. Бархатовой — ул. 3-й Разъезд",
    "514А": "пос. Булатова — Арена-Омск",
    "515А": "ПО Иртыш — Арена-Омск",
    "516А": "ПО Иртыш — Арена-Омск",
    "517А": "МСЧ №9 — Арена-Омск",
    "518А": "пос. Солнечный — Арена-Омск",
    "519А": "микр. Ясная поляна — Арена-Омск",
    "520А": "Микрорайон Входной — Арена-Омск",
    "550": "ПО «Иртыш» — микрорайон «Амурский-2»",
    "568": "ООО «Лента» СНТ «Золотое Руно» — СНТ «Заря-2»",
    "910": "пл. Победы — Ростовка — Новомосковка",
}


async def subscribe(update: Update, route: str) -> None:
    """Общая логика подписки на маршрут."""
    uid = update.effective_user.id
    route = route.strip().upper()

    if route not in OMSK_ROUTES:
        hint = ""
        # Подсказка: ищем похожий маршрут
        close = [r for r in OMSK_ROUTES if r.startswith(route[:2])] if len(route) >= 2 else []
        if close:
            hint = "\n\nПохожие: " + ", ".join(f"<b>{r}</b>" for r in sorted(close)[:5])
        await update.message.reply_html(
            f"❌ Маршрут <b>{route}</b> не найден в списке омских маршрутов.{hint}\n\n"
            f"Напиши точный номер, например: <b>24</b>, <b>55</b>, <b>212</b>"
        )
        return

    vehicles = fetch_vehicles()
    log.info("subscribe route=%s: fetch_vehicles вернул %d ТС", route, len(vehicles))

    for v in vehicles:
        if str(v.get("mr_num", "")).strip().upper() == route:
            mid = str(v.get("mr_id", "")).strip()
            if mid:
                mr_id_cache[route] = mid
                break

    if route not in mr_id_cache:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT mr_id FROM route_navitrans_ids WHERE route_number = ?", (route,))
            row = c.fetchone()
            conn.close()
            if row:
                mr_id_cache[route] = str(row[0])
        except Exception:
            pass

    if route in mr_id_cache:
        mid = mr_id_cache[route]
        if mid not in stops_cache:
            stops_cache[mid] = fetch_route_stops(mid)

    current_ids = {
        str(v.get("u_id", ""))
        for v in vehicles
        if str(v.get("mr_num", "")).strip().upper() == route and v.get("u_id")
    }
    log.info("subscribe route=%s: найдено %d ТС на маршруте", route, len(current_ids))
    known_vehicles[route] = current_ids
    subscriptions[uid].add(route)
    db_add_sub(uid, route)

    count = len(current_ids)
    total = len(subscriptions[uid])
    description = OMSK_ROUTES.get(route, "")
    desc_line   = f"\n<i>{description}</i>" if description else ""
    routes_list = ", ".join(f"<b>{r}</b>" for r in sorted(subscriptions[uid], key=lambda x: (len(x), x)))
    await update.message.reply_html(
        f"🔔 Маршрут <b>{route}</b>{desc_line}\n\n"
        f"Слежение включено. Сейчас на линии: <b>{count} ТС</b>.\n"
        f"Пришлю уведомление, когда появится новое.\n\n"
        f"Отслеживаемых маршрутов ({total}): {routes_list}",
        reply_markup=_track_confirm_markup(route),
    )


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_stop_name(lat: float, lng: float, stops: list[dict]) -> Optional[str]:
    if not stops:
        return None
    best = min(stops, key=lambda s: haversine_m(lat, lng, s["lat"], s["lng"]))
    return best["name"]


def course_to_str(course: float) -> str:
    dirs = ["север", "северо-восток", "восток", "юго-восток",
            "юг", "юго-запад", "запад", "северо-запад"]
    return dirs[round(course / 45) % 8]


def course_to_arrow(course: float) -> str:
    arrows = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]
    return arrows[round(course / 45) % 8]


# ── Состояние бота ─────────────────────────────────────────────────

# user_id -> set of route_numbers
subscriptions: dict[int, set[str]] = defaultdict(set)

# route_number -> set of u_id (ТС, которые уже были видны на карте)
known_vehicles: dict[str, set] = defaultdict(set)

# u_id -> последнее известное состояние ТС (позиция, скорость, курс и т.д.)
last_seen_vehicles: dict[str, dict] = {}

# route_number -> mr_id (Navitrans internal ID)
mr_id_cache: dict[str, str] = {}

# mr_id -> список остановок с именами
stops_cache: dict[str, list] = {}

# mr_id -> список рейсов с упорядоченными остановками (для определения направления)
races_cache: dict[str, list] = {}

POLL_INTERVAL = 30  # секунд между проверками

CARD_COLORS = ["❤️", "🧡", "💛", "💚", "💙", "💜", "🩷", "🤍", "🖤", "🤎"]
ADDCARD_NUMBER, ADDCARD_NAME, ADDCARD_COLOR = range(10, 13)


H = "HTML"  # parse_mode shortcut


def _main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔔 Мои подписки", callback_data="menu:status"),
            InlineKeyboardButton("💳 Мои карты",    callback_data="menu:cards"),
        ],
        [InlineKeyboardButton("🚌 Найти транспорт", callback_data="findbus:start")],
        [InlineKeyboardButton("❓ Справка",          callback_data="menu:help")],
    ])


def _main_menu_text(name: str = "") -> str:
    greeting = f"👋 <b>{name}!</b>\n\n" if name else ""
    return (
        greeting
        + "Я слежу за автобусами Омска в реальном времени.\n\n"
        "💡 Напиши номер маршрута — например <b>24</b> или <b>212</b> — "
        "и выбери что сделать."
    )


def _cards_text_and_markup(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    cards = db_get_cards(uid)
    add_btn = InlineKeyboardButton("➕ Добавить карту", callback_data="card:addnew")
    if not cards:
        return (
            "💳 У тебя пока нет сохранённых карт.\n\nДобавь первую!",
            InlineKeyboardMarkup([[add_btn]]),
        )
    buttons = [
        [InlineKeyboardButton(f"{c['color']} {c['name']}", callback_data=f"card:check:{c['card_number']}")]
        for c in cards
    ]
    buttons.append([add_btn])
    return f"💳 <b>Мои карты ({len(cards)}):</b>\n\nНажми на карту, чтобы узнать баланс.", InlineKeyboardMarkup(buttons)


# ── Команды ────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    name = update.effective_user.first_name or ""
    await update.message.reply_html(
        _main_menu_text(name),
        reply_markup=_main_menu_markup(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_html(
        _main_menu_text(),
        reply_markup=_main_menu_markup(),
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    routes = subscriptions.get(uid, set())
    if not routes:
        await update.message.reply_html(
            "У тебя нет активных подписок.\n\n"
            "Напиши номер маршрута или используй /track <i>номер</i>"
        )
        return

    lines = []
    for r in sorted(routes, key=lambda x: (len(x), x)):
        known = len(known_vehicles.get(r, set()))
        desc  = OMSK_ROUTES.get(r, "")
        lines.append(
            f"  🚌 <b>{r}</b> — {desc}\n"
            f"       На линии сейчас: <b>{known} ТС</b>"
        )

    await update.message.reply_html(
        f"🔔 <b>Отслеживаемые маршруты ({len(routes)}):</b>\n\n"
        + "\n\n".join(lines)
        + "\n\n"
        "/stop <i>номер</i> — снять маршрут\n"
        "/stop — снять все"
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    routes = subscriptions.get(uid, set())
    if not routes:
        await update.message.reply_html("У тебя нет активных подписок.")
        return

    if context.args:
        route = context.args[0].strip().upper()
        if route in routes:
            routes.discard(route)
            db_remove_sub(uid, route)
            remaining = len(routes)
            tail = f"\nОсталось подписок: <b>{remaining}</b>" if remaining else "\nВсе подписки сняты."
            await update.message.reply_html(f"✅ Маршрут <b>{route}</b> снят с отслеживания.{tail}")
        else:
            active = ", ".join(f"<b>{r}</b>" for r in sorted(routes, key=lambda x: (len(x), x)))
            await update.message.reply_html(
                f"Маршрут <b>{route}</b> не отслеживается.\n"
                f"Активные: {active}"
            )
    else:
        subscriptions.pop(uid, None)
        db_remove_all_subs(uid)
        await update.message.reply_html("🛑 Слежение за всеми маршрутами остановлено.")


async def cmd_track(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_html("Укажи номер маршрута: /track <i>212</i>")
        return
    await subscribe(update, context.args[0])


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get("findbus_waiting_from"):
        context.user_data.pop("findbus_waiting_from")
        await _handle_findbus_from_text(update, context)
        return

    if context.user_data.get("findbus_waiting_dest"):
        context.user_data.pop("findbus_waiting_dest")
        await _handle_findbus_dest(update, context)
        return

    route = (update.message.text or "").strip().upper()
    if not route:
        return

    if route not in OMSK_ROUTES:
        close = [r for r in OMSK_ROUTES if r.startswith(route[:2])] if len(route) >= 2 else []
        hint  = ("\n\nПохожие: " + ", ".join(f"<b>{r}</b>" for r in sorted(close)[:5])) if close else ""
        await update.message.reply_html(
            f"❌ Маршрут <b>{route}</b> не найден в списке омских маршрутов.{hint}\n\n"
            f"Напиши точный номер, например: <b>24</b>, <b>55</b>, <b>212</b>"
        )
        return

    description = OMSK_ROUTES.get(route, "")
    desc_line   = f"\n<i>{description}</i>" if description else ""
    uid = update.effective_user.id
    already = route in subscriptions.get(uid, set())

    await update.message.reply_html(
        f"Маршрут <b>{route}</b>{desc_line}\n\nЧто сделать?",
        reply_markup=_route_menu_markup(route, uid),
    )


def _vehicle_buttons(
    route: str,
    route_vehicles: list[dict],
    stops: Optional[list[dict]] = None,
) -> list[list[InlineKeyboardButton]]:
    buttons = []
    for v in route_vehicles:
        plate  = str(v.get("u_statenum", "") or "").strip() or "б/н"
        uid_v  = str(v.get("u_id", ""))
        speed  = int(float(v.get("u_speed", 0) or 0))
        label  = f"🚌 {plate}  {speed} км/ч"
        if stops is not None:
            lat  = float(v.get("u_lat",  0) or 0)
            lng  = float(v.get("u_long", 0) or 0)
            stop = nearest_stop_name(lat, lng, stops) if lat and lng else None
            if stop:
                label += f"  📍 {stop}"
        else:
            terminal = str(v.get("rl_laststation_title", "") or "").strip()
            if terminal:
                label += f"  → {terminal}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"where:{uid_v}:{route}")])
    return buttons


def _track_confirm_markup(route: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 ТС на линии", callback_data=f"route:where:{route}")],
        [
            InlineKeyboardButton(f"🛑 Снять {route}", callback_data=f"route:stop:{route}"),
            InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back"),
        ],
    ])


def _route_menu_markup(route: str, uid: int) -> InlineKeyboardMarkup:
    already = route in subscriptions.get(uid, set())
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Уже отслеживается" if already else "🔔 Отслеживать",
                callback_data=f"route:track:{route}",
            ),
            InlineKeyboardButton("📍 ТС на линии", callback_data=f"route:where:{route}"),
        ],
        [InlineKeyboardButton("🗺 Список остановок", callback_data=f"route:stops:{route}")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back")],
    ])


async def _show_direction_filter(edit_fn, route: str, context, back_uid: Optional[int] = None) -> None:
    """Показывает фильтр по направлениям или сразу список ТС (если направление одно).
    back_uid: если передан, добавляет кнопку «Назад» к меню маршрута."""
    vehicles = fetch_vehicles()
    route_vehicles = [
        v for v in vehicles
        if str(v.get("mr_num", "")).strip().upper() == route
        and v.get("u_lat") and v.get("u_long")
    ]

    description = OMSK_ROUTES.get(route, "")
    header = f"📍 Маршрут <b>{route}</b>"
    if description:
        header += f"\n<i>{description}</i>"

    back_row = [[InlineKeyboardButton("◀️ Назад", callback_data=f"route:menu:{route}")]] if back_uid is not None else []

    if not route_vehicles:
        await edit_fn(
            header + "\n\nСейчас нет ТС на линии.",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup(back_row) if back_row else None,
        )
        return

    # Уникальные направления, сохраняем порядок появления
    terminals: list[str] = []
    seen: set[str] = set()
    for v in route_vehicles:
        t = str(v.get("rl_laststation_title", "") or "").strip()
        if t and t not in seen:
            terminals.append(t)
            seen.add(t)

    context.user_data[f"filter_terms:{route}"] = terminals

    if len(terminals) <= 1:
        buttons = _vehicle_buttons(route, route_vehicles) + back_row
        await edit_fn(
            header + f"\n\nНа линии <b>{len(route_vehicles)} ТС</b>. Выбери автобус:",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    buttons = [[InlineKeyboardButton("🚌 Все ТС", callback_data=f"filter:{route}:all")]]
    for i, t in enumerate(terminals):
        buttons.append([InlineKeyboardButton(f"→ {t}", callback_data=f"filter:{route}:{i}")])
    buttons += back_row

    await edit_fn(
        header + f"\n\nНа линии <b>{len(route_vehicles)} ТС</b>. Выбери направление:",
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def on_route_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    parts  = query.data.split(":", 3)
    action = parts[1]
    route  = parts[2] if len(parts) > 2 else ""
    uid    = query.from_user.id

    if action == "track":
        # Логика подписки (аналог subscribe(), но редактирует сообщение)
        vehicles = fetch_vehicles()
        for v in vehicles:
            if str(v.get("mr_num", "")).strip().upper() == route:
                mid = str(v.get("mr_id", "")).strip()
                if mid:
                    mr_id_cache[route] = mid
                    break
        if route not in mr_id_cache:
            try:
                conn = sqlite3.connect(DB_PATH)
                row  = conn.execute(
                    "SELECT mr_id FROM route_navitrans_ids WHERE route_number = ?", (route,)
                ).fetchone()
                conn.close()
                if row:
                    mr_id_cache[route] = str(row[0])
            except Exception:
                pass
        if route in mr_id_cache:
            mid = mr_id_cache[route]
            if mid not in stops_cache:
                stops_cache[mid] = fetch_route_stops(mid)

        current_ids = {
            str(v.get("u_id", ""))
            for v in vehicles
            if str(v.get("mr_num", "")).strip().upper() == route and v.get("u_id")
        }
        known_vehicles[route] = current_ids
        subscriptions[uid].add(route)
        db_add_sub(uid, route)

        count       = len(current_ids)
        total       = len(subscriptions[uid])
        description = OMSK_ROUTES.get(route, "")
        desc_line   = f"\n<i>{description}</i>" if description else ""
        routes_list = ", ".join(
            f"<b>{r}</b>" for r in sorted(subscriptions[uid], key=lambda x: (len(x), x))
        )
        await query.edit_message_text(
            f"🔔 Маршрут <b>{route}</b>{desc_line}\n\n"
            f"Слежение включено. Сейчас на линии: <b>{count} ТС</b>.\n"
            f"Пришлю уведомление, когда появится новое.\n\n"
            f"Отслеживаемых маршрутов ({total}): {routes_list}",
            parse_mode=H,
            reply_markup=_track_confirm_markup(route),
        )

    elif action == "where":
        await query.edit_message_text("⏳ Загружаю ТС...", parse_mode=H)
        await _show_direction_filter(query.edit_message_text, route, context, back_uid=uid)

    elif action == "stop":
        routes_set = subscriptions.get(uid, set())
        if route in routes_set:
            routes_set.discard(route)
            db_remove_sub(uid, route)
        description = OMSK_ROUTES.get(route, "")
        desc_line   = f"\n<i>{description}</i>" if description else ""
        await query.edit_message_text(
            f"🛑 Маршрут <b>{route}</b>{desc_line} снят с отслеживания.",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔔 Снова подписаться", callback_data=f"route:track:{route}")],
                [
                    InlineKeyboardButton("🔔 Мои подписки", callback_data="menu:status"),
                    InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back"),
                ],
            ]),
        )

    elif action == "menu":
        description = OMSK_ROUTES.get(route, "")
        desc_line   = f"\n<i>{description}</i>" if description else ""
        await query.edit_message_text(
            f"Маршрут <b>{route}</b>{desc_line}\n\nЧто сделать?",
            parse_mode=H,
            reply_markup=_route_menu_markup(route, uid),
        )

    elif action == "stops":
        await query.edit_message_text("⏳ Загружаю остановки...")
        mr_id = mr_id_cache.get(route)
        if not mr_id:
            vehicles = await asyncio.to_thread(fetch_vehicles)
            for v in vehicles:
                if str(v.get("mr_num", "")).strip().upper() == route:
                    mr_id = str(v.get("mr_id", "")).strip()
                    if mr_id:
                        mr_id_cache[route] = mr_id
                        break
        if mr_id and mr_id not in stops_cache:
            stops_cache[mr_id] = await asyncio.to_thread(fetch_route_stops, mr_id)

        back_btn = InlineKeyboardButton("◀️ Назад", callback_data=f"route:menu:{route}")
        home_btn = InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back")

        races = races_cache.get(mr_id, []) if mr_id else []
        if not races:
            await query.edit_message_text(
                f"Маршрут <b>{route}</b>\n\nНе удалось загрузить список остановок.",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[back_btn, home_btn]]),
            )
            return

        # Уникальные направления по последней остановке рейса
        seen_dirs: set[str] = set()
        dirs: list[tuple[int, str]] = []  # (race_idx, terminal_name)
        for i, race in enumerate(races):
            stops_list = race.get("stopList", [])
            if not stops_list:
                continue
            terminal = (stops_list[-1].get("st_title") or stops_list[-1].get("st_name") or "").strip()
            if terminal and terminal not in seen_dirs:
                seen_dirs.add(terminal)
                dirs.append((i, terminal))

        if len(dirs) == 1:
            # Одно направление — сразу показываем
            context.user_data[f"stops_races:{route}"] = races
            race_idx, terminal = dirs[0]
            await _show_route_stops(query.edit_message_text, route, races, race_idx, terminal)
        else:
            context.user_data[f"stops_races:{route}"] = races
            buttons = [
                [InlineKeyboardButton(f"→ {terminal}", callback_data=f"route:stops_dir:{route}:{i}")]
                for i, (race_idx, terminal) in enumerate(dirs)
            ]
            buttons.append([back_btn, home_btn])
            await query.edit_message_text(
                f"🗺 Маршрут <b>{route}</b> — выбери направление:",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup(buttons),
            )
            context.user_data[f"stops_dirs:{route}"] = dirs

    elif action == "stops_dir":
        # route:stops_dir:ROUTE:DIR_IDX
        parts   = query.data.split(":", 3)
        dir_idx = int(parts[3]) if len(parts) > 3 else 0
        races   = context.user_data.get(f"stops_races:{route}", [])
        dirs    = context.user_data.get(f"stops_dirs:{route}", [])
        if not races or not dirs or dir_idx >= len(dirs):
            await query.edit_message_text("Данные устарели. Попробуй снова.",
                                          reply_markup=InlineKeyboardMarkup([[
                                              InlineKeyboardButton("🔄", callback_data=f"route:stops:{route}"),
                                              InlineKeyboardButton("🏠", callback_data="menu:back"),
                                          ]]))
            return
        race_idx, terminal = dirs[dir_idx]
        await _show_route_stops(query.edit_message_text, route, races, race_idx, terminal)


async def _show_route_stops(edit_fn, route: str, races: list, race_idx: int, terminal: str) -> None:
    """Показывает нумерованный список остановок для одного направления."""
    back_btn = InlineKeyboardButton("◀️ Назад к направлениям", callback_data=f"route:stops:{route}")
    home_btn = InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back")

    stops_list = races[race_idx].get("stopList", [])
    names = [(s.get("st_title") or s.get("st_name") or "").strip() for s in stops_list]
    names = [n for n in names if n]

    if not names:
        await edit_fn(
            f"🗺 Маршрут <b>{route}</b> → {terminal}\n\nСписок остановок недоступен.",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[back_btn], [home_btn]]),
        )
        return

    # Нумерованный список; Telegram лимит 4096 символов — обрезаем если нужно
    lines = [f"🗺 <b>{route}</b> → <b>{terminal}</b>\n"]
    for i, name in enumerate(names, 1):
        lines.append(f"{i}. {name}")
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n…"

    await edit_fn(
        text,
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup([[back_btn], [home_btn]]),
    )


_BACK_TO_MENU = [[InlineKeyboardButton("◀️ Главное меню", callback_data="menu:back")]]


async def on_menu_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, action = query.data.split(":", 1)
    uid = query.from_user.id

    if action == "back":
        name = query.from_user.first_name or ""
        await query.edit_message_text(
            _main_menu_text(name), parse_mode=H, reply_markup=_main_menu_markup()
        )

    elif action == "help":
        await query.edit_message_text(
            "<b>Маршруты</b>\n"
            "Напиши номер маршрута в чат (например <b>212</b>) и выбери что сделать.\n\n"
            "/track <i>номер</i> — начать отслеживать\n"
            "/where <i>номер</i> — где ТС прямо сейчас\n"
            "/status — мои подписки\n"
            "/stop <i>номер</i> — снять маршрут\n\n"
            "<b>Карты ОМКА</b>\n"
            "/cards — мои карты\n"
            "/addcard — добавить карту\n"
            "/card <i>номер</i> — разовая проверка баланса",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup(_BACK_TO_MENU),
        )

    elif action == "status":
        routes = subscriptions.get(uid, set())
        if not routes:
            text = "У тебя нет активных подписок.\n\n💡 Напиши номер маршрута чтобы начать отслеживать."
            markup = InlineKeyboardMarkup(_BACK_TO_MENU)
        else:
            lines   = []
            buttons = []
            for r in sorted(routes, key=lambda x: (len(x), x)):
                known = len(known_vehicles.get(r, set()))
                desc  = OMSK_ROUTES.get(r, "")
                lines.append(f"🚌 <b>{r}</b> — {desc}\n     На линии: <b>{known} ТС</b>")
                buttons.append([
                    InlineKeyboardButton(f"📍 {r}", callback_data=f"route:where:{r}"),
                    InlineKeyboardButton("🛑", callback_data=f"route:stop:{r}"),
                ])
            buttons.append([InlineKeyboardButton("🛑 Снять все", callback_data="menu:stopall")])
            buttons += _BACK_TO_MENU
            text   = f"🔔 <b>Отслеживаемые маршруты ({len(routes)}):</b>\n\n" + "\n\n".join(lines)
            markup = InlineKeyboardMarkup(buttons)
        await query.edit_message_text(text, parse_mode=H, reply_markup=markup)

    elif action == "stopall":
        subscriptions.pop(uid, None)
        db_remove_all_subs(uid)
        await query.edit_message_text(
            "🛑 Все подписки сняты.",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup(_BACK_TO_MENU),
        )

    elif action == "cards":
        text, base_markup = _cards_text_and_markup(uid)
        buttons = list(base_markup.inline_keyboard) + _BACK_TO_MENU
        await query.edit_message_text(text, parse_mode=H, reply_markup=InlineKeyboardMarkup(buttons))


_TOPUP_BUTTON = InlineKeyboardMarkup([[
    InlineKeyboardButton("💳 Пополнить на сайте", url="https://etk55.ru/balance/")
]])


# ── Список карт и управление ───────────────────────────────────────

async def cmd_cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    text, markup = _cards_text_and_markup(uid)
    await update.message.reply_html(text, reply_markup=markup)


async def cmd_addcard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = "Введи номер карты ОМКА (только цифры):"
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text)
    else:
        await update.message.reply_text(text)
    return ADDCARD_NUMBER


async def addcard_got_number(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    card = update.message.text.strip()
    if not card or not all(c.isdigit() or c == " " for c in card) or not any(c.isdigit() for c in card):
        await update.message.reply_text("❌ Только цифры. Попробуй ещё раз:")
        return ADDCARD_NUMBER
    context.user_data["new_card_number"] = card
    await update.message.reply_text("Дай название этой карте (например: «Основная», «Рабочая»):")
    return ADDCARD_NAME


async def addcard_got_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = update.message.text.strip()[:30]
    if not name:
        await update.message.reply_text("Название не может быть пустым. Попробуй ещё раз:")
        return ADDCARD_NAME
    context.user_data["new_card_name"] = name
    rows = [CARD_COLORS[:5], CARD_COLORS[5:]]
    buttons = [[InlineKeyboardButton(c, callback_data=f"cardcolor:{c}") for c in row] for row in rows]
    await update.message.reply_text("Выбери цвет:", reply_markup=InlineKeyboardMarkup(buttons))
    return ADDCARD_COLOR


async def addcard_got_color(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    color = query.data.split(":", 1)[1]
    uid = query.from_user.id
    card_number = context.user_data.pop("new_card_number", "")
    name = context.user_data.pop("new_card_name", "Карта")
    db_add_card(uid, card_number, name, color)
    text, markup = _cards_text_and_markup(uid)
    await query.edit_message_text(
        f"✅ Карта добавлена: {color} <b>{name}</b>\n\n" + text,
        parse_mode=H,
        reply_markup=markup,
    )
    return ConversationHandler.END


async def addcard_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("new_card_number", None)
    context.user_data.pop("new_card_name", None)
    await update.message.reply_text(
        "Добавление карты отменено.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back")
        ]]),
    )
    return ConversationHandler.END


async def on_card_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":", 2)
    action = parts[1]
    uid = query.from_user.id

    if action == "addnew":
        await query.answer("Отправь /addcard чтобы добавить карту")
        return

    if action == "list":
        text, markup = _cards_text_and_markup(uid)
        await query.edit_message_text(text, parse_mode=H, reply_markup=markup)
        return

    card_number = parts[2] if len(parts) > 2 else ""
    cards = db_get_cards(uid)
    card = next((c for c in cards if c["card_number"] == card_number), None)
    name  = card["name"]  if card else "Карта"
    color = card["color"] if card else "💳"

    if action == "check":
        await query.edit_message_text("⏳ Проверяю баланс...")
        data = fetch_card_balance(card_number)

        back_btn   = InlineKeyboardButton("◀️ Мои карты", callback_data="card:list")
        delete_btn = InlineKeyboardButton("🗑 Удалить", callback_data=f"card:delete:{card_number}")
        topup_btn  = InlineKeyboardButton("💳 Пополнить на сайте", url="https://etk55.ru/balance/")
        markup = InlineKeyboardMarkup([[topup_btn], [delete_btn, back_btn]])

        def _build_text(balance_line: str, extra: str = "") -> str:
            return (
                f"{color} <b>{name}</b>\n"
                f"<code>{card_number}</code>\n\n"
                f"💰 Баланс: {balance_line}" + extra
            )

        if not data:
            await query.edit_message_text(_build_text("нет данных", "\n❌ Сервер не ответил"), parse_mode=H, reply_markup=markup)
            return

        if data.get("success"):
            resp    = data.get("response", {})
            info    = resp.get("info", {})
            balance = info.get("balance")
            tariff_text = (resp.get("tariff") or {}).get("text") or ""
            warning     = resp.get("warningMsg") or ""
            b_line = f"<b>{balance} ₽</b>" if isinstance(balance, (int, float)) else "нет данных"
            extra  = (f"\n📋 Тариф: {tariff_text}" if tariff_text else "") + (f"\n⚠️ {warning}" if warning else "")
            await query.edit_message_text(_build_text(b_line, extra), parse_mode=H, reply_markup=markup)
        else:
            error     = data.get("error", {})
            error_msg = error.get("errorMsg") or data.get("message") or "Неизвестная ошибка"
            inner_bal = (error.get("response") or {}).get("info", {}).get("balance")
            warning   = (error.get("response") or {}).get("warningMsg") or ""
            if isinstance(inner_bal, (int, float)):
                extra = f"\n⚠️ {error_msg}" + (f"\n{warning}" if warning else "")
                await query.edit_message_text(_build_text(f"<b>{inner_bal} ₽</b>", extra), parse_mode=H, reply_markup=markup)
            else:
                await query.edit_message_text(_build_text(f"❌ {error_msg}"), parse_mode=H, reply_markup=markup)

    elif action == "delete":
        await query.edit_message_text(
            f"Удалить карту {color} <b>{name}</b>?\n<code>{card_number}</code>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Да, удалить", callback_data=f"card:confirmdelete:{card_number}"),
                InlineKeyboardButton("❌ Отмена", callback_data="card:list"),
            ]]),
        )

    elif action == "confirmdelete":
        db_remove_card(uid, card_number)
        text, markup = _cards_text_and_markup(uid)
        await query.edit_message_text(
            f"🗑 Карта {color} <b>{name}</b> удалена.\n\n" + text,
            parse_mode=H,
            reply_markup=markup,
        )


async def cmd_card(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_html(
            "💳 Укажи номер карты ОМКА:\n"
            "/card <i>123456789</i>\n\n"
            "Номер карты напечатан на обратной стороне карты."
        )
        return

    card = " ".join(context.args).strip()
    if not all(c.isdigit() or c == " " for c in card):
        await update.message.reply_html(
            "❌ Номер карты должен содержать только цифры.\n"
            "Пример: <code>/card 123456789</code>"
        )
        return

    msg = await update.message.reply_text("⏳ Запрашиваю баланс...")
    data = fetch_card_balance(card)

    if not data:
        await msg.edit_text("❌ Сервер etk55.ru не ответил. Попробуй позже.", reply_markup=_TOPUP_BUTTON)
        return

    if data.get("success"):
        resp    = data.get("response", {})
        info    = resp.get("info", {})
        balance = info.get("balance")
        tariff_text = (resp.get("tariff") or {}).get("text") or ""
        warning = resp.get("warningMsg") or ""

        balance_line = f"<b>{balance} ₽</b>" if isinstance(balance, (int, float)) else "нет данных"
        text = (
            f"💳 Карта ОМКА: <code>{card}</code>\n\n"
            f"💰 Баланс: {balance_line}"
        )
        if tariff_text:
            text += f"\n📋 Тариф: {tariff_text}"
        if warning:
            text += f"\n⚠️ {warning}"
        await msg.edit_text(text, parse_mode=H, reply_markup=_TOPUP_BUTTON)
    else:
        error     = data.get("error", {})
        error_msg = error.get("errorMsg") or data.get("message") or "Неизвестная ошибка"
        inner_balance = (error.get("response") or {}).get("info", {}).get("balance")
        warning       = (error.get("response") or {}).get("warningMsg") or ""

        if isinstance(inner_balance, (int, float)):
            text = (
                f"💳 Карта ОМКА: <code>{card}</code>\n\n"
                f"💰 Баланс: <b>{inner_balance} ₽</b>\n"
                f"⚠️ {error_msg}"
            )
            if warning:
                text += f"\n{warning}"
            await msg.edit_text(text, parse_mode=H, reply_markup=_TOPUP_BUTTON)
        else:
            await msg.edit_text(
                f"❌ Не удалось получить баланс.\n\n{error_msg}",
                parse_mode=H,
                reply_markup=_TOPUP_BUTTON,
            )


# ── Диагностика API ───────────────────────────────────────────────

async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    route = (context.args[0].strip().upper() if context.args else "").strip()
    msg = await update.message.reply_text("⏳ Тестирую сессию API...")

    # Тест startSession напрямую — показываем сырой ответ в боте
    try:
        r = http.post(BUS55_BASE, headers=BUS55_HEADERS, json={
            "jsonrpc": BUS55_RPC, "method": "startSession",
            "ts": _ts(), "params": {}, "id": 1,
        }, timeout=8)
        session_line = f"HTTP {r.status_code} — <code>{r.text[:300]}</code>"
        session_ok = "sid" in r.text
    except Exception as e:
        session_line = f"Исключение: <code>{e}</code>"
        session_ok = False

    if not session_ok:
        await msg.edit_text(
            f"❌ startSession провалился\n\n{session_line}",
            parse_mode=H,
        )
        return

    await msg.edit_text("✅ Сессия OK. Запрашиваю ТС...", parse_mode=H)
    sid2 = _get_sid()
    rid2 = _next_id()
    url2, magic2 = _sign("getUnitsInRect", rid2, sid2)
    try:
        r2 = http.post(url2, headers=BUS55_HEADERS, json={
            "jsonrpc": BUS55_RPC, "method": "getUnitsInRect",
            "ts": _ts(), "id": rid2,
            "params": {"sid": sid2, "magic": magic2, "minlat": 54.80, "maxlat": 55.15, "minlong": 73.10, "maxlong": 73.70},
        }, timeout=12)
        rect_line = f"HTTP {r2.status_code} — <code>{r2.text[:400]}</code>"
    except Exception as e:
        rect_line = f"Исключение: <code>{e}</code>"

    vehicles = fetch_vehicles()
    total = len(vehicles)

    if total == 0:
        await msg.edit_text(
            f"⚠️ Сессия открылась, но getUnitsInRect вернул 0 ТС.\n\n"
            f"<b>startSession:</b>\n{session_line}\n\n"
            f"<b>getUnitsInRect:</b>\n{rect_line}",
            parse_mode=H,
        )
        return

    all_nums = sorted({str(v.get("mr_num", "")).strip() for v in vehicles if v.get("mr_num")})
    matched = [v for v in vehicles if str(v.get("mr_num", "")).strip().upper() == route] if route else []

    sample = ", ".join(all_nums[:40])
    text = (
        f"✅ Сессия OK\n"
        f"Всего ТС из API: <b>{total}</b>\n"
        f"Уникальных маршрутов: <b>{len(all_nums)}</b>\n"
        f"Примеры mr_num: <code>{sample}</code>"
    )
    if route:
        text += f"\n\nМаршрут <b>{route}</b>: найдено <b>{len(matched)} ТС</b>"
        if matched:
            plates = ", ".join(str(v.get("u_statenum", "б/н") or "б/н") for v in matched[:5])
            text += f"\nГосномера: {plates}"

    await msg.edit_text(text, parse_mode=H)


# ── Где конкретное ТС ─────────────────────────────────────────────

async def cmd_where(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_html("Укажи маршрут: /where <i>212</i>")
        return

    route = context.args[0].strip().upper()
    msg = await update.message.reply_text(f"⏳ Загружаю ТС маршрута {route}...")
    await _show_direction_filter(msg.edit_text, route, context)


async def on_filter_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    _, route, idx_str = query.data.split(":", 2)
    await query.edit_message_text("⏳ Загружаю ТС...")

    vehicles = fetch_vehicles()
    route_vehicles = [
        v for v in vehicles
        if str(v.get("mr_num", "")).strip().upper() == route
        and v.get("u_lat") and v.get("u_long")
    ]

    description = OMSK_ROUTES.get(route, "")
    header = f"📍 Маршрут <b>{route}</b>"
    if description:
        header += f"\n<i>{description}</i>"

    back_btn = InlineKeyboardButton("◀️ Назад к фильтру", callback_data=f"route:where:{route}")

    if idx_str == "all":
        filtered   = route_vehicles
        dir_suffix = ""
        stops      = None
    else:
        terminals     = context.user_data.get(f"filter_terms:{route}", [])
        idx           = int(idx_str)
        terminal_name = terminals[idx] if idx < len(terminals) else ""
        filtered = [
            v for v in route_vehicles
            if str(v.get("rl_laststation_title", "") or "").strip() == terminal_name
        ] if terminal_name else route_vehicles
        dir_suffix = f"\n→ <b>{terminal_name}</b>" if terminal_name else ""

        # Подгружаем остановки для отображения в кнопках
        mr_id = mr_id_cache.get(route)
        if not mr_id:
            for v in vehicles:
                if str(v.get("mr_num", "")).strip().upper() == route:
                    mr_id = str(v.get("mr_id", "")).strip()
                    if mr_id:
                        mr_id_cache[route] = mr_id
                        break
        if mr_id:
            if mr_id not in stops_cache:
                stops_cache[mr_id] = fetch_route_stops(mr_id)
            stops = stops_cache.get(mr_id) or None
        else:
            stops = None

    if not filtered:
        await query.edit_message_text(
            header + dir_suffix + "\n\nСейчас нет ТС в этом направлении.",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[back_btn]]),
        )
        return

    buttons = _vehicle_buttons(route, filtered, stops=stops)
    buttons.append([back_btn])
    await query.edit_message_text(
        header + dir_suffix + f"\n\nНа линии <b>{len(filtered)} ТС</b>. Выбери автобус:",
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def on_where_vehicle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    _, uid_v, route = query.data.split(":", 2)

    vehicles = fetch_vehicles()
    v = next(
        (x for x in vehicles if str(x.get("u_id", "")) == uid_v),
        None,
    )

    if not v:
        await query.edit_message_text(
            "ТС уже не на линии.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ К списку ТС", callback_data=f"route:where:{route}")
            ]]),
        )
        return

    lat    = float(v.get("u_lat",  0) or 0)
    lng    = float(v.get("u_long", 0) or 0)
    plate  = str(v.get("u_statenum", "") or "").strip() or "б/н"
    speed  = int(float(v.get("u_speed", 0) or 0))
    course = v.get("u_course")
    now    = datetime.now().strftime("%H:%M:%S")

    # Ближайшая остановка
    stop_name = "нет данных"
    mr_id = mr_id_cache.get(route)
    if not mr_id:
        for x in vehicles:
            if str(x.get("mr_num", "")).strip().upper() == route:
                mr_id = str(x.get("mr_id", "")).strip()
                if mr_id:
                    mr_id_cache[route] = mr_id
                    break
    if mr_id:
        if mr_id not in stops_cache:
            stops_cache[mr_id] = fetch_route_stops(mr_id)
        stops = stops_cache.get(mr_id, [])
        if stops:
            stop_name = nearest_stop_name(lat, lng, stops) or "нет данных"

    terminal = str(v.get("rl_laststation_title", "") or "").strip()
    terminal_line = f"\n🏁 В сторону: <b>{terminal}</b>" if terminal else ""

    description = OMSK_ROUTES.get(route, "")
    desc_line   = f"\n<i>{description}</i>" if description else ""
    caption = (
        f"🚌 Маршрут <b>{route}</b>{desc_line}\n\n"
        f"🚗 Госномер: <b>{plate}</b>\n"
        f"🕐 Время: <b>{now}</b>\n"
        f"⚡ Скорость: <b>{speed} км/ч</b>\n"
        f"📍 Остановка: <b>{stop_name}</b>"
        f"{terminal_line}"
    )

    back_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("◀️ К списку ТС", callback_data=f"route:where:{route}")
    ]])
    await query.edit_message_text(caption, parse_mode=H, reply_markup=back_markup)


async def on_findbus_where_vehicle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Детали ТС, открытые из findbus-результатов. Кнопка назад ведёт к списку подходящих маршрутов."""
    query = update.callback_query
    await query.answer()

    _, uid_v, route = query.data.split(":", 2)

    vehicles = fetch_vehicles()
    v = next((x for x in vehicles if str(x.get("u_id", "")) == uid_v), None)

    back_btn = InlineKeyboardButton("◀️ Назад к маршрутам", callback_data="findbus:refresh")

    if not v:
        await query.edit_message_text(
            "ТС уже не на линии.",
            reply_markup=InlineKeyboardMarkup([[back_btn]]),
        )
        return

    lat    = float(v.get("u_lat",  0) or 0)
    lng    = float(v.get("u_long", 0) or 0)
    plate  = str(v.get("u_statenum", "") or "").strip() or "б/н"
    speed  = int(float(v.get("u_speed", 0) or 0))
    course = v.get("u_course")
    now    = datetime.now().strftime("%H:%M:%S")

    stop_name = "нет данных"
    mr_id = mr_id_cache.get(route)
    if not mr_id:
        for x in vehicles:
            if str(x.get("mr_num", "")).strip().upper() == route:
                mr_id = str(x.get("mr_id", "")).strip()
                if mr_id:
                    mr_id_cache[route] = mr_id
                    break
    if mr_id:
        if mr_id not in stops_cache:
            stops_cache[mr_id] = fetch_route_stops(mr_id)
        stops = stops_cache.get(mr_id, [])
        if stops:
            stop_name = nearest_stop_name(lat, lng, stops) or "нет данных"

    terminal    = str(v.get("rl_laststation_title", "") or "").strip()
    terminal_ln = f"\n🏁 В сторону: <b>{terminal}</b>" if terminal else ""
    description = OMSK_ROUTES.get(route, "")
    desc_line   = f"\n<i>{description}</i>" if description else ""

    caption = (
        f"🚌 Маршрут <b>{route}</b>{desc_line}\n\n"
        f"🚗 Госномер: <b>{plate}</b>\n"
        f"🕐 Время: <b>{now}</b>\n"
        f"⚡ Скорость: <b>{speed} км/ч</b>\n"
        f"📍 Остановка: <b>{stop_name}</b>"
        f"{terminal_ln}"
    )

    await query.edit_message_text(
        caption,
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup([[back_btn]]),
    )


# ── Фоновый опрос ──────────────────────────────────────────────────

async def poll_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запускается каждые POLL_INTERVAL секунд."""
    if not subscriptions:
        return

    watched: set[str] = set()
    for routes in subscriptions.values():
        watched |= routes
    if not watched:
        return

    try:
        vehicles = fetch_vehicles()
    except Exception as e:
        log.warning("poll_job: ошибка при получении ТС: %s", e)
        return

    # Обновляем mr_id из живых данных
    for v in vehicles:
        mr_num = str(v.get("mr_num", "")).strip().upper()
        mr_id  = str(v.get("mr_id", "")).strip()
        if mr_num and mr_id and mr_num not in mr_id_cache:
            mr_id_cache[mr_num] = mr_id

    # Группируем по маршруту
    by_route: dict[str, list[dict]] = defaultdict(list)
    for v in vehicles:
        mr_num = str(v.get("mr_num", "")).strip().upper()
        if mr_num in watched:
            by_route[mr_num].append(v)

    # Обновляем кеш последнего положения всех ТС
    for v in vehicles:
        vid = str(v.get("u_id", ""))
        if vid:
            last_seen_vehicles[vid] = v

    # Определяем новые и пропавшие ТС по каждому маршруту один раз
    new_by_route:  dict[str, list[dict]] = {}
    gone_by_route: dict[str, list[dict]] = {}
    for route in watched:
        current_list = by_route.get(route, [])
        current_ids  = {str(v.get("u_id", "")) for v in current_list if v.get("u_id")}
        prev_ids     = known_vehicles.get(route, set())
        new_ids      = current_ids - prev_ids
        gone_ids     = prev_ids - current_ids
        known_vehicles[route] = current_ids
        new_by_route[route]  = [v for v in current_list if str(v.get("u_id", "")) in new_ids]
        gone_by_route[route] = [last_seen_vehicles[vid] for vid in gone_ids if vid in last_seen_vehicles]

    def _stop_for(v: dict, route: str) -> str:
        lat = float(v.get("u_lat", 0) or 0)
        lng = float(v.get("u_long", 0) or 0)
        if not lat or not lng:
            return "нет данных"
        mr_id = mr_id_cache.get(route)
        if not mr_id:
            return "нет данных"
        if mr_id not in stops_cache:
            stops_cache[mr_id] = fetch_route_stops(mr_id)
        stops = stops_cache.get(mr_id, [])
        return nearest_stop_name(lat, lng, stops) or "нет данных"

    for uid, user_routes in list(subscriptions.items()):
        for route in list(user_routes):
            now = datetime.now().strftime("%H:%M:%S")

            for v in new_by_route.get(route, []):
                plate    = str(v.get("u_statenum", "") or "").strip() or "нет данных"
                terminal = str(v.get("rl_laststation_title", "") or "").strip()
                stop_name = _stop_for(v, route)
                text = (
                    f"🟢 Маршрут {route} — новое ТС на линии\n"
                    f"🚗 Госномер: {plate}\n"
                    f"🕐 Время: {now}\n"
                    f"📍 Остановка: {stop_name}"
                    + (f"\n🏁 В сторону: {terminal}" if terminal else "")
                )
                try:
                    await context.bot.send_message(chat_id=uid, text=text)
                except Exception as e:
                    log.warning("send_message uid=%s: %s", uid, e)

            for v in gone_by_route.get(route, []):
                plate    = str(v.get("u_statenum", "") or "").strip() or "нет данных"
                speed    = int(float(v.get("u_speed", 0) or 0))
                terminal = str(v.get("rl_laststation_title", "") or "").strip()
                stop_name = _stop_for(v, route)

                text = (
                    f"🔴 Маршрут {route} — ТС сошло с линии\n"
                    f"🚗 Госномер: {plate}\n"
                    f"🕐 Время: {now}\n"
                    f"📍 Последняя остановка: {stop_name}\n"
                    f"⚡ Скорость: {speed} км/ч"
                    + (f"\n🏁 Ехало в сторону: {terminal}" if terminal else "")
                )
                try:
                    await context.bot.send_message(chat_id=uid, text=text)
                except Exception as e:
                    log.warning("send_message uid=%s: %s", uid, e)


# ── Найти транспорт ────────────────────────────────────────────────


def _fetch_stops_near(lat: float, lng: float, vehicles: list[dict], radius_m: float = 4000) -> None:
    """Загружает остановки для маршрутов с активными ТС в радиусе radius_m от точки."""
    for v in vehicles:
        vlat = float(v.get("u_lat", 0) or 0)
        vlng = float(v.get("u_long", 0) or 0)
        if not vlat or not vlng:
            continue
        if haversine_m(lat, lng, vlat, vlng) <= radius_m:
            mr_num = str(v.get("mr_num", "")).strip().upper()
            mr_id  = str(v.get("mr_id",  "")).strip()
            if mr_num and mr_id:
                mr_id_cache[mr_num] = mr_id
                if mr_id not in stops_cache:
                    stops_cache[mr_id] = fetch_route_stops(mr_id)


def _nearest_stop_from_cache(lat: float, lng: float) -> Optional[tuple]:
    """Возвращает (название, lat, lng, расстояние_м) ближайшей кешированной остановки."""
    best = None
    best_d = float("inf")
    for stops in stops_cache.values():
        for s in stops:
            d = haversine_m(lat, lng, s["lat"], s["lng"])
            if d < best_d:
                best_d = d
                best = (s["name"], s["lat"], s["lng"], d)
    return best


def _fetch_active_stops(vehicles: list[dict], max_routes: int = 25) -> None:
    """Загружает остановки для активных маршрутов, которых ещё нет в кеше.
    mr_id_cache обновляется для ВСЕХ маршрутов из vehicles (без лимита).
    Стоп-листы загружаются для первых max_routes новых маршрутов."""
    count = 0
    seen: set[str] = set()
    for v in vehicles:
        mr_num = str(v.get("mr_num", "")).strip().upper()
        mr_id  = str(v.get("mr_id",  "")).strip()
        if not mr_num or not mr_id or mr_num in seen:
            continue
        seen.add(mr_num)
        mr_id_cache[mr_num] = mr_id  # всегда обновляем, даже если stops не грузим
        if mr_id not in stops_cache:
            if count < max_routes:
                stops_cache[mr_id] = fetch_route_stops(mr_id)
                count += 1


def _stop_match_score(query: str, stop_name: str) -> float:
    """Качество совпадения 0–1. Штрафует остановки, у которых много лишних слов.
    Например: 'мега' vs 'Торговый центр МЕГА Магазин Леруа Мерлен' → ~0.17
               'мега' vs 'ТРК Мега' → 0.5     'мега' vs 'Мега' → 1.0"""
    import re as _re
    from difflib import SequenceMatcher as _SM
    q = query.lower().strip()
    n = stop_name.lower().strip()
    if q == n:
        return 1.0
    q_words = _re.findall(r'[а-яёa-z0-9]+', q)
    n_words = _re.findall(r'[а-яёa-z0-9]+', n)
    if not q_words or not n_words:
        return 0.0

    def _wm(a: str, b: str) -> bool:
        if a == b: return True
        if a.isdigit() or b.isdigit(): return False
        la, lb = len(a), len(b)
        if la < 3 or lb < 3: return False
        if abs(la - lb) > max(la, lb) * 0.3: return False
        return _SM(None, a, b).ratio() >= 0.82

    matched = sum(1 for qw in q_words if any(_wm(qw, nw) for nw in n_words))
    if matched == 0:
        return 0.0
    return matched / max(len(q_words), len(n_words))


def _search_stops_in_cache(query_str: str, min_score: float = 0.0) -> list[tuple]:
    """Ищет остановки по названию. Возвращает [(stop_name, mr_id), ...].
    min_score > 0 — фильтрует слабые совпадения (запрос покрывает мало слов названия)."""
    results: list[tuple] = []
    seen: set = set()
    for mr_id, stops in stops_cache.items():
        for s in stops:
            name = s["name"]
            if min_score > 0:
                if _stop_match_score(query_str, name) < min_score:
                    continue
            elif not _stop_name_matches(query_str, name):
                continue
            key = (name, mr_id)
            if key not in seen:
                seen.add(key)
                results.append((name, mr_id))
    return results


async def _fetch_all_active_stops_async(vehicles: list[dict]) -> None:
    """Загружает стопы ВСЕХ активных маршрутов параллельно (asyncio.gather)."""
    seen: set[str] = set()
    to_load: list[str] = []
    for v in vehicles:
        mr_num = str(v.get("mr_num", "")).strip().upper()
        mr_id  = str(v.get("mr_id",  "")).strip()
        if not mr_num or not mr_id or mr_num in seen:
            continue
        seen.add(mr_num)
        mr_id_cache[mr_num] = mr_id
        if mr_id not in stops_cache and mr_id not in to_load:
            to_load.append(mr_id)
    if to_load:
        loaded = await asyncio.gather(*[asyncio.to_thread(fetch_route_stops, mid) for mid in to_load])
        for mid, stops in zip(to_load, loaded):
            stops_cache[mid] = stops


def _find_direction_for_stops(mr_id: str, from_stop: str, to_stop: str) -> Optional[str]:
    """
    Возвращает терминал рейса, в котором from_stop стоит перед to_stop.
    Использует races_cache — не делает лишних API-запросов если данные уже есть.
    """
    races = races_cache.get(mr_id)
    if races is None:
        try:
            data  = _rpc("getRoute", {"mr_id": mr_id})
            races = data.get("result", {}).get("races", [])
            races_cache[mr_id] = races
        except Exception as e:
            log.warning("_find_direction_for_stops mr_id=%s: %s", mr_id, e)
            return None
    for race in races:
        names = [
            (s.get("st_title") or s.get("st_name") or "").strip()
            for s in race.get("stopList", [])
        ]
        from_i = _find_stop_idx(from_stop, names)
        to_i   = _find_stop_idx(to_stop,   names)
        if from_i != -1 and to_i != -1 and to_i > from_i and names:
            return names[-1]
    return None


def _direction_is_correct(mr_id: str, arr_dir: str, from_stop: str, to_stop: str) -> bool:
    """True если рейс mr_id с конечной arr_dir везёт from_stop → to_stop в правильном порядке."""
    races = races_cache.get(mr_id, [])
    ad = arr_dir.lower()
    for race in races:
        names = [(s.get("st_title") or s.get("st_name") or "").strip() for s in race.get("stopList", [])]
        if not names:
            continue
        last = names[-1].lower()
        if not (last == ad or ad in last or last in ad):
            continue
        from_i = _find_stop_idx(from_stop, names)
        to_i   = _find_stop_idx(to_stop,   names)
        if from_i != -1 and to_i != -1 and to_i > from_i:
            return True
    return False


def _find_routes_connecting(
    from_stop: str,
    to_stop: str,
    allowed_mr_ids: Optional[set] = None,
    from_stop_st_id: Optional[str] = None,
) -> list[tuple]:
    """
    Возвращает [(route_num, mr_id, terminal), ...] — маршруты, по которым можно
    доехать от from_stop до to_stop напрямую (to_stop стоит после from_stop в рейсе).
    allowed_mr_ids — если задано, проверяются только маршруты из этого множества.
    from_stop_st_id — если задано, проверяет вхождение по st_id (точнее fuzzy имени).
    """
    route_by_mr = {v: k for k, v in mr_id_cache.items()}
    results: list[tuple] = []
    seen_routes: set[str] = set()
    for mr_id, stops in stops_cache.items():
        if allowed_mr_ids is not None and mr_id not in allowed_mr_ids:
            continue
        route_num = route_by_mr.get(mr_id)
        if not route_num or route_num in seen_routes:
            continue
        if from_stop_st_id:
            from_found = any(s.get("st_id") == from_stop_st_id for s in stops)
        else:
            from_found = any(_stop_name_matches(from_stop, s["name"]) for s in stops)
        if not from_found:
            continue
        to_found = any(_stop_name_matches(to_stop, s["name"]) for s in stops)
        if not to_found:
            continue
        terminal = _find_direction_for_stops(mr_id, from_stop, to_stop)
        if terminal:
            seen_routes.add(route_num)
            results.append((route_num, mr_id, terminal))
    return sorted(results, key=lambda x: (len(x[0]), x[0]))


def _vehicle_is_before_stop(mr_id: str, terminal: str, vlat: float, vlng: float, from_stop: str) -> bool:
    """
    Возвращает True если ТС ещё не доехало до from_stop (будет там).
    Сравнивает индекс ближайшей к ТС остановки с индексом from_stop в рейсе.
    """
    races = races_cache.get(mr_id, [])
    for race in races:
        raw_stops = race.get("stopList", [])
        names = [(s.get("st_title") or s.get("st_name") or "").strip() for s in raw_stops]
        if not names:
            continue
        last = names[-1]
        if not (last == terminal or terminal.lower() in last.lower() or last.lower() in terminal.lower()):
            continue

        from_i = _find_stop_idx(from_stop, names)
        if from_i == -1:
            return True

        race_coords = [
            {"lat": float(s["st_lat"]), "lng": float(s["st_long"])}
            for s in raw_stops
            if s.get("st_lat") and s.get("st_long")
        ]
        if not race_coords:
            return True

        nearest_i = min(range(len(race_coords)), key=lambda i: haversine_m(vlat, vlng, race_coords[i]["lat"], race_coords[i]["lng"]))
        return nearest_i <= from_i

    return True  # рейс не найден — включаем по умолчанию


async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Пользователь отправил геолокацию — определяем ближайшую остановку."""
    loc = update.message.location
    lat, lng = loc.latitude, loc.longitude

    msg = await update.message.reply_text(
        "⏳ Определяю ближайшую остановку...",
        reply_markup=ReplyKeyboardRemove(),
    )

    vehicles = fetch_vehicles()
    _fetch_stops_near(lat, lng, vehicles)

    result = _nearest_stop_from_cache(lat, lng)
    if not result:
        await msg.edit_text(
            "Не удалось определить остановку. Попробуй ещё раз или введи название вручную.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Ввести название", callback_data="findbus:manual")],
                [InlineKeyboardButton("🏠 Главное меню",    callback_data="menu:back")],
            ]),
        )
        return

    stop_name, _, _, dist_m = result
    context.user_data["findbus_from_stop"] = stop_name
    context.user_data["findbus_from_lat"]  = lat
    context.user_data["findbus_from_lng"]  = lng

    dist_str = f"{int(dist_m)} м" if dist_m < 1000 else f"{dist_m / 1000:.1f} км"
    await msg.edit_text(
        f"📍 Похоже, вы у остановки:\n\n<b>{stop_name}</b>\n"
        f"(~{dist_str} от вас)\n\nВерно?",
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Да, верно", callback_data="findbus:geo_confirm"),
                InlineKeyboardButton("❌ Нет",       callback_data="findbus:geo_retry"),
            ],
        ]),
    )


async def _show_findbus_results(edit_fn, context, from_stop: str, to_stop: str) -> None:
    """Находит маршруты A→B и показывает до 5 ТС, отсортированных по близости."""
    home_btn      = InlineKeyboardButton("🏠 Главное меню",          callback_data="menu:back")
    retry_btn     = InlineKeyboardButton("🔄 Обновить",              callback_data="findbus:refresh")
    new_dest_btn  = InlineKeyboardButton("✏️ Другая конечная",       callback_data="findbus:askdest")
    new_from_btn  = InlineKeyboardButton("📍 Другая начальная",      callback_data="findbus:manual")

    vehicles = await asyncio.to_thread(fetch_vehicles)
    # Обновляем mr_id_cache для всех активных ТС — без HTTP-запросов
    for _v in vehicles:
        _rn  = str(_v.get("mr_num", "")).strip().upper()
        _mid = str(_v.get("mr_id",  "")).strip()
        if _rn and _mid:
            mr_id_cache[_rn] = _mid

    from_lat = context.user_data.get("findbus_from_lat")
    from_lng = context.user_data.get("findbus_from_lng")
    # При ручном вводе геокоординаты не сохранены — ищем в кеше остановок
    if not from_lat:
        for _stops in stops_cache.values():
            for _s in _stops:
                if _stop_name_matches(from_stop, _s["name"]):
                    from_lat, from_lng = _s["lat"], _s["lng"]
                    break
            if from_lat:
                break

    # Прогноз прибытия через API.
    # Сначала ищем st_id в кеше; если нет — запрашиваем через getStopsByName.
    st_id = _get_stop_id(from_stop) or await asyncio.to_thread(fetch_stop_id_by_name, from_stop)
    arrivals: list[dict] = await asyncio.to_thread(fetch_stop_arrivals, st_id) if st_id else []
    log.info("findbus: from=%r st_id=%s arrivals=%d", from_stop, st_id, len(arrivals))

    # Собираем mr_id маршрутов из прогноза, которых ещё нет в кеше
    forecast_mnums = {str(a.get("mr_num", "")).strip().upper() for a in arrivals}
    to_load: list[str] = []
    forecast_mr_ids: set[str] = set()  # mr_id только тех маршрутов, что в прогнозе
    for _v in vehicles:
        _rn  = str(_v.get("mr_num", "")).strip().upper()
        _mid = str(_v.get("mr_id",  "")).strip()
        if _rn not in forecast_mnums or not _mid:
            continue
        forecast_mr_ids.add(_mid)
        if _mid not in stops_cache and _mid not in to_load:
            to_load.append(_mid)
    log.info("findbus: forecast_mnums=%s forecast_mr_ids=%s", forecast_mnums, forecast_mr_ids)

    # Загружаем стопы параллельно — все сразу, не последовательно
    if to_load:
        loaded = await asyncio.gather(*[asyncio.to_thread(fetch_route_stops, mid) for mid in to_load])
        for mid, stops in zip(to_load, loaded):
            stops_cache[mid] = stops

    # Ищем только среди маршрутов из прогноза, а не по всему stops_cache
    # (иначе старые записи кеша дают ложные совпадения с чужими маршрутами)
    routes = _find_routes_connecting(from_stop, to_stop, forecast_mr_ids or None, from_stop_st_id=st_id or None)
    log.info("findbus: routes=%s", [(r[0], r[2]) for r in routes])
    if not routes:
        await edit_fn(
            f"Не нашли прямых маршрутов из «{from_stop}» до «{to_stop}».\n\n"
            "Возможно, нужна пересадка, другое название конечной или начальной остановки.",
            reply_markup=InlineKeyboardMarkup([[new_dest_btn, new_from_btn], [home_btn]]),
        )
        return

    from_display = _canonical_stop_name(from_stop)
    to_display   = _canonical_stop_name(to_stop)

    # mr_id для маршрутов, которые прошли проверку _find_routes_connecting — самый надёжный источник
    mr_id_by_route: dict[str, str] = {rn: mid for rn, mid, _ in routes}

    # Список ТС по маршруту, отсортированный по расстоянию до from_stop.
    # Используется чтобы сопоставить прибытие из прогноза с конкретным ТС (для near_stp и u_id).
    from collections import defaultdict
    vehicle_list: dict[str, list[dict]] = defaultdict(list)
    for v in vehicles:
        r_num = str(v.get("mr_num", "")).strip().upper()
        if not v.get("u_lat") or not v.get("u_long"):
            continue
        vlat = float(v["u_lat"])
        vlng = float(v["u_long"])
        # Предпочитаем mr_id из routes (прошёл проверку), иначе из общего кеша
        mr_id = mr_id_by_route.get(r_num) or mr_id_cache.get(r_num, "")
        route_stops = stops_cache.get(mr_id, []) if mr_id else []
        near_stp = nearest_stop_name(vlat, vlng, route_stops) or ""
        dist = haversine_m(from_lat, from_lng, vlat, vlng) if (from_lat and from_lng) else float("inf")
        vehicle_list[r_num].append({
            "u_id":     str(v.get("u_id", "")),
            "term":     str(v.get("rl_laststation_title", "") or "").strip(),
            "near_stp": near_stp,
            "dist":     dist,
            "vlat":     vlat,
            "vlng":     vlng,
            "mr_id":    mr_id,
            "plate":    str(v.get("u_statenum", "") or "").strip(),
        })
    for lst in vehicle_list.values():
        lst.sort(key=lambda x: x["dist"])

    # Фильтруем прибытия: маршрут в данном направлении должен везти from_stop → to_stop.
    # Проверка идёт напрямую по races_cache — не зависит от совпадения строки конечной.
    all_entries: list[dict] = []
    seen_arrivals: set[tuple] = set()
    assigned_uids: set[str] = set()
    route_count: dict[str, int] = defaultdict(int)  # не более 2 ТС одного маршрута

    for arr in arrivals:
        if len(all_entries) >= 5:
            break

        mr_num   = str(arr.get("mr_num", "")).strip().upper()
        arr_time = str(arr.get("tc_arrivetime", "")).strip()
        arr_dir  = str(arr.get("laststation_title", "")).strip()

        if route_count[mr_num] >= 2:
            continue

        mr_id = mr_id_by_route.get(mr_num) or mr_id_cache.get(mr_num)
        if not mr_id:
            continue
        if not _direction_is_correct(mr_id, arr_dir, from_stop, to_stop):
            continue

        key = (mr_num, arr_time, arr_dir)
        if key in seen_arrivals:
            continue
        seen_arrivals.add(key)

        # Ищем ближайшее незанятое ТС с совпадающим маршрутом, направлением,
        # которое ещё НЕ доехало до начальной остановки пользователя
        uid_v = ""
        near_stp = ""
        plate = ""
        for candidate in vehicle_list.get(mr_num, []):
            if candidate["u_id"] in assigned_uids:
                continue
            t = candidate["term"]
            if not (t == arr_dir or arr_dir.lower() in t.lower() or t.lower() in arr_dir.lower()):
                continue
            c_mr_id = candidate["mr_id"]
            if c_mr_id and not _vehicle_is_before_stop(c_mr_id, t, candidate["vlat"], candidate["vlng"], from_stop):
                continue  # ТС уже проехало from_stop — пропускаем
            uid_v    = candidate["u_id"]
            near_stp = candidate["near_stp"]
            plate    = candidate.get("plate", "")
            assigned_uids.add(uid_v)
            break

        route_count[mr_num] += 1
        all_entries.append({
            "route_num": mr_num,
            "arr_time":  arr_time,
            "near_stp":  near_stp,
            "uid_v":     uid_v,
            "plate":     plate,
        })

    if not all_entries:
        # Откат: нет прогноза (нерабочее время / st_id не найден) — ищем по активным ТС
        all_entries_fallback: list[dict] = []
        for route_num, mr_id, terminal in routes:
            route_stops = stops_cache.get(mr_id, [])
            for v in vehicles:
                if str(v.get("mr_num", "")).strip().upper() != route_num:
                    continue
                if not v.get("u_lat") or not v.get("u_long"):
                    continue
                vlat = float(v["u_lat"])
                vlng = float(v["u_long"])
                if not _vehicle_is_before_stop(mr_id, terminal, vlat, vlng, from_stop):
                    continue
                dist_m   = haversine_m(from_lat, from_lng, vlat, vlng) if (from_lat and from_lng) else float("inf")
                near_stp = nearest_stop_name(vlat, vlng, route_stops) or "нет данных"
                all_entries_fallback.append({
                    "route_num": route_num,
                    "arr_time":  None,
                    "near_stp":  near_stp,
                    "uid_v":     str(v.get("u_id", "")),
                    "dist_m":    dist_m,
                    "plate":     str(v.get("u_statenum", "") or "").strip(),
                })
        all_entries_fallback.sort(key=lambda x: x["dist_m"])
        all_entries = all_entries_fallback[:5]

    if not all_entries:
        await edit_fn(
            f"Сейчас нет транспорта, следующего из «{from_display}» до «{to_display}».",
            reply_markup=InlineKeyboardMarkup([[retry_btn], [new_dest_btn], [home_btn]]),
        )
        return

    vehicle_btns: list[list[InlineKeyboardButton]] = []
    for e in all_entries[:5]:
        near  = e.get("near_stp") or ""
        plate = e.get("plate") or ""
        plate_part = f"  {plate}" if plate else ""
        if e.get("arr_time"):
            loc_part  = f"  📍 {near}" if near else ""
            time_part = f"  ⏱ {e['arr_time']}"
            label = f"🚌 {e['route_num']}{plate_part}{loc_part}{time_part}"
        else:
            dist = e.get("dist_m", float("inf"))
            dist_str = f"{int(dist)} м" if dist < 1000 else f"{dist / 1000:.1f} км"
            loc_part = f"  📍 {near}" if near else ""
            label = f"🚌 {e['route_num']}{plate_part}{loc_part}  ({dist_str})"
        cb = f"findbus_where:{e['uid_v']}:{e['route_num']}" if e["uid_v"] else "findbus:refresh"
        vehicle_btns.append([InlineKeyboardButton(label, callback_data=cb)])

    vehicle_btns.append([retry_btn, new_dest_btn])
    vehicle_btns.append([new_from_btn, home_btn])

    await edit_fn(
        f"🚏 <b>{from_display}</b> → <b>{to_display}</b>\n\nПрибытие на вашу остановку:",
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup(vehicle_btns),
    )


async def on_findbus_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    parts    = query.data.split(":", 2)
    action   = parts[1]
    home_btn = InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back")

    if action == "start":
        await query.edit_message_text(
            "🚌 <b>Найти транспорт</b>\n\nКак определим, где вы?",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📍 По геолокации",   callback_data="findbus:geo"),
                    InlineKeyboardButton("✏️ Ввести название", callback_data="findbus:manual"),
                ],
                [home_btn],
            ]),
        )

    elif action == "geo":
        rk = RKMarkup(
            [[KeyboardButton("📍 Отправить геолокацию", request_location=True)]],
            one_time_keyboard=True,
            resize_keyboard=True,
        )
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📍 Нажми кнопку ниже, чтобы отправить своё местоположение:",
            reply_markup=rk,
        )

    elif action == "manual":
        context.user_data["findbus_waiting_from"] = True
        await query.edit_message_text(
            "✏️ Введи название своей остановки (или часть названия):\n\n"
            "<i>Например: Гагарина, вокзал, Лобкова</i>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[home_btn]]),
        )

    elif action == "geo_confirm":
        stop = context.user_data.get("findbus_from_stop", "")
        context.user_data["findbus_waiting_dest"] = True
        await query.edit_message_text(
            f"📍 Остановка: <b>{stop}</b>\n\n"
            "Куда едешь? Введи название конечной остановки:",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[home_btn]]),
        )

    elif action == "geo_retry":
        rk = RKMarkup(
            [[KeyboardButton("📍 Отправить геолокацию снова", request_location=True)]],
            one_time_keyboard=True,
            resize_keyboard=True,
        )
        await query.edit_message_text(
            "Попробуй отправить геолокацию ещё раз или введи остановку вручную.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Ввести название", callback_data="findbus:manual")],
                [home_btn],
            ]),
        )
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📍 Нажми кнопку ниже:",
            reply_markup=rk,
        )

    elif action == "from_pick":
        idx  = int(parts[2]) if len(parts) > 2 else 0
        opts = context.user_data.get("findbus_from_opts", [])
        if idx < len(opts):
            context.user_data["findbus_from_stop"] = opts[idx]
        stop = context.user_data.get("findbus_from_stop", "")
        context.user_data["findbus_waiting_dest"] = True
        await query.edit_message_text(
            f"📍 Остановка: <b>{stop}</b>\n\n"
            "Куда едешь? Введи название конечной остановки:",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[home_btn]]),
        )

    elif action == "askdest":
        stop = context.user_data.get("findbus_from_stop", "")
        context.user_data["findbus_waiting_dest"] = True
        await query.edit_message_text(
            f"📍 Остановка: <b>{stop}</b>\n\n"
            "Куда едешь? Введи название конечной остановки:",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[home_btn]]),
        )

    elif action == "refresh":
        from_stop = context.user_data.get("findbus_from_stop", "")
        to_stop   = context.user_data.get("findbus_to_stop",   "")
        await query.edit_message_text("⏳ Обновляю данные...")
        await _show_findbus_results(query.edit_message_text, context, from_stop, to_stop)


async def _handle_findbus_from_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает ввод названия своей остановки (ручной режим)."""
    stop_query = (update.message.text or "").strip()
    home_btn   = InlineKeyboardButton("🏠 Главное меню", callback_data="menu:back")

    msg = await update.message.reply_text("⏳ Ищу остановку...")

    matches = _search_stops_in_cache(stop_query)
    if not matches:
        await msg.edit_text("⏳ Загружаю данные об остановках...")
        vehicles = await asyncio.to_thread(fetch_vehicles)
        await _fetch_all_active_stops_async(vehicles)
        matches = _search_stops_in_cache(stop_query)

    if not matches:
        await msg.edit_text("⏳ Ищу через базу остановок...")
        api_stops = await asyncio.to_thread(fetch_stops_by_name_api, stop_query)
        api_stops = [s for s in api_stops if _stop_name_matches(stop_query, s["name"])]
        if api_stops:
            matches = [(s["name"], None) for s in api_stops]
        else:
            await msg.edit_text(
                f"Не нашли остановку «{stop_query}». Попробуй другое название.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Попробовать снова", callback_data="findbus:manual")],
                    [home_btn],
                ]),
            )
            return

    unique_names: list[str] = []
    seen_names: set[str] = set()
    for name, _ in matches:
        if name not in seen_names:
            seen_names.add(name)
            unique_names.append(name)

    if len(unique_names) == 1:
        context.user_data["findbus_from_stop"]    = unique_names[0]
        context.user_data["findbus_waiting_dest"] = True
        other_btn = InlineKeyboardButton("✏️ Другая остановка", callback_data="findbus:manual")
        await msg.edit_text(
            f"📍 Остановка: <b>{unique_names[0]}</b>\n\n"
            "Куда едешь? Введи название конечной остановки:",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[other_btn], [home_btn]]),
        )
    else:
        context.user_data["findbus_from_opts"] = unique_names[:4]
        buttons = [
            [InlineKeyboardButton(n, callback_data=f"findbus:from_pick:{i}")]
            for i, n in enumerate(unique_names[:4])
        ]
        buttons.append([home_btn])
        await msg.edit_text(
            "Нашли несколько похожих остановок. Выбери свою:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def _handle_findbus_dest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает ввод конечной остановки и показывает маршруты."""
    dest_text = (update.message.text or "").strip()
    from_stop = context.user_data.get("findbus_from_stop", "")

    context.user_data["findbus_to_stop"] = dest_text
    msg = await update.message.reply_text("⏳ Ищу маршруты...")
    await _show_findbus_results(msg.edit_text, context, from_stop, dest_text)


# ── Health-check сервер (для Render / UptimeRobot) ─────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()
    def log_message(self, *args):
        pass  # не засорять лог


def _start_health_server() -> None:
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("Health-check сервер запущен на порту %d", port)


# ── Запуск ─────────────────────────────────────────────────────────

def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError(
            "Токен не задан. Установи переменную окружения TELEGRAM_BOT_TOKEN.\n"
            "Пример: set TELEGRAM_BOT_TOKEN=1234567890:AAFxxx..."
        )

    async def post_init(application):
        init_db()
        loaded = db_load_subscriptions()
        for uid, routes in loaded.items():
            subscriptions[uid] = routes
        log.info("Загружено подписок из БД: %d пользователей", len(loaded))

        await application.bot.delete_webhook(drop_pending_updates=True)
        await application.bot.set_my_commands([
            BotCommand("track",   "Отслеживать маршрут — /track 212"),
            BotCommand("where",   "Где сейчас ТС маршрута — /where 212"),
            BotCommand("status",  "Мои подписки"),
            BotCommand("stop",    "Снять маршрут — /stop 212 или все"),
            BotCommand("cards",   "Мои карты ОМКА"),
            BotCommand("addcard", "Добавить карту ОМКА"),
            BotCommand("card",    "Разовая проверка баланса — /card 123456789"),
            BotCommand("help",    "Справка по командам"),
            BotCommand("start",   "Начало работы"),
        ])

    _start_health_server()
    app = Application.builder().token(token).post_init(post_init).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_help))
    app.add_handler(CommandHandler("track",  cmd_track))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stop",   cmd_stop))
    app.add_handler(CommandHandler("where",  cmd_where))
    app.add_handler(CommandHandler("cards",  cmd_cards))
    app.add_handler(CommandHandler("card",   cmd_card))
    app.add_handler(CommandHandler("debug",  cmd_debug))
    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("addcard", cmd_addcard),
            CallbackQueryHandler(cmd_addcard, pattern=r"^card:addnew$"),
        ],
        states={
            ADDCARD_NUMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, addcard_got_number)],
            ADDCARD_NAME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, addcard_got_name)],
            ADDCARD_COLOR:  [CallbackQueryHandler(addcard_got_color, pattern=r"^cardcolor:")],
        },
        fallbacks=[CommandHandler("cancel", addcard_cancel)],
    ))
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(CallbackQueryHandler(on_menu_action,    pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(on_card_action,    pattern=r"^card:"))
    app.add_handler(CallbackQueryHandler(on_route_action,  pattern=r"^route:"))
    app.add_handler(CallbackQueryHandler(on_filter_action, pattern=r"^filter:"))
    app.add_handler(CallbackQueryHandler(on_where_vehicle,        pattern=r"^where:"))
    app.add_handler(CallbackQueryHandler(on_findbus_where_vehicle, pattern=r"^findbus_where:"))
    app.add_handler(CallbackQueryHandler(on_findbus_action, pattern=r"^findbus:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.job_queue.run_repeating(poll_job, interval=POLL_INTERVAL, first=15)

    log.info("Бот запущен. Интервал опроса: %d сек.", POLL_INTERVAL)
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
