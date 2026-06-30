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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
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
    """Список остановок маршрута с именами: [{name, lat, lng}, ...]."""
    try:
        data = _rpc("getRoute", {"mr_id": mr_id})
        races = data.get("result", {}).get("races", [])
        stops: list[dict] = []
        seen: set[str] = set()
        for race in races:
            for s in race.get("stopList", []):
                lat  = s.get("st_lat")
                lng  = s.get("st_long")
                name = (s.get("st_title") or s.get("st_name") or "").strip()
                if lat and lng and name and name not in seen:
                    stops.append({"name": name, "lat": float(lat), "lng": float(lng)})
                    seen.add(name)
        return stops
    except Exception as e:
        log.warning("fetch_route_stops mr_id=%s: %s", mr_id, e)
        return []


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
        f"Отслеживаемых маршрутов ({total}): {routes_list}\n\n"
        f"/stop {route} — снять этот\n"
        f"/stop — снять все"
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

POLL_INTERVAL = 30  # секунд между проверками

CARD_COLORS = ["❤️", "🧡", "💛", "💚", "💙", "💜", "🩷", "🤍", "🖤", "🤎"]
ADDCARD_NUMBER, ADDCARD_NAME, ADDCARD_COLOR = range(10, 13)


H = "HTML"  # parse_mode shortcut


def _menu_text() -> str:
    return (
        "Что умею:\n\n"
        "🔔 <b>Отслеживание</b> — получай уведомление, когда новое ТС\n"
        "выходит на маршрут (данные ГЛОНАСС, bus-55.ru)\n\n"
        "📍 <b>Где сейчас</b> — смотри геолокацию конкретного автобуса\n\n"
        "💳 <b>Мои карты ОМКА</b> — сохрани свои карты и проверяй баланс\n\n"
        "<b>Команды:</b>\n"
        "/track <i>номер</i> — начать отслеживать маршрут\n"
        "/where <i>номер</i> — где сейчас ТС маршрута\n"
        "/status — мои подписки\n"
        "/stop <i>номер</i> — снять маршрут\n"
        "/cards — мои карты ОМКА\n"
        "/addcard — добавить карту\n"
        "/card <i>номер</i> — разовая проверка баланса\n"
        "/help — это меню\n\n"
        "💡 Можно просто написать номер маршрута — например: <b>212</b>"
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
    name = update.effective_user.first_name or "Привет"
    await update.message.reply_html(
        f"👋 <b>{name}!</b>\n\n"
        f"Я бот мониторинга автобусов Омска.\n\n"
        + _menu_text()
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_html(_menu_text())


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

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "✅ Уже отслеживается" if already else "🔔 Отслеживать",
            callback_data=f"route:track:{route}",
        ),
        InlineKeyboardButton("📍 ТС на линии", callback_data=f"route:where:{route}"),
    ]])
    await update.message.reply_html(
        f"Маршрут <b>{route}</b>{desc_line}\n\nЧто сделать?",
        reply_markup=keyboard,
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


async def _show_direction_filter(edit_fn, route: str, context) -> None:
    """Показывает фильтр по направлениям или сразу список ТС (если направление одно)."""
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

    if not route_vehicles:
        await edit_fn(header + "\n\nСейчас нет ТС на линии.", parse_mode=H)
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
        # Одно направление или нет данных — сразу список
        await edit_fn(
            header + f"\n\nНа линии <b>{len(route_vehicles)} ТС</b>. Выбери автобус:",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup(_vehicle_buttons(route, route_vehicles)),
        )
        return

    buttons = [[InlineKeyboardButton("🚌 Все ТС", callback_data=f"filter:{route}:all")]]
    for i, t in enumerate(terminals):
        buttons.append([InlineKeyboardButton(f"→ {t}", callback_data=f"filter:{route}:{i}")])

    await edit_fn(
        header + f"\n\nНа линии <b>{len(route_vehicles)} ТС</b>. Выбери направление:",
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def on_route_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    _, action, route = query.data.split(":", 2)
    uid = query.from_user.id

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
            f"Отслеживаемых маршрутов ({total}): {routes_list}\n\n"
            f"/stop {route} — снять этот\n/stop — снять все",
            parse_mode=H,
        )

    elif action == "where":
        await query.edit_message_text("⏳ Загружаю ТС...", parse_mode=H)
        await _show_direction_filter(query.edit_message_text, route, context)


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
    await update.message.reply_text("Добавление карты отменено.")
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
        await query.edit_message_text("ТС уже не на линии.")
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
    await context.bot.send_location(
        chat_id=query.message.chat_id,
        latitude=lat,
        longitude=lng,
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
    app.add_handler(CallbackQueryHandler(on_card_action,    pattern=r"^card:"))
    app.add_handler(CallbackQueryHandler(on_route_action,  pattern=r"^route:"))
    app.add_handler(CallbackQueryHandler(on_filter_action, pattern=r"^filter:"))
    app.add_handler(CallbackQueryHandler(on_where_vehicle, pattern=r"^where:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.job_queue.run_repeating(poll_job, interval=POLL_INTERVAL, first=15)

    log.info("Бот запущен. Интервал опроса: %d сек.", POLL_INTERVAL)
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
