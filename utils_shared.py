# utils_shared.py
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Set

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def now_local(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))


def normalize_date(text: str, tz: str) -> Optional[str]:
    text = (text or "").strip().replace("/", ".")
    if not text:
        return None

    now_year = now_local(tz).year

    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})", text)
    if m:
        d, mth = map(int, m.groups())
        try:
            return datetime(now_year, mth, d).strftime("%d.%m.%Y")
        except ValueError:
            return None

    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{2}|\d{4})", text)
    if m:
        d, mth, y = m.groups()
        y = int(y)
        if y < 100:
            y += 2000
        try:
            return datetime(y, int(mth), int(d)).strftime("%d.%m.%Y")
        except ValueError:
            return None

    return None


def is_admin(user_id: int, admin_ids: Set[int]) -> bool:
    return user_id in admin_ids


def main_menu(is_registered: bool, is_admin_flag: bool = False) -> ReplyKeyboardMarkup:
    kb = (
        [[KeyboardButton(text="Зробити запис")]]
        if is_registered
        else [[KeyboardButton(text="Зареєструватися")]]
    )
    if is_admin_flag:
        kb.append([KeyboardButton(text="🛠 Адмін")])
    kb.append([KeyboardButton(text="Скасувати")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def route_url_default() -> Optional[str]:
    url = os.getenv("ROUTE_URL", "").strip()
    return url or None
