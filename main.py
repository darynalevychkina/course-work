import asyncio
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import unquote, urlparse, parse_qs
import re as _re

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from loguru import logger
import aiohttp
import holidays

from utils_shared import now_local, main_menu, is_admin, normalize_date
from google_calendar import (
    get_calendar_service,
    can_access_calendar as gcal_can_access,
    list_visible_calendars as gcal_list_visible,
    get_service_account_email,
)

try:
    from google_calendar import create_event_for_order as gcal_create_event_for_order

    HAS_CREATE_FOR_ORDER = True
except Exception:
    gcal_create_event_for_order = None
    HAS_CREATE_FOR_ORDER = False

try:
    from google_calendar import create_event as gcal_create_event_basic
except Exception:
    gcal_create_event_basic = None

try:
    from google_calendar import ensure_order_id as gcal_ensure_order_id

    HAS_ENSURE_ORDER = True
except Exception:
    HAS_ENSURE_ORDER = False
    gcal_ensure_order_id = None

from admin import r_admin, init_admin_context
from payments import r_pay, init_pay_context, set_receipts_dir
from receipts_store import ensure_receipts_dir
from plate_api import fetch_plate_info, plate_format_ok, normalize_plate

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
AUTO_DEV_API_KEY = os.getenv("AUTO_DEV_API_KEY")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {int(x) for x in re.findall(r"\d+", ADMIN_IDS_RAW)} if ADMIN_IDS_RAW else set()

GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
GOOGLE_CALENDAR_ID_RAW = os.getenv("GOOGLE_CALENDAR_ID", "")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")

RECEIPTS_DIR = os.getenv("RECEIPTS_DIR", "./receipts")
ensure_receipts_dir(RECEIPTS_DIR)

BAZAGAI_API_KEY = os.getenv("BAZAGAI_API_KEY", "")
BAZAGAI_MOCK = os.getenv("BAZAGAI_MOCK", "0") == "1" or not BAZAGAI_API_KEY
BAZAGAI_TIMEOUT = int(os.getenv("BAZAGAI_TIMEOUT", "10"))
logger.info(f"BazaGAI: mock={BAZAGAI_MOCK} timeout={BAZAGAI_TIMEOUT}s")

if not BOT_TOKEN:
    raise RuntimeError("Немає BOT_TOKEN у .env")
if not AUTO_DEV_API_KEY:
    raise RuntimeError("Немає AUTO_DEV_API_KEY у .env (Auto.dev обов'язковий)")

logger.info(f"TIMEZONE in use: {TIMEZONE}")
logger.info(f"Receipts dir: {os.path.abspath(RECEIPTS_DIR)}")


def normalize_calendar_id(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()

    if raw.startswith(("http://", "https://")):
        u = urlparse(raw)
        qs = parse_qs(u.query or "")
        if "src" in qs and qs["src"]:
            raw = qs["src"][0]
        else:
            parts = u.path.split("/")
            if "ical" in parts:
                i = parts.index("ical")
                if i + 1 < len(parts):
                    raw = parts[i + 1]

    for _ in range(2):
        dec = unquote(raw)
        if dec == raw:
            break
        raw = dec
    raw = raw.strip()

    m = _re.search(r"([A-Za-z0-9._+-]+@group\.calendar\.google\.com)", raw)
    if m:
        return m.group(1)
    m = _re.search(r"([A-Za-z0-9._%+-]+@gmail\.com)", raw)
    if m:
        return m.group(1)
    return raw


GOOGLE_CALENDAR_ID = normalize_calendar_id(GOOGLE_CALENDAR_ID_RAW)
logger.info(f"Calendar ID in use: {GOOGLE_CALENDAR_ID!r}")

if not (GOOGLE_SERVICE_ACCOUNT_FILE and GOOGLE_CALENDAR_ID):
    logger.warning(
        "Google Calendar не налаштовано (GOOGLE_SERVICE_ACCOUNT_FILE або GOOGLE_CALENDAR_ID відсутні)."
    )


def _chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


USERS: dict[int, dict] = {}
BOOKED: dict[str, set[str]] = {}
APPOINTMENTS: dict[str, list[dict]] = {}

HOURS_RANGE = list(range(9, 20))
REASONS = {
    "oil": "заміна мастила",
    "diag": "діагностика",
    "tires": "заміни шин",
    "other": "інша причина",
}
UA_HOLIDAYS_CACHE: dict[int, holidays.HolidayBase] = {}

gcal_service = None
gcal_enabled = False


class RegStates(StatesGroup):
    full_name = State()
    phone = State()
    vin = State()


class RegByPlateStates(StatesGroup):
    plate = State()
    confirm = State()


class RegByVinConfirm(StatesGroup):
    confirm = State()


class BookStates(StatesGroup):
    date = State()
    time = State()
    reason_other = State()


def cancel_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Скасувати")]], resize_keyboard=True
    )


def contact_or_cancel_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📲 Надіслати мій номер", request_contact=True)],
            [KeyboardButton(text="Скасувати")],
        ],
        resize_keyboard=True,
    )


def time_inline_kb(date_key: str):
    taken = BOOKED.get(date_key, set())
    today_str = now_local(TIMEZONE).strftime("%d.%m.%Y")
    cur_hour = now_local(TIMEZONE).hour

    times: list[str] = []
    for h in HOURS_RANGE:
        if date_key == today_str and h <= cur_hour:
            continue
        t = f"{h:02d}:00"
        if t not in taken:
            times.append(t)

    b = InlineKeyboardBuilder()
    for row in _chunked(times, 4):
        b.row(*[InlineKeyboardButton(text=t, callback_data=f"time:{t}") for t in row])
    b.row(InlineKeyboardButton(text="Назад", callback_data="time_back"))

    if not times:
        logger.info(f"На дату {date_key} усі години зайняті або час минув.")
    return b.as_markup()


def reasons_inline_kb():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(
            text=REASONS["oil"], callback_data="reason:oil"
        ),
        InlineKeyboardButton(
            text=REASONS["diag"], callback_data="reason:diag"
        ),
    )
    b.row(
        InlineKeyboardButton(
            text=REASONS["tires"], callback_data="reason:tires"
        ),
        InlineKeyboardButton(
            text=REASONS["other"], callback_data="reason:other"
        ),
    )
    b.row(InlineKeyboardButton(text="Назад", callback_data="reason_back"))
    return b.as_markup()


_TRANSLIT = {
    **{str(i): i for i in range(10)},
    "A": 1,
    "B": 2,
    "C": 3,
    "D": 4,
    "E": 5,
    "F": 6,
    "G": 7,
    "H": 8,
    "J": 1,
    "K": 2,
    "L": 3,
    "M": 4,
    "N": 5,
    "P": 7,
    "R": 9,
    "S": 2,
    "T": 3,
    "U": 4,
    "V": 5,
    "W": 6,
    "X": 7,
    "Y": 8,
    "Z": 9,
}
_WEIGHTS = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]


def vin_checksum_ok(vin: str) -> bool:
    vin = vin.upper()
    total = 0
    for i, ch in enumerate(vin):
        if ch not in _TRANSLIT:
            return False
        total += _TRANSLIT[ch] * _WEIGHTS[i]
    check = total % 11
    expected = "X" if check == 10 else str(check)
    return vin[8] == expected


AUTODEV_URL = "https://api.auto.dev/vin/{vin}"
VPIC_URL = (
    "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValues/{vin}"
    "?format=json&modelyear={year}"
)


def _extract_vehicle_from_autodev(payload: dict) -> dict:
    if not payload:
        return {}
    make = payload.get("make") or payload.get("manufacturer")
    model = payload.get("model")
    year = payload.get("year")
    data = payload.get("data") or payload.get("vehicle") or payload.get("specs") or {}
    make = make or data.get("make") or data.get("manufacturer")
    model = model or data.get("model")
    year = (
        year
        or data.get("year")
        or data.get("model_year")
        or data.get("year_of_manufacture")
    )
    results = payload.get("results") or payload.get("Result") or []
    if isinstance(results, list) and results:
        r0 = results[0]
        make = make or r0.get("make") or r0.get("manufacturer")
        model = model or r0.get("model")
        year = year or r0.get("year") or r0.get("model_year")
    out = {}
    if make:
        out["make"] = make
    if model:
        out["model"] = model
    if year:
        out["year"] = year
    return out


async def decode_vin_autodev(vin: str) -> dict | None:
    headers = {"x-api-key": AUTO_DEV_API_KEY}
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(AUTODEV_URL.format(vin=vin), headers=headers) as r:
            if r.status == 200:
                raw = await r.json()
                vehicle = _extract_vehicle_from_autodev(raw)
                return {"raw": raw, "vehicle": vehicle}
            logger.warning(f"Auto.dev HTTP {r.status}")
            return None


async def decode_vin_vpic(vin: str) -> tuple[bool, str]:
    timeout = aiohttp.ClientTimeout(total=10)
    year_candidates = [now_local(TIMEZONE).year, now_local(TIMEZONE).year - 1]
    try:
        async with aiohttp.ClientSession(timeout=timeout) as s:
            for y in year_candidates:
                async with s.get(VPIC_URL.format(vin=vin), year=y) as resp:  # type: ignore
                    data = await resp.json()
                    row = (data.get("Results") or [{}])[0]
                    code = str(row.get("ErrorCode", "")).strip()
                    text = row.get("ErrorText", "") or ""
                    if code.startswith(("0", "7", "8")):
                        return True, text or "vPIC OK"
        return False, "vPIC: VIN не підтверджено"
    except Exception as e:
        logger.warning(f"vPIC error: {e}")
        return True, "vPIC недоступний"


async def verify_vin(vin: str) -> tuple[bool, str, dict | None]:
    if not re.fullmatch(r"^[A-HJ-NPR-Z0-9]{17}$", vin, flags=re.IGNORECASE):
        return False, "Формат VIN має бути 17 символів (без I/O/Q).", None
    if not vin_checksum_ok(vin):
        return False, "Контрольна цифра VIN не сходиться (ISO-3779).", None
    extra = None
    try:
        extra = await decode_vin_autodev(vin)
        if extra:
            return True, "VIN підтверджено (Auto.dev).", extra
    except Exception as e:
        logger.warning(f"Auto.dev error: {e}")
    ok, msg = await decode_vin_vpic(vin)
    if ok:
        return True, f"VIN підтверджено ({msg}).", None
    return False, "Не вдалося підтвердити VIN. Перевір правильність або спробуй інший.", None


r = Router()


@r.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    is_reg = m.from_user.id in USERS
    await m.answer(
        "Привіт! 👋 Це бот запису на СТО.\n\n"
        "• Якщо ти вже зареєстрований — тисни «Зробити запис».\n"
        "• Якщо ні — тисни «Зареєструватися».",
        reply_markup=main_menu(is_reg, is_admin(m.from_user.id, ADMIN_IDS)),
    )


@r.message(F.text == "Зареєструватися")
async def start_reg(m: Message, state: FSMContext):
    if m.from_user.id in USERS:
        await m.answer(
            "Ти вже зареєстрований ✅",
            reply_markup=main_menu(
                True, is_admin(m.from_user.id, ADMIN_IDS)
            ),
        )
        return
    await state.set_state(RegStates.full_name)
    await m.answer(
        "Введи *Ім’я та прізвище* одним рядком:",
        reply_markup=cancel_menu(),
        parse_mode="Markdown",
    )


@r.message(RegStates.full_name, F.text)
async def reg_fullname(m: Message, state: FSMContext):
    full = " ".join(m.text.split())
    if len(full) < 3 or " " not in full:
        await m.answer(
            "Будь ласка, введи *Ім’я та прізвище* (через пробіл).",
            parse_mode="Markdown",
        )
        return
    await state.update_data(full_name=full)
    await state.set_state(RegStates.phone)
    await m.answer(
        "Введи номер телефону (10 цифр, без +38) або натисни кнопку нижче:",
        reply_markup=contact_or_cancel_menu(),
    )


@r.message(RegStates.phone)
async def reg_phone(m: Message, state: FSMContext):
    phone = None
    if m.contact:
        if m.contact.user_id != m.from_user.id:
            await m.answer(
                "❌ Можна надіслати тільки власний номер.",
                reply_markup=contact_or_cancel_menu(),
            )
            return
        digits = re.sub(r"\D", "", m.contact.phone_number or "")
        if len(digits) >= 10:
            phone = digits[-10:]
    else:
        text = (m.text or "").strip()
        if text == "Скасувати":
            await cancel_any(m, state)
            return
        if re.fullmatch(r"^\d{10}$", text):
            phone = text
    if not phone:
        await m.answer(
            "Телефон має містити **рівно 10 цифр**. "
            "Спробуй ще раз або натисни кнопку нижче.",
            reply_markup=contact_or_cancel_menu(),
        )
        return

    await state.update_data(phone=phone)

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(
            text="🔑 За VIN", callback_data="reg:via_vin"
        ),
        InlineKeyboardButton(
            text="🔤 За номером авто", callback_data="reg:via_plate"
        ),
    )
    await m.answer(
        "Оберіть спосіб реєстрації автомобіля:",
        reply_markup=kb.as_markup(),
    )


@r.callback_query(F.data == "reg:via_vin")
async def reg_choose_vin(cq: CallbackQuery, state: FSMContext):
    await state.set_state(RegStates.vin)
    await cq.message.edit_text(
        "Введи VIN (17 символів, латиниця/цифри, без I/O/Q):"
    )
    await cq.answer()


@r.callback_query(F.data == "reg:via_plate")
async def reg_choose_plate(cq: CallbackQuery, state: FSMContext):
    await state.set_state(RegByPlateStates.plate)
    await cq.message.edit_text(
        "Введи держномер авто (наприклад, **АА1234ВС**).",
        parse_mode="Markdown",
    )
    await cq.answer()


@r.message(RegStates.vin, F.text)
async def reg_vin(m: Message, state: FSMContext):
    vin = (m.text or "").strip().upper()
    if vin == "Скасувати":
        await cancel_any(m, state)
        return
    ok, info, extra = await verify_vin(vin)
    if not ok:
        await m.answer(f"❌ {info}")
        return

    vehicle = (extra or {}).get("vehicle") if extra else {}
    make = (vehicle or {}).get("make") or "—"
    model = (vehicle or {}).get("model") or "—"
    year = (vehicle or {}).get("year") or "—"

    await state.update_data(vin=vin, vehicle_guess=vehicle or {})

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(
            text="✅ Так, це моє авто", callback_data="vin:confirm_yes"
        ),
        InlineKeyboardButton(
            text="❌ Ні, ввести інший VIN", callback_data="vin:confirm_no"
        ),
    )
    await m.answer(
        "VIN підтверджено.\n"
        f"Знайшов авто: {make} {model}, {year}\n\n"
        "Підтверджуєш?",
        reply_markup=kb.as_markup(),
    )
    await state.set_state(RegByVinConfirm.confirm)


@r.callback_query(RegByVinConfirm.confirm, F.data == "vin:confirm_yes")
async def reg_vin_confirm_yes(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    USERS[cq.from_user.id] = {
        "full_name": data.get("full_name"),
        "phone": data.get("phone"),
        "vin": data.get("vin"),
        "plate": "",
        "vehicle": data.get("vehicle_guess") or {},
    }
    await state.clear()
    await cq.message.edit_text("Реєстрацію завершено ✅")
    await cq.message.answer(
        "Тепер натисни «Зробити запис».",
        reply_markup=main_menu(
            True, is_admin(cq.from_user.id, ADMIN_IDS)
        ),
    )
    await cq.answer()


@r.callback_query(RegByVinConfirm.confirm, F.data == "vin:confirm_no")
async def reg_vin_confirm_no(cq: CallbackQuery, state: FSMContext):
    await state.set_state(RegStates.vin)
    await cq.message.edit_text("Введи інший VIN (17 символів):")
    await cq.answer()


@r.message(RegByPlateStates.plate, F.text)
async def reg_plate_enter(m: Message, state: FSMContext):
    plate = normalize_plate(m.text or "")
    if not plate_format_ok(plate):
        await m.answer(
            "Невірний формат. Приклад: **АА1234ВС** (без пробілів/дефісів).",
            parse_mode="Markdown",
        )
        return

    info = None
    try:
        info = await fetch_plate_info(
            plate, BAZAGAI_API_KEY, mock=BAZAGAI_MOCK, timeout_sec=BAZAGAI_TIMEOUT
        )
    except Exception as e:
        logger.error(f"Baza-GAI fetch error: {e}")

    if not info:
        await m.answer(
            "Не вдалося підтягнути авто за номером. "
            "Спробуй інший номер або реєстрацію за VIN."
        )
        return

    vendor = info.get("vendor") or "—"
    model = info.get("model") or "—"
    year = info.get("model_year") or "—"
    stolen = info.get("is_stolen")

    await state.update_data(
        plate=info["plate"],
        vehicle_guess={"make": vendor, "model": model, "year": year},
    )

    warn = "⚠️ В базі позначено як можливе викрадення!\n" if stolen else ""
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(
            text="✅ Так, це моє авто", callback_data="plate:confirm_yes"
        ),
        InlineKeyboardButton(
            text="❌ Ні, не моє", callback_data="plate:confirm_no"
        ),
    )
    await m.answer(
        f"{warn}Знайшов авто:\n"
        f"• Марка/модель: {vendor} {model}\n"
        f"• Рік: {year}\n\n"
        f"Підтверджуєш?",
        reply_markup=kb.as_markup(),
    )
    await state.set_state(RegByPlateStates.confirm)


@r.callback_query(RegByPlateStates.confirm, F.data == "plate:confirm_yes")
async def reg_plate_confirm_yes(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    USERS[cq.from_user.id] = {
        "full_name": data.get("full_name"),
        "phone": data.get("phone"),
        "vin": "",
        "plate": data.get("plate"),
        "vehicle": data.get("vehicle_guess") or {},
    }
    await state.clear()
    await cq.message.edit_text("Реєстрацію завершено ✅")
    await cq.message.answer(
        "Тепер натисни «Зробити запис».",
        reply_markup=main_menu(
            True, is_admin(cq.from_user.id, ADMIN_IDS)
        ),
    )
    await cq.answer()


@r.callback_query(RegByPlateStates.confirm, F.data == "plate:confirm_no")
async def reg_plate_confirm_no(cq: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(
            text="🔁 Ввести інший номер", callback_data="reg:via_plate"
        )
    )
    kb.row(
        InlineKeyboardButton(
            text="🔑 Реєстрація за VIN", callback_data="reg:via_vin"
        )
    )
    await cq.message.edit_text(
        "Окей. Обери інший спосіб:", reply_markup=kb.as_markup()
    )
    await cq.answer()


@r.message(F.text == "Скасувати")
async def cancel_any(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(
        "Дію скасовано. Повертаю в головне меню.",
        reply_markup=main_menu(
            m.from_user.id in USERS, is_admin(m.from_user.id, ADMIN_IDS)
        ),
    )


@r.message(F.text == "Зробити запис")
async def start_booking(m: Message, state: FSMContext):
    if m.from_user.id not in USERS:
        await m.answer(
            "Спочатку зареєструйся, будь ласка.",
            reply_markup=main_menu(False),
        )
        return
    await state.set_state(BookStates.date)
    await m.answer(
        "Введи дату *dd.mm* або *dd.mm.yy*:",
        reply_markup=cancel_menu(),
        parse_mode="Markdown",
    )


@r.message(BookStates.date, F.text)
async def get_date(m: Message, state: FSMContext):
    date_key = normalize_date(m.text, TIMEZONE)
    if not date_key:
        await m.answer(
            "Дата некоректна. Приклад: `15.02` або `15.02.25`",
            parse_mode="Markdown",
        )
        return

    dt = datetime.strptime(date_key, "%d.%m.%Y").replace(
        tzinfo=ZoneInfo(TIMEZONE)
    )
    now = now_local(TIMEZONE)

    if dt.date() < now.date():
        await m.answer(
            "❌ Не можна записуватись на минулу дату. Обери іншу.",
            reply_markup=cancel_menu(),
        )
        return

    if _is_closed_day(dt):
        await m.answer(
            f"❌ На {date_key} запис недоступний. Обери іншу дату.",
            reply_markup=cancel_menu(),
        )
        return

    await state.update_data(date_key=date_key)
    await state.set_state(BookStates.time)
    await m.answer(
        f"Оберіть час (09–19) на {date_key}:",
        reply_markup=time_inline_kb(date_key),
    )


@r.callback_query(BookStates.time, F.data.startswith("time:"))
async def pick_time(cq: CallbackQuery, state: FSMContext):
    time_str = cq.data.split(":", 1)[1]
    data = await state.get_data()
    date_key: str = data["date_key"]

    try:
        start_dt = datetime.strptime(
            f"{date_key} {time_str}", "%d.%m.%Y %H:%M"
        ).replace(tzinfo=ZoneInfo(TIMEZONE))
    except ValueError:
        await cq.answer("Некоректний час.", show_alert=True)
        return

    if start_dt <= now_local(TIMEZONE):
        await cq.answer("Цей час уже минув. Обери інший.", show_alert=True)
        await cq.message.edit_text(
            f"Оберіть час (09–19) на {date_key}:",
            reply_markup=time_inline_kb(date_key),
        )
        return

    taken = BOOKED.get(date_key, set())
    if time_str in taken:
        await cq.answer("Ця година вже зайнята 😕", show_alert=True)
        await cq.message.edit_text(
            f"Оберіть інший час на {date_key}:",
            reply_markup=time_inline_kb(date_key),
        )
        return

    await state.update_data(time_str=time_str)
    await cq.message.edit_text(
        f"Обери причину візиту на {date_key} о {time_str}:",
        reply_markup=reasons_inline_kb(),
    )
    await cq.answer()


@r.callback_query(BookStates.time, F.data == "time_back")
async def time_back(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BookStates.date)
    await cq.message.edit_text(
        "Введи нову дату *dd.mm* або *dd.mm.yy*:",
        parse_mode="Markdown",
    )
    await cq.answer()


@r.callback_query(F.data.startswith("reason"))
async def pick_reason(cq: CallbackQuery, state: FSMContext):
    if cq.data == "reason_back":
        data = await state.get_data()
        date_key: str = data.get("date_key")
        await state.set_state(BookStates.time)
        await cq.message.edit_text(
            f"Оберіть час (09–19) на {date_key}:",
            reply_markup=time_inline_kb(date_key),
        )
        await cq.answer()
        return

    tag = cq.data.split(":", 1)[1]
    data = await state.get_data()
    date_key: str = data.get("date_key")
    time_str: str = data.get("time_str")

    if tag == "other":
        await state.set_state(BookStates.reason_other)
        await cq.message.edit_text("Введи коротко іншу причину:")
        await cq.answer()
        return

    reason = REASONS.get(tag)
    if not reason:
        await cq.answer("Невідома причина", show_alert=True)
        return

    ok = await finalize_booking(
        user_id=cq.from_user.id,
        date_key=date_key,
        time_str=time_str,
        reason=reason,
    )
    if not ok:
        await cq.answer(
            "Цей слот недоступний (можливо, час уже минув або його зайняли).",
            show_alert=True,
        )
        await cq.message.edit_text(
            f"Оберіть інший час на {date_key}:",
            reply_markup=time_inline_kb(date_key),
        )
        return

    await state.clear()
    await cq.message.edit_text(
        f"✅ Запис створено на {date_key} о {time_str}.\n"
        f"Причина: {reason}\n\n"
        "Дякуємо! Чекаємо 🤝"
    )
    await cq.message.answer(
        "Повертаю в головне меню.",
        reply_markup=main_menu(
            True, is_admin(cq.from_user.id, ADMIN_IDS)
        ),
    )
    await cq.answer()


@r.message(BookStates.reason_other, F.text)
async def reason_other_text(m: Message, state: FSMContext):
    reason = " ".join(m.text.split())
    if len(reason) < 3:
        await m.answer(
            "Дуже коротко. Опиши трохи детальніше (від 3 символів)."
        )
        return
    data = await state.get_data()
    date_key: str = data.get("date_key")
    time_str: str = data.get("time_str")
    ok = await finalize_booking(
        user_id=m.from_user.id,
        date_key=date_key,
        time_str=time_str,
        reason=reason,
    )
    if not ok:
        await m.answer(
            "Цей слот недоступний (можливо, час уже минув або його зайняли). "
            "Обери інший:",
            reply_markup=time_inline_kb(date_key),
        )
        await state.set_state(BookStates.time)
        return
    await state.clear()
    await m.answer(
        f"✅ Запис створено на {date_key} о {time_str}.\n"
        f"Причина: {reason}\n\n"
        "Дякуємо! Чекаємо 🤝"
    )
    await m.answer(
        "Повертаю в головне меню.",
        reply_markup=main_menu(
            True, is_admin(m.from_user.id, ADMIN_IDS)
        ),
    )


def _get_ua_holidays(year: int) -> holidays.HolidayBase:
    if year not in UA_HOLIDAYS_CACHE:
        UA_HOLIDAYS_CACHE[year] = holidays.country_holidays("UA", years=year)
    return UA_HOLIDAYS_CACHE[year]


def _is_closed_day(dt: datetime) -> bool:
    if dt.weekday() == 6:
        return True
    ua = _get_ua_holidays(dt.year)
    if dt.date() in ua:
        return True
    return False


def _gen_order_id(date_key: str, time_str: str, user_id: int) -> str:
    dt = datetime.strptime(
        f"{date_key} {time_str}", "%d.%m.%Y %H:%M"
    )
    return f"{dt.strftime('%Y%m%d-%H%M')}-{user_id}"


async def finalize_booking(
    user_id: int, date_key: str, time_str: str, reason: str
) -> bool:
    if not date_key or not time_str:
        logger.debug("finalize_booking: empty date/time")
        return False

    try:
        start_dt = datetime.strptime(
            f"{date_key} {time_str}", "%d.%m.%Y %H:%M"
        ).replace(tzinfo=ZoneInfo(TIMEZONE))
    except ValueError:
        logger.debug("finalize_booking: bad datetime parse")
        return False

    if start_dt <= now_local(TIMEZONE):
        logger.info(
            f"finalize_booking: past slot rejected → {date_key} {time_str}"
        )
        return False

    if _is_closed_day(start_dt):
        logger.info(f"finalize_booking: closed day rejected → {date_key}")
        return False

    taken = BOOKED.setdefault(date_key, set())
    if time_str in taken:
        logger.info(
            f"finalize_booking: already taken → {date_key} {time_str}"
        )
        return False

    taken.add(time_str)
    order_id = _gen_order_id(date_key, time_str, user_id)
    rec = {
        "time": time_str,
        "user_id": user_id,
        "reason": reason,
        "order_id": order_id,
        "amount_uah": 0,
    }
    APPOINTMENTS.setdefault(date_key, []).append(rec)

    if gcal_enabled and gcal_service and GOOGLE_CALENDAR_ID:
        try:
            user = USERS.get(user_id, {})
            end_dt = start_dt + timedelta(hours=1)
            fio = user.get("full_name", "")
            phone = user.get("phone", "")
            vin = user.get("vin", "")
            veh = user.get("vehicle") or {}
            car = (
                ", ".join(
                    [
                        str(veh.get(k))
                        for k in ("make", "model", "year")
                        if veh.get(k)
                    ]
                )
                if veh
                else ""
            )
            if HAS_CREATE_FOR_ORDER and gcal_create_event_for_order:
                event_id = await asyncio.to_thread(
                    gcal_create_event_for_order,
                    gcal_service,
                    GOOGLE_CALENDAR_ID,
                    order_id=order_id,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    customer_name=fio,
                    phone=phone,
                    vin=vin,
                    car_line=car,
                    reason=reason,
                    location=None,
                )
            elif gcal_create_event_basic:
                summary = f"СТО: {fio} — {reason}"
                description = (
                    f"Замовлення: #{order_id}\n"
                    f"Клієнт: {fio}\n"
                    f"Телефон: +380{phone}\n"
                    f"VIN: {vin or '—'}\n"
                    f"Авто: {car or '—'}\n"
                    f"Причина: {reason}"
                )
                event_id = await asyncio.to_thread(
                    gcal_create_event_basic,
                    gcal_service,
                    GOOGLE_CALENDAR_ID,
                    start_dt,
                    end_dt,
                    summary,
                    description,
                )
                if (
                    HAS_ENSURE_ORDER
                    and gcal_ensure_order_id
                    and event_id
                ):
                    await asyncio.to_thread(
                        gcal_ensure_order_id,
                        gcal_service,
                        GOOGLE_CALENDAR_ID,
                        event_id,
                        order_id,
                    )
            else:
                event_id = ""

            if event_id:
                rec["gcal_event_id"] = event_id
                logger.info(
                    f"Google Calendar: подію створено ({event_id})"
                )
            else:
                logger.warning(
                    "Google Calendar: не вдалося створити подію (нема відповідної функції)."
                )
        except Exception as e:
            logger.error(
                f"Google Calendar: не вдалося створити подію: {e}"
            )

    logger.info(
        f"BOOKED: {date_key} {time_str} by {user_id} — {reason} (order_id={order_id})"
    )
    return True


async def main():
    global gcal_service, gcal_enabled

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(r)
    dp.include_router(r_admin)
    dp.include_router(r_pay)

    bot = Bot(BOT_TOKEN)

    if GOOGLE_SERVICE_ACCOUNT_FILE and GOOGLE_CALENDAR_ID:
        try:
            gcal_service = await asyncio.to_thread(
                get_calendar_service, GOOGLE_SERVICE_ACCOUNT_FILE
            )
            gcal_enabled = True
            logger.info("Google Calendar: клієнт ініціалізовано.")

            sa_email = await asyncio.to_thread(
                get_service_account_email, GOOGLE_SERVICE_ACCOUNT_FILE
            )
            logger.info(f"Service Account email: {sa_email}")

            visible = await asyncio.to_thread(
                gcal_list_visible, gcal_service
            )
            if visible:
                logger.info("Calendars visible to service account:")
                for c in visible:
                    logger.info(f"  • {c['summary']} ({c['id']})")
            else:
                logger.warning(
                    "Service account currently sees 0 calendars in calendarList."
                )

            has_access = await asyncio.to_thread(
                gcal_can_access, gcal_service, GOOGLE_CALENDAR_ID
            )
            if not has_access:
                logger.error(
                    "Service account НЕ має доступу до GOOGLE_CALENDAR_ID → вставка подій дасть 404."
                )
                logger.error(
                    "Поділи календар «СТО» з цим email сервісного акаунта (Make changes to events)."
                )
        except Exception as e:
            gcal_service = None
            gcal_enabled = False
            logger.error(
                f"Google Calendar: помилка ініціалізації — {e}"
            )

    init_admin_context(
        users=USERS,
        appointments=APPOINTMENTS,
        booked=BOOKED,
        timezone=TIMEZONE,
        admin_ids=ADMIN_IDS,
        gcal_ok=gcal_enabled,
        gcal_svc=gcal_service,
        gcal_id=GOOGLE_CALENDAR_ID,
    )
    init_pay_context(
        users=USERS,
        appointments=APPOINTMENTS,
        gcal_ok=gcal_enabled,
        gcal_svc=gcal_service,
        gcal_id=GOOGLE_CALENDAR_ID,
    )
    set_receipts_dir(RECEIPTS_DIR)

    logger.info("Bot started.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
