import os
from aiogram import Router, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from loguru import logger

from payments import PAY_CALLBACK_PREFIX
from utils_shared import now_local, main_menu, is_admin, normalize_date, route_url_default

r_admin = Router(name="admin")


def admin_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Записи на сьогодні")],
            [KeyboardButton(text="📅 Записи на дату")],
            [KeyboardButton(text="⬅️ В головне меню")],
        ],
        resize_keyboard=True,
    )


def cancel_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Скасувати")]],
        resize_keyboard=True,
    )


USERS = None
APPOINTMENTS = None
BOOKED = None
TIMEZONE = "Europe/Kyiv"
ADMIN_IDS = set()
gcal_enabled = False
gcal_service = None
GOOGLE_CALENDAR_ID = ""


def init_admin_context(
    *,
    users,
    appointments,
    booked,
    timezone,
    admin_ids,
    gcal_ok,
    gcal_svc,
    gcal_id,
):
    global USERS, APPOINTMENTS, BOOKED, TIMEZONE, ADMIN_IDS
    global gcal_enabled, gcal_service, GOOGLE_CALENDAR_ID
    USERS, APPOINTMENTS, BOOKED = users, appointments, booked
    TIMEZONE = timezone
    ADMIN_IDS = admin_ids
    gcal_enabled = gcal_ok
    gcal_service = gcal_svc
    GOOGLE_CALENDAR_ID = gcal_id


class ReadyStates(StatesGroup):
    wait_amount = State()


class AdminDateStates(StatesGroup):
    wait_date = State()


def _find_appt(date_key: str, time_str: str, uid: int) -> dict | None:
    items = APPOINTMENTS.get(date_key, [])
    time_str = (time_str or "").strip()
    for it in items:
        if it.get("time") == time_str and int(it.get("user_id")) == int(uid):
            return it
    return None


def render_schedule_plain(date_key: str) -> str:
    items = sorted(APPOINTMENTS.get(date_key, []), key=lambda x: x["time"])
    if not items:
        return f"📭 На {date_key} записів немає."
    lines = [f"📅 Записи на {date_key}:", ""]
    for it in items:
        uid = it["user_id"]
        u = USERS.get(uid, {})
        fio = u.get("full_name", "—")
        phone = u.get("phone", "—")
        vin = u.get("vin", "—")
        plate = u.get("plate", "—")
        veh = u.get("vehicle") or {}
        car = (
            ", ".join(
                [str(veh.get(k)) for k in ("make", "model", "year") if veh.get(k)]
            )
            if veh
            else (plate or "—")
        )
        gcal = it.get("gcal_event_id", "—")
        order_id = it.get("order_id", "—")
        amount_uah = it.get("amount_uah", "—")
        lines.append(
            f"• {it['time']} — {fio}\n"
            f"  📞 +380{phone} | VIN: {vin} | №: {plate}\n"
            f"  🚗 {car}\n"
            f"  🎯 {it['reason']}\n"
            f"  💵 {amount_uah} грн\n"
            f"  🧾 Order ID: {order_id}\n"
            f"  🗓 Google Event ID: {gcal}"
        )
        lines.append("─" * 20)
    return "\n".join(lines)


async def send_schedule_with_ready_buttons(msg_or_bot, chat_id: int, date_key: str):
    items = sorted(APPOINTMENTS.get(date_key, []), key=lambda x: x["time"])
    if not items:
        text = f"📭 На {date_key} записів немає."
        if hasattr(msg_or_bot, "send_message"):
            await msg_or_bot.send_message(chat_id=chat_id, text=text)
        else:
            await msg_or_bot.answer(text=text)
        return

    lines = [f"📅 Записи на {date_key}:", ""]
    kb = InlineKeyboardBuilder()

    for it in items:
        uid = it["user_id"]
        u = USERS.get(uid, {})
        fio = u.get("full_name", "—")
        phone = u.get("phone", "—")
        vin = u.get("vin", "—")
        plate = u.get("plate", "—")
        veh = u.get("vehicle") or {}
        car = (
            ", ".join(
                [str(veh.get(k)) for k in ("make", "model", "year") if veh.get(k)]
            )
            if veh
            else (plate or "—")
        )
        gcal = it.get("gcal_event_id", "—")
        order_id = it.get("order_id", "—")
        amount = int(it.get("amount_uah") or 0)

        lines.append(
            f"• {it['time']} — {fio}\n"
            f"  📞 +380{phone} | VIN: {vin} | №: {plate}\n"
            f"  🚗 {car}\n"
            f"  🎯 {it['reason']}\n"
            f"  💵 {amount} грн\n"
            f"  🧾 Order ID: {order_id}\n"
            f"  🗓 Google Event ID: {gcal}"
        )
        lines.append("─" * 20)

        cb = f"ready:{date_key}|{it['time']}|{uid}"
        kb.row(
            InlineKeyboardButton(
                text=f"💬 Авто готове • {it['time']}",
                callback_data=cb,
            )
        )

    text = "\n".join(lines)

    if hasattr(msg_or_bot, "send_message"):
        await msg_or_bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb.as_markup(),
        )
    else:
        await msg_or_bot.answer(
            text=text,
            reply_markup=kb.as_markup(),
        )


@r_admin.message(F.text == "🛠 Адмін")
async def admin_entry(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, ADMIN_IDS):
        await m.answer("❌ Доступ тільки для адміністратора.")
        return
    await m.answer("Адмін-меню:", reply_markup=admin_menu())


@r_admin.message(F.text == "⬅️ В головне меню")
async def back_to_main(m: Message):
    await m.answer(
        "Повертаю в головне:",
        reply_markup=main_menu(
            m.from_user.id in USERS,
            is_admin(m.from_user.id, ADMIN_IDS),
        ),
    )


@r_admin.message(F.text == "📋 Записи на сьогодні")
async def admin_today(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, ADMIN_IDS):
        await m.answer("❌ Доступ тільки для адміністратора.")
        return
    cur = await state.get_state()
    if cur:
        return
    today = now_local(TIMEZONE).strftime("%d.%m.%Y")
    await send_schedule_with_ready_buttons(m, m.chat.id, today)


@r_admin.message(F.text == "📅 Записи на дату")
async def admin_pick_date(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, ADMIN_IDS):
        await m.answer("❌ Доступ тільки для адміністратора.")
        return
    cur = await state.get_state()
    if cur:
        return
    await state.set_state(AdminDateStates.wait_date)
    await m.answer(
        "Введіть дату у форматі *dd.mm* або *dd.mm.yy*:",
        parse_mode="Markdown",
        reply_markup=cancel_menu(),
    )


@r_admin.message(AdminDateStates.wait_date, F.text)
async def admin_date_entered(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, ADMIN_IDS):
        return
    txt = (m.text or "").strip()
    if txt == "Скасувати":
        await state.clear()
        await admin_entry(m, state)
        return
    date_key = normalize_date(txt, TIMEZONE)
    if not date_key:
        await m.answer(
            "Дата некоректна. Приклад: `15.02` або `15.02.25`",
            parse_mode="Markdown",
        )
        return
    await state.clear()
    await send_schedule_with_ready_buttons(m, m.chat.id, date_key)


@r_admin.callback_query(F.data.startswith("ready:"))
async def on_ready_click(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id, ADMIN_IDS):
        await cq.answer("Доступ лише для адміністратора", show_alert=True)
        return

    try:
        _, payload = cq.data.split(":", 1)
        date_key, time_str, uid_s = payload.split("|")
        uid = int(uid_s)
    except Exception:
        await cq.answer("Некоректні дані кнопки.", show_alert=True)
        return

    appt = _find_appt(date_key, time_str, uid)
    if not appt:
        await cq.answer("Запис не знайдено", show_alert=True)
        return

    await state.set_state(ReadyStates.wait_amount)
    await state.update_data(date_key=date_key, time_str=time_str, uid=uid)

    fio = USERS.get(uid, {}).get("full_name", "Клієнт")
    current = int(appt.get("amount_uah") or 0)
    await cq.message.answer(
        f"Введи суму до сплати для {fio} на {date_key} о {time_str} "
        f"(зараз: {current} грн).\nНапр.: 1850",
        reply_markup=cancel_menu(),
    )
    await cq.answer()


@r_admin.message(ReadyStates.wait_amount, F.text)
async def on_ready_amount(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, ADMIN_IDS):
        return

    txt = (m.text or "").strip().replace(",", ".")
    if txt == "Скасувати":
        await state.clear()
        await m.answer("Скасовано. Повертаю в меню.", reply_markup=admin_menu())
        return

    try:
        amount_uah = int(float(txt))
    except ValueError:
        await m.answer("Введи число (грн), напр.: 1850")
        return

    data = await state.get_data()
    date_key = data["date_key"]
    time_str = data["time_str"]
    uid = int(data["uid"])

    appt = _find_appt(date_key, time_str, uid)
    if not appt:
        await m.answer("Запис не знайдено після перевірки.")
        await state.clear()
        return

    appt["amount_uah"] = amount_uah
    if not appt.get("order_id"):
        appt["order_id"] = (
            f"{date_key.replace('.','')}-"
            f"{time_str.replace(':','')}-"
            f"{uid}"
        )
    order_id = appt["order_id"]

    route = route_url_default() or os.getenv("ROUTE_URL", "")
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(
            text="💳 Оплатити",
            callback_data=f"{PAY_CALLBACK_PREFIX}:{order_id}",
        )
    )
    if route:
        kb.row(
            InlineKeyboardButton(
                text="📍 Маршрут до СТО",
                url=route,
            )
        )

    try:
        await m.bot.send_message(
            chat_id=uid,
            text=(
                "🚗 Авто готове до видачі.\n"
                f"Замовлення #{order_id}\n"
                f"До сплати: {amount_uah} грн"
            ),
            reply_markup=kb.as_markup(),
        )
        await m.answer(
            "✅ Суму встановлено і повідомлення надіслано клієнту.\n"
            f"Дата: {date_key}, час: {time_str}\n"
            f"Сума: {amount_uah} грн\n"
            f"Order: #{order_id}",
            reply_markup=admin_menu(),
        )
    except Exception as e:
        logger.error(f"[admin] send car ready failed: {e}")
        await m.answer(
            "Не вдалося надіслати клієнту. Перевір, що бот може писати користувачу.",
            reply_markup=admin_menu(),
        )

    await state.clear()


@r_admin.message(F.text)
async def _admin_catch_all(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, ADMIN_IDS):
        return
    cur = await state.get_state()
    if cur:
        return
    if m.text and "готове" in m.text.lower():
        await m.answer(
            "Оберіть дату через «📅 Записи на дату» та натисніть "
            "«💬 Готове» біля потрібного запису.",
            reply_markup=admin_menu(),
        )
        return
    await m.answer("Адмін-меню:", reply_markup=admin_menu())
