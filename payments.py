# payments.py
import os
from datetime import datetime

from aiogram import Router, F
from aiogram.types import CallbackQuery, FSInputFile
from loguru import logger

from receipts_store import save_receipt_bytes

r_pay = Router()

PAY_CALLBACK_PREFIX = "pay"

_USERS = {}
_APPOINTMENTS = {}
_RECEIPTS_DIR = "./receipts" 


def set_receipts_dir(path: str):
    global _RECEIPTS_DIR
    _RECEIPTS_DIR = path or "./receipts"
    os.makedirs(_RECEIPTS_DIR, exist_ok=True)
    logger.info(f"[payments] receipts dir = {os.path.abspath(_RECEIPTS_DIR)}")


def init_pay_context(*, users, appointments, gcal_ok, gcal_svc, gcal_id):
    global _USERS, _APPOINTMENTS
    _USERS = users
    _APPOINTMENTS = appointments
    logger.info("[payments] context inited (calendar ignored)")


def _format_receipt_text(
    order_id: str,
    amount_uah: int | float,
    customer_name: str,
    phone: str,
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "=== TEST RECEIPT ===",
        f"Date:       {now}",
        f"Order ID:   {order_id}",
        f"Customer:   {customer_name or '—'}",
        f"Phone:      +380{phone if phone else '—'}",
        f"Amount:     {int(amount_uah)} UAH",
        "Status:     PAID (test)",
        "Note:       This is a test receipt (no real acquiring).",
    ]
    return "\n".join(lines) + "\n"


def on_payment_success(
    order_id: str,
    receipt_bytes: bytes,
    *,
    ext: str = "txt",
    user_name: str | None = None,
) -> str:
    if not receipt_bytes:
        raise ValueError("Порожній вміст квитанції")

    path = save_receipt_bytes(
        order_id,
        receipt_bytes,
        receipts_dir=_RECEIPTS_DIR,
        ext=ext,
        user_name=user_name,
    )
    logger.info(f"[payments] receipt saved for order {order_id}: {path}")
    return path


@r_pay.callback_query(F.data.startswith(f"{PAY_CALLBACK_PREFIX}:"))
async def simulate_payment(cq: CallbackQuery):
    try:
        _, order_id = cq.data.split(":", 1)
        order_id = (order_id or "").strip()
    except Exception:
        await cq.answer("Некоректні дані платежу", show_alert=True)
        return

    amount = 0
    customer_name = ""
    phone = ""
    found_rec = None

    for _, items in _APPOINTMENTS.items():
        for rec in items:
            if str(rec.get("order_id")) == order_id:
                found_rec = rec
                amount = int(rec.get("amount_uah") or 0)
                u = _USERS.get(int(rec.get("user_id")), {})
                customer_name = u.get("full_name", "")
                phone = u.get("phone", "")
                break
        if found_rec:
            break

    if not found_rec:
        await cq.answer("Замовлення не знайдено.", show_alert=True)
        return
    if amount <= 0:
        await cq.answer("Сума не встановлена адміністратором.", show_alert=True)
        return

    receipt_text = _format_receipt_text(order_id, amount, customer_name, phone)
    receipt_path = on_payment_success(
        order_id,
        receipt_text.encode("utf-8"),
        ext="txt",
        user_name=customer_name,
    )

    try:
        file = FSInputFile(receipt_path)
        await cq.message.bot.send_document(
            chat_id=cq.from_user.id,
            document=file,
            caption=(
                f"🧾 Квитанція по замовленню #{order_id}\n"
                f"Сума: {amount} грн\n"
                f"Дякуємо за оплату!"
            ),
        )
    except Exception as e:
        logger.error(f"[payments] send receipt failed: {e}")
        await cq.answer("Не вдалося надіслати файл квитанції 😕", show_alert=True)
        return

    await cq.answer("Оплату проведено (тест). Квитанцію надіслано.", show_alert=True)
