# receipts_store.py
import os
from datetime import datetime
from loguru import logger

UA_MONTHS = {
    1: "Січень", 2: "Лютий", 3: "Березень", 4: "Квітень",
    5: "Травень", 6: "Червень", 7: "Липень", 8: "Серпень",
    9: "Вересень", 10: "Жовтень", 11: "Листопад", 12: "Грудень",
}

def ensure_receipts_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    abs_path = os.path.abspath(path)
    logger.info(f"Receipts directory: {abs_path}")
    return abs_path

def _safe_filename(name: str) -> str:
    return "".join(ch for ch in name if ch.isalnum() or ch in ("-", "_", ".", "#", " ")).strip()

def _parse_order_dt(order_id: str) -> datetime | None:
    try:
        head = order_id.split("-", 2)[:2]
        d, t = head[0], head[1]
        return datetime.strptime(f"{d} {t}", "%Y%m%d %H%M")
    except Exception:
        return None

def _month_dir_name(dt: datetime) -> str:
    return f"{UA_MONTHS.get(dt.month, str(dt.month))} {dt.year}"

def _make_filename(dt: datetime, user_name: str | None, order_id: str, ext: str) -> str:
    stamp = dt.strftime("%Y-%m-%d_%H%M")
    if user_name:
        base = f"{stamp}__{user_name}"
    else:
        base = f"{stamp}__order_{order_id}"
    return _safe_filename(base) + f".{ext.lstrip('.')}"

def save_receipt_bytes(
    order_id: str,
    raw_bytes: bytes,
    *,
    receipts_dir: str,
    ext: str = "pdf",
    user_name: str | None = None,
) -> str:
    if not raw_bytes:
        raise ValueError("Порожній вміст квитанції")

    os.makedirs(receipts_dir, exist_ok=True)

    dt = _parse_order_dt(order_id) or datetime.now()
    month_dir = os.path.join(receipts_dir, _month_dir_name(dt))
    os.makedirs(month_dir, exist_ok=True)

    filename = _make_filename(dt, user_name, order_id, ext)
    path = os.path.join(month_dir, filename)

    with open(path, "wb") as f:
        f.write(raw_bytes)

    abs_path = os.path.abspath(path)
    logger.info(f"Receipt saved: {abs_path}")
    return abs_path
