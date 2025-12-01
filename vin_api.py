import re
import os
import aiohttp
from loguru import logger

_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")

_TRANSLIT = {
    "A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7, "H": 8,
    "J": 1, "K": 2, "L": 3, "M": 4, "N": 5, "P": 7, "R": 9,
    "S": 2, "T": 3, "U": 4, "V": 5, "W": 6, "X": 7, "Y": 8, "Z": 9,
    "0": 0, "1": 1, "2": 2, "3": 3, "4": 4,
    "5": 5, "6": 6, "7": 7, "8": 8, "9": 9,
}

_WEIGHTS = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]

AUTODEV_URL = "https://api.auto.dev/vin/{vin}"


def normalize_vin(s: str) -> str:
    s = (s or "").strip().upper()
    s = re.sub(r"\s+", "", s)
    return s


def vin_format_ok(s: str) -> bool:
    return bool(_VIN_RE.fullmatch(normalize_vin(s)))


def vin_checksum_ok(s: str) -> bool:
    vin = normalize_vin(s)
    if not _VIN_RE.fullmatch(vin):
        return False
    total = 0
    for i, ch in enumerate(vin):
        v = _TRANSLIT.get(ch)
        if v is None:
            return False
        total += v * _WEIGHTS[i]
    remainder = total % 11
    expected = "X" if remainder == 10 else str(remainder)
    return vin[8] == expected


def validate_vin(s: str) -> bool:
    return vin_format_ok(s) and vin_checksum_ok(s)


def _extract_vehicle(payload: dict) -> dict:
    """
    Акуратно витягуємо make/model/year з різних форматів відповіді Auto.dev.
    """
    if not isinstance(payload, dict):
        return {}

    make = payload.get("make") or payload.get("manufacturer")
    model = payload.get("model")
    year = payload.get("year")

    data = payload.get("data") or payload.get("vehicle") or payload.get("specs") or {}
    if isinstance(data, dict):
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
        if isinstance(r0, dict):
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


async def fetch_vehicle_by_vin(
    vin: str,
    api_key: str | None = None,
    *,
    timeout_sec: int | None = None,
) -> dict | None:
    vin = normalize_vin(vin)
    if not validate_vin(vin):
        return None

    api_key = api_key or os.getenv("AUTO_DEV_API_KEY", "")
    if not api_key:
        logger.error("[auto.dev] AUTO_DEV_API_KEY не заданий у .env")
        return None

    timeout_sec = timeout_sec or int(os.getenv("AUTO_DEV_TIMEOUT", "10"))

    url = AUTODEV_URL.format(vin=vin)
    headers = {
        "accept": "application/json",
        "x-api-key": api_key,
    }
    timeout = aiohttp.ClientTimeout(total=timeout_sec)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(
                    f"[auto.dev] HTTP {resp.status} for VIN {vin} → {text[:300]}"
                )
                return None

            data = await resp.json()
            vehicle = _extract_vehicle(data)

            make = vehicle.get("make")
            model = vehicle.get("model")
            year = vehicle.get("year")

            return {
                "vin": vin,
                "make": make,
                "model": model,
                "year": year,
                "trim": None,
                "raw": data,
            }
