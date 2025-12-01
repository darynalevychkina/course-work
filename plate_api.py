# plate_api.py
import os
import re
import aiohttp
from loguru import logger

BAZAGAI_BASE = "https://baza-gai.com.ua/nomer/{plate}"

_PLATE_RE = re.compile(r"^[A-ZА-ЯІЇЄ]{2}\d{4}[A-ZА-ЯІЇЄ]{2}$", re.IGNORECASE)


def normalize_plate(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[\s\-–—]", "", s)
    return s


def plate_format_ok(s: str) -> bool:
    return bool(_PLATE_RE.fullmatch(normalize_plate(s)))


async def fetch_plate_info(
    plate: str,
    api_key: str | None = None,
    *,
    mock: bool | None = None,       
    timeout_sec: int | None = None,
) -> dict | None:
    plate = normalize_plate(plate)
    if not plate_format_ok(plate):
        return None

    api_key = api_key or os.getenv("BAZAGAI_API_KEY", "")
    if not api_key:
        logger.warning("[BazaGAI] API key is missing, request skipped")
        return None

    timeout_sec = timeout_sec or int(os.getenv("BAZAGAI_TIMEOUT", "10"))

    url = BAZAGAI_BASE.format(plate=plate)
    headers = {"Accept": "application/json", "X-Api-Key": api_key}
    timeout = aiohttp.ClientTimeout(total=timeout_sec)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.warning(f"[BazaGAI] HTTP {resp.status} for plate {plate}")
                return None

            data = await resp.json()
            vendor = data.get("vendor") or data.get("make")
            model = data.get("model")
            year = data.get("model_year") or data.get("year")
            vin = data.get("vin")
            is_stolen = bool(data.get("is_stolen"))

            return {
                "plate": plate,
                "vendor": vendor,
                "model": model,
                "model_year": year,
                "vin": vin,
                "is_stolen": is_stolen,
                "raw": data,
            }
