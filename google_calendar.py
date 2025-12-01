from __future__ import annotations
from typing import Optional, List, Dict
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/calendar"]


def get_calendar_service(sa_json_path: str):
    creds = Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
    return build("calendar", "v3", credentials=creds)


def get_service_account_email(sa_json_path: str) -> str:
    creds = Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
    return creds.service_account_email


def list_visible_calendars(service) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    page_token = None
    while True:
        resp = service.calendarList().list(pageToken=page_token, minAccessRole="reader").execute()
        for item in resp.get("items", []):
            out.append({"id": item.get("id"), "summary": item.get("summary")})
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out


def can_access_calendar(service, calendar_id: str) -> bool:
    try:
        service.calendars().get(calendarId=calendar_id).execute()
        return True
    except HttpError:
        return False


def _make_description(
    order_id: str,
    customer_name: str,
    phone: str,
    vin: str,
    car_line: str,
    reason: str,
    receipt_url: Optional[str] = None,
) -> str:
    desc = [
        f"Замовлення: #{order_id}",
        f"Клієнт: {customer_name or '—'}",
        f"Телефон: +380{phone}" if phone else "Телефон: —",
        f"VIN: {vin or '—'}",
        f"Авто: {car_line or '—'}",
        f"Причина: {reason or '—'}",
    ]
    if receipt_url:
        desc.append(f"Квитанція: {receipt_url}")
    return "\n".join(desc)


def create_event(
    service,
    calendar_id: str,
    start_dt: datetime,
    end_dt: datetime,
    summary: str,
    description: str,
    location: Optional[str] = None,
) -> str:
    body = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_dt.isoformat()},
        "end": {"dateTime": end_dt.isoformat()},
        "reminders": {"useDefault": True},
    }
    if location:
        body["location"] = location

    event = service.events().insert(calendarId=calendar_id, body=body).execute()
    return event.get("id")


def create_event_for_order(
    service,
    calendar_id: str,
    *,
    order_id: str,
    start_dt: datetime,
    end_dt: datetime,
    customer_name: str,
    phone: str,
    vin: str,
    car_line: str,
    reason: str,
    location: Optional[str] = None,
) -> str:
    summary = f"СТО: {customer_name or 'Клієнт'} — {reason or 'візит'}"
    description = _make_description(order_id, customer_name, phone, vin, car_line, reason)

    body = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_dt.isoformat()},
        "end": {"dateTime": end_dt.isoformat()},
        "reminders": {"useDefault": True},
        "extendedProperties": {"private": {"order_id": str(order_id)}},
    }
    if location:
        body["location"] = location

    event = service.events().insert(calendarId=calendar_id, body=body).execute()
    return event.get("id")


def ensure_order_id(service, calendar_id: str, event_id: str, order_id: str) -> None:
    ev = service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    ext = ev.get("extendedProperties", {}) or {}
    pvt = ext.get("private", {}) or {}
    if pvt.get("order_id") == str(order_id):
        return
    pvt["order_id"] = str(order_id)
    ext["private"] = pvt
    service.events().patch(
        calendarId=calendar_id,
        eventId=event_id,
        body={"extendedProperties": ext},
    ).execute()


def update_event_append_receipt_link(service, calendar_id: str, event_id: str, receipt_url: str) -> None:
    ev = service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    desc = ev.get("description") or ""
    suffix = f"\nКвитанція: {receipt_url}"
    if receipt_url and receipt_url not in desc:
        desc = (desc + suffix).strip()
        service.events().patch(
            calendarId=calendar_id,
            eventId=event_id,
            body={"description": desc},
        ).execute()


def find_event_by_order_id(
    service,
    calendar_id: str,
    order_id: str,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
) -> Optional[Dict]:
    q = service.events().list(
        calendarId=calendar_id,
        privateExtendedProperty=[f"order_id={order_id}"],
        timeMin=time_min,
        timeMax=time_max,
        singleEvents=True,
        maxResults=50,
        orderBy="startTime",
    ).execute()
    items = q.get("items", [])
    return items[0] if items else None
