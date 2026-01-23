"""
telegram_exclude_commands.py

Native-async Telegram command handlers that talk to your FastAPI exclude API.

Mounted under prefix="/exclude" on the FastAPI side:
    Sellers:
        GET    /exclude/sellers
        POST   /exclude/sellers               {"name": "<seller>"}
        DELETE /exclude/sellers/{name}
        PUT    /exclude/sellers/{old_name}    {"name": "<new_name>"}

    Canon Keywords:
        GET    /exclude/canon/keywords
        POST   /exclude/canon/keywords              {"phrase": "<keyword_or_phrase>"}
        DELETE /exclude/canon/keywords/{phrase}
        PUT    /exclude/canon/keywords/{old_phrase} {"phrase": "<new_phrase>"}
"""

from __future__ import annotations

import os
from typing import List, Tuple

import httpx
from telegram import Update
from telegram.ext import ContextTypes

# ---- Configuration ----
API_BASE = os.getenv("EXCLUDE_API_BASE", "http://localhost:8000/exclude")
EXCLUDE_API_KEY = os.getenv("EXCLUDE_API_KEY", "")

# ---- HTTP helpers ----
def _api_headers() -> dict:
    h = {"Content-Type": "application/json"}
    if EXCLUDE_API_KEY:
        h["X-API-Key"] = EXCLUDE_API_KEY
    return h

def _safe_text(s: str) -> str:
    return " ".join(s.strip().split())

async def _request_json(
    method: str,
    path: str,
    *,
    json: dict | None = None,
    timeout: float = 15.0,
) -> Tuple[int, dict | list | str]:
    url = f"{API_BASE}{path}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.request(method, url, headers=_api_headers(), json=json)
        # Try to parse a structured payload if possible
        try:
            payload = r.json()
        except Exception:
            payload = r.text
        return r.status_code, payload

def _fmt_error(status: int, payload: dict | list | str) -> str:
    # Prefer FastAPI-style {"detail": "..."} or {"message": "..."}
    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("message")
        if detail:
            return f"{status} {detail}"
    return f"{status} {payload}"

# =========================
# Sellers commands
# =========================

async def list_sellers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    status, payload = await _request_json("GET", "/sellers")
    if status == 200 and isinstance(payload, list):
        if not payload:
            await m.reply_text("No excluded sellers.")
            return
        lines = "\n".join(f"• {s}" for s in payload)
        await m.reply_text(f"Excluded sellers ({len(payload)}):\n{lines}")
    else:
        await m.reply_text(f"Error listing sellers: {_fmt_error(status, payload)}")

async def add_seller(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if not context.args:
        await m.reply_text("Usage: /add_seller <seller_name>")
        return
    name = _safe_text(" ".join(context.args))
    status, payload = await _request_json("POST", "/sellers", json={"name": name})
    if status in (200, 201):
        msg = payload.get("message") if isinstance(payload, dict) else "Added"
        await m.reply_text(msg)
    else:
        await m.reply_text(f"Error adding seller: {_fmt_error(status, payload)}")

async def remove_seller(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if not context.args:
        await m.reply_text("Usage: /remove_seller <seller_name>")
        return
    name = _safe_text(" ".join(context.args))
    status, payload = await _request_json("DELETE", f"/sellers/{name}")
    if status == 200:
        msg = payload.get("message") if isinstance(payload, dict) else "Removed"
        await m.reply_text(msg)
    else:
        await m.reply_text(f"Error removing seller: {_fmt_error(status, payload)}")

async def update_seller(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if len(context.args) < 2:
        await m.reply_text("Usage: /update_seller <old_name> <new_name>")
        return
    old_name = _safe_text(context.args[0])
    new_name = _safe_text(" ".join(context.args[1:]))
    status, payload = await _request_json("PUT", f"/sellers/{old_name}", json={"name": new_name})
    if status == 200:
        msg = payload.get("message") if isinstance(payload, dict) else "Updated"
        await m.reply_text(msg)
    else:
        await m.reply_text(f"Error updating seller: {_fmt_error(status, payload)}")

# =========================
# Keywords commands
# =========================

async def list_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    status, payload = await _request_json("GET", "/canon/keywords")
    if status == 200 and isinstance(payload, list):
        if not payload:
            await m.reply_text("No Canon excluded keywords/phrases.")
            return
        lines = "\n".join(f"• {k}" for k in payload)
        await m.reply_text(f"Canon excluded keywords ({len(payload)}):\n{lines}")
    else:
        await m.reply_text(f"Error listing keywords: {_fmt_error(status, payload)}")

async def add_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if not context.args:
        await m.reply_text("Usage: /add_keyword <keyword_or_phrase>")
        return
    phrase = _safe_text(" ".join(context.args))
    status, payload = await _request_json("POST", "/canon/keywords", json={"phrase": phrase})
    if status in (200, 201):
        msg = payload.get("message") if isinstance(payload, dict) else "Added"
        await m.reply_text(msg)
    else:
        await m.reply_text(f"Error adding keyword: {_fmt_error(status, payload)}")

async def remove_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if not context.args:
        await m.reply_text("Usage: /remove_keyword <keyword_or_phrase>")
        return
    phrase = _safe_text(" ".join(context.args))
    status, payload = await _request_json("DELETE", f"/canon/keywords/{phrase}")
    if status == 200:
        msg = payload.get("message") if isinstance(payload, dict) else "Removed"
        await m.reply_text(msg)
    else:
        await m.reply_text(f"Error removing keyword: {_fmt_error(status, payload)}")

async def update_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if len(context.args) < 2:
        await m.reply_text("Usage: /update_keyword <old_phrase> <new_phrase>")
        return
    old_phrase = _safe_text(context.args[0])
    new_phrase = _safe_text(" ".join(context.args[1:]))
    status, payload = await _request_json("PUT", f"/canon/keywords/{old_phrase}", json={"phrase": new_phrase})
    if status == 200:
        msg = payload.get("message") if isinstance(payload, dict) else "Updated"
        await m.reply_text(msg)
    else:
        await m.reply_text(f"Error updating keyword: {_fmt_error(status, payload)}")
