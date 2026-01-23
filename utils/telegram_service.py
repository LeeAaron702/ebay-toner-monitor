# telegram_utils.py
"""
Telegram utilities with PTB v21+ for webhook mode.

This module provides:
- Native-async command handlers registration
- Channel router (channels don’t auto-parse slash commands)
- Diagnostics (start, ping, unknown)
- /help command listing all available commands
- Plain HTTP helpers for sending messages/media outside PTB (used by canon.py)

Env (used by HTTP helpers):
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
  TELEGRAM_SEND_MIN_INTERVAL_SEC  (default 0.8)
  TELEGRAM_MAX_RETRIES            (default 4)
"""

from __future__ import annotations

import os
import time
import logging
from typing import Iterable, List

import requests
from telegram import Update, InputMediaPhoto, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Bring in the async command implementations
from api.routers.telegram_exclude_commands import (
    list_sellers, add_seller, remove_seller, update_seller,
    list_keywords, add_keyword, remove_keyword, update_keyword,
)

logger = logging.getLogger(__name__)

# ─────────────────────── Command catalog (single source of truth) ────────────
# Keep this list in sync with registered handlers. api_server.py will use it to
# set Telegram's /menu command list, and /help will render from it.
COMMANDS: list[tuple[str, str]] = [
    ("start", "Bot status"),
    ("help", "Show available commands"),
    ("ping", "Health check"),
    # Sellers
    ("list_sellers", "List excluded sellers"),
    ("add_seller", "Add a seller to exclude"),
    ("remove_seller", "Remove a seller from exclude"),
    ("update_seller", "Rename an excluded seller"),
    # Canon keywords
    ("list_keywords", "List Canon excluded keywords"),
    ("add_keyword", "Add a Canon keyword to exclude"),
    ("remove_keyword", "Remove a Canon keyword"),
    ("update_keyword", "Rename a Canon keyword"),
]

def get_bot_commands() -> list[BotCommand]:
    """Return PTB BotCommand objects for set_my_commands()."""
    return [BotCommand(command=name, description=desc) for name, desc in COMMANDS]

# ─────────────────────── Utility commands (async) ────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if m:
        await m.reply_text("Bot is alive.")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if m:
        await m.reply_text("pong")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if not m:
        return
    lines = ["Available commands:"]
    for name, desc in COMMANDS:
        lines.append(f"/{name} — {desc}")
    lines.append("")
    lines.append("Notes:")
    lines.append("• In channels, post commands as plain text (e.g., /list_sellers).")
    lines.append("• Examples:")
    lines.append("  /add_seller ExampleStore")
    lines.append("  /update_seller OldName NewName")
    lines.append("  /add_keyword cracked box (Canon)")
    await m.reply_text("\n".join(lines))

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if not m:
        return
    txt = m.text or ""
    if txt.startswith("/"):
        await m.reply_text("Unknown command. Try /help")

async def log_all_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        utype = (
            "channel_post" if update.channel_post else
            "edited_channel_post" if update.edited_channel_post else
            "message" if update.message else
            "edited_message" if update.edited_message else
            "callback_query" if update.callback_query else
            "other"
        )
        logger.info("Update %s: chat=%s text=%r",
                    utype,
                    update.effective_chat.id if update.effective_chat else None,
                    (update.effective_message.text if update.effective_message else None))
    except Exception:
        logger.exception("log_all_updates error")

# ─────────────────────── Channel router (async) ──────────────────────────────

# Map of command name -> handler function for manual routing in channels
_COMMAND_MAP = {
    "start": start,
    "help": help_command,
    "ping": ping,
    # Sellers
    "list_sellers": list_sellers,
    "add_seller": add_seller,
    "remove_seller": remove_seller,
    "update_seller": update_seller,
    # Keywords
    "list_keywords": list_keywords,
    "add_keyword": add_keyword,
    "remove_keyword": remove_keyword,
    "update_keyword": update_keyword,
}

def _split_command(text: str) -> tuple[str | None, list[str]]:
    """
    Return (command, args) from a raw message text that starts with '/'.
    Strips bot mentions like '/foo@MyBot'.
    """
    parts = text.strip().split()
    if not parts or not parts[0].startswith("/"):
        return None, []
    cmd = parts[0][1:]
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd, parts[1:]

async def channel_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if not m or not (m.text or "").startswith("/"):
        return
    cmd, args = _split_command(m.text)
    if not cmd:
        return
    context.args = args
    handler = _COMMAND_MAP.get(cmd)
    if handler is None:
        return
    await handler(update, context)

# ─────────────────────── Registration ────────────────────────────────────────

def register_handlers(app: Application) -> None:
    """
    Register all command, message, and error handlers on the given PTB Application.
    """
    # DMs & groups commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping))

    # Sellers
    app.add_handler(CommandHandler("list_sellers", list_sellers))
    app.add_handler(CommandHandler("add_seller", add_seller))
    app.add_handler(CommandHandler("remove_seller", remove_seller))
    app.add_handler(CommandHandler("update_seller", update_seller))

    # Keywords
    app.add_handler(CommandHandler("list_keywords", list_keywords))
    app.add_handler(CommandHandler("add_keyword", add_keyword))
    app.add_handler(CommandHandler("remove_keyword", remove_keyword))
    app.add_handler(CommandHandler("update_keyword", update_keyword))

    # Channels: manual command router
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.TEXT, channel_router))

    # Fallbacks / diagnostics
    app.add_handler(MessageHandler(filters.COMMAND & ~filters.ChatType.CHANNEL, unknown_command))
    app.add_handler(MessageHandler(filters.ALL, log_all_updates))

# ─────────────────────── Plain HTTP helpers (sync) ───────────────────────────
# Used by canon.py to post albums/text to a fixed chat via raw Bot API.

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
TELEGRAM_SEND_MIN_INTERVAL_SEC = 4
TELEGRAM_MAX_RETRIES = 4

_last_post_ts = 0.0

def _require_env(require_chat: bool = False) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    if require_chat and not CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID is not set")

def _chunked(iterable: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch

def _pace_outbound_post() -> None:
    global _last_post_ts
    now = time.monotonic()
    elapsed = now - _last_post_ts
    wait = TELEGRAM_SEND_MIN_INTERVAL_SEC - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_post_ts = time.monotonic()

def _post_telegram(method: str, payload: dict) -> requests.Response:
    _pace_outbound_post()
    url = f"{BASE_URL}/{method}"
    return requests.post(url, json=payload, timeout=30)

def _post_with_retries(method: str, payload: dict) -> dict:
    last_exc = None
    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            resp = _post_telegram(method, payload)
            if resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 1)
                time.sleep(max(1.0, float(retry_after)))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok", False):
                raise RuntimeError(f"Telegram API error: {data}")
            return data
        except Exception as e:
            last_exc = e
            time.sleep(min(10, 1.5 ** attempt))
    raise RuntimeError(f"Failed after retries: {last_exc}")

def send_media_group_with_caption(caption: str, image_urls: List[str]) -> None:
    """
    Send one or more photos as albums. The first photo of the FIRST album carries the caption.
    Telegram limits media groups to 10 items; longer lists are sent in multiple albums.
    """
    try:
        _require_env(require_chat=True)
    except RuntimeError as e:
        logger.error(str(e))
        return
    if not image_urls:
        return

    # Limit to first 5 images only
    image_urls = image_urls[:5]
    
    # Validate and filter image URLs
    valid_urls = [url for url in image_urls if url and isinstance(url, str) and url.startswith(('http://', 'https://'))]
    if not valid_urls:
        logger.warning("No valid image URLs to send")
        return
    
    first_album = True
    for batch in _chunked(valid_urls, 5):
        media = []
        for idx, url in enumerate(batch):
            obj = {"type": "photo", "media": url}
            if first_album and idx == 0 and caption:
                # Telegram caption limit is 1024 characters
                obj["caption"] = caption[:1024] if len(caption) > 1024 else caption
                obj["parse_mode"] = "HTML"
            media.append(obj)

        payload = {"chat_id": CHAT_ID, "media": media}
        try:
            _post_with_retries("sendMediaGroup", payload)
        except Exception as ex:
            logger.exception("Failed to send media group: %s", ex)
        first_album = False
        time.sleep(max(0.0, TELEGRAM_SEND_MIN_INTERVAL_SEC / 2))

def send_telegram_message(text: str) -> None:
    """Send a plain text message (HTML parse mode) to the configured chat via HTTP."""
    try:
        _require_env(require_chat=True)
    except RuntimeError as e:
        logger.error(str(e))
        return
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        _post_with_retries("sendMessage", payload)
    except Exception as ex:
        logger.exception("Failed to send Telegram message: %s", ex)
