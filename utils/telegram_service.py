# telegram_utils.py
"""
Telegram utilities for sending notifications.

This module provides:
- Plain HTTP helpers for sending messages/media (used by engine/*.py)
- Basic bot status commands (start, ping)

Note: Exclusion management is now handled via the admin panel at /admin/exclusions.
Slash commands have been removed in favor of the web interface.

Env:
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
  TELEGRAM_SEND_MIN_INTERVAL_SEC  (default 4)
  TELEGRAM_MAX_RETRIES            (default 4)
"""

from __future__ import annotations

import os
import time
import logging
from typing import Iterable, List, Optional

import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logger = logging.getLogger(__name__)

# ─────────────────────── Basic bot commands (async) ──────────────────────────

ADMIN_PANEL_URL = os.getenv("ADMIN_PANEL_URL", "")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if m:
        msg = "Bot is alive."
        if ADMIN_PANEL_URL:
            msg += f"\n\nManage exclusions and products at:\n{ADMIN_PANEL_URL}/admin"
        await m.reply_text(msg)

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    m = update.effective_message
    if m:
        await m.reply_text("pong")

async def log_all_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log all incoming updates for debugging."""
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

# ─────────────────────── eBay message reply handlers ────────────────────────

async def handle_reply_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the 'Reply' inline keyboard button on eBay message notifications."""
    query = update.callback_query
    if not query or not query.data:
        return

    # Callback data format: "ebay_reply:{db_row_id}"
    if not query.data.startswith("ebay_reply:"):
        return

    await query.answer()

    try:
        row_id = int(query.data.split(":")[1])
    except (IndexError, ValueError):
        await query.message.reply_text("Error: invalid message reference.")
        return

    from db.listings_db import get_ebay_message_by_row_id
    msg = get_ebay_message_by_row_id(row_id)
    if not msg:
        await query.message.reply_text("Error: message not found in database.")
        return

    # Store pending reply state
    context.user_data["pending_ebay_reply"] = {
        "row_id": row_id,
        "sender": msg.get("sender") or msg.get("sender_id", "Unknown"),
        "sender_id": msg.get("sender_id", ""),
        "item_id": msg.get("item_id", ""),
        "ebay_message_id": msg.get("ebay_message_id", ""),
        "subject": msg.get("subject", ""),
    }

    sender = msg.get("sender") or msg.get("sender_id", "Unknown")
    cancel_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel", callback_data="ebay_reply_cancel")]
    ])
    await query.message.reply_text(
        f"Type your reply to <b>{sender}</b>:\n"
        f"(Re: {msg.get('subject', 'no subject')})",
        parse_mode="HTML",
        reply_markup=cancel_kb,
    )


async def handle_cancel_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel a pending eBay reply."""
    query = update.callback_query
    if not query or query.data != "ebay_reply_cancel":
        return

    await query.answer()
    context.user_data.pop("pending_ebay_reply", None)
    await query.message.reply_text("Reply cancelled.")


async def handle_reply_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Capture typed text as an eBay reply when a reply is pending."""
    pending = context.user_data.get("pending_ebay_reply")
    if not pending:
        return  # No pending reply — fall through to log_all_updates

    reply_body = update.message.text
    if not reply_body:
        return

    # Clear pending state immediately
    context.user_data.pop("pending_ebay_reply", None)

    item_id = pending["item_id"]
    recipient_id = pending["sender_id"]
    parent_msg_id = pending["ebay_message_id"]
    row_id = pending["row_id"]

    if not item_id or not recipient_id:
        await update.message.reply_text(
            "Cannot reply — missing item ID or sender ID from the original message."
        )
        return

    await update.message.reply_text("Sending reply to eBay...")

    from utils.ebay_messages import send_reply
    success = send_reply(item_id, recipient_id, reply_body, parent_msg_id)

    if success:
        from db.listings_db import mark_ebay_message_replied
        mark_ebay_message_replied(row_id, reply_body)
        await update.message.reply_text(
            f"Reply sent to <b>{pending['sender']}</b>.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            "Failed to send reply via eBay. Check logs for details."
        )


# ─────────────────────── Registration ────────────────────────────────────────

def register_handlers(app: Application) -> None:
    """
    Register command handlers and eBay message reply handlers.

    Note: Exclusion management commands have been removed.
    Use the admin panel at /admin/exclusions instead.
    """
    # Basic status commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))

    # eBay message reply flow
    app.add_handler(CallbackQueryHandler(handle_reply_button, pattern=r"^ebay_reply:\d+$"))
    app.add_handler(CallbackQueryHandler(handle_cancel_reply, pattern=r"^ebay_reply_cancel$"))
    # Text handler for reply capture — must come before the catch-all
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reply_text))

    # Log all updates for debugging (catch-all, must be last)
    app.add_handler(MessageHandler(filters.ALL, log_all_updates))

# ─────────────────────── Plain HTTP helpers (sync) ───────────────────────────
# Used by engine/*.py to post albums/text to a fixed chat via raw Bot API.

TELEGRAM_SEND_MIN_INTERVAL_SEC = 4
TELEGRAM_MAX_RETRIES = 4

_last_post_ts = 0.0

def _get_bot_token() -> str | None:
    """Read TELEGRAM_BOT_TOKEN lazily so load_dotenv() has time to run."""
    return os.getenv("TELEGRAM_BOT_TOKEN")

def _get_chat_id() -> str | None:
    """Read TELEGRAM_CHAT_ID lazily so load_dotenv() has time to run."""
    return os.getenv("TELEGRAM_CHAT_ID")

def _require_env(require_chat: bool = False) -> None:
    if not _get_bot_token():
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    if require_chat and not _get_chat_id():
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
    url = f"https://api.telegram.org/bot{_get_bot_token()}/{method}"
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


# Telegram caption limit is 1024 characters
TELEGRAM_CAPTION_LIMIT = 1024
# Telegram message limit is 4096 characters
TELEGRAM_MESSAGE_LIMIT = 4096


def _split_message_at_line_boundary(text: str, limit: int) -> tuple[str, str]:
    """
    Split a message at a line boundary, preferring to split at double newlines (paragraphs)
    or single newlines. Returns (first_part, remainder).
    """
    if len(text) <= limit:
        return text, ""
    
    # Reserve space for continuation marker
    effective_limit = limit - 30  # "...\n\n(continued below)" = ~22 chars
    
    # Try to find a good split point: double newline, then single newline
    search_text = text[:effective_limit]
    
    # Prefer splitting at paragraph boundary (double newline)
    last_para = search_text.rfind('\n\n')
    if last_para > effective_limit * 0.5:  # At least halfway through
        return text[:last_para] + "\n\n(continued below)", text[last_para + 2:]
    
    # Fall back to single newline
    last_newline = search_text.rfind('\n')
    if last_newline > effective_limit * 0.5:
        return text[:last_newline] + "\n\n(continued below)", text[last_newline + 1:]
    
    # Last resort: hard cut at limit
    return text[:effective_limit] + "\n\n(continued below)", text[effective_limit:]


def send_media_group_with_caption(caption: str, image_urls: List[str]) -> None:
    """
    Send one or more photos as albums. The first photo of the FIRST album carries the caption.
    Telegram limits media groups to 10 items; longer lists are sent in multiple albums.
    
    If the caption exceeds 1024 characters, it will be intelligently split:
    1. The first 1024 characters (at a line boundary) go on the image caption
    2. The remainder is sent as a follow-up text message
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
    
    # Handle long captions by splitting at line boundaries
    caption_part, continuation = "", ""
    if caption:
        if len(caption) > TELEGRAM_CAPTION_LIMIT:
            caption_part, continuation = _split_message_at_line_boundary(caption, TELEGRAM_CAPTION_LIMIT)
            logger.info(f"Caption split: {len(caption)} chars -> {len(caption_part)} + {len(continuation)}")
        else:
            caption_part = caption
    
    first_album = True
    for batch in _chunked(valid_urls, 5):
        media = []
        for idx, url in enumerate(batch):
            obj = {"type": "photo", "media": url}
            if first_album and idx == 0 and caption_part:
                obj["caption"] = caption_part
                obj["parse_mode"] = "HTML"
            media.append(obj)

        payload = {"chat_id": _get_chat_id(), "media": media}
        try:
            _post_with_retries("sendMediaGroup", payload)
        except Exception as ex:
            logger.exception("Failed to send media group: %s", ex)
        first_album = False
        time.sleep(max(0.0, TELEGRAM_SEND_MIN_INTERVAL_SEC / 2))
    
    # Send continuation as a follow-up text message if needed
    if continuation:
        send_telegram_message(continuation)


def send_telegram_message(text: str) -> None:
    """
    Send a plain text message (HTML parse mode) to the configured chat via HTTP.
    
    If the message exceeds 4096 characters, it will be split into multiple messages.
    """
    try:
        _require_env(require_chat=True)
    except RuntimeError as e:
        logger.error(str(e))
        return
    
    # Handle long messages by splitting
    remaining = text
    part_num = 1
    while remaining:
        if len(remaining) <= TELEGRAM_MESSAGE_LIMIT:
            msg_part = remaining
            remaining = ""
        else:
            msg_part, remaining = _split_message_at_line_boundary(remaining, TELEGRAM_MESSAGE_LIMIT)
            # Add part indicator for multi-part messages
            if remaining:
                msg_part = f"(Part {part_num})\n\n{msg_part}" if part_num > 1 else msg_part
                part_num += 1
        
        payload = {"chat_id": _get_chat_id(), "text": msg_part, "parse_mode": "HTML"}
        try:
            _post_with_retries("sendMessage", payload)
        except Exception as ex:
            logger.exception("Failed to send Telegram message: %s", ex)
            break  # Don't spam if failing
        
        if remaining:
            time.sleep(max(0.0, TELEGRAM_SEND_MIN_INTERVAL_SEC / 2))


def send_telegram_message_with_keyboard(text: str, reply_markup: dict) -> Optional[int]:
    """
    Send a message with an inline keyboard to the configured chat.
    Returns the Telegram message_id on success, or None on failure.
    """
    try:
        _require_env(require_chat=True)
    except RuntimeError as e:
        logger.error(str(e))
        return None

    payload = {
        "chat_id": _get_chat_id(),
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": reply_markup,
    }
    try:
        data = _post_with_retries("sendMessage", payload)
        return data.get("result", {}).get("message_id")
    except Exception as ex:
        logger.exception("Failed to send Telegram message with keyboard: %s", ex)
        return None
