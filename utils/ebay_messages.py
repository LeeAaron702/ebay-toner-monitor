# utils/ebay_messages.py
"""
eBay Trading API message polling and reply functionality.

Polls GetMyMessages for new inbox messages and sends replies
via AddMemberMessageRTQ. Uses the same OAuth token infrastructure
as order_history/ebay_order_history.py.

Env:
  EBAY_MESSAGE_ACCOUNT  (default "business") — which token file to use
"""

from __future__ import annotations

import os
import re
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from html import unescape
from html.parser import HTMLParser
from io import StringIO
from pathlib import Path
from typing import List, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── eBay config (shared with order_history) ──────────────────────────────────

CLIENT_ID = os.getenv("EBAY_APP_ID", "")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "")
DEV_ID = os.getenv("DEV_ID", "")
CERT_ID = os.getenv("CERT_ID", CLIENT_SECRET)

EBAY_TRADING_ENDPOINT = "https://api.ebay.com/ws/api.dll"
EBAY_NAMESPACE = {"e": "urn:ebay:apis:eBLBaseComponents"}

# Which account's token file to use for messages
EBAY_MESSAGE_ACCOUNT = os.getenv("EBAY_MESSAGE_ACCOUNT", "business")
TOKEN_FILE = Path(f"ebay_tokens_{EBAY_MESSAGE_ACCOUNT}.json")

SCOPES = [
    "https://api.ebay.com/oauth/api_scope",
    "https://api.ebay.com/oauth/api_scope/sell.account",
]


def _text(parent: ET.Element, path: str) -> str:
    el = parent.find(path, EBAY_NAMESPACE)
    return el.text.strip() if el is not None and el.text else ""


def _extract_message_body(html: str) -> str:
    """
    Extract the full user-typed message from eBay's HTML email wrapper.

    eBay puts the actual message text inside <div id="UserInputtedText">.
    That's the authoritative source — not the preview "New message:" summary
    which is truncated with [...].
    """
    # Primary: extract from <div id="UserInputtedText">...</div>
    match = re.search(
        r'<div[^>]+id=["\']?UserInputtedText["\']?[^>]*>([\s\S]*?)</div>',
        html,
        re.IGNORECASE,
    )
    if match:
        inner = match.group(1)
        # Convert <br> to newline, strip remaining tags
        inner = re.sub(r"<br\s*/?>", "\n", inner, flags=re.IGNORECASE)
        inner = re.sub(r"<[^>]+>", "", inner)
        text = unescape(inner).strip()
        if text:
            return text

    # Fallback: strip the whole HTML but skip the truncated preview section
    # by looking for text after the "New message:" summary block ends
    clean = re.sub(r"<style[^>]*>[\s\S]*?</style>", "", html, flags=re.IGNORECASE)
    clean = re.sub(r"<script[^>]*>[\s\S]*?</script>", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"<!--[\s\S]*?-->", "", clean)
    clean = re.sub(r"<br\s*/?>", "\n", clean, flags=re.IGNORECASE)
    clean = re.sub(r"<[^>]+>", " ", clean)
    clean = unescape(clean)
    lines = [re.sub(r"[ \t]+", " ", l).strip() for l in clean.splitlines()]
    lines = [l for l in lines if l and "{" not in l and "}" not in l
             and "!important" not in l
             and not re.match(r"[\w-]+\s*:\s*\d+px", l)]
    return "\n".join(lines)


# ── Token management (delegates to order_history module) ─────────────────────

def _get_access_token() -> Optional[str]:
    """Get a fresh access token for the configured account."""
    from order_history.ebay_order_history import get_access_token
    return get_access_token(SCOPES, TOKEN_FILE)


def _trading_headers(call_name: str, access_token: str) -> dict:
    return {
        "X-EBAY-API-CALL-NAME": call_name,
        "X-EBAY-API-COMPATIBILITY-LEVEL": "1423",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-APP-NAME": CLIENT_ID,
        "X-EBAY-API-DEV-NAME": DEV_ID,
        "X-EBAY-API-CERT-NAME": CERT_ID,
        "X-EBAY-API-IAF-TOKEN": access_token,
        "Content-Type": "text/xml",
    }


# ── GetMyMessages ────────────────────────────────────────────────────────────
# Two-step process: first get message headers/IDs, then fetch full bodies.

def _build_getmymessages_headers_xml(start_time: str, end_time: str, page: int) -> str:
    """Step 1: Get message headers (IDs, sender, subject, etc.) without body text."""
    root = ET.Element("GetMyMessagesRequest", xmlns=EBAY_NAMESPACE["e"])
    ET.SubElement(root, "DetailLevel").text = "ReturnHeaders"

    ET.SubElement(root, "FolderID").text = "0"  # Inbox
    ET.SubElement(root, "StartCreationTime").text = start_time
    ET.SubElement(root, "EndCreationTime").text = end_time

    pagination = ET.SubElement(root, "Pagination")
    ET.SubElement(pagination, "EntriesPerPage").text = "25"
    ET.SubElement(pagination, "PageNumber").text = str(page)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def _build_getmymessages_detail_xml(message_ids: List[str]) -> str:
    """Step 2: Get full message bodies for specific message IDs."""
    root = ET.Element("GetMyMessagesRequest", xmlns=EBAY_NAMESPACE["e"])
    ET.SubElement(root, "DetailLevel").text = "ReturnMessages"

    ids_el = ET.SubElement(root, "MessageIDs")
    for mid in message_ids:
        ET.SubElement(ids_el, "MessageID").text = mid

    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def _parse_header(msg_el: ET.Element) -> Dict:
    """Parse a message header (no body text)."""
    return {
        "ebay_message_id": _text(msg_el, "e:MessageID"),
        "external_message_id": _text(msg_el, "e:ExternalMessageID"),
        "sender": _text(msg_el, "e:Sender"),
        "sender_id": _text(msg_el, "e:SendingUserID"),
        "subject": _text(msg_el, "e:Subject"),
        "item_id": _text(msg_el, "e:ItemID"),
        "item_title": _text(msg_el, "e:ItemTitle"),
        "creation_date": _text(msg_el, "e:ReceiveDate") or _text(msg_el, "e:CreationDate"),
        "is_read": _text(msg_el, "e:Read") == "true",
        "message_type": _text(msg_el, "e:MessageType"),
    }


def _parse_message(msg_el: ET.Element) -> Dict:
    """Parse a full message element including body text."""
    body_raw = _text(msg_el, "e:Text")
    # SendingUserID is "-99" for M2M messages; use RecipientUserID to know our account
    sender_id = _text(msg_el, "e:SendingUserID")
    if sender_id == "-99":
        sender_id = _text(msg_el, "e:Sender")
    return {
        "ebay_message_id": _text(msg_el, "e:MessageID"),
        "external_message_id": _text(msg_el, "e:ExternalMessageID"),
        "sender": _text(msg_el, "e:Sender"),
        "sender_id": sender_id,
        "subject": _text(msg_el, "e:Subject"),
        "body": _extract_message_body(body_raw) if body_raw else "",
        "item_id": _text(msg_el, "e:ItemID"),
        "item_title": _text(msg_el, "e:ItemTitle"),
        "creation_date": _text(msg_el, "e:ReceiveDate") or _text(msg_el, "e:CreationDate"),
        "is_read": _text(msg_el, "e:Read") == "true",
        "message_type": _text(msg_el, "e:MessageType"),
    }


def _call_trading_api(access_token: str, xml_body: str) -> Optional[ET.Element]:
    """Make a GetMyMessages call and return the parsed XML root, or None on error."""
    headers = _trading_headers("GetMyMessages", access_token)
    resp = requests.post(EBAY_TRADING_ENDPOINT, headers=headers, data=xml_body, timeout=30)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ack = _text(root, "e:Ack")

    if ack not in ("Success", "Warning"):
        errors = root.findall("e:Errors", EBAY_NAMESPACE)
        err_msgs = [f"[{_text(e, 'e:ErrorCode')}] {_text(e, 'e:ShortMessage')}" for e in errors]
        logger.error("GetMyMessages failed: %s", " | ".join(err_msgs))
        return None

    return root


def fetch_messages(access_token: str, since_hours: int = 2) -> List[Dict]:
    """
    Fetch inbox messages from the last `since_hours` hours.
    Two-step: get headers first (for IDs), then fetch full bodies.
    Returns list of parsed message dicts.
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=since_hours)
    start_str = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Step 1: Get message headers (paginated)
    all_headers: List[Dict] = []
    page = 1
    more = True

    while more:
        xml_body = _build_getmymessages_headers_xml(start_str, end_str, page)
        root = _call_trading_api(access_token, xml_body)
        if root is None:
            break

        msg_array = root.find("e:Messages", EBAY_NAMESPACE)
        if msg_array is not None:
            for msg_el in msg_array.findall("e:Message", EBAY_NAMESPACE):
                all_headers.append(_parse_header(msg_el))

        has_more = root.find("e:HasMoreItems", EBAY_NAMESPACE)
        more = has_more is not None and has_more.text == "true"
        page += 1

    if not all_headers:
        return []

    logger.info("Found %d message headers, fetching bodies...", len(all_headers))

    # Build lookup from headers so we can merge fields the detail response omits
    header_by_id = {h["ebay_message_id"]: h for h in all_headers if h["ebay_message_id"]}

    # Step 2: Fetch full bodies in batches of 10 (API limit)
    messages: List[Dict] = []
    message_ids = list(header_by_id.keys())

    for i in range(0, len(message_ids), 10):
        batch = message_ids[i:i + 10]
        xml_body = _build_getmymessages_detail_xml(batch)
        root = _call_trading_api(access_token, xml_body)
        if root is None:
            continue

        msg_array = root.find("e:Messages", EBAY_NAMESPACE)
        if msg_array is not None:
            for msg_el in msg_array.findall("e:Message", EBAY_NAMESPACE):
                msg = _parse_message(msg_el)
                # Merge header fields that the detail response may omit
                header = header_by_id.get(msg["ebay_message_id"], {})
                for key in ("creation_date", "item_title", "message_type"):
                    if not msg.get(key) and header.get(key):
                        msg[key] = header[key]
                messages.append(msg)

    return messages


# ── AddMemberMessageRTQ (Reply) ─────────────────────────────────────────────

def _build_reply_xml(item_id: str, recipient_id: str, body: str, parent_message_id: str) -> str:
    """Build XML for AddMemberMessageRTQ to reply to a buyer/seller message."""
    root = ET.Element("AddMemberMessageRTQRequest", xmlns=EBAY_NAMESPACE["e"])

    ET.SubElement(root, "ItemID").text = item_id

    member_msg = ET.SubElement(root, "MemberMessage")
    ET.SubElement(member_msg, "Body").text = body
    ET.SubElement(member_msg, "ParentMessageID").text = parent_message_id
    ET.SubElement(member_msg, "RecipientID").text = recipient_id

    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def send_reply(item_id: str, recipient_id: str, body: str, parent_message_id: str) -> bool:
    """
    Send a reply to an eBay message via AddMemberMessageRTQ.
    Gets a fresh token each time (handles expiry).
    Returns True on success.
    """
    access_token = _get_access_token()
    if not access_token:
        logger.error("Cannot send reply — no access token available")
        return False

    xml_body = _build_reply_xml(item_id, recipient_id, body, parent_message_id)
    headers = _trading_headers("AddMemberMessageRTQ", access_token)

    try:
        resp = requests.post(EBAY_TRADING_ENDPOINT, headers=headers, data=xml_body, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        ack = _text(root, "e:Ack")

        if ack in ("Success", "Warning"):
            logger.info("Reply sent to %s for item %s", recipient_id, item_id)
            return True

        errors = root.findall("e:Errors", EBAY_NAMESPACE)
        err_msgs = [f"[{_text(e, 'e:ErrorCode')}] {_text(e, 'e:LongMessage')}" for e in errors]
        logger.error("AddMemberMessageRTQ failed: %s", " | ".join(err_msgs))
        return False

    except Exception as e:
        logger.exception("Error sending eBay reply: %s", e)
        return False


# ── Poll orchestrator ────────────────────────────────────────────────────────

EBAY_MESSAGE_ACCOUNTS = [a.strip() for a in os.getenv("EBAY_MESSAGE_ACCOUNTS", "business,personal").split(",") if a.strip()]


def _get_access_token_for_account(account: str) -> Optional[str]:
    """Get a fresh access token for a specific account."""
    from order_history.ebay_order_history import get_access_token
    token_file = Path(f"ebay_tokens_{account}.json")
    return get_access_token(SCOPES, token_file)


def poll_new_messages() -> List[Dict]:
    """
    High-level: fetch messages from all configured eBay accounts,
    filter to only new/unseen ones, store in DB, and return the
    new messages for Telegram notification.
    """
    from db.listings_db import (
        get_ebay_message_by_ebay_id,
        insert_ebay_message,
    )

    new_messages = []

    for account in EBAY_MESSAGE_ACCOUNTS:
        access_token = _get_access_token_for_account(account)
        if not access_token:
            logger.error("Cannot poll messages — no access token for %s", account)
            continue

        # Fetch last 2 hours of messages (overlap to avoid gaps between hourly polls)
        all_messages = fetch_messages(access_token, since_hours=2)
        logger.info("Fetched %d messages from eBay inbox [%s]", len(all_messages), account)

        for msg in all_messages:
            # Skip system notifications — only want member-to-member messages
            if msg["message_type"] not in ("AskSellerQuestion", "ResponseToASQQuestion", "ContactEbayMember"):
                continue

            # Deduplicate against DB
            existing = get_ebay_message_by_ebay_id(msg["ebay_message_id"])
            if existing:
                continue

            # Store in DB
            row_id = insert_ebay_message(
                account_id=account,
                ebay_message_id=msg["ebay_message_id"],
                external_message_id=msg.get("external_message_id", ""),
                sender=msg["sender"],
                sender_id=msg.get("sender_id", ""),
                subject=msg["subject"],
                body=msg["body"],
                item_id=msg["item_id"],
                item_title=msg.get("item_title", ""),
                creation_date=msg["creation_date"],
            )
            msg["db_row_id"] = row_id
            msg["account_id"] = account
            new_messages.append(msg)

    logger.info("Found %d new messages across %d accounts", len(new_messages), len(EBAY_MESSAGE_ACCOUNTS))
    return new_messages


def format_message_for_telegram(msg: Dict) -> str:
    """Format an eBay message for Telegram — listing title, sender, timestamp, message."""
    sender = msg.get("sender") or msg.get("sender_id", "Unknown")
    body = msg.get("body", "").strip()
    item_id = msg.get("item_id", "")
    # Use ItemTitle from API (clean), fall back to parsing subject
    item_title = msg.get("item_title", "")
    if not item_title:
        subj = msg.get("subject", "")
        item_title = re.sub(r"^Re:\s*", "", subj, flags=re.IGNORECASE)
        item_title = re.sub(r"^\S+\s+sent a message about\s+", "", item_title, flags=re.IGNORECASE)
        item_title = re.sub(r"\s*#\d{9,15}\s*$", "", item_title)

    # Format creation date in PST for readability
    creation_date = msg.get("creation_date", "")
    time_str = ""
    if creation_date:
        try:
            from zoneinfo import ZoneInfo
            dt = datetime.fromisoformat(creation_date.replace("Z", "+00:00"))
            pst = dt.astimezone(ZoneInfo("America/Los_Angeles"))
            time_str = pst.strftime("%b %d, %I:%M %p PST")
        except Exception:
            time_str = creation_date

    account_id = msg.get("account_id") or "unknown"
    lines = [f"📬 <b>eBay Message [{account_id}]</b>", f"<b>{item_title}</b>"]

    if item_id:
        lines.append(f"<a href=\"https://www.ebay.com/itm/{item_id}\">View listing</a>")

    if time_str:
        lines.append(f"🕐 {time_str}")

    lines.append("")
    lines.append(f"<b>{sender}:</b> {body}")

    return "\n".join(lines)
