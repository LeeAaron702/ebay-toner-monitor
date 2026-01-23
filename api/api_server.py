# api_server.py
"""
Entrypoint for FastAPI + python-telegram-bot (PTB) in webhook mode.

- Starts your monitor in a background subprocess
- Builds, initializes, and starts the PTB Application
- Sets Telegram command list from a single source of truth (telegram_utils.COMMANDS)
- Receives Telegram webhooks at /telegram/webhook and enqueues Updates
- Cleanly stops PTB on shutdown
- Serves Admin Panel at /admin/*
"""

from __future__ import annotations

import os
import sys
import asyncio
import logging
import threading
import subprocess
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from telegram import Update
from telegram.ext import ApplicationBuilder

from api.routers.exclude_api import router as exclude_router
from api.routers.products_api import router as products_router
from api.routers.admin import router as admin_router
import utils.telegram_service as telegram_service

# ---- Env ----
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "")  # must include /telegram/webhook
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
EXCLUDE_API_KEY = os.getenv("EXCLUDE_API_KEY", "")

# ---- Logging ----
logger = logging.getLogger("api_server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

# ---- FastAPI app ----
app = FastAPI()
ptb_app = None  # type: ignore

# ---- API key dependency for /exclude/* ----
def require_api_key(x_api_key: str = Header(None)):
    if EXCLUDE_API_KEY and x_api_key != EXCLUDE_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")

# Mount /exclude with optional API key protection
if EXCLUDE_API_KEY:
    app.include_router(exclude_router, prefix="/exclude", tags=["Exclude"], dependencies=[Depends(require_api_key)])
else:
    app.include_router(exclude_router, prefix="/exclude", tags=["Exclude"])

# Mount /api/v1/products (no API key required for now)
app.include_router(products_router, prefix="/api/v1/products", tags=["Products"])

# Mount /admin for the Admin Panel
app.include_router(admin_router, prefix="/admin", tags=["Admin"])


# ---- Redirect root to admin ----
@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/admin/")


# ---- Launch monitor() from main.py as a subprocess ----
def run_monitor_subprocess():
    # inherit stdio so child logs show in `docker compose logs`
    subprocess.Popen(
        [sys.executable, "-u", "main.py"],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

@app.on_event("startup")
async def on_startup():
    global ptb_app

    # Start background monitor
    threading.Thread(target=run_monitor_subprocess, daemon=True).start()
    logger.info("Started monitor subprocess")

    # Sanity check Telegram env
    missing = [k for k, v in {
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_WEBHOOK_URL": TELEGRAM_WEBHOOK_URL,
        "TELEGRAM_WEBHOOK_SECRET": TELEGRAM_WEBHOOK_SECRET,
    }.items() if not v]
    if missing:
        logger.error("Missing required env: %s", ", ".join(missing))
        raise RuntimeError(f"Missing required env: {', '.join(missing)}")

    # Build PTB app and register handlers
    ptb_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    telegram_service.register_handlers(ptb_app)

    # Initialize & start PTB (required in manual webhook mode)
    await ptb_app.initialize()
    await ptb_app.start()

    # Commands and webhook
    await ptb_app.bot.set_my_commands(telegram_service.get_bot_commands())
    await ptb_app.bot.set_webhook(
        url=TELEGRAM_WEBHOOK_URL,              # e.g. https://your.host/telegram/webhook
        secret_token=TELEGRAM_WEBHOOK_SECRET,  # Telegram will echo this in the header
        drop_pending_updates=True,
    )
    logger.info("Telegram webhook set")

@app.on_event("shutdown")
async def on_shutdown():
    global ptb_app
    if ptb_app:
        try:
            await ptb_app.stop()
            await ptb_app.shutdown()
            logger.info("PTB app stopped & shutdown")
        except Exception:
            logger.exception("Error during PTB shutdown")

# ---- Telegram webhook endpoint ----
@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str = Header(None),
):
    if x_telegram_bot_api_secret_token != TELEGRAM_WEBHOOK_SECRET:
        logger.warning("Webhook secret header mismatch")
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        data = await request.json()
        update = Update.de_json(data, ptb_app.bot)  # type: ignore
    except Exception as e:
        logger.warning("Invalid JSON in webhook: %s", e)
        raise HTTPException(status_code=400, detail="Invalid JSON")

    try:
        await ptb_app.update_queue.put(update)  # type: ignore
        logger.info("Update enqueued: %s", update.update_id)
    except Exception as e:
        logger.error("PTB processing failure: %s", e)
        raise HTTPException(status_code=500, detail="PTB processing failure")

    return {"ok": True}

@app.get("/healthz")
def healthz():
    return {"ok": True}
