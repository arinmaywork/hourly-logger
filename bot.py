"""Hourly Logger — entrypoint.

This module is intentionally tiny. All real logic lives in the
:mod:`hourly_logger` package (config, database, sheets, state,
background, handlers, scheduler). See README.md for an overview.

Run with::

    python bot.py

It can also be run as ``python -m hourly_logger`` (see
``hourly_logger/__main__.py``).
"""

from __future__ import annotations

import asyncio

from telegram import Update
from telegram.error import NetworkError, TelegramError
from telegram.ext import Application

from hourly_logger import background
from hourly_logger.config import settings
from hourly_logger.database import backfill_missed_prompts_sync, db_init
from hourly_logger.handlers import register_handlers
from hourly_logger.logger import configure_logging, get_logger
from hourly_logger.scheduler import start_scheduler, stop_scheduler


configure_logging()
log = get_logger(__name__)


async def _bg_failure_notifier(task_name: str, exc: BaseException) -> None:
    """Bug #3: surface fire-and-forget failures to the owner via Telegram."""
    # Build a one-shot bot client just for the notification — using the same
    # token but no Application machinery, so this works even from inside the
    # tracker's done-callback.
    from telegram import Bot
    try:
        bot = Bot(settings.TELEGRAM_TOKEN)
        await bot.send_message(
            chat_id=settings.CHAT_ID,
            text=(
                f"⚠️ Background task `{task_name}` failed.\n"
                f"Error: `{type(exc).__name__}: {exc}`\n"
                "Run /sync to retry the affected entries."
            ),
            parse_mode="Markdown",
        )
    except (NetworkError, TelegramError) as e:
        log.error("failed to notify owner of bg failure", extra={"err": str(e)})


def main() -> None:
    db_init()
    inserted = backfill_missed_prompts_sync()
    if inserted:
        log.info("startup backfill done", extra={"inserted": inserted})

    background.set_notifier(_bg_failure_notifier)

    app: Application = Application.builder().token(settings.TELEGRAM_TOKEN).build()
    register_handlers(app)

    async def on_startup(application: Application) -> None:
        start_scheduler(application.bot)

    async def on_shutdown(application: Application) -> None:
        stop_scheduler()
        await background.shutdown()

    app.post_init = on_startup
    app.post_stop = on_shutdown

    log.info("bot starting", extra={"tz": settings.TIMEZONE})
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
