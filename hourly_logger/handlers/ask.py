"""/ask — natural-language questions over the hourly log + journal."""

from __future__ import annotations

import asyncio

from telegram import Update
from telegram.ext import ContextTypes

from ..ai.ask import answer
from ..ai.llm import LlmError
from ..config import settings
from ..logger import get_logger
from ._common import is_owner

log = get_logger(__name__)

_USAGE = (
    "Usage: /ask <question>\n"
    "e.g. /ask what do my good days have in common?\n"
    "     /ask which planned activity do I skip most?"
)


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update):
        return
    question = " ".join(context.args or []).strip()
    if not question:
        await update.message.reply_text(_USAGE)
        return
    if not settings.GEMINI_API_KEY:
        await update.message.reply_text(
            "⚠️ GEMINI_API_KEY is not set — add it to .env to enable /ask."
        )
        return

    placeholder = await update.message.reply_text("🤔 Analysing your data…")
    try:
        text = await asyncio.to_thread(answer, question)
    except LlmError as e:
        log.warning("/ask failed", extra={"err": str(e)})
        text = f"⚠️ AI backend error — try again later.\n({e})"
    except Exception as e:  # noqa: BLE001 — never leave the placeholder hanging
        log.exception("/ask crashed")
        text = f"⚠️ Unexpected error: {e}"
    await placeholder.edit_text(text[:4000])
