"""Shared helpers for handlers."""

from __future__ import annotations

from telegram import Update

from ..config import settings


def is_owner(update: Update) -> bool:
    chat = update.effective_chat
    return chat is not None and chat.id == settings.CHAT_ID


def escape_md(text: str) -> str:
    """Escape Telegram Markdown v1 special characters in user-provided text."""
    out = text
    for ch in ("*", "_", "`", "["):
        out = out.replace(ch, f"\\{ch}")
    return out
