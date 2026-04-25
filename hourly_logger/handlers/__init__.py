"""Telegram handler registration.

Single :func:`register_handlers` entry point so the bot's main module
stays free of import-by-import wiring noise.
"""

from __future__ import annotations

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
)

from . import commands, edit, flow, log, maintenance, reports


def register_handlers(app: Application) -> None:
    """Wire all command + message handlers onto the Application."""
    app.add_handler(CommandHandler("start",    commands.cmd_start))
    app.add_handler(CommandHandler("help",     commands.cmd_help))
    app.add_handler(CommandHandler("log",      log.cmd_log))
    app.add_handler(CommandHandler("skip",     commands.cmd_skip))
    app.add_handler(CommandHandler("skipall",  commands.cmd_skipall))
    app.add_handler(CommandHandler("cancel",   commands.cmd_cancel))
    app.add_handler(CommandHandler("status",   reports.cmd_status))
    app.add_handler(CommandHandler("monthly",  reports.cmd_monthly))
    app.add_handler(CommandHandler("weekly",   reports.cmd_weekly))
    app.add_handler(CommandHandler("trend",    reports.cmd_trend))
    app.add_handler(CommandHandler("edit",     edit.cmd_edit))
    app.add_handler(CommandHandler("missing",  maintenance.cmd_missing))
    app.add_handler(CommandHandler("sync",     maintenance.cmd_sync))
    app.add_handler(CommandHandler("repair",   maintenance.cmd_repair))
    app.add_handler(CommandHandler("fixcats",  maintenance.cmd_fixcats))
    app.add_handler(CommandHandler("uncat",    maintenance.cmd_uncat))
    app.add_handler(CommandHandler("dedup",    maintenance.cmd_dedup))
    app.add_handler(CommandHandler("auditlog", maintenance.cmd_auditlog))
    # /migrate retained as a function but intentionally not registered.
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, flow.handle_message))
