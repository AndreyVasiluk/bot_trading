import logging
import time
import re
from datetime import time as dtime
from typing import Any, Dict, Optional

import requests
import subprocess
import sys

from .config import TradingConfig
from .ib_client import IBClient
from .scheduler import DailyScheduler


class TelegramNotifier:
    """Simple wrapper for sending messages to Telegram."""

    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id
        if not token or not chat_id:
            logging.warning("Telegram token/chat_id not set. Notifications disabled.")

    def send(self, text: str, keyboard: Optional[Dict[str, Any]] = None) -> None:
        if not self.token or not self.chat_id:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload: Dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
        }
        if keyboard:
            payload["reply_markup"] = keyboard

        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                logging.error("Telegram send failed: %s %s", resp.status_code, resp.text)
        except Exception as exc:
            logging.error("Telegram send exception: %s", exc)


def _default_keyboard(cfg: TradingConfig) -> Dict[str, Any]:
    """Reply keyboard for managing TP/SL/time/positions/config/close."""
    return {
        "keyboard": [
            # TP presets
            [
                {"text": "TP 5"},
                {"text": "TP 10"},
                {"text": "TP 15"},
                {"text": "TP 20"},
                {"text": "TP 25"},
                {"text": "TP 30"},
            ],
            # SL presets
            [
                {"text": "SL 5"},
                {"text": "SL 10"},
                {"text": "SL 15"},
                {"text": "SL 20"},
                {"text": "SL 25"},
                {"text": "SL 30"},
            ],
            # Time presets
            [
                {"text": "TIME 13:00:00"},
                {"text": "TIME 00:00:00"},
            ],
            # Force close
            [
                {"text": "CLOSE ALL"},
            ],
            # Status
            [
                {"text": "/positions"},
                {"text": "/config"},
            ],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _send_message(
    token: str,
    chat_id: str,
    text: str,
    keyboard: Optional[Dict[str, Any]] = None,
) -> None:
    notifier = TelegramNotifier(token, chat_id)
    notifier.send(text, keyboard=keyboard)


def _format_config(cfg: TradingConfig) -> str:
    return (
        f"*Current trading config:*\n"
        f"Symbol: `{cfg.symbol} {cfg.expiry}`\n"
        f"Side: `{cfg.side}` qty=`{cfg.quantity}`\n"
        f"TP offset: `{cfg.take_profit_offset}`\n"
        f"SL offset: `{cfg.stop_loss_offset}`\n"
        f"Entry time (UTC): `{cfg.entry_time_utc}`\n"
        f"Mode: `{cfg.mode}`"
    )


def _handle_tp_command(
    text: str,
    cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """
    Formats:
      - "TP 30" (button)
      - "/settp 30"
    """
    parts = text.split()
    if len(parts) != 2:
        _send_message(
            token,
            chat_id,
            "Usage: `TP 30` or `/settp 30`",
            _default_keyboard(cfg),
        )
        return

    try:
        new_tp = float(parts[1])
    except ValueError:
        _send_message(
            token,
            chat_id,
            "TP must be a number, e.g. `30`",
            _default_keyboard(cfg),
        )
        return

    cfg.take_profit_offset = new_tp
    logging.info("Take profit offset updated via Telegram: %s", new_tp)
    _send_message(
        token,
        chat_id,
        f"✅ Take Profit offset updated to *{new_tp}* points.\n\n{_format_config(cfg)}",
        _default_keyboard(cfg),
    )


def _handle_sl_command(
    text: str,
    cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """
    Formats:
      - "SL 10"
      - "/setsl 10"
    """
    parts = text.split()
    if len(parts) != 2:
        _send_message(
            token,
            chat_id,
            "Usage: `SL 10` or `/setsl 10`",
            _default_keyboard(cfg),
        )
        return

    try:
        new_sl = float(parts[1])
    except ValueError:
        _send_message(
            token,
            chat_id,
            "SL must be a number, e.g. `10`",
            _default_keyboard(cfg),
        )
        return

    cfg.stop_loss_offset = new_sl
    logging.info("Stop loss offset updated via Telegram: %s", new_sl)
    _send_message(
        token,
        chat_id,
        f"✅ Stop Loss offset updated to *{new_sl}* points.\n\n{_format_config(cfg)}",
        _default_keyboard(cfg),
    )


def _handle_time_command(
    text: str,
    cfg: TradingConfig,
    token: str,
    chat_id: str,
    scheduler: DailyScheduler,
) -> None:
    """
    Formats:
      - "TIME 13:00:00"
      - "/settime 13:00:00"
      - plain "13:00:00"
    """
    parts = text.split()

    # Якщо користувач просто надіслав "13:00:00"
    if len(parts) == 1 and re.fullmatch(r"\d{2}:\d{2}:\d{2}", parts[0]):
        time_str = parts[0]
    elif len(parts) == 2:
        time_str = parts[1]
    else:
        _send_message(
            token,
            chat_id,
            "Usage: `TIME HH:MM:SS` або просто `13:00:00`.\n"
            "Приклад: `TIME 13:00:00` або `00:00:00`",
            _default_keyboard(cfg),
        )
        return

    try:
        hh, mm, ss = time_str.split(":")
        new_time = dtime(int(hh), int(mm), int(ss))
    except Exception:
        _send_message(
            token,
            chat_id,
            "❌ Невірний формат часу.\nВикористовуй `HH:MM:SS`, напр.: `13:00:00`.",
            _default_keyboard(cfg),
        )
        return

    cfg.entry_time_utc = new_time
    # 🔹 оновлюємо час у шедулері
    try:
        scheduler.set_time(new_time)
        logging.info("Scheduler time updated via Telegram: %s", new_time)
    except Exception as exc:
        logging.exception("Failed to update scheduler time: %s", exc)

    logging.info("Entry time updated via Telegram: %s", new_time)
    _send_message(
        token,
        chat_id,
        f"✅ Entry time (UTC) updated to *{new_time}*.\n\n{_format_config(cfg)}",
        _default_keyboard(cfg),
    )


def _handle_positions(
    ib_client: IBClient,
    cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    try:
        positions = ib_client.ib.positions()
        if not positions:
            _send_message(
                token,
                chat_id,
                "No open positions.",
                _default_keyboard(cfg),
            )
            return

        lines = ["*Open positions:*"]
        for pos in positions:
            contract = pos.contract
            lines.append(
                f"- `{contract.symbol} {getattr(contract, 'lastTradeDateOrContractMonth', '')}` "
                f"qty=`{pos.position}` avg=`{pos.avgCost}`"
            )

        _send_message(
            token,
            chat_id,
            "\n".join(lines),
            _default_keyboard(cfg),
        )
    except Exception as exc:
        logging.exception("Failed to fetch positions: %s", exc)
        _send_message(
            token,
            chat_id,
            f"❌ Failed to fetch positions: `{exc}`",
            _default_keyboard(cfg),
        )


def _handle_close_all(
    cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """
    Handle CLOSE ALL from Telegram.

    ВАЖЛИВО:
    Не викликаємо ib_client.close_all_positions() з Telegram-потоку,
    бо ib_insync вимагає свій event loop в основному потоці.
    Замість цього запускаємо окремий процес, який:
    - конектиться до IB
    - закриває всі позиції
    - шле нотифікації в Telegram
    """
    logging.info("Telegram requested CLOSE ALL, spawning helper process...")
    _send_message(
        token,
        chat_id,
        "⏳ CLOSE ALL requested. Starting helper process...",
        _default_keyboard(cfg),
    )

    try:
        # Запускаємо: python -m app.close_all
        # WORKDIR у контейнері: /app
        subprocess.Popen(
            [sys.executable, "-m", "app.close_all"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _send_message(
            token,
            chat_id,
            "✅ CLOSE ALL helper started. It will connect to IB and close all positions.",
            _default_keyboard(cfg),
        )
    except Exception as exc:
        logging.error("Failed to spawn CLOSE ALL helper: %s", exc, exc_info=True)
        _send_message(
            token,
            chat_id,
            f"❌ Error starting CLOSE ALL helper: {exc}",
            _default_keyboard(cfg),
        )


def telegram_command_loop(
    token: str,
    chat_id: str,
    trading_cfg: TradingConfig,
    ib_client: IBClient,
    scheduler: DailyScheduler,
) -> None:
    """
    Long-polling loop for handling Telegram commands and button presses.
    Runs in a background thread.
    """
    if not token or not chat_id:
        logging.warning("Telegram token/chat_id not set, command loop disabled.")
        return

    logging.info("Starting Telegram command loop...")
    base_url = f"https://api.telegram.org/bot{token}"
    offset: Optional[int] = None

    # Initial info + keyboard
    _send_message(
        token,
        chat_id,
        "🤖 IBKR bot Telegram control is online.\n"
        "Використовуй кнопки або команди:\n"
        "- `TP 30` або `/settp 30`\n"
        "- `SL 10` або `/setsl 10`\n"
        "- `TIME 13:00:00` / `/settime 13:00:00` або просто `13:00:00`\n"
        "- `/positions` — відкриті позиції\n"
        "- `/config` — поточна конфігурація\n"
        "- `/close` або кнопка *CLOSE ALL* — примусово закрити всі позиції (MKT)",
        _default_keyboard(trading_cfg),
    )

    while True:
        try:
            resp = requests.get(
                f"{base_url}/getUpdates",
                params={"timeout": 30, "offset": offset},
                timeout=35,
            )
            if resp.status_code != 200:
                logging.error("getUpdates failed: %s %s", resp.status_code, resp.text)
                time.sleep(5)
                continue

            data = resp.json()
            if not data.get("ok"):
                logging.error("getUpdates response not ok: %s", data)
                time.sleep(5)
                continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1

                message = update.get("message") or update.get("edited_message")
                if not message:
                    continue

                if str(message.get("chat", {}).get("id")) != str(chat_id):
                    continue

                text = (message.get("text") or "").strip()
                if not text:
                    continue

                logging.info("Telegram message: %s", text)

                # Plain time like "13:00:00"
                if re.fullmatch(r"\d{2}:\d{2}:\d{2}", text):
                    _handle_time_command(text, trading_cfg, token, chat_id, scheduler)

                elif text.upper().startswith("CLOSE") or text.startswith("/close"):
                    _handle_close_all(trading_cfg, token, chat_id)

                elif text.startswith("/settp") or text.startswith("TP "):
                    _handle_tp_command(text, trading_cfg, token, chat_id)

                elif text.startswith("/setsl") or text.startswith("SL "):
                    _handle_sl_command(text, trading_cfg, token, chat_id)

                elif text.startswith("/settime") or text.startswith("TIME "):
                    _handle_time_command(text, trading_cfg, token, chat_id, scheduler)

                elif text.startswith("/positions"):
                    _handle_positions(ib_client, trading_cfg, token, chat_id)

                elif text.startswith("/config"):
                    _send_message(
                        token,
                        chat_id,
                        _format_config(trading_cfg),
                        _default_keyboard(trading_cfg),
                    )

                else:
                    _send_message(
                        token,
                        chat_id,
                        "Unknown command.\n"
                        "Використовуй кнопки, час у форматі `HH:MM:SS`, `/config` або `/close`.",
                        _default_keyboard(trading_cfg),
                    )

        except Exception as exc:
            logging.exception("Telegram command loop error: %s", exc)
            time.sleep(5)