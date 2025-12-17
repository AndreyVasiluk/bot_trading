import logging
import time
import re
from datetime import time as dtime
from typing import Any, Dict, Optional, List, Tuple

import requests
import threading  # ← додаємо для фонового worker-а

from .config import TradingConfig
from .ib_client import IBClient
from .scheduler import DailyScheduler

# Global flag to prevent multiple CLOSE ALL workers running in parallel
_close_all_running = False
_close_all_started_at: Optional[float] = None

# Seconds after which we consider CLOSE ALL "stuck" and allow new run
_CLOSE_ALL_TIMEOUT = 60


class TelegramNotifier:
    """Simple wrapper for sending messages to a single Telegram chat."""

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
                logging.error(
                    "Telegram send failed: %s %s",
                    resp.status_code,
                    resp.text,
                )
        except Exception as exc:
            logging.error("Telegram send exception: %s", exc)


class BroadcastNotifier:
    """
    Wrapper для відправки одного й того ж повідомлення
    у декілька Telegram-чатів одночасно (кілька ботів / чатів).
    """

    def __init__(self, targets: List[Tuple[str, str]]) -> None:
        """
        targets: список (token, chat_id).
        Порожні значення (""/"0"/None) ігноруються.
        """
        self._notifiers: List[TelegramNotifier] = []
        for token, chat_id in targets:
            token = token or ""
            chat_id = chat_id or ""
            if token.strip() and chat_id.strip():
                self._notifiers.append(TelegramNotifier(token, chat_id))

        if not self._notifiers:
            logging.warning(
                "BroadcastNotifier created with no valid Telegram targets. "
                "All notifications will be dropped."
            )

    def send(
        self,
        text: str,
        keyboard: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Надсилає повідомлення в усі валідні чати.
        Помилка одного не блокує інші.
        """
        for notifier in self._notifiers:
            try:
                notifier.send(text, keyboard=keyboard)
            except Exception as exc:
                logging.error("BroadcastNotifier send error: %s", exc)


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
            # Long/Short buttons
            [
                {"text": "LONG"},
                {"text": "SHORT"},
            ],
            # Quantity buttons
            [
                {"text": "QTY 1"},
                {"text": "QTY 2"},
                {"text": "QTY 3"},
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
    """
    Службова функція для відповіді в ОДИН Telegram-чат
    (використовується в командному лупі).
    """
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
    """
    Показати відкриті позиції, максимально синхронно з тим,
    що видно в TWS (через get_positions_from_broker() - напрямую с брокера).
    Також скидаємо флаг CLOSE ALL, якщо позицій вже немає.
    """
    global _close_all_running, _close_all_started_at

    try:
        logging.info("_handle_positions: starting")
        if not ib_client.ib.isConnected():
            logging.warning("_handle_positions: IB not connected")
            _send_message(
                token,
                chat_id,
                "⚠️ IB не підключений, не можу отримати позиції.\n"
                "Перевірте, будь ласка, TWS / IB Gateway.",
                _default_keyboard(cfg),
            )
            return

        # Явно оновлюємо позиції з брокера (напрямую через API)
        logging.info("_handle_positions: calling get_positions_from_broker()")
        positions = ib_client.get_positions_from_broker()
        logging.info("_handle_positions: got %d positions from broker", len(positions))

        # Фильтруем только позиции с ненулевым количеством
        open_positions = [pos for pos in positions if abs(float(pos.position)) > 0.001]
        logging.info("_handle_positions: %d open positions (non-zero qty)", len(open_positions))

        # якщо позицій немає — вважаємо, що CLOSE ALL завершився
        if not open_positions:
            if _close_all_running:
                logging.info("No open positions, resetting CLOSE ALL flag.")
                _close_all_running = False
                _close_all_started_at = None

            logging.info("_handle_positions: sending 'No open positions' message")
            _send_message(
                token,
                chat_id,
                "No open positions.",
                _default_keyboard(cfg),
            )
            return

        lines = ["*Open positions:*"]
        for pos in open_positions:
            contract = pos.contract
            symbol = getattr(contract, "localSymbol", "") or getattr(
                contract,
                "symbol",
                "",
            )
            expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
            lines.append(
                f"- `{symbol} {expiry}` "
                f"qty=`{pos.position}` avg=`{pos.avgCost}`"
            )

        message_text = "\n".join(lines)
        logging.info("_handle_positions: sending positions message: %s", message_text)
        _send_message(
            token,
            chat_id,
            message_text,
            _default_keyboard(cfg),
        )
        logging.info("_handle_positions: completed successfully")
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
    ib_client: IBClient,
) -> None:
    """
    Handle CLOSE ALL from Telegram.
    Використовує поточний ib_client (той самий конект, що і стратегія),
    без окремого процесу/модуля app.close_all.
    """
    global _close_all_running, _close_all_started_at

    now = time.time()

    if _close_all_running:
        # перевіряємо, чи не "застряг" worker
        if _close_all_started_at and now - _close_all_started_at > _CLOSE_ALL_TIMEOUT:
            logging.warning(
                "CLOSE ALL flag has been set for >%s seconds, resetting.",
                _CLOSE_ALL_TIMEOUT,
            )
            _close_all_running = False
            _close_all_started_at = None
        else:
            logging.info("CLOSE ALL already running, ignoring duplicate request.")
            _send_message(
                token,
                chat_id,
                "⏳ CLOSE ALL уже виконується. Дочекайся завершення, потім можеш "
                "перевірити `/positions`.",
                _default_keyboard(cfg),
            )
            return

    # тут ми точно можемо стартувати новий worker
    _close_all_running = True
    _close_all_started_at = now

    logging.info("Telegram requested CLOSE ALL, starting background worker thread...")
    _send_message(
        token,
        chat_id,
        "⏳ CLOSE ALL requested. Starting worker to close all positions...",
        _default_keyboard(cfg),
    )

    def _worker():
        global _close_all_running, _close_all_started_at
        try:
            # Переконатись, що є конект
            if not ib_client.ib.isConnected():
                logging.warning("IB not connected in CLOSE ALL worker, trying reconnect...")
                try:
                    ib_client.connect()
                except Exception as exc:
                    logging.exception("Reconnect failed in CLOSE ALL worker: %s", exc)
                    _send_message(
                        token,
                        chat_id,
                        f"❌ CLOSE ALL failed: cannot connect to IB: `{exc}`",
                        _default_keyboard(cfg),
                    )
                    return

            logging.info("Calling ib_client.close_all_positions() from CLOSE ALL worker...")
            ib_client.close_all_positions()
            logging.info("close_all_positions() finished in worker.")

            _send_message(
                token,
                chat_id,
                "✅ CLOSE ALL finished. Check `/positions` to confirm all is flat.",
                _default_keyboard(cfg),
            )
        except Exception as exc:
            logging.exception("CLOSE ALL worker error: %s", exc)
            _send_message(
                token,
                chat_id,
                f"❌ CLOSE ALL worker error: `{exc}`",
                _default_keyboard(cfg),
            )
        finally:
            _close_all_running = False
            _close_all_started_at = None

    threading.Thread(target=_worker, daemon=True).start()


def _handle_quantity_command(
    text: str,
    trading_cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """Handle QTY command - update config quantity."""
    # Extract number from "QTY 2" or "QTY2" or "/setqty 2"
    parts = text.upper().split()
    if len(parts) < 2:
        _send_message(
            token,
            chat_id,
            "❌ Invalid format. Use: QTY 2 or /setqty 2",
            _default_keyboard(trading_cfg),
        )
        return

    try:
        new_qty = int(parts[1])
        if new_qty < 1:
            _send_message(
                token,
                chat_id,
                "❌ Quantity must be >= 1",
                _default_keyboard(trading_cfg),
            )
            return
    except ValueError:
        _send_message(
            token,
            chat_id,
            f"❌ Invalid quantity: {parts[1]}. Must be a number.",
            _default_keyboard(trading_cfg),
        )
        return

    old_qty = trading_cfg.quantity
    trading_cfg.set_quantity(new_qty)
    logging.info("Quantity updated via Telegram: %s -> %s", old_qty, new_qty)

    _send_message(
        token,
        chat_id,
        f"✅ Quantity updated: {old_qty} → {new_qty}\n"
        f"New config: side={trading_cfg.side}, qty={trading_cfg.quantity}",
        _default_keyboard(trading_cfg),
    )


def _handle_side_command(
    text: str,
    trading_cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """Handle LONG or SHORT command - update config side."""
    new_side = text.upper()
    
    if new_side not in ["LONG", "SHORT"]:
        _send_message(
            token,
            chat_id,
            f"❌ Invalid side: {new_side}. Use LONG or SHORT.",
            _default_keyboard(trading_cfg),
        )
        return

    old_side = trading_cfg.side
    trading_cfg.set_side(new_side)
    logging.info("Side updated via Telegram: %s -> %s", old_side, new_side)

    _send_message(
        token,
        chat_id,
        f"✅ Side updated: {old_side} → {new_side}\n"
        f"New config: side={trading_cfg.side}, qty={trading_cfg.quantity}",
        _default_keyboard(trading_cfg),
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
        "- `LONG` / `SHORT` — змінити сторону входу\n"
        "- `QTY 2` або `/setqty 2` — змінити кількість контрактів\n"
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
            # Формируем params правильно (offset может быть None в начале)
            params = {"timeout": 30}
            if offset is not None:
                params["offset"] = offset
                
            resp = requests.get(
                f"{base_url}/getUpdates",
                params=params,
                timeout=35,
            )
            if resp.status_code != 200:
                logging.error(
                    "getUpdates failed: %s %s",
                    resp.status_code,
                    resp.text,
                )
                # При ошибке 409 (Conflict) ждем дольше - другой бот работает
                wait_time = 10 if resp.status_code == 409 else 5
                time.sleep(wait_time)
                continue

            data = resp.json()
            if not data.get("ok"):
                error_code = data.get("error_code")
                error_desc = data.get("description", "")
                logging.error("getUpdates response not ok: %s - %s", error_code, error_desc)
                
                # При 409 ждем дольше, но не сбрасываем offset
                if error_code == 409:
                    logging.warning("Another bot instance is running (409 Conflict). Waiting 10 seconds...")
                    time.sleep(10)
                else:
                    time.sleep(5)
                continue

            updates = data.get("result", [])
            if not updates:
                # Нет новых сообщений - продолжаем polling
                continue
                
            logging.info("Received %d updates from Telegram", len(updates))
            
            for update in updates:
                update_id = update["update_id"]
                # Обновляем offset сразу, чтобы не обрабатывать одно сообщение дважды
                offset = update_id + 1

                # Обработка текстовых сообщений (ReplyKeyboard кнопки тоже приходят как текстовые сообщения)
                message = update.get("message") or update.get("edited_message")
                if not message:
                    logging.debug("Update %d: no message field, skipping", update_id)
                    continue

                msg_chat_id = str(message.get("chat", {}).get("id"))
                if msg_chat_id != str(chat_id):
                    logging.debug("Update %d: chat_id mismatch (%s != %s), skipping", update_id, msg_chat_id, chat_id)
                    continue

                text = (message.get("text") or "").strip()
                if not text:
                    logging.debug("Update %d: empty text, skipping", update_id)
                    continue

                logging.info("Telegram message received: %s (chat_id: %s)", text, msg_chat_id)

                # Обрабатываем каждую команду в отдельном try-except, чтобы одна ошибка не блокировала остальные
                try:
                    # Сначала проверяем команды, потом формат времени
                    if text.upper().startswith("CLOSE") or text.startswith("/close"):
                        logging.info("Handling CLOSE command")
                        _handle_close_all(trading_cfg, token, chat_id, ib_client)

                    elif text.startswith("/settp") or text.startswith("TP "):
                        logging.info("Handling TP command: %s", text)
                        _handle_tp_command(text, trading_cfg, token, chat_id)

                    elif text.startswith("/setsl") or text.startswith("SL "):
                        logging.info("Handling SL command: %s", text)
                        _handle_sl_command(text, trading_cfg, token, chat_id)

                    elif text.startswith("/settime") or text.startswith("TIME "):
                        logging.info("Handling TIME command: %s", text)
                        _handle_time_command(
                            text,
                            trading_cfg,
                            token,
                            chat_id,
                            scheduler,
                        )

                    elif text.startswith("/positions"):
                        logging.info("Handling /positions command")
                        _handle_positions(ib_client, trading_cfg, token, chat_id)

                    elif text.startswith("/config"):
                        logging.info("Handling /config command")
                        _send_message(
                            token,
                            chat_id,
                            _format_config(trading_cfg),
                            _default_keyboard(trading_cfg),
                        )

                    elif text.upper() in ["LONG", "SHORT"]:
                        logging.info("Handling SIDE command: %s", text)
                        _handle_side_command(
                            text,
                            trading_cfg,
                            token,
                            chat_id,
                        )

                    elif text.upper().startswith("QTY ") or text.startswith("/setqty"):
                        logging.info("Handling QTY command: %s", text)
                        _handle_quantity_command(
                            text,
                            trading_cfg,
                            token,
                            chat_id,
                        )

                    # Plain time like "13:00:00" (проверяем в конце)
                    elif re.fullmatch(r"\d{2}:\d{2}:\d{2}", text):
                        logging.info("Handling time format: %s", text)
                        _handle_time_command(
                            text,
                            trading_cfg,
                            token,
                            chat_id,
                            scheduler,
                        )

                    else:
                        logging.warning("Unhandled command: %s", text)
                        _send_message(
                            token,
                            chat_id,
                            "Unknown command.\n"
                            "Використовуй кнопки, час у форматі `HH:MM:SS`, "
                            "`/config` або `/close`.",
                            _default_keyboard(trading_cfg),
                        )
                except Exception as cmd_exc:
                    logging.exception("Error handling command '%s': %s", text, cmd_exc)
                    _send_message(
                        token,
                        chat_id,
                        f"❌ Error processing command: `{cmd_exc}`",
                        _default_keyboard(trading_cfg),
                    )

        except Exception as exc:
            logging.exception("Telegram command loop error: %s", exc)
            time.sleep(5)