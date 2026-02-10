import logging
import time
import re
from datetime import time as dtime
from typing import Any, Dict, Optional, List, Tuple

import requests
import threading  # added for background workers

from .config import TradingConfig
from .ib_client import IBClient
from .scheduler import DailyScheduler

# Global flag to prevent multiple CLOSE ALL workers running in parallel
_close_all_running = False
_close_all_started_at: Optional[float] = None

# Global flag to prevent multiple OPEN POSITION workers running in parallel
_open_position_running = False
_open_position_started_at: Optional[float] = None

# Seconds after which we consider CLOSE ALL "stuck" and allow new run
_CLOSE_ALL_TIMEOUT = 60

# Seconds after which we consider OPEN POSITION "stuck" and allow new run
_OPEN_POSITION_TIMEOUT = 120


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
        # Remove parse_mode to avoid markdown parsing errors
            # "parse_mode": "Markdown",
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
    Wrapper for sending the same message to multiple Telegram chats simultaneously.
    """

    def __init__(self, targets: List[Tuple[str, str]]) -> None:
        """
        targets: list of (token, chat_id) tuples.
        Empty values (""/"0"/None) are ignored.
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
        Send a message to all registered chats.
        Failures in one chat do not block others.
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
            # Force open/close
            [
                {"text": "OPEN POSITION"},
                {"text": "CLOSE ALL"},
            ],
            # Status
            [
                {"text": "/positions"},
                {"text": "/status"},
                {"text": "/config"},
                {"text": "/price"},
            ],
            # Mode selection
            [
                {"text": "⚙️ MODE: TIME"},
                {"text": "⚙️ MODE: LIMIT"},
                {"text": "⚙️ MODE: TIME+LIMIT"},
            ],
            # Limit order
            [
                {"text": "📊 SET LIMIT"},
                {"text": "❌ CANCEL LIMIT"},
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
    Internal helper for replying to a single Telegram chat (used in the command loop).
    """
    notifier = TelegramNotifier(token, chat_id)
    notifier.send(text, keyboard=keyboard)


def _format_config(cfg: TradingConfig) -> str:
    mode_display = getattr(cfg, 'entry_mode', 'time')
    limit_info = ""
    if hasattr(cfg, 'limit_order_price') and cfg.limit_order_price:
        limit_info = (
            f"\nLimit order: {cfg.limit_order_price} "
            f"(range: {cfg.limit_order_min_price}-{cfg.limit_order_max_price})"
        )
    
    return (
        f"*Current config:*\n"
        f"Symbol: {cfg.symbol} {cfg.expiry}\n"
        f"Side: {cfg.side}\n"
        f"Quantity: {cfg.quantity}\n"
        f"TP: {cfg.take_profit_offset} points\n"
        f"SL: {cfg.stop_loss_offset} points\n"
        f"Entry time (UTC): {cfg.entry_time_utc}\n"
        f"Mode: {mode_display}{limit_info}"
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

    # If the user just sent "13:00:00"
    if len(parts) == 1 and re.fullmatch(r"\d{2}:\d{2}:\d{2}", parts[0]):
        time_str = parts[0]
    elif len(parts) == 2:
        time_str = parts[1]
    else:
        _send_message(
            token,
            chat_id,
            "Usage: `TIME HH:MM:SS` or just `13:00:00`.\n"
            "Example: `TIME 13:00:00` or `00:00:00`",
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
            "❌ Invalid time format.\nUse `HH:MM:SS`, e.g.: `13:00:00`.",
            _default_keyboard(cfg),
        )
        return

    cfg.entry_time_utc = new_time
    # 🔹 update the scheduler time
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
    Show open positions with detailed info (entry, SL, TP, current price).
    """
    global _close_all_running, _close_all_started_at

    try:
        logging.info("_handle_positions: starting")
        if not ib_client.ib.isConnected():
            logging.warning("_handle_positions: IB not connected")
            _send_message(
                token,
                chat_id,
                "⚠️ IB is not connected, cannot fetch positions.\n"
                "Please check TWS / IB Gateway.",
                _default_keyboard(cfg),
            )
            return

        # Fetch latest positions directly from the broker
        logging.info("_handle_positions: requesting fresh positions directly from broker...")
        try:
            positions = ib_client.get_positions_from_broker()
            logging.info("_handle_positions: got %d positions directly from broker (not from cache)", len(positions))
        except Exception as exc:
            logging.error(f"_handle_positions: failed to get positions from broker: {exc}")
            _send_message(
                token,
                chat_id,
                f"❌ Failed to fetch active positions from the broker: `{exc}`\n"
                f"Please verify the connection to IB Gateway/TWS.",
                _default_keyboard(cfg),
            )
            return

        # Filter out positions with zero quantity
        open_positions = [pos for pos in positions if abs(float(pos.position)) > 0.001]
        logging.info("_handle_positions: %d open positions (non-zero qty)", len(open_positions))
        
        # Log each position for debugging
        for pos in open_positions:
            symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
            expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
            qty = float(pos.position)
            logging.info(f"_handle_positions: DISPLAYING position from BROKER: {symbol} {expiry} qty={qty}")

        # if no positions are open, consider CLOSE ALL complete
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
            symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
            expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
            qty = float(pos.position)
            
            # Retrieve full state of the position
            status = ib_client.get_position_status(pos)
            
            # Build the info string
            entry_str = f"{status['entry']:.2f}" if status['entry'] else "N/A"
            sl_str = f"{status['sl']:.2f}" if status['sl'] else "N/A"
            tp_str = f"{status['tp']:.2f}" if status['tp'] else "N/A"
            current_str = f"{status['current_price']:.2f}" if status['current_price'] else "N/A"
            
            # Compute PnL if entry and current price are available
            pnl_str = ""
            if status['entry'] and status['current_price']:
                side_multiplier = 1.0 if qty > 0 else -1.0
                pnl_points = (status['current_price'] - status['entry']) * side_multiplier
                multiplier = float(getattr(contract, "multiplier", "1") or "1")
                pnl_usd = pnl_points * multiplier * abs(qty)
                pnl_str = f" | PnL: {pnl_points:.2f} pts ({pnl_usd:.2f} USD)"
            
            # Escape special characters
            symbol_escaped = str(symbol).replace("`", "\\`").replace("*", "\\*").replace("_", "\\_")
            expiry_escaped = str(expiry).replace("`", "\\`").replace("*", "\\*").replace("_", "\\_")
            
            lines.append(
                f"*{symbol_escaped} {expiry_escaped}* (qty: {qty})\n"
                f"Entry: `{entry_str}` | SL: `{sl_str}` | TP: `{tp_str}`\n"
                f"Current: `{current_str}`{pnl_str}"
            )

        message_text = "\n\n".join(lines)
        logging.info("_handle_positions: sending positions message with status")
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
    Handle CLOSE ALL from Telegram using the current ib_client (same connection as the strategy).
    """
    global _close_all_running, _close_all_started_at

    now = time.time()

    if _close_all_running:
        # Check if CLOSE ALL worker is stuck
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
                "⏳ CLOSE ALL is already running. Wait for completion, then check `/positions`.",
                _default_keyboard(cfg),
            )
            return

    # We can safely start a new worker here
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
            # Ensure the connection is still alive
            if not ib_client.ib.isConnected():
                logging.error("IB not connected in CLOSE ALL worker. Cannot reconnect from worker thread (no event loop).")
                _send_message(
                    token,
                    chat_id,
                    "❌ CLOSE ALL failed: IB is not connected. Please wait for automatic reconnection or restart the bot.",
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


def _handle_status(
    ib_client: IBClient,
    cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """
    Show detailed status for open positions: entry, SL, TP, current price, and PnL.
    """
    try:
        logging.info("_handle_status: starting")
        if not ib_client.ib.isConnected():
            logging.warning("_handle_status: IB not connected")
            _send_message(
                token,
                chat_id,
                "⚠️ IB is not connected, cannot provide position status.\n"
                "Please check TWS / IB Gateway.",
                _default_keyboard(cfg),
            )
            return

        # Fetch current positions directly from the broker
        logging.info("_handle_status: requesting fresh positions from broker...")
        try:
            positions = ib_client.get_positions_from_broker()
            logging.info("_handle_status: got %d positions from broker", len(positions))
        except Exception as exc:
            logging.error(f"_handle_status: failed to get positions: {exc}")
            _send_message(
                token,
                chat_id,
                f"❌ Failed to get positions: `{exc}`",
                _default_keyboard(cfg),
            )
            return

        # Filter only positions with non-zero quantity
        open_positions = [pos for pos in positions if abs(float(pos.position)) > 0.001]
        
        # Log position details for debugging
        for pos in open_positions:
            symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
            expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
            qty = float(pos.position)
            logging.info(f"_handle_status: DISPLAYING position from BROKER: {symbol} {expiry} qty={qty}")
        
        if not open_positions:
            _send_message(
                token,
                chat_id,
                "No open positions.",
                _default_keyboard(cfg),
            )
            return

        lines = ["*Position Status:*"]
        for pos in open_positions:
            contract = pos.contract
            symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
            expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
            qty = float(pos.position)
            
            # Gather full status for this position
            status = ib_client.get_position_status(pos)
            
            # Format the status information
            entry_str = f"{status['entry']:.2f}" if status['entry'] else "N/A"
            sl_str = f"{status['sl']:.2f}" if status['sl'] else "N/A"
            tp_str = f"{status['tp']:.2f}" if status['tp'] else "N/A"
            current_str = f"{status['current_price']:.2f}" if status['current_price'] else "N/A"
            
            # Compute PnL
            pnl_info = ""
            if status['entry'] and status['current_price']:
                side_multiplier = 1.0 if qty > 0 else -1.0
                pnl_points = (status['current_price'] - status['entry']) * side_multiplier
                multiplier = float(getattr(contract, "multiplier", "1") or "1")
                pnl_usd = pnl_points * multiplier * abs(qty)
                pnl_sign = "📈" if pnl_points > 0 else "📉" if pnl_points < 0 else "➡️"
                pnl_info = f"\n{pnl_sign} PnL: {pnl_points:+.2f} pts ({pnl_usd:+.2f} USD)"
            
            # Escape special characters
            symbol_escaped = str(symbol).replace("`", "\\`").replace("*", "\\*").replace("_", "\\_")
            expiry_escaped = str(expiry).replace("`", "\\`").replace("*", "\\*").replace("_", "\\_")
            
            lines.append(
                f"*{symbol_escaped} {expiry_escaped}*\n"
                f"Qty: `{qty}`\n"
                f"Entry: `{entry_str}`\n"
                f"SL: `{sl_str}` | TP: `{tp_str}`\n"
                f"Current: `{current_str}`{pnl_info}"
            )

        message_text = "\n\n".join(lines)
        logging.info("_handle_status: sending status message")
        _send_message(
            token,
            chat_id,
            message_text,
            _default_keyboard(cfg),
        )
    except Exception as exc:
        logging.exception("Failed to get position status: %s", exc)
        _send_message(
            token,
            chat_id,
            f"❌ Failed to get status: `{exc}`",
            _default_keyboard(cfg),
        )

def _handle_price(
    ib_client: IBClient,
    cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """Fetch the latest market price from the broker."""
    try:
        logging.info("_handle_price: starting")
        if not ib_client.ib.isConnected():
            logging.warning("_handle_price: IB not connected")
            _send_message(
                token,
                chat_id,
                "⚠️ IB is not connected, cannot get the price.\n"
                "Please check TWS / IB Gateway.",
                _default_keyboard(cfg),
            )
            return

        # Build the contract from the config
        contract = ib_client.make_future_contract(
            symbol=cfg.symbol,
            expiry=cfg.expiry,
            exchange=cfg.exchange,
            currency=cfg.currency,
        )
        
        # Retrieve the price
        price = ib_client.get_market_price(contract)
        
        if price is None:
            _send_message(
                token,
                chat_id,
                f"❌ Failed to retrieve the price for {cfg.symbol} {cfg.expiry}",
                _default_keyboard(cfg),
            )
            return
        
        _send_message(
            token,
            chat_id,
            f"💰 Latest price:\n{cfg.symbol} {cfg.expiry}: {price:.2f}",
            _default_keyboard(cfg),
        )
    except Exception as exc:
        logging.exception("Failed to get price: %s", exc)
        _send_message(
            token,
            chat_id,
            f"❌ Failed to get price: `{exc}`",
            _default_keyboard(cfg),
        )

def _handle_open_position(
    cfg: TradingConfig,
    token: str,
    chat_id: str,
    ib_client: IBClient,
) -> None:
    """
    Handle OPEN POSITION from Telegram.
    Executes the trading strategy just like the scheduled job does.
    """
    global _open_position_running, _open_position_started_at
    
    from .strategy import TimeEntryBracketStrategy
    
    now = time.time()
    
    if _open_position_running:
        # verify the worker is not stuck
        if _open_position_started_at and now - _open_position_started_at > _OPEN_POSITION_TIMEOUT:
            logging.warning(
                "OPEN POSITION flag has been set for >%s seconds, resetting.",
                _OPEN_POSITION_TIMEOUT,
            )
            _open_position_running = False
            _open_position_started_at = None
        else:
            logging.info("OPEN POSITION already running, ignoring duplicate request.")
            _send_message(
                token,
                chat_id,
                "⏳ OPEN POSITION is already running. Please wait for it to finish."
                "Then check `/positions`.",
                _default_keyboard(cfg),
            )
            return
    
    # Check connection before starting
    if not ib_client.ib.isConnected():
        logging.warning("IB is not connected, trying to reconnect before OPEN POSITION...")
        try:
            ib_client.connect()
        except Exception as exc:
            logging.exception("Reconnect to IB failed: %s", exc)
            _send_message(
                token,
                chat_id,
                "❌ IB Gateway is not connected — cannot execute OPEN POSITION.\n"
                "Please check TWS / IB Gateway and Internet connection.",
                _default_keyboard(cfg),
            )
            return
        
        # If still not connected after reconnect attempt
        if not ib_client.ib.isConnected():
            logging.error("Still not connected to IB after reconnect attempt")
            _send_message(
                token,
                chat_id,
                "❌ After reconnect attempt IB API is still not connected.\n"
                "OPEN POSITION cancelled.",
                _default_keyboard(cfg),
            )
            return
    
    # Set the running flag
    _open_position_running = True
    _open_position_started_at = now
    
    logging.info("Telegram requested OPEN POSITION, starting background worker thread...")
    _send_message(
        token,
        chat_id,
        f"⏳ OPEN POSITION requested.\n"
        f"Config: {cfg.side} {cfg.quantity} {cfg.symbol} {cfg.expiry}\n"
        f"TP: {cfg.take_profit_offset} points, SL: {cfg.stop_loss_offset} points\n"
        f"Starting worker...",
        _default_keyboard(cfg),
    )
    
    def _worker():
        global _open_position_running, _open_position_started_at
        try:
            # Re-check the connection inside the worker
            if not ib_client.ib.isConnected():
                logging.error("IB not connected in OPEN POSITION worker")
                _send_message(
                    token,
                    chat_id,
                    "❌ OPEN POSITION failed: IB is not connected in worker thread.",
                    _default_keyboard(cfg),
                )
                return
            
            # Set the correct event loop for the worker thread (same as CLOSE ALL)
            ib_loop = ib_client._loop
            if ib_loop is None:
                logging.error("IB event loop not available in OPEN POSITION worker")
                _send_message(
                    token,
                    chat_id,
                    "❌ OPEN POSITION failed: IB event loop not available.",
                    _default_keyboard(cfg),
                )
                return
            
            # Temporarily set the event loop so ib.placeOrder() can access it
            import asyncio
            old_loop = None
            try:
                old_loop = asyncio.get_event_loop()
            except RuntimeError:
                pass
            
            # Assign the correct loop to the current thread
            asyncio.set_event_loop(ib_loop)
            try:
                # Create and run the strategy
                strategy = TimeEntryBracketStrategy(ib_client, cfg)
                
                logging.info("Running strategy from OPEN POSITION worker...")
                result = strategy.run(force=True)
                
                msg = (
                    f"✅ Trade executed:\n"
                    f"{result.side} {result.quantity} {cfg.symbol} {cfg.expiry}\n"
                    f"Entry: {result.entry_price}\n"
                    f"TP: {result.take_profit_price}\n"
                    f"SL: {result.stop_loss_price}"
                )
                _send_message(
                    token,
                    chat_id,
                    msg,
                    _default_keyboard(cfg),
                )
            finally:
            # Restore the previous loop (if it existed)
                if old_loop is not None:
                    asyncio.set_event_loop(old_loop)
                else:
                    asyncio.set_event_loop(None)
        except Exception as exc:
            logging.exception("OPEN POSITION worker error: %s", exc)
            _send_message(
                token,
                chat_id,
                f"❌ OPEN POSITION failed: `{exc}`",
                _default_keyboard(cfg),
            )
        finally:
            _open_position_running = False
            _open_position_started_at = None
    
    threading.Thread(target=_worker, daemon=True).start()


def _handle_mode_selection(
    text: str,
    trading_cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """Handle mode selection button press."""
    mode_map = {
        "⚙️ MODE: TIME": "time",
        "⚙️ MODE: LIMIT": "limit",
        "⚙️ MODE: TIME+LIMIT": "time_and_limit",
    }
    
    mode = mode_map.get(text)
    if not mode:
        return
    
    # Update config
    trading_cfg.entry_mode = mode
    logging.info(f"Entry mode changed to: {mode}")
    
    _send_message(
        token,
        chat_id,
        f"✅ Entry mode set to: *{mode.upper()}*\n\n"
        f"Current settings:\n"
        f"Mode: {mode}\n"
        f"Limit price: {trading_cfg.limit_order_price or 'Not set'}\n"
        f"Price range: {trading_cfg.limit_order_min_price or 'N/A'} - {trading_cfg.limit_order_max_price or 'N/A'}",
        _default_keyboard(trading_cfg),
    )

def _handle_set_limit(
    text: str,
    trading_cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """Handle SET LIMIT button - ask for parameters."""
    _send_message(
        token,
        chat_id,
        "📊 Setting limit order\n\n"
        "Provide parameters as:\n"
        "`/limit <price> <min> <max>`\n\n"
        "Example: `/limit 6955 6950 6960`\n"
        "where:\n"
        "- 6955 is the limit price\n"
        "- 6950 is the minimum acceptable fill\n"
        "- 6960 is the maximum acceptable fill",
        _default_keyboard(trading_cfg),
    )

def _handle_limit_command(
    text: str,
    trading_cfg: TradingConfig,
    token: str,
    chat_id: str,
) -> None:
    """Handle /limit command with parameters."""
    # Parse: /limit 6955 6950 6960
    parts = text.split()
    if len(parts) != 4:
        _send_message(
            token,
            chat_id,
            "❌ Invalid format. Use:\n"
            "`/limit <price> <min> <max>`\n\n"
            "Example: `/limit 6955 6950 6960`",
            _default_keyboard(trading_cfg),
        )
        return
    
    try:
        price = float(parts[1])
        min_price = float(parts[2])
        max_price = float(parts[3])
        
        if min_price >= max_price:
            _send_message(
                token,
                chat_id,
                "❌ Minimum price must be lower than the maximum price",
                _default_keyboard(trading_cfg),
            )
            return
        
        if not (min_price <= price <= max_price):
            _send_message(
                token,
                chat_id,
                f"❌ Limit price ({price}) must fall within [{min_price}, {max_price}]",
                _default_keyboard(trading_cfg),
            )
            return
        
        # Save the limit order parameters
        trading_cfg.limit_order_price = price
        trading_cfg.limit_order_min_price = min_price
        trading_cfg.limit_order_max_price = max_price
        
        _send_message(
            token,
            chat_id,
            f"✅ Limit order parameters set:\n"
            f"Price: `{price}`\n"
            f"Range: `{min_price}` - `{max_price}`\n\n"
            "The limit order will be placed once the price enters the range.",
            _default_keyboard(trading_cfg),
        )
    except ValueError:
        _send_message(
            token,
            chat_id,
            "❌ All parameters must be numbers",
            _default_keyboard(trading_cfg),
        )

def _handle_cancel_limit(
    trading_cfg: TradingConfig,
    token: str,
    chat_id: str,
    ib_client: IBClient,
) -> None:
    """Handle CANCEL LIMIT button."""
    if ib_client._active_limit_order:
        ib_client.cancel_limit_order()
        trading_cfg.limit_order_price = None
        trading_cfg.limit_order_min_price = None
        trading_cfg.limit_order_max_price = None
        
        _send_message(
            token,
            chat_id,
            "✅ Limit order cancelled",
            _default_keyboard(trading_cfg),
        )
    else:
        _send_message(
            token,
            chat_id,
            "ℹ️ No active limit order to cancel",
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
        "Use buttons or commands:\n"
        "- `LONG` / `SHORT` — change entry side\n"
        "- `QTY 2` or `/setqty 2` — change the contract quantity\n"
        "- `TP 30` or `/settp 30`\n"
        "- `SL 10` or `/setsl 10`\n"
        "- `TIME 13:00:00` / `/settime 13:00:00` or just `13:00:00`\n"
        "- `/positions` — list open positions\n"
        "- `/status` — detailed status (entry, SL, TP, price, PnL)\n"
        "- `/config` — current configuration\n"
        "- `/refresh` — refresh the keyboard\n"
        "- `/close` or the *CLOSE ALL* button — force-close all positions (MKT)",
        _default_keyboard(trading_cfg),
    )

    while True:
        try:
            # Build the params properly (offset can be None initially)
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
                # On 409 (Conflict) wait longer because another bot might be running
                wait_time = 10 if resp.status_code == 409 else 5
                time.sleep(wait_time)
                continue

            data = resp.json()
            if not data.get("ok"):
                error_code = data.get("error_code")
                error_desc = data.get("description", "")
                logging.error("getUpdates response not ok: %s - %s", error_code, error_desc)
                
                # For 409 we wait longer but keep the last offset
                if error_code == 409:
                    logging.warning("Another bot instance is running (409 Conflict). Waiting 10 seconds...")
                    time.sleep(10)
                else:
                    time.sleep(5)
                continue

            updates = data.get("result", [])
            if not updates:
                # No new messages arrived – continue polling
                continue
                
            logging.info("Received %d updates from Telegram", len(updates))
            
            for update in updates:
                update_id = update["update_id"]
                # Update offset immediately to avoid processing the same message twice
                offset = update_id + 1

                # Handle text messages from users (buttons are also text)
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

                # Wrap each command in try-except so one failure doesn't stop others
                try:
                    # First check commands, then time format
                    if text.upper().startswith("OPEN") or text.startswith("/open"):
                        logging.info("Handling OPEN POSITION command")
                        _handle_open_position(trading_cfg, token, chat_id, ib_client)
                    
                    elif text.upper().startswith("CLOSE") or text.startswith("/close"):
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

                    elif text == "/positions" or text == "Positions":
                        logging.info("Handling /positions command")
                        _handle_positions(ib_client, trading_cfg, token, chat_id)
                    
                    elif text == "/status" or text.upper() == "STATUS":
                        logging.info("Handling /status command")
                        _handle_status(ib_client, trading_cfg, token, chat_id)

                    elif text == "/price" or text.upper() == "PRICE":
                        logging.info("Handling /price command")
                        _handle_price(ib_client, trading_cfg, token, chat_id)
                    
                    elif text == "/refresh" or text.upper() == "REFRESH":
                        logging.info("Handling /refresh command - updating keyboard")
                        _send_message(
                            token,
                            chat_id,
                            "🔄 Keyboard updated!",
                            _default_keyboard(trading_cfg),
                        )


                    elif text == "/sync" or text.startswith("/sync"):
                        logging.info("Handling /sync command")
                        _send_message(
                            token,
                            chat_id,
                            "🔄 Syncing positions via socket...",
                            _default_keyboard(trading_cfg),
                        )
                        positions = ib_client.force_sync_positions()
                        logging.info(f"force_sync_positions() returned {len(positions)} positions")
                        # Log the position details for debugging
                        for pos in positions:
                            symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                            expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                            qty = float(pos.position)
                            logging.info(f"force_sync_positions() DISPLAYING position: {symbol} {expiry} qty={qty}")
                        # After syncing, show the positions
                        _handle_positions(ib_client, trading_cfg, token, chat_id)

                    elif text == "/config" or text.startswith("/config"):
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

                    elif text.startswith("/limit"):
                        logging.info("Handling /limit command: %s", text)
                        _handle_limit_command(text, trading_cfg, token, chat_id)
                    
                    elif text == "📊 SET LIMIT":
                        logging.info("Handling SET LIMIT button")
                        _handle_set_limit(text, trading_cfg, token, chat_id)
                    
                    elif text == "❌ CANCEL LIMIT":
                        logging.info("Handling CANCEL LIMIT button")
                        _handle_cancel_limit(trading_cfg, token, chat_id, ib_client)
                    
                    elif text.startswith("⚙️ MODE:"):
                        logging.info("Handling mode selection: %s", text)
                        _handle_mode_selection(text, trading_cfg, token, chat_id)

                    # Plain time like "13:00:00" (check at the end)
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
                            "Use the buttons, keep time in `HH:MM:SS` format, "
                            "`/config`, or `/close`.",
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
