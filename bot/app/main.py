import logging
from datetime import datetime, timezone
import threading
import os
import time
from typing import Optional, Dict

from .config import load_trading_config, load_env_config
from .ib_client import IBClient
from .notifier import TelegramNotifier, telegram_command_loop
from .strategy import TimeEntryBracketStrategy
from .scheduler import DailyScheduler


def setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)

    log_dir = "/app/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "bot.log")

    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.StreamHandler(),        # console (docker logs)
            logging.FileHandler(log_file),  # file /app/logs/bot.log
        ],
    )


class MultiNotifier:
    """Send the same message to multiple Telegram notifiers."""

    def __init__(self, *notifiers: Optional[TelegramNotifier]) -> None:
        # Filter out None (in case second bot is not configured)
        self._notifiers = [n for n in notifiers if n is not None]
        logging.info(f"MultiNotifier initialized with {len(self._notifiers)} notifier(s)")
        for i, n in enumerate(self._notifiers, 1):
            logging.info(f"  Notifier {i}: chat_id={getattr(n, 'chat_id', 'unknown')[:15]}...")

    def send(self, text: str, keyboard=None) -> None:
        for i, n in enumerate(self._notifiers, 1):
            try:
                # We ignore keyboard for now in main(), but support it for future use
                if keyboard is not None:
                    n.send(text, keyboard=keyboard)
                else:
                    n.send(text)
                logging.debug(f"Message sent to notifier {i} (chat_id={n.chat_id[:10]}...)")
            except Exception as exc:
                logging.exception(
                    f"Failed to send Telegram message to notifier {i} "
                    f"(chat_id={getattr(n, 'chat_id', 'unknown')[:10]}...): {exc}"
                )


def position_monitor_loop(ib_client: IBClient, notifier: MultiNotifier, check_interval: int = 60) -> None:
    """
    Периодически проверяет позиции и отправляет уведомления при закрытии.
    check_interval: интервал проверки в секундах (по умолчанию 60 секунд = 1 минута)
    """
    last_positions: Dict[str, float] = {}  # symbol -> qty
    
    logging.info("Position monitor loop started (checking every %s seconds)", check_interval)
    
    while True:
        try:
            time.sleep(check_interval)
            
            logging.debug("Position monitor: checking positions...")
            
            if not ib_client.ib.isConnected():
                logging.debug("Position monitor: IB not connected, skipping check")
                continue
            
            # Получаем текущие позиции (с явным запросом к брокеру)
            current_positions = ib_client.refresh_positions()
            
            # Создаем словарь текущих позиций
            current_pos_dict: Dict[str, float] = {}
            for pos in current_positions:
                symbol = getattr(pos.contract, 'localSymbol', '') or getattr(pos.contract, 'symbol', '')
                if symbol and abs(pos.position) > 0.01:  # Игнорируем нулевые позиции
                    current_pos_dict[symbol] = pos.position
            
            logging.debug("Position monitor: current positions: %s", current_pos_dict)
            logging.debug("Position monitor: last positions: %s", last_positions)
            
            # Проверяем изменения
            all_symbols = set(last_positions.keys()) | set(current_pos_dict.keys())
            
            for symbol in all_symbols:
                last_qty = last_positions.get(symbol, 0.0)
                current_qty = current_pos_dict.get(symbol, 0.0)
                
                # Если позиция закрылась (была не 0, стала 0)
                if abs(last_qty) > 0.01 and abs(current_qty) < 0.01:
                    logging.info(f"Position monitor: {symbol} CLOSED (was {last_qty}, now {current_qty})")
                    # Форматируем сообщение с информацией о закрытии
                    side = "LONG" if last_qty > 0 else "SHORT"
                    msg = (
                        f"✅ Position closed: {symbol}\n"
                        f"Side: {side}\n"
                        f"Quantity: {abs(last_qty)}"
                    )
                    notifier.send(msg)
                
                # Если позиция открылась (была 0, стала не 0)
                elif abs(last_qty) < 0.01 and abs(current_qty) > 0.01:
                    logging.info(f"Position monitor: {symbol} opened (qty={current_qty})")
                    # Не отправляем уведомление при открытии, т.к. это уже обрабатывается в market_entry
            
            # Обновляем последнее известное состояние
            last_positions = current_pos_dict.copy()
            
        except Exception as exc:
            logging.error("Position monitor error: %s", exc, exc_info=True)
            time.sleep(check_interval)  # Ждем перед следующей попыткой


def main() -> None:
    # Load configs
    trading_cfg = load_trading_config()
    env_cfg = load_env_config()

    setup_logging(env_cfg.log_level)

    logging.info("Starting IBKR trading bot with config: %s", trading_cfg)

    # Connect IB Gateway
    ib_client = IBClient(env_cfg.ib_host, env_cfg.ib_port, env_cfg.ib_client_id)
    ib_client.connect()

    # --- Telegram notifiers (two bots) ---

    # Primary bot: with commands (/positions, /config, CLOSE ALL, etc.)
    notifier1 = TelegramNotifier(
        env_cfg.telegram_bot_token,
        env_cfg.telegram_chat_id,
    )

    # Optional second bot: only for notifications (no command loop here)
    bot2_token = getattr(env_cfg, "telegram_bot2_token", None)
    bot2_chat_id = getattr(env_cfg, "telegram_chat2_id", None)
    notifier2: Optional[TelegramNotifier] = None
    if bot2_token and bot2_chat_id:
        notifier2 = TelegramNotifier(bot2_token, bot2_chat_id)
        logging.info(f"✅ Second Telegram bot configured (chat_id={bot2_chat_id[:15]}...)")
    else:
        logging.warning(
            "⚠️ Second Telegram bot NOT configured: "
            f"TELEGRAM_BOT2_TOKEN={'✅ set' if bot2_token else '❌ NOT SET'}, "
            f"TELEGRAM_CHAT2_ID={'✅ set' if bot2_chat_id else '❌ NOT SET'}"
        )

    # Unified notifier that broadcasts to both bots
    notifier = MultiNotifier(notifier1, notifier2)

    # Attach TP/SL/CLOSE ALL / PnL-style notifications from IB client to Telegram
    ib_client.set_notify_callback(lambda text: notifier.send(text))

    notifier.send(
        f"✅ IBKR bot started.\n"
        f"Symbol: {trading_cfg.symbol} {trading_cfg.expiry}\n"
        f"Side: {trading_cfg.side} qty={trading_cfg.quantity}\n"
        f"Entry time (UTC): {trading_cfg.entry_time_utc}"
    )

    # Trading job executed at scheduled time
    def job() -> None:
        now = datetime.now(timezone.utc).isoformat()
        logging.info("Executing scheduled trade job at %s", now)

        # 1) Ensure IB connection (пытаемся до 5 минут)
        if not ib_client.ib.isConnected():
            logging.warning("IB is not connected, attempting to reconnect (max 5 minutes)...")
            
            if not ib_client.connect_with_timeout(max_duration=300, retry_delay=20):
                logging.error("Could not establish IB connection within 5 minutes, skipping this run")
                return

        # 2) Connection is OK — run the strategy
        strategy = TimeEntryBracketStrategy(ib_client, trading_cfg)

        try:
            result = strategy.run()
            msg = (
                f"✅ Trade executed:\n"
                f"{result.side} {result.quantity} {trading_cfg.symbol} {trading_cfg.expiry}\n"
                f"Entry: {result.entry_price}\n"
                f"TP: {result.take_profit_price}\n"
                f"SL: {result.stop_loss_price}"
            )
            notifier.send(msg)
        except Exception as exc:
            logging.exception("Trade job failed: %s", exc)
            notifier.send(f"❌ Trade job failed: {exc}")

    # Daily scheduler (runs job at cfg.entry_time_utc)
    scheduler = DailyScheduler(trading_cfg.entry_time_utc, job)

    # Start Telegram command loop (buttons: TP, SL, TIME, /positions, /config, CLOSE ALL)
    # Command loop is only for the primary bot (notifier1)
    if env_cfg.telegram_bot_token and env_cfg.telegram_chat_id:
        cmd_thread = threading.Thread(
            target=telegram_command_loop,
            args=(
                env_cfg.telegram_bot_token,
                env_cfg.telegram_chat_id,
                trading_cfg,
                ib_client,
                scheduler,
            ),
            daemon=True,
        )
        cmd_thread.start()
    
    # 🔧 Запускаем мониторинг позиций раз в минуту
    position_monitor_thread = threading.Thread(
        target=position_monitor_loop,
        args=(ib_client, notifier, 60),  # проверка каждые 60 секунд
        daemon=True,
    )
    position_monitor_thread.start()
    logging.info("Position monitor started (checking every 60 seconds)")

    try:
        scheduler.run_forever()
    finally:
        ib_client.disconnect()


if __name__ == "__main__":
    main()