import logging
from datetime import datetime, timezone
import threading
import os
from typing import Optional
import time

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
        logging.info("Running scheduled job at %s", now)

        # до 5 попыток, пауза 20 сек
        max_retries = 5
        retry_delay = 20

        for attempt in range(1, max_retries + 1):
            try:
                # Проверка коннекта; если отвалился — пытаемся переподключиться
                if not ib_client.ib.isConnected():
                    logging.warning("IB not connected, attempt %s/%s: reconnecting...", attempt, max_retries)
                    ib_client.connect()

                # 2) Connection is OK — run the strategy
                strategy = TimeEntryBracketStrategy(ib_client, trading_cfg)
                result = strategy.run()
                msg = (
                    f"✅ Trade executed:\n"
                    f"{result.side} {result.quantity} {trading_cfg.symbol} {trading_cfg.expiry}\n"
                    f"Entry: {result.entry_price}\n"
                    f"TP: {result.take_profit_price}\n"
                    f"SL: {result.stop_loss_price}"
                )
                notifier.send(msg)
                break  # успех, выходим из цикла

            except Exception as exc:
                logging.exception("Trade job failed (attempt %s/%s): %s", attempt, max_retries, exc)
                notifier.send(f"❌ Trade job failed (attempt {attempt}/{max_retries}): {exc}")

                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                else:
                    notifier.send("❌ All retry attempts exhausted.")

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

    try:
        scheduler.run_forever()
    finally:
        ib_client.disconnect()


if __name__ == "__main__":
    main()