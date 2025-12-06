import logging
from datetime import datetime, timezone
import threading
import os
from typing import Optional

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

    def send(self, text: str, keyboard=None) -> None:
        for n in self._notifiers:
            try:
                # We ignore keyboard for now in main(), but support it for future use
                if keyboard is not None:
                    n.send(text, keyboard=keyboard)
                else:
                    n.send(text)
            except Exception as exc:
                logging.exception("Failed to send Telegram message: %s", exc)


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

        # 1) Check IB connection before running the strategy
        try:
            if not ib_client.ib.isConnected():
                logging.warning(
                    "IB is not connected, trying to reconnect before running strategy..."
                )

                try:
                    ib_client.connect()
                except Exception as exc:
                    logging.exception("Reconnect to IB failed: %s", exc)
                    notifier.send(
                        "❌ IB Gateway is not connected — cannot execute scheduled entry.\n"
                        "Please check TWS / IB Gateway and Internet connection."
                    )
                    return

                # If still not connected after reconnect attempt — skip this run
                if not ib_client.ib.isConnected():
                    logging.error(
                        "Still not connected to IB after reconnect attempt, skipping run"
                    )
                    notifier.send(
                        "❌ After reconnect attempt IB API is still not connected.\n"
                        "This run is skipped, next attempt will be at the next scheduled time."
                    )
                    return

        except Exception as exc:
            # Fallback if something goes wrong even while checking the connection
            logging.exception("Error while checking IB connection before job: %s", exc)
            notifier.send(f"❌ Error while checking IB connection: `{exc}`")
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

    try:
        scheduler.run_forever()
    finally:
        ib_client.disconnect()


if __name__ == "__main__":
    main()