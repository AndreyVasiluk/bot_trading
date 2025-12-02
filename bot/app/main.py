import logging
from datetime import datetime, timezone
import threading
import os

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
            logging.StreamHandler(),           # консоль (docker logs)
            logging.FileHandler(log_file),     # файл /app/logs/bot.log
        ],
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

    # Telegram notifier (simple send)
    notifier = TelegramNotifier(env_cfg.telegram_bot_token, env_cfg.telegram_chat_id)

    # Привʼязуємо нотифікації TP/SL/CLOSE ALL до Telegram
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

        # 1️⃣ Перевіряємо, чи є конект до IB перед запуском стратегії
        try:
            if not ib_client.ib.isConnected():
                logging.warning("IB is not connected, trying to reconnect before running strategy...")

                try:
                    ib_client.connect()
                except Exception as exc:
                    logging.exception("Reconnect to IB failed: %s", exc)
                    notifier.send(
                        "❌ IB Gateway не підключений — не можу виконати запланований вхід.\n"
                        "Перевірте, будь ласка, TWS / IB Gateway та інтернет."
                    )
                    return

                # Якщо після connect() все ще немає конекту — скіпаємо цей запуск
                if not ib_client.ib.isConnected():
                    logging.error("Still not connected to IB after reconnect attempt, skipping run")
                    notifier.send(
                        "❌ Після спроби перепідключення IB все одно не конектиться.\n"
                        "Цей запуск пропущено, наступна спроба буде в наступний запланований час."
                    )
                    return

        except Exception as exc:
            # На всяк випадок, якщо щось піде не так навіть при перевірці конекту
            logging.exception("Error while checking IB connection before job: %s", exc)
            notifier.send(f"❌ Помилка при перевірці підключення до IB: `{exc}`")
            return

        # 2️⃣ Конект є — запускаємо стратегію
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

    # 🔹 Start Telegram command loop (buttons: TP, SL, TIME, /positions, /config, CLOSE ALL)
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