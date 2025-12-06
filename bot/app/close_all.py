import logging
import os

from .config import load_env_config
from .ib_client import IBClient
from .notifier import TelegramNotifier


def setup_logging(level: str) -> None:
    """
    Simple logging setup for CLOSE ALL helper.
    Logs go to console + /app/logs/close_all.log
    """
    lvl = getattr(logging, level.upper(), logging.INFO)

    log_dir = "/app/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "close_all.log")

    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.StreamHandler(),        # видимі через docker logs
            logging.FileHandler(log_file),  # окремий лог-хелпер
        ],
    )


def main() -> None:
    # Завантажуємо ENV (IB host/port/clientId, Telegram токен/чат, log_level)
    env_cfg = load_env_config()
    setup_logging(env_cfg.log_level)

    logging.info("=== CLOSE ALL helper started ===")

    # Підключення до IB
    ib_client = IBClient(env_cfg.ib_host, env_cfg.ib_port, env_cfg.ib_client_id)

    # Telegram для нотифікацій саме з хелпера
    notifier = None
    if getattr(env_cfg, "telegram_bot_token", None) and getattr(env_cfg, "telegram_chat_id", None):
        notifier = TelegramNotifier(env_cfg.telegram_bot_token, env_cfg.telegram_chat_id)
        ib_client.set_notify_callback(lambda text: notifier.send(text))

    try:
        logging.info(
            "Connecting to IB Gateway from CLOSE ALL helper %s:%s (clientId=%s)...",
            env_cfg.ib_host,
            env_cfg.ib_port,
            env_cfg.ib_client_id,
        )
        ib_client.connect()
        logging.info("Connected to IB in CLOSE ALL helper.")

        # Основна логіка закриття всіх позицій
        logging.info("Calling ib_client.close_all_positions()...")
        ib_client.close_all_positions()
        logging.info("close_all_positions() finished.")

        if notifier:
            notifier.send("✅ CLOSE ALL helper finished. All positions should now be flat (if any).")

    except Exception as exc:
        logging.exception("CLOSE ALL helper error: %s", exc)
        if notifier:
            notifier.send(f"❌ CLOSE ALL helper error: `{exc}`")
    finally:
        try:
            ib_client.disconnect()
        except Exception:
            pass

        logging.info("=== CLOSE ALL helper finished ===")


if __name__ == "__main__":
    main()