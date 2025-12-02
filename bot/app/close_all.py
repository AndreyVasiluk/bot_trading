import logging
import os

from app.ib_client import IBClient
from app.config import load_trading_config  # або як у тебе називається функція конфігу
from app import notifier


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)8s | %(message)s",
    )

    cfg = load_trading_config()

    host = os.getenv("IB_HOST", "ib-gateway")
    port = int(os.getenv("IB_PORT", "4002"))
    client_id = int(os.getenv("IB_CLIENT_ID", "99"))  # окремий clientId для цього процесу

    ib_client = IBClient(host=host, port=port, client_id=client_id)
    ib_client.set_notify_callback(lambda text: notifier.send(text))

    logging.info("Starting CLOSE ALL helper process...")
    notifier.send("⏳ CLOSE ALL helper starting: connecting to IB...")

    ib_client.connect()
    try:
        ib_client.close_all_positions()
    finally:
        ib_client.disconnect()
        logging.info("CLOSE ALL helper done, disconnecting.")
        notifier.send("✅ CLOSE ALL helper finished and disconnected from IB.")


if __name__ == "__main__":
    main()