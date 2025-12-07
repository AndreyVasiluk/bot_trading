import logging
import os
import time

from app.ib_client import IBClient
from app.config import load_trading_config
from app.notifier import TelegramNotifier


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)8s | %(message)s",
    )

    cfg = load_trading_config()

    host = os.getenv("IB_HOST", "ib-gateway")
    port = int(os.getenv("IB_PORT", "4002"))

    client_id = int(os.getenv("IB_CLOSE_CLIENT_ID", "99"))

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    tg = TelegramNotifier(token, chat_id)

    ib_client = IBClient(host=host, port=port, client_id=client_id)
    ib_client.set_notify_callback(lambda text: tg.send(text))

    logging.info("Starting CLOSE ALL helper process...")
    tg.send("⏳ CLOSE ALL helper starting: connecting to IB...")

    ib_client.connect()
    try:
        ib_client.close_all_positions()
        
        # Wait for orders to be submitted and filled
        logging.info("Waiting for orders to be processed...")
        time.sleep(5)  # Give orders time to submit
        
        # Check if positions are closed
        ib = ib_client.ib
        positions = list(ib.positions() or [])
        if positions:
            logging.warning(f"Still have {len(positions)} open positions after close attempt")
            for pos in positions:
                logging.warning(f"  - {pos.contract.localSymbol}: {pos.position}")
        else:
            logging.info("All positions closed successfully")
            
    finally:
        ib_client.disconnect()
        logging.info("CLOSE ALL helper done, disconnecting.")
        tg.send("✅ CLOSE ALL helper finished and disconnected from IB.")


if __name__ == "__main__":
    main()