from dataclasses import dataclass
from datetime import time
from pathlib import Path
import os
import yaml
from typing import Optional

# Шлях до YAML-конфігу
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "/app/config/config.yaml"))


@dataclass
class TradingConfig:
    symbol: str
    exchange: str
    currency: str
    expiry: str
    entry_time_utc: time
    side: str
    quantity: int
    take_profit_offset: float
    stop_loss_offset: float
    mode: str
    
    # Режим входа и параметры лимитного ордера
    entry_mode: str = "time"  # "time", "limit", "time_and_limit"
    limit_order_price: Optional[float] = None
    limit_order_min_price: Optional[float] = None
    limit_order_max_price: Optional[float] = None
    limit_order_timeout: float = 300.0

    # �� Методи для оновлення параметрів у рантаймі (через Telegram)
    def set_take_profit(self, value: float) -> None:
        self.take_profit_offset = float(value)

    def set_stop_loss(self, value: float) -> None:
        self.stop_loss_offset = float(value)

    def set_entry_time(self, hh: int, mm: int, ss: int = 0) -> None:
        self.entry_time_utc = time(hour=hh, minute=mm, second=ss)

    def set_side(self, new_side: str) -> None:
        self.side = str(new_side).upper()

    def set_quantity(self, qty: int) -> None:
        self.quantity = int(qty)


@dataclass
class EnvConfig:
    ib_host: str
    ib_port: int
    ib_client_id: int

    telegram_bot_token: str
    telegram_chat_id: str

    # 🔹 Другий бот (опціонально)
    telegram_bot2_token: str
    telegram_chat2_id: str

    log_level: str


# Глобальна змінна, щоб до конфіга можна було дотягнутися з Telegram-лупа
GLOBAL_TRADING_CONFIG: TradingConfig | None = None


def load_trading_config() -> TradingConfig:
    global GLOBAL_TRADING_CONFIG

    with open(CONFIG_PATH, "r") as f:
        raw = yaml.safe_load(f)

    # entry_time_utc з YAML у форматі "HH:MM:SS"
    hh, mm, ss = map(int, str(raw["entry_time_utc"]).split(":"))

    cfg = TradingConfig(
        symbol=str(raw["symbol"]),
        exchange=str(raw["exchange"]),
        currency=str(raw["currency"]),
        expiry=str(raw["expiry"]),
        entry_time_utc=time(hour=hh, minute=mm, second=ss),
        side=str(raw["side"]).upper(),
        quantity=int(raw["quantity"]),
        take_profit_offset=float(raw["take_profit_offset"]),
        stop_loss_offset=float(raw["stop_loss_offset"]),
        mode=str(raw.get("mode", "paper")),
        limit_order_timeout=float(raw.get("limit_order_timeout", 300.0)),
    )

    GLOBAL_TRADING_CONFIG = cfg
    return cfg


def load_env_config() -> EnvConfig:
    return EnvConfig(
        ib_host=os.getenv("IB_HOST", "ib-gateway"),
        ib_port=int(os.getenv("IB_PORT", "4002")),
        ib_client_id=int(os.getenv("IB_CLIENT_ID", "1")),

        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),

        telegram_bot2_token=os.getenv("TELEGRAM_BOT2_TOKEN", ""),
        telegram_chat2_id=os.getenv("TELEGRAM_CHAT2_ID", ""),

        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )