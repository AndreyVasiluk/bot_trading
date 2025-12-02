import logging
from dataclasses import dataclass

from .config import TradingConfig
from .ib_client import IBClient

@dataclass
class StrategyResult:
    side: str
    quantity: int
    entry_price: float
    take_profit_price: float
    stop_loss_price: float

class TimeEntryBracketStrategy:
    def __init__(self, ib_client: IBClient, cfg: TradingConfig) -> None:
        self.ib_client = ib_client
        self.cfg = cfg

    def run(self) -> StrategyResult:
        logging.info("Running TimeEntryBracketStrategy for %s", self.cfg.symbol)

        contract = self.ib_client.make_future_contract(
            symbol=self.cfg.symbol,
            expiry=self.cfg.expiry,
            exchange=self.cfg.exchange,
            currency=self.cfg.currency,
        )

        entry_price = self.ib_client.market_entry(
            contract=contract,
            side=self.cfg.side,
            quantity=self.cfg.quantity,
        )

        tp_price, sl_price = self.ib_client.place_exit_bracket(
            contract=contract,
            position_side=self.cfg.side,
            quantity=self.cfg.quantity,
            entry_price=entry_price,
            tp_offset=self.cfg.take_profit_offset,
            sl_offset=self.cfg.stop_loss_offset,
        )

        logging.info(
            "Strategy completed: side=%s qty=%s entry=%s TP=%s SL=%s",
            self.cfg.side, self.cfg.quantity, entry_price, tp_price, sl_price
        )

        return StrategyResult(
            side=self.cfg.side,
            quantity=self.cfg.quantity,
            entry_price=entry_price,
            take_profit_price=tp_price,
            stop_loss_price=sl_price,
        )
