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

    def _pre_trade_account_check(self) -> None:
        """
        Basic pre-trade checks:
        - IB connection is alive
        - No existing open position for the same symbol+expiry
        (щоб не нарощувати випадково позицію, якщо щось пішло не так).
        """
        ib = self.ib_client.ib

        # 1) Перевірка конекту
        if not ib.isConnected():
            raise RuntimeError("IB API is not connected (pre-trade check failed).")

        # 2) Оновлюємо позиції з брокера
        try:
            ib.reqPositions()
            ib.sleep(1.0)
        except Exception as exc:
            logging.warning("Failed to explicitly refresh positions: %s", exc)

        positions = ib.positions()

        symbol = self.cfg.symbol
        expiry = self.cfg.expiry

        for pos in positions:
            contract = pos.contract
            c_symbol = getattr(contract, "symbol", "")
            c_expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
            qty = pos.position

            # Якщо вже є не-нульова позиція по цьому ж інструменту — не входимо
            if qty != 0 and c_symbol == symbol and expiry in (c_expiry, c_expiry[:6]):
                # expiry in ( '202512' , '20251219' ) — невелике послаблення по формату
                msg = (
                    f"Pre-trade check: existing position detected for {symbol} {expiry} "
                    f"(qty={qty}, avgCost={pos.avgCost}). Skipping new entry."
                )
                logging.warning(msg)
                raise RuntimeError(
                    "Existing open position for this contract — new entry is skipped."
                )

        logging.info(
            "Pre-trade account check passed: no open positions for %s %s",
            symbol,
            expiry,
        )

    def run(self) -> StrategyResult:
        logging.info("Running TimeEntryBracketStrategy for %s", self.cfg.symbol)

        # 🔍 Перевірка акаунта / позицій перед входом
        self._pre_trade_account_check()

        # 1) Кваліфікуємо фʼючерсний контракт
        contract = self.ib_client.make_future_contract(
            symbol=self.cfg.symbol,
            expiry=self.cfg.expiry,
            exchange=self.cfg.exchange,
            currency=self.cfg.currency,
        )

        # 2) Вхід по ринку
        entry_price = self.ib_client.market_entry(
            contract=contract,
            side=self.cfg.side,
            quantity=self.cfg.quantity,
        )

        # 3) Виставлення брекет-ордера (TP/SL) на стороні брокера
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
            self.cfg.side,
            self.cfg.quantity,
            entry_price,
            tp_price,
            sl_price,
        )

        return StrategyResult(
            side=self.cfg.side,
            quantity=self.cfg.quantity,
            entry_price=entry_price,
            take_profit_price=tp_price,
            stop_loss_price=sl_price,
        )