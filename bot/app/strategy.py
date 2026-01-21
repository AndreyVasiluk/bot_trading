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

        # 2) Получаем актуальные позиции напрямую с брокера (НЕ из кеша)
        logging.info("Pre-trade check: requesting fresh positions from broker (not from cache)...")
        try:
            positions = self.ib_client.get_positions_from_broker()
            logging.info(f"Pre-trade check: got {len(positions)} positions directly from broker")
            
            # Логируем детали позиций для отладки
            for pos in positions:
                symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                qty = float(pos.position)
                if abs(qty) > 0.001:
                    logging.info(f"Pre-trade check: CHECKING position from BROKER: {symbol} {expiry} qty={qty}")
        except Exception as exc:
            logging.error(f"Pre-trade check: failed to get positions from broker: {exc}")
            raise RuntimeError(f"Cannot check existing positions: {exc}")

        symbol = self.cfg.symbol
        expiry = self.cfg.expiry

        for pos in positions:
            contract = pos.contract
            c_symbol = getattr(contract, "symbol", "")
            c_expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
            qty = pos.position

            # Игнорируем позиции с quantity=0 (закрытые позиции)
            if abs(qty) < 0.001:  # Используем небольшой epsilon для сравнения с нулем
                continue

            # Якщо вже є не-нульова позиція по цьому ж інструменту — не входимо
            if c_symbol == symbol and expiry in (c_expiry, c_expiry[:6]):
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