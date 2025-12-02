import logging
import time
from typing import Callable, Optional, Tuple, List, Dict

from ib_insync import IB, Future, Order, Contract, Trade, Fill


class IBClient:
    """
    Thin wrapper around ib_insync.IB with:
    - auto-retry connect()
    - helpers to create future contract
    - market entry
    - TP/SL bracket placement
    - close-all via market orders
    - optional notify callback for Telegram messages
    - execDetails hook for TP/SL fills (bracket exits)
    """

    def __init__(self, host: str, port: int, client_id: int) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()

        # Simple callback that will be set from main() to send messages to Telegram.
        self._notify: Callable[[str], None] = lambda msg: None

        # Map OCA group -> human-readable description (entry side/qty/symbol)
        self._oca_meta: Dict[str, str] = {}

        # Attach handler for execution details (fills of any orders)
        self.ib.execDetailsEvent += self._on_exec_details

    # ---- notification wiring ----

    def set_notify_callback(self, callback: Optional[Callable[[str], None]]) -> None:
        """
        Set a function that will receive text messages (for Telegram).
        If None is passed, notifications are disabled.
        """
        if callback is None:
            self._notify = lambda msg: None
        else:
            self._notify = callback

    def _safe_notify(self, text: str) -> None:
        """Call notify callback and ignore any errors."""
        try:
            if text:
                self._notify(text)
        except Exception as exc:  # pragma: no cover
            logging.error("Notify callback failed: %s", exc)

    # ---- IB connection ----

    def connect(self) -> None:
        """
        Connect to IB Gateway / TWS with auto-retry loop.
        Blocks until successful connection.
        """
        while True:
            try:
                logging.info(
                    "Connecting to IB Gateway %s:%s with clientId %s...",
                    self.host,
                    self.port,
                    self.client_id,
                )
                print(f"Connecting to {self.host}:{self.port} with clientId {self.client_id}...")
                self.ib.connect(self.host, self.port, clientId=self.client_id)

                if self.ib.isConnected():
                    logging.info("Connected to IB Gateway")
                    self._safe_notify("✅ Connected to IB Gateway/TWS.")
                    return
                else:
                    logging.error("IB connection failed (isConnected() is False)")
            except Exception as exc:
                logging.error("API connection failed: %s", exc)
                logging.error("Make sure API port on TWS/IBG is open")
                self._safe_notify(f"❌ IB API connection error: {exc}")

            logging.error("Connection error, retrying in 3 seconds...")
            time.sleep(3)

    def disconnect(self) -> None:
        if self.ib.isConnected():
            logging.info("Disconnecting")
            self.ib.disconnect()
            logging.info("Disconnected.")

    # ---- contracts ----

    def make_future_contract(
        self,
        symbol: str,
        expiry: str,
        exchange: str,
        currency: str,
    ) -> Future:
        """
        Create and qualify a Future contract.

        - First try the given exchange.
        - If not found and exchange == 'GLOBEX', try 'CME' fallback (ES case).
        """
        def _try_qualify(exch: str) -> Optional[Future]:
            logging.info(
                "Trying to qualify contract: symbol=%s expiry=%s exchange=%s",
                symbol,
                expiry,
                exch,
            )
            contract = Future(
                symbol=symbol,
                lastTradeDateOrContractMonth=expiry,
                exchange=exch,
                currency=currency,
            )
            contracts = self.ib.qualifyContracts(contract)
            if not contracts:
                logging.warning("No contract found for %s %s on exchange %s", symbol, expiry, exch)
                return None
            qualified = contracts[0]
            logging.info("Qualified contract: %s", qualified)
            return qualified

        # Try primary exchange
        qualified = _try_qualify(exchange)
        # ES on GLOBEX fallback to CME
        if not qualified and exchange.upper() == "GLOBEX":
            qualified = _try_qualify("CME")

        if not qualified:
            raise RuntimeError(
                f"Cannot qualify future contract for {symbol} {expiry} "
                f"on {exchange} or fallback."
            )

        return qualified

    # ---- trading helpers ----

    def market_entry(self, contract: Contract, side: str, quantity: int) -> float:
        """
        Place a market order to open position.
        side: 'LONG' -> BUY, 'SHORT' -> SELL
        Returns: average fill price.
        Blocks until order is done (Filled/Cancelled).
        """
        action = "BUY" if side.upper() == "LONG" else "SELL"
        order = Order(
            action=action,
            orderType="MKT",
            totalQuantity=quantity,
        )
        trade = self.ib.placeOrder(contract, order)
        logging.info("Market order sent: %s %s", action, quantity)

        # Wait for fill
        while not trade.isDone():
            self.ib.waitOnUpdate(timeout=5)

        fill_price = float(trade.orderStatus.avgFillPrice or 0.0)
        logging.info("Market order status: %s avgFillPrice=%s", trade.orderStatus.status, fill_price)

        if fill_price > 0:
            self._safe_notify(
                f"✅ Entry filled: {action} {quantity} {contract.localSymbol or contract.symbol} "
                f"@ {fill_price}"
            )
        else:
            self._safe_notify(
                f"⚠️ Entry order {action} {quantity} {contract.localSymbol or contract.symbol} "
                f"finished with status={trade.orderStatus.status}, no fill price."
            )

        return fill_price

    def place_exit_bracket(
        self,
        contract: Contract,
        position_side: str,
        quantity: int,
        entry_price: float,
        tp_offset: float,
        sl_offset: float,
    ) -> Tuple[float, float]:
        """
        Place TP / SL as OCA bracket on broker side.

        position_side: 'LONG' or 'SHORT' (side of OPEN position)
        tp_offset, sl_offset: in points
        """
        exit_action = "SELL" if position_side.upper() == "LONG" else "BUY"

        if position_side.upper() == "LONG":
            tp_price = entry_price + tp_offset
            sl_price = entry_price - sl_offset
        else:
            tp_price = entry_price - tp_offset
            sl_price = entry_price + sl_offset

        oca_group = f"BRACKET_{int(time.time())}"

        tp_order = Order(
            action=exit_action,
            orderType="LMT",
            totalQuantity=quantity,
            lmtPrice=tp_price,
            tif="GTC",
            ocaGroup=oca_group,
            ocaType=1,
        )

        sl_order = Order(
            action=exit_action,
            orderType="STP",
            totalQuantity=quantity,
            auxPrice=sl_price,
            tif="GTC",
            ocaGroup=oca_group,
            ocaType=1,
        )

        # Save metadata for future notifications on fills
        desc = (
            f"{position_side.upper()} {quantity} {contract.localSymbol or contract.symbol} "
            f"entry={entry_price}"
        )
        self._oca_meta[oca_group] = desc

        self.ib.placeOrder(contract, tp_order)
        self.ib.placeOrder(contract, sl_order)

        logging.info(
            "Exit bracket placed: side=%s qty=%s TP=%s SL=%s OCA=%s",
            exit_action,
            quantity,
            tp_price,
            sl_price,
            oca_group,
        )

        self._safe_notify(
            f"📌 Bracket placed for {contract.localSymbol or contract.symbol} "
            f"({desc}): TP={tp_price}, SL={sl_price}"
        )

        return tp_price, sl_price

    def close_all_positions(self) -> None:
        """
        Force-close all open positions with market orders:
        - Long -> SELL MKT
        - Short -> BUY MKT

        Blocks until all orders are done.
        """
        positions = self.ib.positions()
        if not positions:
            logging.info("No open positions to close.")
            self._safe_notify("ℹ️ No open positions to close.")
            return

        logging.info("Closing all open positions via market orders...")
        self._safe_notify("⛔ CLOSE ALL: sending market orders to close all positions.")

        summary_lines: List[str] = []
        for pos in positions:
            contract = pos.contract
            qty = pos.position
            if qty == 0:
                continue

            action = "SELL" if qty > 0 else "BUY"
            order = Order(
                action=action,
                orderType="MKT",
                totalQuantity=abs(qty),
            )

            trade = self.ib.placeOrder(contract, order)
            logging.info(
                "Closing position: %s %s qty=%s",
                action,
                getattr(contract, "localSymbol", "") or getattr(contract, "symbol", ""),
                qty,
            )

            while not trade.isDone():
                self.ib.waitOnUpdate(timeout=5)

            fill_price = trade.orderStatus.avgFillPrice
            logging.info(
                "Closed %s %s at %s",
                getattr(contract, "localSymbol", "") or getattr(contract, "symbol", ""),
                qty,
                fill_price,
            )

            line = (
                f"{action} {abs(qty)} {contract.localSymbol or contract.symbol} "
                f"@ {fill_price} (status={trade.orderStatus.status})"
            )
            summary_lines.append(line)

        if summary_lines:
            self._safe_notify("✅ CLOSE ALL complete:\n" + "\n".join(summary_lines))
        else:
            self._safe_notify("ℹ️ CLOSE ALL: nothing to close after filtering positions.")

    # ---- event handlers ----

    def _on_exec_details(self, trade: Trade, fill: Fill) -> None:
        """
        Handle execution details for all orders.
        We use this to detect when TP/SL (bracket exits) are actually filled.
        """
        try:
            order = trade.order
            contract = trade.contract
            exec_data = fill.execution

            oca_group = getattr(order, "ocaGroup", "") or ""
            price = exec_data.price
            qty = exec_data.shares
            action = order.action

            # Only interested in our bracket exits
            if oca_group.startswith("BRACKET_"):
                base_desc = self._oca_meta.get(oca_group, "")
                msg = (
                    f"✅ Bracket exit filled: {contract.localSymbol or contract.symbol} "
                    f"{action} {qty} @ {price}.\n"
                )
                if base_desc:
                    msg += f"Base position: {base_desc}"
                self._safe_notify(msg)
        except Exception as exc:  # pragma: no cover
            logging.error("Error in _on_exec_details: %s", exc)