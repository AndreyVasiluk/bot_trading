import logging
import time
import threading
from typing import Callable, Optional, Tuple, List, Dict

from ib_insync import IB, Future, Order, Contract, Trade, Fill
from ib_insync.util import getLoop


class IBClient:
    """
    Thin wrapper around ib_insync.IB with:
    - auto-retry connect()
    - helpers to create future contract
    - market entry
    - TP/SL bracket placement
    - close-all via market orders
    - optional notify callback for Telegram messages
    - execDetails hook for TP/SL fills (bracket exits + PnL)
    """

    def __init__(self, host: str, port: int, client_id: int) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()

        # Event loop, в якому працює IB (заповнюється після connect()).
        self._loop = None  # type: ignore

        # Simple callback that will be set from main() to send messages to Telegram.
        self._notify: Callable[[str], None] = lambda msg: None

        # Map OCA group -> human-readable description (entry side/qty/symbol)
        self._oca_meta: Dict[str, str] = {}

        # Attach handler for execution details (fills of any orders)
        self.ib.execDetailsEvent += self._on_exec_details
        logging.info("execDetailsEvent handler attached: %s", self._on_exec_details)
        
        # Attach handler for order status changes (to track cancellations)
        self.ib.orderStatusEvent += self._on_order_status
        
        # Attach handler for IB API errors
        self.ib.errorEvent += self._on_error
        
        # 🔧 Подписываемся на обновления позиций
        self.ib.positionEvent += self._on_position_update

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
                print(
                    f"Connecting to {self.host}:{self.port} "
                    f"with clientId {self.client_id}..."
                )
                self.ib.connect(self.host, self.port, clientId=self.client_id)

                if self.ib.isConnected():
                    # Зберігаємо loop, в якому працює IB.
                    try:
                        self._loop = getLoop()
                        logging.info("IB event loop stored: %s", self._loop)
                    except Exception as exc:
                        logging.error("Failed to get IB event loop: %s", exc)
                        self._loop = None

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
            self._safe_notify("⚠️ Disconnected from IB Gateway/TWS.")

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
        - Supports both YYYYMM and YYYYMMDD formats for expiry.
        """
        
        # Normalize expiry format: if YYYYMM, try to find the contract
        # For ES futures, expiry is typically the 3rd Friday of the month
        # But IB API usually needs full date or contract month format
        normalized_expiry = expiry
        if len(expiry) == 6:  # YYYYMM format
            # Try to find contract by searching for the month
            # IB usually accepts YYYYMM format for contract month
            # But we might need to try different formats
            logging.info(f"Expiry format YYYYMM detected: {expiry}, using as-is for qualification")

        def _try_qualify(exch: str) -> Optional[Future]:
            logging.info(
                "Trying to qualify contract: symbol=%s expiry=%s exchange=%s",
                symbol,
                normalized_expiry,
                exch,
            )
            contract = Future(
                symbol=symbol,
                lastTradeDateOrContractMonth=normalized_expiry,
                exchange=exch,
                currency=currency,
            )
            contracts = self.ib.qualifyContracts(contract)
            if not contracts:
                logging.warning(
                    "No contract found for %s %s on exchange %s",
                    symbol,
                    normalized_expiry,
                    exch,
                )
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

    # ---- positions helpers ----

    def refresh_positions(self) -> List:
        """
        Return latest known positions from IB cache.
        Явно запрашивает обновление позиций у брокера через event loop.
        """
        ib = self.ib
        try:
            # Пытаемся явно обновить позиции через event loop
            ib_loop = self._loop
            if ib_loop and ib_loop.is_running():
                # Используем call_soon_threadsafe для безопасного вызова из worker thread
                import asyncio
                
                def _request_positions():
                    try:
                        ib.reqPositions()
                    except Exception as exc:
                        logging.warning("reqPositions failed in event loop: %s", exc)
                
                try:
                    # Планируем задачу на event loop
                    ib_loop.call_soon_threadsafe(_request_positions)
                    # Даем время на обновление (но не блокируем слишком долго)
                    import time
                    time.sleep(0.8)  # Увеличиваем время для получения ответа от брокера
                except Exception as exc:
                    logging.warning("Failed to request positions update via event loop: %s (using cached positions)", exc)
            else:
                # Если нет event loop, пытаемся синхронно (работает только в main thread)
                try:
                    ib.reqPositions()
                    ib.sleep(0.8)
                except Exception as exc:
                    logging.warning("Failed to request positions update: %s (using cached positions)", exc)
            
            # Возвращаем позиции (обновленные через reqPositions или из кеша)
            positions = list(ib.positions())
            logging.info("Refreshed positions: %s", positions)
            return positions
        except Exception as exc:
            logging.exception("Failed to read positions: %s", exc)
            self._safe_notify(f"❌ Failed to read positions: {exc}")
            return []

    # ---- trading helpers ----

    def market_entry(self, contract: Contract, side: str, quantity: int) -> float:
        """
        Place a market order to open position.
        side: 'LONG' -> BUY, 'SHORT' -> SELL
        Returns: average fill price.
        Blocks until order is done (Filled/Cancelled).
        """
        if not self.ib.isConnected():
            msg = "❌ Cannot place market entry: IB is not connected."
            logging.error(msg)
            self._safe_notify(msg)
            raise ConnectionError("IB not connected in market_entry")

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
        logging.info(
            "Market order status: %s avgFillPrice=%s",
            trade.orderStatus.status,
            fill_price,
        )

        if fill_price > 0:
            self._safe_notify(
                f"✅ Entry filled: {action} {quantity} "
                f"{contract.localSymbol or contract.symbol} @ {fill_price}"
            )
        else:
            self._safe_notify(
                f"⚠️ Entry order {action} {quantity} "
                f"{contract.localSymbol or contract.symbol} "
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
        if not self.ib.isConnected():
            msg = "❌ Cannot place exit bracket: IB is not connected."
            logging.error(msg)
            self._safe_notify(msg)
            raise ConnectionError("IB not connected in place_exit_bracket")

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
            f"{position_side.upper()} {quantity} "
            f"{contract.localSymbol or contract.symbol} entry={entry_price}"
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

    # ---- CLOSE ALL (thread-safe wrapper + core) ----

    def close_all_positions(self) -> None:
        """
        Thread-safe wrapper.

        Якщо ми в тому ж треді, де loop IB — викликаємо core напряму.
        Якщо в іншому треді (Telegram worker) — кидаємо задачу в loop через
        call_soon_threadsafe і повертаємось.
        """
        ib_loop = self._loop

        # Якщо loop ще не збережений — робимо best-effort у поточному треді.
        if ib_loop is None:
            logging.warning(
                "IB loop is not set; running close_all_positions core in current thread."
            )
            self._close_all_positions_core()
            return

        # Якщо це той самий тред, де живе loop (зазвичай main) —
        # просто викликаємо core.
        if threading.current_thread() is threading.main_thread():
            self._close_all_positions_core()
            return

        # Інакше — ми в іншому треді (Telegram worker): тимчасово встановлюємо
        # правильний event loop для поточного потоку і виконуємо core
        logging.info("Executing _close_all_positions_core() in worker thread with correct event loop...")
        import asyncio
        
        # Тимчасово встановлюємо правильний event loop для поточного потоку
        # щоб ib.placeOrder() міг його знайти
        old_loop = None
        try:
            old_loop = asyncio.get_event_loop()
        except RuntimeError:
            pass
        
        # Встановлюємо правильний loop для поточного потоку
        asyncio.set_event_loop(ib_loop)
        try:
            self._close_all_positions_core()
        finally:
            # Відновлюємо старий loop (якщо був)
            if old_loop is not None:
                asyncio.set_event_loop(old_loop)
            else:
                asyncio.set_event_loop(None)

    def _close_all_positions_core(self) -> None:
        """
        Реальна логіка CLOSE ALL.

        Викликати тільки з треда, де доступний event loop IB
        (або через close_all_positions(), яка керує цим).
        """
        ib = self.ib

        if not ib.isConnected():
            msg = "❌ Cannot CLOSE ALL: IB is not connected."
            logging.error(msg)
            self._safe_notify(msg)
            return

        # 1) Скасувати всі відкриті ордери (TP/SL, ліміти тощо), використовуючи кешовані openTrades()
        try:
            open_trades = list(ib.openTrades() or [])
        except Exception as exc:
            logging.exception("Failed to read openTrades in CLOSE ALL: %s", exc)
            open_trades = []

        if open_trades:
            logging.info("Cancelling all open orders before closing positions (cached openTrades)...")
            for t in open_trades:
                order = t.order
                try:
                    logging.info("Cancel order: %s", order)
                    ib.cancelOrder(order)
                except Exception as exc:
                    logging.exception("Error cancelling order %s: %s", order, exc)
                    self._safe_notify(
                        f"❌ Error cancelling order `{getattr(order, 'orderId', '?')}`: `{exc}`"
                    )

        # 2) Взяти поточні позиції з кешу
        try:
            positions = list(ib.positions() or [])
        except Exception as exc:
            logging.exception("Failed to read positions in CLOSE ALL: %s", exc)
            self._safe_notify(f"❌ Cannot read positions for CLOSE ALL: `{exc}`")
            return

        if not positions:
            logging.info("No open positions to close (cached positions empty).")
            self._safe_notify("ℹ️ No open positions to close.")
            return

        logging.info("Closing all open positions via market orders (fire-and-forget)...")
        self._safe_notify("⛔ CLOSE ALL: sending market orders to close all positions (no wait for fills).")

        summary_lines: List[str] = []

        for pos in positions:
            contract = pos.contract
            qty = pos.position
            if not qty:
                continue

            symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
            action = "SELL" if qty > 0 else "BUY"
            account = pos.account

            # Переконатися, що exchange встановлено для контракту
            if not contract.exchange:
                if hasattr(contract, 'primaryExchange') and contract.primaryExchange:
                    contract.exchange = contract.primaryExchange
                    logging.info(f"Set exchange to {contract.exchange} (from primaryExchange) for {symbol}")
                elif contract.localSymbol == 'ESZ5':  # Fallback для ES
                    contract.exchange = 'CME'
                    logging.info(f"Set exchange to CME (fallback) for {symbol}")
                else:
                    try:
                        logging.info(f"Qualifying contract {symbol} to get exchange...")
                        qualified = ib.qualifyContracts(contract)
                        if qualified and qualified[0].exchange:
                            contract.exchange = qualified[0].exchange
                            logging.info(f"Set exchange to {contract.exchange} (from qualification) for {symbol}")
                    except Exception as exc:
                        logging.warning(f"Failed to qualify contract {symbol}: {exc}")

            order = Order(
                action=action,
                orderType="MKT",
                totalQuantity=abs(qty),
                account=account,
            )

            try:
                ib.placeOrder(contract, order)
                logging.info(
                    "Closing position (fire-and-forget): %s %s qty=%s",
                    action,
                    symbol,
                    qty,
                )
                line = f"{action} {abs(qty)} {symbol} (order sent)"
            except Exception as exc:
                logging.exception(
                    "Error placing CLOSE ALL order for %s %s: %s",
                    symbol,
                    qty,
                    exc,
                )
                line = (
                    f"{action} {abs(qty)} {symbol} "
                    f"FAILED to send order: `{exc}`"
                )

            summary_lines.append(line)

        if summary_lines:
            self._safe_notify(
                "✅ CLOSE ALL orders sent (fire-and-forget):\n" + "\n".join(summary_lines)
            )
        else:
            self._safe_notify(
                "ℹ️ CLOSE ALL: nothing was closed (no positions or all sends failed)."
            )

    # ---- event handlers ----

    def _on_exec_details(self, trade: Trade, fill: Fill) -> None:
        """
        Handle execution details for all orders.
        We use this to detect when TP/SL (bracket exits) are actually filled
        and відправити PnL.
        """
        try:
            order = trade.order
            contract = trade.contract
            exec_data = fill.execution

            oca_group = getattr(order, "ocaGroup", "") or ""
            price = exec_data.price
            qty = exec_data.shares
            action = order.action
            
            # 🔧 Логируем все fills для отладки
            logging.info(
                "execDetails: orderId=%s action=%s qty=%s price=%s ocaGroup=%s",
                getattr(order, 'orderId', 'N/A'),
                action,
                qty,
                price,
                oca_group,
            )

            # Only interested in our bracket exits
            if not oca_group.startswith("BRACKET_"):
                logging.debug("Not a bracket order, skipping: ocaGroup=%s", oca_group)
                return

            base_desc = self._oca_meta.get(oca_group, "")
            
            # 🔧 Если метаданные не найдены, все равно отправляем уведомление
            if not base_desc:
                logging.warning(
                    "Bracket order filled but no metadata found: ocaGroup=%s orderId=%s",
                    oca_group,
                    getattr(order, 'orderId', 'N/A'),
                )
                # Отправляем базовое уведомление без PnL
                msg = (
                    f"✅ Bracket exit filled: {contract.localSymbol or contract.symbol} "
                    f"{action} {qty} @ {price}.\n"
                    f"OrderId: {getattr(order, 'orderId', 'N/A')}"
                )
                self._safe_notify(msg)
                return
            
            msg = (
                f"✅ Bracket exit filled: {contract.localSymbol or contract.symbol} "
                f"{action} {qty} @ {price}.\n"
            )

            # Try to parse entry price and side from base_desc for PnL
            pnl_part = ""
            try:
                entry_price = None
                side = None

                if "LONG" in base_desc:
                    side = "LONG"
                elif "SHORT" in base_desc:
                    side = "SHORT"

                if "entry=" in base_desc:
                    # base_desc: "LONG 1 ESZ5 entry=6858.25"
                    after = base_desc.split("entry=", 1)[1]
                    entry_str = after.split()[0]
                    entry_price = float(entry_str)

                if side and entry_price is not None:
                    # PnL in points
                    sign = 1 if side == "LONG" else -1
                    points = (price - entry_price) * sign

                    # Multiplier (e.g. "50" for ES futures)
                    try:
                        multiplier = float(getattr(contract, "multiplier", "1") or "1")
                    except Exception:
                        multiplier = 1.0

                    money = points * multiplier * abs(qty)

                    currency = getattr(contract, "currency", "") or ""
                    pnl_part = (
                        f"PnL: {points:.2f} points, {money:.2f} {currency}".strip()
                    )
            except Exception as exc:
                logging.error("Failed to compute PnL for bracket exit: %s", exc)

            if base_desc:
                msg += f"Base position: {base_desc}"

            if pnl_part:
                msg += f"\n{pnl_part}"

            logging.info("Sending bracket exit notification: %s", msg)
            self._safe_notify(msg)

        except Exception as exc:  # pragma: no cover
            logging.error("Error in _on_exec_details: %s", exc)
            import traceback
            logging.error(traceback.format_exc())

    def _on_order_status(self, order: Order) -> None:
        """
        Handle order status changes.
        This is useful for tracking cancellations.
        """
        if order.status == "Cancelled":
            oca_group = getattr(order, "ocaGroup", "") or ""
            if oca_group.startswith("BRACKET_"):
                logging.info(f"Order {order.orderId} cancelled: {order.status} (OCA group: {oca_group})")
                self._safe_notify(f"⚠️ Order {order.orderId} cancelled: {order.status} (OCA group: {oca_group})")

    def _on_error(self, reqId: int, errorCode: int, errorString: str, contract: Optional[Contract] = None) -> None:
        """Handle IB API errors."""
        # Skip informational messages (errorCode < 1000)
        if errorCode < 1000:
            return
            
        # Log all errors
        if contract:
            symbol = getattr(contract, 'localSymbol', '') or getattr(contract, 'symbol', '')
            logging.error(
                "IB error: reqId=%s code=%s symbol=%s msg=%s",
                reqId,
                errorCode,
                symbol,
                errorString,
            )
        else:
            logging.error(
                "IB error: reqId=%s code=%s msg=%s",
                reqId,
                errorCode,
                errorString,
            )
        
        # Notify about critical errors (order-related)
        if errorCode in [201, 202, 399, 400, 401, 402, 403, 404, 405]:
            self._safe_notify(f"❌ IB order error {errorCode}: {errorString}")

    def _on_position_update(self, position) -> None:
        """Обработчик обновлений позиций от IB API."""
        logging.info("Position updated: %s qty=%s", 
                    getattr(position.contract, 'localSymbol', 'N/A'),
                    position.position)