import logging
import time
import threading
import asyncio
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
        
        # Attach handler for order status changes (to track cancellations)
        self.ib.orderStatusEvent += self._on_order_status
        
        # Attach handler for IB API errors
        self.ib.errorEvent += self._on_error

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
                        logging.info("IB event loop stored: %s (running: %s)", self._loop, self._loop.is_running() if self._loop else None)
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

    def find_available_es_contracts(self) -> List[str]:
        """
        Находит доступные контракты ES через попытку квалификации с разными localSymbol.
        Возвращает список доступных контрактов в формате localSymbol.
        """
        available = []
        
        # Месяцы ES: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
        month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
        
        # Пробуем контракты на ближайшие 2 года (2025-2027)
        for year_suffix in ['5', '6', '7', '25', '26', '27']:
            for month_code in month_codes:
                local_symbol = f"ES{month_code}{year_suffix}"
                try:
                    contract = Future(localSymbol=local_symbol, exchange="CME", currency="USD")
                    contracts = self.ib.qualifyContracts(contract)
                    if contracts:
                        qualified = contracts[0]
                        expiry = getattr(qualified, 'lastTradeDateOrContractMonth', '')
                        available.append(f"{local_symbol} ({expiry})")
                        logging.debug(f"Found available contract: {local_symbol} ({expiry})")
                except Exception:
                    pass  # Игнорируем ошибки квалификации
        
        # Если не нашли через CME, пробуем без exchange
        if not available:
            for year_suffix in ['5', '6', '7', '25', '26', '27']:
                for month_code in month_codes:
                    local_symbol = f"ES{month_code}{year_suffix}"
                    try:
                        contract = Future(localSymbol=local_symbol, currency="USD")
                        contracts = self.ib.qualifyContracts(contract)
                        if contracts:
                            qualified = contracts[0]
                            expiry = getattr(qualified, 'lastTradeDateOrContractMonth', '')
                            available.append(f"{local_symbol} ({expiry})")
                            logging.debug(f"Found available contract: {local_symbol} ({expiry})")
                    except Exception:
                        pass
        
        return available

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
        - If still not found, try without exchange (IB will auto-detect).
        - For ES with YYYYMM format, also try to use localSymbol (e.g., ESH6 for 202603).
        """
        
        # Для ES фьючерсов, если формат YYYYMM, можем попробовать localSymbol
        # ES месяцы: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
        month_codes = {'01': 'F', '02': 'G', '03': 'H', '04': 'J', '05': 'K', '06': 'M',
                       '07': 'N', '08': 'Q', '09': 'U', '10': 'V', '11': 'X', '12': 'Z'}
        
        normalized_expiry = expiry
        local_symbols = []  # Try multiple localSymbol variants
        
        if len(expiry) == 6 and symbol.upper() == "ES":  # YYYYMM format for ES
            year = expiry[:4]
            month = expiry[4:6]
            year_int = int(year)
            year_code_single = year[-1]  # Last digit (6 for 2026)
            year_code_double = year[-2:]  # Last two digits (26 for 2026)
            
            if month in month_codes:
                # Try single digit year code (ESH6 for 2026)
                local_symbols.append(f"ES{month_codes[month]}{year_code_single}")
                # For years >= 2020, also try two-digit year code (ESH26 for 2026)
                if year_int >= 2020:
                    local_symbols.append(f"ES{month_codes[month]}{year_code_double}")
                # Также пробуем формат без года (только месяц) - IB может автоматически определить год
                # Но это маловероятно, поэтому пробуем в последнюю очередь
                logging.info(f"ES contract: calculated localSymbols={local_symbols} for expiry={expiry}")
        
        # Try multiple expiry formats
        expiry_formats = [expiry]  # Original format
        if len(expiry) == 6:  # YYYYMM
            expiry_formats.append(f"{expiry[:4]}-{expiry[4:6]}")  # YYYY-MM
            # Для ES фьючерсов пробуем также формат YYYYMMDD (дата экспирации)
            # ES обычно экспирируется в третью пятницу месяца
            year = expiry[:4]
            month = expiry[4:6]
            # Примерные даты экспирации для каждого месяца (третья пятница, приблизительно)
            expiry_dates = {
                '01': '15', '02': '19', '03': '20', '04': '17', '05': '15', '06': '19',
                '07': '17', '08': '21', '09': '18', '10': '16', '11': '20', '12': '18'
            }
            if month in expiry_dates:
                expiry_formats.append(f"{year}{month}{expiry_dates[month]}")  # YYYYMMDD
                expiry_formats.append(f"{year}-{month}-{expiry_dates[month]}")  # YYYY-MM-DD
                # Пробуем также другие даты вокруг третьей пятницы
                base_date = int(expiry_dates[month])
                for offset in [-2, -1, 1, 2]:
                    alt_date = base_date + offset
                    if 1 <= alt_date <= 31:
                        expiry_formats.append(f"{year}{month}{alt_date:02d}")  # YYYYMMDD
                        expiry_formats.append(f"{year}-{month}-{alt_date:02d}")  # YYYY-MM-DD
        
        logging.info(f"Trying to qualify contract: symbol={symbol}, expiry={expiry}, exchange={exchange}")
        logging.info(f"Expiry formats to try: {expiry_formats}")
        logging.info(f"LocalSymbol variants to try: {local_symbols}")
        
        qualified = None  # Инициализируем переменную
        
        def _try_qualify(exch: Optional[str] = None, use_local_symbol: bool = False, local_sym: Optional[str] = None, exp_format: Optional[str] = None) -> Optional[Future]:
            if use_local_symbol and local_sym:
                # Попытка с localSymbol
                logging.info(
                    "Trying to qualify contract with localSymbol: %s exchange=%s",
                    local_sym,
                    exch or "auto",
                )
                if exch:
                    contract = Future(
                        localSymbol=local_sym,
                        exchange=exch,
                        currency=currency,
                    )
                else:
                    # Без exchange - IB определит автоматически
                    contract = Future(
                        localSymbol=local_sym,
                        currency=currency,
                    )
            elif exch:
                exp_to_use = exp_format if exp_format else normalized_expiry
                logging.info(
                    "Trying to qualify contract: symbol=%s expiry=%s exchange=%s",
                    symbol,
                    exp_to_use,
                    exch,
                )
                contract = Future(
                    symbol=symbol,
                    lastTradeDateOrContractMonth=exp_to_use,
                    exchange=exch,
                    currency=currency,
                )
            else:
                exp_to_use = exp_format if exp_format else normalized_expiry
                logging.info(
                    "Trying to qualify contract without exchange (auto-detect): symbol=%s expiry=%s",
                    symbol,
                    exp_to_use,
                )
                contract = Future(
                    symbol=symbol,
                    lastTradeDateOrContractMonth=exp_to_use,
                    currency=currency,
                )
            try:
                contracts = self.ib.qualifyContracts(contract)
                if not contracts:
                    logging.warning(
                        "No contract found for %s %s on exchange %s",
                        symbol,
                        exp_to_use if not use_local_symbol else local_sym,
                        exch or "auto",
                    )
                    return None
                qualified = contracts[0]
                logging.info("Qualified contract: %s", qualified)
                return qualified
            except Exception as exc:
                logging.warning("Exception during contract qualification: %s", exc)
                return None
        
        # Для ES контрактов пробуем localSymbol ПЕРВЫМ, т.к. это самый надежный способ
        if not qualified and local_symbols and symbol.upper() == "ES":
            logging.info("Trying localSymbol FIRST for ES contract (most reliable method)")
            for local_sym in local_symbols:
                qualified = _try_qualify("CME", use_local_symbol=True, local_sym=local_sym)
                if qualified:
                    logging.info(f"Successfully qualified ES contract using localSymbol: {local_sym}")
                    return qualified
                # Также пробуем без exchange
                qualified = _try_qualify(None, use_local_symbol=True, local_sym=local_sym)
                if qualified:
                    logging.info(f"Successfully qualified ES contract using localSymbol (no exchange): {local_sym}")
                    return qualified
        
        # Try primary exchange with different expiry formats
        for exp_fmt in expiry_formats:
            qualified = _try_qualify(exchange, exp_format=exp_fmt)
            if qualified:
                return qualified
        
        # ES on GLOBEX fallback to CME with different expiry formats
        if not qualified and exchange.upper() == "GLOBEX":
            for exp_fmt in expiry_formats:
                qualified = _try_qualify("CME", exp_format=exp_fmt)
                if qualified:
                    return qualified
        
        # Try with localSymbol variants (with CME) - если еще не пробовали
        if not qualified and local_symbols:
            for local_sym in local_symbols:
                qualified = _try_qualify("CME", use_local_symbol=True, local_sym=local_sym)
                if qualified:
                    return qualified
        
        # Try with localSymbol without exchange (auto-detect)
        if not qualified and local_symbols:
            for local_sym in local_symbols:
                qualified = _try_qualify(None, use_local_symbol=True, local_sym=local_sym)
                if qualified:
                    return qualified
        
        # Try with GLOBEX and localSymbol
        if not qualified and local_symbols:
            for local_sym in local_symbols:
                qualified = _try_qualify("GLOBEX", use_local_symbol=True, local_sym=local_sym)
                if qualified:
                    return qualified
        
        # Last resort: try without exchange (IB auto-detect) with different expiry formats
        if not qualified:
            for exp_fmt in expiry_formats:
                qualified = _try_qualify(None, exp_format=exp_fmt)
                if qualified:
                    return qualified

        if not qualified:
            # Пробуем найти доступные контракты ES
            logging.info("Trying to find available ES contracts")
            available_expiries = []
            
            try:
                # Используем новую функцию для поиска доступных контрактов
                available_expiries = self.find_available_es_contracts()
                
                if available_expiries:
                    logging.info(f"Available ES contracts found: {available_expiries[:10]}")
                else:
                    # Fallback: проверяем открытые позиции
                    try:
                        positions = self.ib.positions()
                        for pos in positions:
                            if pos.contract.symbol == "ES":
                                local_sym = getattr(pos.contract, 'localSymbol', '')
                                expiry = getattr(pos.contract, 'lastTradeDateOrContractMonth', '')
                                if local_sym or expiry:
                                    available_expiries.append(f"{local_sym} ({expiry})" if local_sym else expiry)
                    except Exception as pos_exc:
                        logging.debug(f"Could not get contracts from positions: {pos_exc}")
            except Exception as exc:
                logging.warning(f"Error finding available contracts: {exc}")
            
            # Формируем сообщение об ошибке
            if available_expiries:
                error_msg = (
                    f"Cannot qualify future contract for {symbol} {expiry} "
                    f"on {exchange} or fallback.\n"
                    f"Tried formats: {expiry_formats}, localSymbols: {local_symbols}.\n\n"
                    f"✅ Available ES contracts found:\n"
                    f"{chr(10).join(['  - ' + exp for exp in available_expiries[:10]])}\n\n"
                    f"❌ Contract ES {expiry} (March 2026) is NOT available in IB.\n"
                    f"Please update config.yaml with an available contract.\n"
                    f"For example, use expiry from the list above (format: YYYYMM)."
                )
            else:
                error_msg = (
                    f"Cannot qualify future contract for {symbol} {expiry} "
                    f"on {exchange} or fallback.\n"
                    f"Tried formats: {expiry_formats}, localSymbols: {local_symbols}.\n"
                    f"Could not retrieve available contracts list.\n"
                    f"Contract ES {expiry} may not be available yet in IB. "
                    f"Please check TWS/IB Gateway."
                )
            
            logging.error(
                f"Failed to qualify contract after trying all formats:\n"
                f"  Symbol: {symbol}\n"
                f"  Expiry: {expiry}\n"
                f"  Exchange: {exchange}\n"
                f"  Tried expiry formats: {expiry_formats}\n"
                f"  Tried localSymbols: {local_symbols}"
            )
            raise RuntimeError(error_msg)

        return qualified

    # ---- positions helpers ----

    def refresh_positions(self) -> List:
        """
        Return latest known positions from IB cache.

        ВАЖЛИВО:
        - Не викликаємо тут ib.reqPositions(), бо цей метод часто викликається
          з Telegram-потоку, де немає asyncio event loop.
        - ib_insync автоматично оновлює positions при підключенні та подальших апдейтах.
        """
        ib = self.ib
        try:
            positions = list(ib.positions())
            logging.info("Cached positions: %s", positions)
            return positions
        except Exception as exc:
            logging.exception("Failed to read positions: %s", exc)
            self._safe_notify(f"❌ Failed to read positions: {exc}")
            return []

    # ВАЖЛИВО: НЕ МЕНЯТЬ ЭТУ ФУНКЦИЮ!
    # Она гарантированно запрашивает свежие позиции напрямую с брокера, а не из кеша.
    # Использует thread-safe подход через run_coroutine_threadsafe для работы из любого потока.
    def get_positions_from_broker(self) -> List:
        """
        Request fresh positions directly from broker and return them.
        Always requests positions from broker, waits for update, then returns.
        Thread-safe: works from any thread (including Telegram command loop).
        
        ВАЖЛИВО: НЕ МЕНЯТЬ! Эта функция должна всегда тянуть данные напрямую с брокера.
        НЕ возвращает кеш - только свежие данные с брокера или пустой список при ошибке.
        Использует positionEvent для надежного ожидания обновления позиций.
        """
        ib = self.ib
        if not ib.isConnected():
            logging.warning("IB not connected, cannot get positions from broker")
            return []
        
        ib_loop = self._loop
        
        try:
            if ib_loop is not None and not ib_loop.is_closed():
                logging.info("get_positions_from_broker: requesting fresh positions from broker")
                
                # Используем синхронный подход через call_soon_threadsafe
                import concurrent.futures
                import threading
                
                position_requested = threading.Event()
                request_error = None
                
                def _do_req_positions():
                    """Выполняем reqPositions в правильном event loop."""
                    try:
                        ib.reqPositions()
                        position_requested.set()
                    except Exception as exc:
                        nonlocal request_error
                        request_error = exc
                        position_requested.set()
                
                # Вызываем reqPositions в правильном loop
                ib_loop.call_soon_threadsafe(_do_req_positions)
                
                # Ждем завершения запроса (максимум 2 секунды)
                if position_requested.wait(timeout=2.0):
                    if request_error:
                        logging.warning(f"get_positions_from_broker: reqPositions() error: {request_error}, but continuing")
                else:
                    logging.warning("get_positions_from_broker: reqPositions() call timed out, but continuing")
                
                # Ждем обновления позиций в кеше (IB обновит их асинхронно)
                time.sleep(3.0)
                logging.info("get_positions_from_broker: request completed")
            else:
                if ib_loop is None:
                    logging.error("get_positions_from_broker: no ib_loop available - NO CACHE FALLBACK")
                else:
                    logging.error("get_positions_from_broker: ib_loop is closed - NO CACHE FALLBACK")
                raise RuntimeError("Cannot get positions from broker: event loop not available")
            
            # Читаем позиции после успешного запроса
            positions = list(ib.positions())
            logging.info(f"Positions refreshed from broker: {len(positions)} positions found")
            if positions:
                for pos in positions:
                    logging.info(f"  Position: {pos.contract.localSymbol} qty={pos.position}")
            return positions
        except RuntimeError:
            # Пробрасываем RuntimeError дальше (не возвращаем кеш)
            raise
        except Exception as exc:
            logging.exception("Failed to refresh positions from broker: %s", exc)
            # НЕ возвращаем кеш - выбрасываем ошибку
            raise RuntimeError(f"Failed to get positions from broker: {exc}")

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

        # 2) Взяти поточні позиції из кеша (ib_insync автоматически обновляет их)
        try:
            # Используем кешированные позиции - они обновляются автоматически
            # Не вызываем reqPositions() здесь, т.к. event loop уже запущен
            positions = list(ib.positions() or [])
            logging.info(f"CLOSE ALL: found {len(positions)} positions to close")
            if positions:
                for pos in positions:
                    logging.info(f"  Position to close: {pos.contract.localSymbol} qty={pos.position}")
            else:
                logging.info("CLOSE ALL: no positions found in cache")
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
            logging.info(f"CLOSE ALL: processing position: {contract.localSymbol} qty={qty}")
            
            if abs(qty) < 0.001:  # Игнорируем нулевые позиции
                logging.info(f"CLOSE ALL: skipping position {contract.localSymbol} - zero quantity")
                continue

            symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
            action = "SELL" if qty > 0 else "BUY"
            account = pos.account
            
            logging.info(f"CLOSE ALL: preparing to close {symbol}: action={action}, qty={abs(qty)}, account={account}")

            # Переконатися, що exchange встановлено для контракту
            if not contract.exchange:
                logging.info(f"CLOSE ALL: exchange not set for {symbol}, trying to set it...")
                if hasattr(contract, 'primaryExchange') and contract.primaryExchange:
                    contract.exchange = contract.primaryExchange
                    logging.info(f"Set exchange to {contract.exchange} (from primaryExchange) for {symbol}")
                elif contract.localSymbol and contract.localSymbol.startswith('ES'):  # Fallback для всех ES контрактов
                    contract.exchange = 'CME'
                    logging.info(f"Set exchange to CME (fallback for ES) for {symbol}")
                else:
                    try:
                        logging.info(f"Qualifying contract {symbol} to get exchange...")
                        qualified = ib.qualifyContracts(contract)
                        if qualified and qualified[0].exchange:
                            contract.exchange = qualified[0].exchange
                            logging.info(f"Set exchange to {contract.exchange} (from qualification) for {symbol}")
                    except Exception as exc:
                        logging.warning(f"Failed to qualify contract {symbol}: {exc}")
            
            # Если exchange все еще не установлен, устанавливаем CME по умолчанию для ES
            if not contract.exchange and (symbol.startswith('ES') or (contract.localSymbol and contract.localSymbol.startswith('ES'))):
                contract.exchange = 'CME'
                logging.info(f"Set exchange to CME (default for ES) for {symbol}")
            
            # Финальная проверка - если exchange все еще не установлен, это критическая ошибка
            if not contract.exchange:
                error_msg = f"Cannot close position for {symbol}: exchange is not set"
                logging.error(error_msg)
                line = f"{symbol} FAILED: exchange not set"
                summary_lines.append(line)
                continue

            order = Order(
                action=action,
                orderType="MKT",
                totalQuantity=abs(qty),
                account=account,
                outsideRth=True,  # Allow closing outside regular trading hours
            )

            try:
                logging.info(f"Placing CLOSE order: {action} {abs(qty)} {symbol} on exchange {contract.exchange}")
                
                # Проверяем соединение перед отправкой ордера
                if not ib.isConnected():
                    raise ConnectionError("IB is not connected, cannot place order")
                
                trade = ib.placeOrder(contract, order)
                logging.info(
                    "Closing position (fire-and-forget): %s %s qty=%s orderId=%s exchange=%s",
                    action,
                    symbol,
                    qty,
                    trade.order.orderId,
                    contract.exchange,
                )
                
                # Ждем выполнения ордера (максимум 10 секунд)
                # Это важно, чтобы убедиться, что ордер действительно выполнен
                max_wait = 10.0
                waited = 0.0
                check_interval = 0.5
                
                while not trade.isDone() and waited < max_wait:
                    ib.sleep(check_interval)
                    waited += check_interval
                    current_status = trade.orderStatus.status
                    logging.debug(f"Order {trade.order.orderId} status: {current_status} (waited {waited:.1f}s)")
                
                # Проверяем финальный статус
                final_status = trade.orderStatus.status
                fill_price = trade.orderStatus.avgFillPrice
                
                if final_status == "Filled":
                    logging.info(f"Order {trade.order.orderId} FILLED at {fill_price}")
                    line = f"{action} {abs(qty)} {symbol} ✅ FILLED @ {fill_price} (orderId={trade.order.orderId})"
                elif final_status in ["Cancelled", "Inactive"]:
                    logging.warning(f"Order {trade.order.orderId} was {final_status}")
                    line = f"{action} {abs(qty)} {symbol} ⚠️ {final_status} (orderId={trade.order.orderId})"
                else:
                    logging.info(f"Order {trade.order.orderId} status: {final_status} (still pending)")
                    line = f"{action} {abs(qty)} {symbol} ⏳ {final_status} (orderId={trade.order.orderId}, may fill later)"
                
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

            # Only interested in our bracket exits
            if not oca_group.startswith("BRACKET_"):
                return

            base_desc = self._oca_meta.get(oca_group, "")
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

            self._safe_notify(msg)

        except Exception as exc:  # pragma: no cover
            logging.error("Error in _on_exec_details: %s", exc)

    def _on_order_status(self, trade: Trade) -> None:
        """
        Handle order status changes.
        This is useful for tracking cancellations.
        orderStatusEvent provides Trade object, not Order.
        """
        try:
            order = trade.order
            status = trade.orderStatus.status
            order_id = order.orderId
            
            # Логируем все статусы для отслеживания
            logging.info(f"Order {order_id} status changed: {status}")
            
            if status == "Cancelled":
                oca_group = getattr(order, "ocaGroup", "") or ""
                error_msg = ""
                if trade.orderStatus.whyHeld:
                    error_msg = f" reason: {trade.orderStatus.whyHeld}"
                
                logging.warning(f"Order {order_id} cancelled{error_msg}")
                if oca_group.startswith("BRACKET_"):
                    self._safe_notify(f"⚠️ Order {order_id} cancelled: {status} (OCA group: {oca_group}){error_msg}")
                else:
                    # Также уведомляем о отмене CLOSE ALL ордеров
                    self._safe_notify(f"⚠️ Order {order_id} cancelled: {status}{error_msg}")
            elif status == "Filled":
                logging.info(f"Order {order_id} filled: {trade.orderStatus.filled} @ {trade.orderStatus.avgFillPrice}")
            elif status in ["PendingSubmit", "PreSubmitted", "Submitted"]:
                logging.debug(f"Order {order_id} in progress: {status}")
        except Exception as exc:
            logging.exception("Error in _on_order_status: %s", exc)

    def _on_error(self, reqId: int, errorCode: int, errorString: str, contract: Optional[Contract] = None) -> None:
        """Handle IB API errors."""
        # Skip informational messages (errorCode < 1000)
        if errorCode < 1000:
            return
        
        # Информационные сообщения о соединении - логируем как INFO/WARNING, не ERROR
        warning_codes = {
            1100: "Connectivity lost",  # Connectivity between IBKR and TWS has been lost
            2103: "Market data farm connection is broken",  # usfarm broken
            2105: "HMDS data farm connection is broken",  # ushmds broken
            2157: "Sec-def data farm connection is broken",  # secdefil broken
        }
        
        info_codes = {
            1102: "Connectivity restored",  # Connectivity restored - data maintained
            2104: "Market data farm connection is OK",  # usfarm
            2106: "HMDS data farm connection is OK",  # ushmds
            2158: "Sec-def data farm connection is OK",  # secdefil
        }
        
        if errorCode in warning_codes:
            logging.warning(
                "IB warning: reqId=%s code=%s msg=%s",
                reqId,
                errorCode,
                errorString,
            )
            return
        
        if errorCode in info_codes:
            logging.info(
                "IB info: reqId=%s code=%s msg=%s",
                reqId,
                errorCode,
                errorString,
            )
            return
            
        # Log all other errors as ERROR
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
