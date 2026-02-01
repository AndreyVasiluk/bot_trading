import logging
import time
import threading
import asyncio
from typing import Callable, Optional, Tuple, List, Dict
from concurrent.futures import TimeoutError as FuturesTimeoutError

from ib_insync import IB, Future, Order, Contract, Trade, Fill, Position
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
        self._reconnecting = False  # Флаг переподключения
        self._loop_thread: Optional[threading.Thread] = None

        # Simple callback that will be set from main() to send messages to Telegram.
        self._notify: Callable[[str], None] = lambda msg: None

        # Track last connection error notification timestamps to avoid spam
        self._last_connection_notify: Dict[str, float] = {}

        # Map OCA group -> human-readable description (entry side/qty/symbol)
        self._oca_meta: Dict[str, str] = {}
        
        # Map conId -> last market price from portfolio updates
        self._portfolio_prices: Dict[int, float] = {}
        
        # Map conId -> last known position quantity (for tracking position closures)
        # Используется для предотвращения дублирования уведомлений о закрытии
        # Сохраняем даже после закрытия (с qty=0) чтобы знать что позиция была открыта
        self._last_positions: Dict[int, float] = {}
        
        # Map conId -> Contract object (for retrieving contract details when position is closed)
        # Используется для получения контракта закрытой позиции в position_monitor_loop
        self._position_contracts: Dict[int, Contract] = {}
        
        # Set of conIds that already received closure notification (to prevent duplicates)
        self._position_closed_notified: set = set()
        
        # Limit order tracking (for limit-entry mode)
        self._active_limit_order: Optional[Order] = None
        self._active_limit_trade: Optional[Trade] = None
        self._limit_order_lock = threading.Lock()
        
        # Флаг для предотвращения рекурсивных вызовов get_positions_from_broker()
        self._getting_positions = False

        # Attach handler for execution details (fills of any orders)
        self.ib.execDetailsEvent += self._on_exec_details
        
        # Attach handler for order status changes (to track cancellations)
        self.ib.orderStatusEvent += self._on_order_status
        
        # Attach handler for position changes (real-time monitoring)
        self.ib.positionEvent += self._on_position_change
        
        # Attach handler for portfolio updates (market prices)
        self.ib.updatePortfolioEvent += self._on_portfolio_update
        
        # Attach handler for IB API errors
        self.ib.errorEvent += self._on_error

    async def _call_sync_in_loop(self, func: Callable, *args, **kwargs):
        return func(*args, **kwargs)

    def _run_in_loop(self, func: Callable, *args, timeout: float = 10.0, **kwargs):
        if self._loop is None:
            raise RuntimeError("IB event loop is not available")
        future = asyncio.run_coroutine_threadsafe(
            self._call_sync_in_loop(func, *args, **kwargs), self._loop
        )
        return future.result(timeout=timeout)

    def _req_positions_async(self, context: str, *, timeout: float = 5.0) -> bool:
        """Request fresh positions via reqPositionsAsync through the IB loop."""
        if self._loop is None:
            logging.warning(
                "reqPositionsAsync skipped (%s): IB event loop unavailable", context
            )
            return False

        future = asyncio.run_coroutine_threadsafe(
            self.ib.reqPositionsAsync(), self._loop
        )
        try:
            future.result(timeout=timeout)
            return True
        except Exception as exc:
            logging.warning("reqPositionsAsync failed (%s): %s", context, exc)
            return False

    def _schedule_req_positions_with_event(
        self, event: threading.Event, context: str
    ) -> None:
        """Schedule reqPositionsAsync() inside the IB loop and set the event."""
        if self._loop is None:
            logging.debug(
                "Cannot schedule reqPositionsAsync (%s): IB loop unavailable", context
            )
            event.set()
            return

        def _runner() -> None:
            async def _req() -> None:
                try:
                    await self.ib.reqPositionsAsync()
                except Exception as exc:
                    logging.warning(
                        "reqPositionsAsync() error (%s): %s", context, exc
                    )
                finally:
                    event.set()

            asyncio.create_task(_req())

        self._loop.call_soon_threadsafe(_runner)

    def _qualify_contracts_async(
        self, contract: Contract, context: str, *, timeout: float = 10.0
    ) -> Optional[List[Future]]:
        """Run qualifyContractsAsync through the IB event loop."""
        if self._loop is None:
            logging.warning(
                "qualifyContracts (%s) skipped: IB event loop unavailable", context
            )
            return None

        qual_async = getattr(self.ib, "qualifyContractsAsync", None)
        if qual_async is None:
            logging.warning(
                "qualifyContractsAsync not available (%s), falling back to sync call", context
            )
            try:
                return self._run_in_loop(
                    self.ib.qualifyContracts, contract, timeout=timeout
                )
            except Exception as exc:
                logging.warning("qualifyContracts (sync) failed (%s): %s", context, exc)
                return None

        future = asyncio.run_coroutine_threadsafe(qual_async(contract), self._loop)
        try:
            return future.result(timeout=timeout)
        except Exception as exc:
            logging.warning(
                "qualifyContractsAsync failed (%s): %s", context, exc
            )
            return None

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
                logging.info(f"Sending notification to Telegram: {text[:100]}...")
                self._notify(text)
                logging.debug("Notification sent successfully")
        except Exception as exc:  # pragma: no cover
            logging.error("Notify callback failed: %s", exc)

    def _notify_connection_error(self, text: str, interval: float = 10.0) -> None:
        """Notify about connection issues but no more than once per `interval` seconds per message."""
        now = time.time()
        last_sent = self._last_connection_notify.get(text)
        if last_sent and now - last_sent < interval:
            logging.debug("Skipping duplicate connection notification: %s", text)
            return
        self._last_connection_notify[text] = now
        self._safe_notify(text)

    def _trigger_reconnect(self, reason: str) -> None:
        if self._reconnecting:
            logging.debug("Reconnect already in progress (%s)", reason)
            return

        def _reconnect_loop() -> None:
            try:
                self.disconnect()
            except Exception as exc:
                logging.debug("Error during disconnect before reconnect: %s", exc)
            try:
                self.connect()
                self._notify_connection_error("✅ Reconnected to IB Gateway/TWS.", interval=0)
            except Exception as exc:
                logging.error("Failed to reconnect: %s", exc)
                self._notify_connection_error(f"❌ Failed to reconnect: {exc}", interval=30)
            finally:
                self._reconnecting = False

        self._reconnecting = True
        threading.Thread(target=_reconnect_loop, daemon=True).start()

    def _reset_asyncio_loop_for_connect(self) -> None:
        """
        Make sure the current thread has a fresh asyncio event loop before calling IB connect.
        """
        try:
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            logging.debug("Reset asyncio event loop for connect()")
        except Exception as exc:
            logging.debug("Failed to reset asyncio loop before connect(): %s", exc)

    def _cleanup_ib_event_loop(self) -> None:
        """
        Stop and clear any IB event loop/thread that might be left from a previous attempt.
        """
        loop = getattr(self, "_loop", None)
        if loop:
            if loop.is_running():
                try:
                    loop.call_soon_threadsafe(loop.stop)
                    logging.debug("Stopped previous IB event loop")
                except Exception as exc:
                    logging.debug("Failed to stop previous IB loop: %s", exc)
            self._loop = None
        thread = getattr(self, "_loop_thread", None)
        if thread:
            if thread.is_alive():
                thread.join(timeout=1.0)
            self._loop_thread = None
            logging.debug("Joined previous IB loop thread")

    # ---- IB connection ----

    def connect(self) -> None:
        """
        Connect to IB Gateway / TWS with auto-retry loop.
        Blocks until successful connection.
        """
        # Проверяем, есть ли event loop в текущем потоке
        # Если нет - создаем новый для этого потока
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                # Event loop закрыт - создаем новый
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                logging.info("Created new event loop for connect()")
        except RuntimeError:
            # Нет event loop в текущем потоке - создаем новый
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            logging.info("Created new event loop for connect() (no existing loop)")
        
        while True:
            self._reset_asyncio_loop_for_connect()
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
                        # Если loop получен — запускаем его в отдельном потоке
                        if self._loop and not getattr(self, "_loop_thread", None):
                            def _run_loop():
                                asyncio.set_event_loop(self._loop)
                                self._loop.run_forever()

                            loop_thread = threading.Thread(target=_run_loop, daemon=True)
                            loop_thread.start()
                            self._loop_thread = loop_thread
                    except Exception as exc:
                        logging.error("Failed to get IB event loop: %s", exc)
                        self._loop = None

                    logging.info("Connected to IB Gateway")
                    self._safe_notify("✅ Connected to IB Gateway/TWS.")
                    
                    # Инициализируем кеш позиций через reqPositions() (socket-based)
                    try:
                        logging.info("Initializing positions cache via reqPositionsAsync() (socket)...")
                        self._req_positions_async("connect initialization", timeout=5.0)
                        # Ждем обновления кеша через positionEvent
                        self.ib.sleep(2.0)
                        initial_positions = list(self.ib.positions())
                        self._log_positions_source(initial_positions, "CACHE", "connect() initialization")
                        logging.info(f"Positions cache initialized: {len(initial_positions)} positions")
                        
                        # Инициализируем отслеживание состояния позиций
                        self._last_positions.clear()
                        self._position_closed_notified.clear()
                        for pos in initial_positions:
                            qty = float(pos.position)
                            if abs(qty) > 0.001:
                                con_id = pos.contract.conId
                                symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                                expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                                self._last_positions[con_id] = qty
                                self._position_contracts[con_id] = pos.contract
                                logging.info(
                                    f"✅ Initialized position tracking: {symbol} {expiry} "
                                    f"conId={con_id} qty={qty}"
                                )
                        if self._last_positions:
                            logging.info(
                                f"Position tracking initialized: {len(self._last_positions)} position(s) tracked"
                            )
                    except Exception as exc:
                        logging.warning(f"Failed to initialize positions cache: {exc}")
                    
                    return
                else:
                    logging.error("IB connection failed (isConnected() is False)")
            except Exception as exc:
                logging.error("API connection failed: %s", exc)
                logging.error("Make sure API port on TWS/IBG is open")
                self._notify_connection_error(f"❌ IB API connection error: {exc}")
                self._cleanup_ib_event_loop()

            logging.error("Connection error, retrying in 3 seconds...")
            time.sleep(3)

    def disconnect(self) -> None:
        if self.ib.isConnected():
            logging.info("Disconnecting")
            self.ib.disconnect()
            logging.info("Disconnected.")
            self._safe_notify("⚠️ Disconnected from IB Gateway/TWS.")
        if getattr(self, "_loop", None) and getattr(self, "_loop_thread", None):
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass
            self._loop_thread.join(timeout=1.0)
            self._loop_thread = None

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
                    contracts = self._run_in_loop(self.ib.qualifyContracts, contract)
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
                        contracts = self._run_in_loop(self.ib.qualifyContracts, contract)
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
            year = expiry[:4]
            month = expiry[4:6]
            expiry_dates = {
                '01': '15', '02': '19', '03': '20', '04': '17', '05': '15', '06': '19',
                '07': '17', '08': '21', '09': '18', '10': '16', '11': '20', '12': '18'
            }
            if month in expiry_dates:
                expiry_formats.append(f"{year}{month}{expiry_dates[month]}")  # YYYYMMDD
        
        logging.info(f"Trying to qualify contract: symbol={symbol}, expiry={expiry}, exchange={exchange}")
        logging.info(f"Expiry formats to try: {expiry_formats}")
        logging.info(f"LocalSymbol variants to try: {local_symbols}")
        
        qualified = None  # Инициализируем переменную
        
        # Сначала проверяем, есть ли уже открытая позиция по этому контракту
        # Если есть, используем её контракт напрямую (самый надежный способ)
        if symbol.upper() == "ES":
            try:
                # Получаем позиции напрямую от брокера
                positions = self.get_positions_from_broker()
                self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", "make_future_contract()")
                logging.info(f"Checking {len(positions)} existing positions for matching contract")
                for pos in positions:
                    pos_contract = pos.contract
                    pos_symbol = getattr(pos_contract, "symbol", "")
                    pos_local_sym = getattr(pos_contract, "localSymbol", "")
                    pos_expiry = getattr(pos_contract, 'lastTradeDateOrContractMonth', '')
                    
                    logging.info(f"  Checking position: symbol={pos_symbol}, localSymbol={pos_local_sym}, expiry={pos_expiry}")
                    
                    expiry_normalized = expiry.replace("-", "")
                    pos_expiry_normalized = pos_expiry.replace("-", "")
                    
                    if pos_symbol == symbol.upper():
                        if expiry_normalized in pos_expiry_normalized or pos_expiry_normalized.startswith(expiry_normalized):
                            logging.info(f"✅ Found matching position! Using contract from existing position: {pos_local_sym}")
                            qualified = pos_contract
                            logging.info(f"Using contract from existing position: conId={getattr(qualified, 'conId', 'N/A')}, localSymbol={getattr(qualified, 'localSymbol', 'N/A')}")
                            return qualified
                        if pos_local_sym in local_symbols:
                            logging.info(f"✅ Found matching position by localSymbol! Using contract: {pos_local_sym}")
                            qualified = pos_contract
                            logging.info(f"Using contract from existing position: conId={getattr(qualified, 'conId', 'N/A')}")
                            return qualified
            except Exception as exc:
                logging.warning(f"Error checking existing positions: {exc}")
        
        # fallback: если у нас есть контракт в кеше по localSymbol — используем его
        for cached_contract in self._position_contracts.values():
            try:
                cached_local = getattr(cached_contract, "localSymbol", "")
                if cached_local in local_symbols:
                    logging.info(f"✅ Using cached contract {cached_local} from _position_contracts")
                    return cached_contract
            except Exception as exc:
                logging.warning(f"Error checking existing positions: {exc}")
        
        def _select_preferred_contract(contracts: List[Future]) -> Optional[Future]:
            preferences = ["CME", "GLOBEX", "QBALGO"]
            for pref in preferences:
                for candidate in contracts:
                    if getattr(candidate, "exchange", "").upper() == pref:
                        return candidate
            return contracts[0] if contracts else None

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
                context = (
                    f"{symbol if not use_local_symbol else local_sym} "
                    f"{exp_to_use if not use_local_symbol else 'localSymbol'} "
                    f"{exch or 'auto'}"
                )
                contracts = self._qualify_contracts_async(contract, context)
                if not contracts:
                    logging.warning(
                        "No contract found for %s %s on exchange %s",
                        symbol if not use_local_symbol else "ES",
                        exp_to_use if not use_local_symbol else local_sym,
                        exch or "auto",
                    )
                    return None
                selected = _select_preferred_contract(contracts)
                if not selected:
                    logging.warning(
                        "Contract list returned but no preferred exchange match, using first entry"
                    )
                    selected = contracts[0]
                qualified = selected
                logging.info("✅ Qualified contract: %s", qualified)
                logging.info(
                    f"  conId={getattr(qualified, 'conId', 'N/A')}, "
                    f"localSymbol={getattr(qualified, 'localSymbol', 'N/A')}, "
                    f"expiry={getattr(qualified, 'lastTradeDateOrContractMonth', 'N/A')}"
                )
                return qualified
            except Exception as exc:
                logging.warning("Exception during contract qualification: %s", exc)
                logging.debug(
                    f"  Contract details: symbol={symbol if not use_local_symbol else local_sym}, "
                    f"exchange={exch}, expiry={exp_to_use if not use_local_symbol else 'N/A'}"
                )
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
        
        # Last resort: try without exchange (IB auto-detect) with different expiry formats
        if not qualified:
            for exp_fmt in expiry_formats:
                qualified = _try_qualify(None, exp_format=exp_fmt)
                if qualified:
                    return qualified

        # FALLBACK: Если квалификация не работает (event loop issues), 
        # создаем контракт напрямую для ES 202603 (ESH6)
        if not qualified and symbol.upper() == "ES" and expiry == "202603":
            logging.warning("All qualification attempts failed, trying direct contract creation for ES 202603")
            # Известный conId для ESH6 (ES март 2026) из предыдущих позиций
            known_con_id = 649180695
            try:
                # Пробуем создать контракт напрямую с conId
                logging.info(f"Creating contract directly with conId={known_con_id} (ESH6)")
                qualified = Future(conId=known_con_id, exchange="CME", currency=currency)
                logging.warning(f"⚠️ Using unqualified contract with conId {known_con_id}. This should work for ESH6.")
                return qualified
            except Exception as exc:
                logging.warning(f"Failed to create contract with conId: {exc}")
                # Пробуем через localSymbol
                try:
                    logging.info("Trying direct contract creation with localSymbol=ESH6")
                    qualified = Future(localSymbol="ESH6", exchange="CME", currency=currency)
                    logging.warning(f"⚠️ Using unqualified contract with localSymbol ESH6. This should work.")
                    return qualified
                except Exception as exc2:
                    logging.error(f"Failed to create contract with localSymbol: {exc2}")

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
                        # Получаем позиции напрямую от брокера
                        positions = self.get_positions_from_broker()
                        self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", "make_future_contract() fallback")
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

    def _log_positions_source(self, positions: List, source: str, caller: str = "") -> None:
        """
        Логирует источник позиций и их детали для отладки.
        
        Args:
            positions: Список позиций
            source: Источник данных ('CACHE', 'BROKER', 'EVENT', etc.)
            caller: Имя функции/метода, который запросил позиции
        """
        caller_info = f" [{caller}]" if caller else ""
        logging.info(f"📊 POSITIONS SOURCE{caller_info}: {source} - {len(positions)} position(s)")
        
        if positions:
            for pos in positions:
                qty = float(pos.position)
                con_id = pos.contract.conId
                symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                avg_cost = pos.avgCost
                
                if abs(qty) > 0.001:
                    logging.info(
                        f"  📊 {source}: {symbol} {expiry} conId={con_id} "
                        f"qty={qty} avgCost={avg_cost}"
                    )
                else:
                    logging.debug(
                        f"  📊 {source}: {symbol} {expiry} conId={con_id} "
                        f"qty={qty} (closed/zero)"
                    )
        else:
            logging.info(f"  📊 {source}: No positions found")
    
    def refresh_positions(self) -> List:
        """
        Return latest known positions from broker (not from cache).
        
        ИСПРАВЛЕНО: Теперь получает позиции напрямую от брокера через get_positions_from_broker().
        """
        logging.info("refresh_positions: getting positions directly from broker...")
        try:
            # Используем get_positions_from_broker() для получения данных напрямую от брокера
            positions = self.get_positions_from_broker()
            self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", "refresh_positions()")
            logging.info(f"refresh_positions: got {len(positions)} positions from broker")
            return positions
        except Exception as exc:
            logging.exception("Failed to refresh positions from broker: %s", exc)
            self._safe_notify(f"❌ Failed to refresh positions: {exc}")
            return []

    def force_sync_positions(self) -> List:
        """
        Принудительно синхронизировать позиции через сокет (reqPositions()).
        Получает позиции напрямую от брокера через get_positions_from_broker().
        
        Returns:
            List of Position objects from broker (not from cache)
        """
        logging.info("🔌 Force syncing positions via socket (reqPositions())...")
        self._safe_notify("🔄 Syncing positions via socket...")
        
        try:
            # Используем get_positions_from_broker() для получения данных напрямую от брокера
            positions = self.get_positions_from_broker()
            self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", "force_sync_positions()")
            
            logging.info(f"✅ Positions synced from broker: {len(positions)} total positions")
            
            # Логируем открытые позиции
            open_positions = [p for p in positions if abs(float(p.position)) > 0.001]
            if open_positions:
                logging.info(f"  Open positions ({len(open_positions)}):")
                for pos in open_positions:
                    symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                    expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                    qty = pos.position
                    logging.info(f"    {symbol} {expiry} qty={qty} avgCost={pos.avgCost}")
                self._safe_notify(f"✅ Positions synced: {len(open_positions)} open position(s)")
            else:
                logging.info("  No open positions")
                self._safe_notify("✅ Positions synced: no open positions")
            
            return positions
            
        except Exception as exc:
            logging.exception(f"Failed to force sync positions: {exc}")
            self._safe_notify(f"❌ Failed to sync positions: {exc}")
            return []

    async def _get_positions_from_broker_async(self) -> List:
        ib = self.ib
        if not ib.isConnected():
            logging.warning(
                "get_positions_from_broker (async): IB not connected, cannot fetch positions"
            )
            return []

        logging.info(
            "get_positions_from_broker (async): requesting fresh positions via reqPositionsAsync()"
        )
        try:
            await ib.reqPositionsAsync()
        except Exception as exc:
            logging.warning(
                f"get_positions_from_broker (async): reqPositionsAsync failed: {exc}"
            )

        await asyncio.sleep(2.0)

        try:
            positions = list(ib.positions())
            self._log_positions_source(
                positions, "BROKER (async)", "get_positions_from_broker()"
            )
            return positions
        except Exception as exc:
            logging.warning(
                f"get_positions_from_broker (async): cannot read positions cache: {exc}"
            )
            return []

    def get_positions_from_broker(self) -> List:
        if self._getting_positions:
            logging.debug(
                "get_positions_from_broker: recursive call detected, returning cache"
            )
            try:
                positions = list(self.ib.positions())
                self._log_positions_source(
                    positions, "CACHE (recursive call prevention)", "get_positions_from_broker()"
                )
                return positions
            except Exception:
                return []

        self._getting_positions = True
        try:
            if self._loop is None:
                logging.warning(
                    "get_positions_from_broker: IB loop unavailable, using cache"
                )
                return list(self.ib.positions())

            future = asyncio.run_coroutine_threadsafe(
                self._get_positions_from_broker_async(), self._loop
            )
            try:
                positions = future.result(timeout=20.0)
            except (asyncio.TimeoutError, FuturesTimeoutError) as exc:
                logging.warning(
                    "get_positions_from_broker: async request timed out after 20s (%s), retrying once...",
                    exc,
                )
                try:
                    positions = future.result(timeout=10.0)
                except (asyncio.TimeoutError, FuturesTimeoutError) as exc_retry:
                    logging.warning(
                        "get_positions_from_broker: retry timed out (%s), falling back to cache",
                        exc_retry,
                    )
                    raise asyncio.TimeoutError from exc_retry
            return positions
        except Exception as exc:
            logging.warning(
                f"get_positions_from_broker: failed to fetch via loop, falling back to cache: {exc}"
            )
            try:
                positions = list(self.ib.positions())
                self._log_positions_source(
                    positions, "CACHE (fallback)", "get_positions_from_broker()"
                )
                return positions
            except Exception:
                return []
        finally:
            self._getting_positions = False

    def get_market_price(self, contract: Contract, timeout: float = 5.0) -> Optional[float]:
        """
        Получить актуальную рыночную цену для контракта.
        Сначала проверяет кеш цен из portfolioEvent, затем пробует reqMktData.
        Returns: текущая цена или None при ошибке.
        """
        try:
            logging.info(f"get_market_price: called for {contract.localSymbol or contract.symbol} (thread={threading.current_thread().name})")
            
            if not self.ib.isConnected():
                logging.warning("IB not connected, cannot get market price")
                return None
            
            # Сначала проверяем кеш цен из portfolioEvent (самый быстрый способ)
            con_id = contract.conId
            if con_id in self._portfolio_prices:
                price = self._portfolio_prices[con_id]
                logging.info(f"Market price from portfolio cache for {contract.localSymbol or contract.symbol}: {price}")
                return price
            
            # Если нет в кеше, пробуем получить из текущего portfolio
            try:
                portfolio_items = self.ib.portfolio()
                for item in portfolio_items:
                    if (item.contract.conId == con_id or
                        (hasattr(item.contract, 'localSymbol') and 
                         hasattr(contract, 'localSymbol') and
                         item.contract.localSymbol == contract.localSymbol)):
                        if item.marketPrice and item.marketPrice > 0:
                            price = float(item.marketPrice)
                            # Сохраняем в кеш
                            self._portfolio_prices[con_id] = price
                            logging.info(f"Market price from portfolio for {contract.localSymbol or contract.symbol}: {price}")
                            return price
            except Exception as exc:
                logging.debug(f"Could not get price from portfolio: {exc}")
            
            # Если не получили из portfolio, пробуем reqMktData через event loop
            if self._loop is None:
                logging.warning(f"get_market_price: self._loop is None, cannot use reqMktData for {contract.localSymbol or contract.symbol}")
                return None
            
            logging.info(f"get_market_price: requesting price via reqMktData for {contract.localSymbol or contract.symbol} (thread={threading.current_thread().name})")
            
            # Убеждаемся, что у контракта есть exchange (требуется для reqMktData)
            if not contract.exchange or contract.exchange == '':
                logging.debug(f"get_market_price: contract missing exchange, qualifying: {contract.localSymbol or contract.symbol}")
                try:
                    qualified = self._run_in_loop(self.ib.qualifyContracts, contract)
                    if qualified:
                        contract = qualified[0]
                        logging.debug(f"get_market_price: contract qualified: exchange={contract.exchange}")
                    else:
                        logging.warning(f"get_market_price: could not qualify contract, trying default exchange")
                        if hasattr(contract, 'symbol') and contract.symbol == 'ES':
                            contract.exchange = 'GLOBEX'
                        elif hasattr(contract, 'localSymbol') and 'ES' in str(contract.localSymbol):
                            contract.exchange = 'GLOBEX'
                except Exception as qual_exc:
                    logging.warning(f"get_market_price: failed to qualify contract: {qual_exc}")
                    if hasattr(contract, 'symbol') and contract.symbol == 'ES':
                        contract.exchange = 'GLOBEX'
            
            # Используем call_soon_threadsafe для вызова reqMktData из worker thread
            result_event = threading.Event()
            result_price = [None]  # Используем список для передачи по ссылке
            result_error = [None]
            ticker_ref = [None]  # Для хранения ticker в event loop thread
            
            def _do_req_mkt_data():
                try:
                    logging.info(f"get_market_price: calling reqMktData in event loop thread for {contract.localSymbol or contract.symbol}")
                    ticker = self.ib.reqMktData(contract, '', False, False)
                    ticker_ref[0] = ticker
                    logging.info(f"get_market_price: reqMktData requested, waiting for data...")
                    
                    wait_time = 0.0
                    check_interval = 0.1
                    while wait_time < timeout:
                        if ticker.last:
                            price = float(ticker.last)
                            self.ib.cancelMktData(contract)
                            result_price[0] = price
                            logging.info(f"Market price from reqMktData (REAL-TIME) for {contract.localSymbol or contract.symbol}: {price}")
                            result_event.set()
                            return
                        
                        # Используем time.sleep() для ожидания данных
                        time.sleep(check_interval)
                        wait_time += check_interval
                    
                    if ticker.bid and ticker.ask:
                        price = (float(ticker.bid) + float(ticker.ask)) / 2.0
                        self.ib.cancelMktData(contract)
                        result_price[0] = price
                        logging.info(f"Market price (mid) from reqMktData (REAL-TIME) for {contract.localSymbol or contract.symbol}: {price}")
                        result_event.set()
                        return
                    

                    self.ib.cancelMktData(contract)
                    logging.warning(
                        f"Could not get market price from reqMktData for {contract.localSymbol or contract.symbol} "
                        f"(timeout={timeout}s, last={ticker.last}, bid={ticker.bid}, ask={ticker.ask})"
                    )
                    result_event.set()
                except Exception as exc:
                    logging.exception(f"Error in _do_req_mkt_data: {exc}")
                    result_error[0] = exc
                    try:
                        if ticker_ref[0]:
                            self.ib.cancelMktData(contract)
                    except Exception:
                        pass
                    result_event.set()
            
            # Вызываем через event loop из worker thread
            try:
                self._loop.call_soon_threadsafe(_do_req_mkt_data)
                logging.info(f"get_market_price: reqMktData scheduled in event loop, waiting for result (timeout={timeout + 1.0}s)...")
                
                # Ждем результата с таймаутом
                if result_event.wait(timeout=timeout + 1.0):
                    if result_error[0]:
                        logging.warning(f"get_market_price: error in reqMktData: {result_error[0]}")
                        return None
                    
                    if result_price[0] is not None:
                        # Сохраняем в кеш
                        self._portfolio_prices[con_id] = result_price[0]
                        return result_price[0]
                    else:
                        logging.warning(f"get_market_price: reqMktData returned None for {contract.localSymbol or contract.symbol}")
                        return None
                else:
                    logging.warning(f"get_market_price: reqMktData timeout for {contract.localSymbol or contract.symbol}")
                    return None
            except Exception as exc:
                logging.exception(f"Error scheduling reqMktData: {exc}")
                return None
        except Exception as exc:
            logging.exception(f"Error getting market price: {exc}")
            try:
                self.ib.cancelMktData(contract)
            except Exception:
                pass
            return None

    def get_position_status(self, position) -> Dict[str, Optional[float]]:
        """
        Получить полное состояние позиции: entry, SL, TP, current price.
        Returns: dict с ключами 'entry', 'sl', 'tp', 'current_price'
        """
        status = {
            'entry': None,
            'sl': None,
            'tp': None,
            'current_price': None
        }
        
        try:
            # Entry price из avgCost
            status['entry'] = float(position.avgCost) if position.avgCost else None
            
            # Получаем актуальную цену (может быть None если нет event loop)
            try:
                status['current_price'] = self.get_market_price(position.contract)
            except Exception as exc:
                logging.debug(f"Could not get market price: {exc}")
                status['current_price'] = None
            
            # Ищем TP/SL ордера для этой позиции через openTrades
            try:
                open_trades = self.ib.openTrades()
                position_con_id = position.contract.conId
                logging.debug(f"get_position_status: checking {len(open_trades)} open trades for conId={position_con_id}")
                
                # Собираем все ocaGroup для этого контракта
                oca_groups = set()
                for trade in open_trades:
                    if trade.contract.conId == position_con_id and trade.order:
                        oca_group = getattr(trade.order, 'ocaGroup', '')
                        if oca_group and oca_group.startswith('BRACKET_'):
                            oca_groups.add(oca_group)
                            logging.debug(f"Found ocaGroup: {oca_group} for conId={position_con_id}")
                
                # Ищем дочерние ордера (TP/SL) по parentId или ocaGroup
                for trade in open_trades:
                    if trade.contract.conId == position_con_id and trade.order:
                        order = trade.order
                        order_id = getattr(order, 'orderId', 0)
                        parent_id = getattr(order, 'parentId', 0)
                        oca_group = getattr(order, 'ocaGroup', '')
                        order_type = getattr(order, 'orderType', '')
                        
                        logging.debug(f"Checking order {order_id}: type={order_type}, parentId={parent_id}, ocaGroup={oca_group}")
                        
                        # Проверяем дочерние ордера (parentId != 0)
                        if parent_id:
                            if order_type == 'LMT':
                                price = float(order.lmtPrice) if order.lmtPrice else None
                                if price:
                                    status['tp'] = price
                                    logging.debug(f"Found TP order: {order_id} at {price}")
                            elif order_type == 'STP':
                                price = float(order.auxPrice) if order.auxPrice else None
                                if price:
                                    status['sl'] = price
                                    logging.debug(f"Found SL order: {order_id} at {price}")
                        # Также проверяем по ocaGroup (для случаев, когда parentId может быть 0)
                        elif oca_group and oca_group.startswith('BRACKET_'):
                            if order_type == 'LMT' and not status['tp']:
                                price = float(order.lmtPrice) if order.lmtPrice else None
                                if price:
                                    status['tp'] = price
                                    logging.debug(f"Found TP order via ocaGroup: {order_id} at {price}")
                            elif order_type == 'STP' and not status['sl']:
                                price = float(order.auxPrice) if order.auxPrice else None
                                if price:
                                    status['sl'] = price
                                    logging.debug(f"Found SL order via ocaGroup: {order_id} at {price}")
            except Exception as exc:
                logging.debug(f"Could not get TP/SL from openTrades: {exc}")
            
            # Альтернативный способ - через reqAllOpenOrders
            if status['sl'] is None or status['tp'] is None:
                try:
                    orders = self.ib.reqAllOpenOrders()
                    position_con_id = position.contract.conId
                    logging.debug(f"get_position_status: checking {len(orders)} open orders for conId={position_con_id}")
                    for order in orders:
                        if order.contract and order.contract.conId == position_con_id:
                            order_id = getattr(order, 'orderId', 0)
                            parent_id = getattr(order, 'parentId', 0)
                            order_type = getattr(order, 'orderType', '')
                            
                            if parent_id:  # Дочерний ордер
                                if order_type == 'LMT' and not status['tp']:
                                    price = float(order.lmtPrice) if order.lmtPrice else None
                                    if price:
                                        status['tp'] = price
                                        logging.debug(f"Found TP order via reqAllOpenOrders: {order_id} at {price}")
                                elif order_type == 'STP' and not status['sl']:
                                    price = float(order.auxPrice) if order.auxPrice else None
                                    if price:
                                        status['sl'] = price
                                        logging.debug(f"Found SL order via reqAllOpenOrders: {order_id} at {price}")
                except Exception as exc:
                    logging.debug(f"Could not get TP/SL from open orders: {exc}")
        
        except Exception as exc:
            logging.exception(f"Error getting position status: {exc}")
        
        return status

    # ---- trading helpers ----

    def market_entry(self, contract: Contract, side: str, quantity: int) -> float:
        """
        Place a market order to open position.
        side: 'LONG' -> BUY, 'SHORT' -> SELL
        Returns: average fill price.
        Blocks until order is done (Filled/Cancelled).
        Retries on connection loss (ApiCancelled).
        """
        max_retries = 3
        retry_delay = 5.0  # секунд

        async def _entry_impl_async() -> float:
            for attempt in range(max_retries):
                if not self.ib.isConnected():
                    if attempt < max_retries - 1:
                        logging.warning(
                            f"IB not connected, waiting {retry_delay}s before retry "
                            f"({attempt + 1}/{max_retries})..."
                        )
                        await asyncio.sleep(retry_delay)
                        try:
                            self.connect()
                        except Exception as exc:
                            logging.warning(f"Reconnect attempt failed: {exc}")
                            continue
                    else:
                        msg = "❌ Cannot place market entry: IB is not connected after retries."
                        logging.error(msg)
                        self._safe_notify(msg)
                        raise ConnectionError("IB not connected in market_entry after retries")

                if self._reconnecting:
                    logging.info("Waiting for reconnection to complete...")
                    wait_time = 0
                    while self._reconnecting and wait_time < 30:
                        await asyncio.sleep(1)
                        wait_time += 1
                    if self._reconnecting:
                        logging.warning("Reconnection timeout, proceeding anyway...")

                action = "BUY" if side.upper() == "LONG" else "SELL"
                contract.exchange = contract.exchange or "CME"
                order = Order(
                    action=action,
                    orderType="MKT",
                    totalQuantity=quantity,
                )

                try:
                    trade = self.ib.placeOrder(contract, order)
                    logging.info("Market order sent: %s %s (attempt %d/%d)", action, quantity, attempt + 1, max_retries)

                    while not trade.isDone():
                        await asyncio.sleep(0.1)

                        if not self.ib.isConnected():
                            status = trade.orderStatus.status
                            logging.error(
                                f"Connection lost while waiting for order fill. "
                                f"Order status: {status}"
                            )
                            if attempt < max_retries - 1:
                                logging.info(f"Will retry after reconnection...")
                                break
                            else:
                                raise ConnectionError(
                                    f"IB connection lost during order execution. "
                                    f"Order status: {status}"
                                )

                    fill_price = float(trade.orderStatus.avgFillPrice or 0.0)
                    final_status = trade.orderStatus.status

                    logging.info(
                        "Market order status: %s avgFillPrice=%s",
                        final_status,
                        fill_price,
                    )

                    if final_status == "ApiCancelled":
                        if attempt < max_retries - 1:
                            logging.warning(
                                f"Order cancelled due to connection loss. "
                                f"Retrying in {retry_delay}s ({attempt + 1}/{max_retries})..."
                            )
                            self._safe_notify(
                                f"⚠️ Order cancelled due to connection loss. "
                                f"Retrying in {retry_delay}s..."
                            )
                            await asyncio.sleep(retry_delay)
                            try:
                                if not self.ib.isConnected():
                                    self.connect()
                            except Exception as exc:
                                logging.warning(f"Reconnect attempt failed: {exc}")
                            continue
                        else:
                            error_msg = (
                                f"❌ Entry order {action} {quantity} "
                                f"{contract.localSymbol or contract.symbol} "
                                f"was cancelled due to connection loss after {max_retries} attempts. "
                                f"Please check connection and retry manually."
                            )
                            logging.error(error_msg)
                            self._safe_notify(error_msg)
                            raise ConnectionError(
                                f"Order cancelled due to connection loss after {max_retries} attempts: {final_status}"
                            )

                    if fill_price > 0:
                        self._safe_notify(
                            f"✅ Entry filled: {action} {quantity} "
                            f"{contract.localSymbol or contract.symbol} @ {fill_price}"
                        )
                        return fill_price
                    else:
                        self._safe_notify(
                            f"⚠️ Entry order {action} {quantity} "
                            f"{contract.localSymbol or contract.symbol} failed to fill "
                        )
                        raise RuntimeError("Entry order did not fill")

                except Exception as exc:
                    logging.exception("Error placing market order: %s", exc)
                    if attempt >= max_retries - 1:
                        raise
                    logging.info(f"Retrying market entry in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
            raise RuntimeError("Market entry failed after retries")

        if not self._loop:
            raise RuntimeError("IB event loop is not available for market_entry")

        future = asyncio.run_coroutine_threadsafe(_entry_impl_async(), self._loop)
        return future.result()

    async def _limit_entry_impl(
        self,
        contract: Contract,
        side: str,
        quantity: int,
        limit_price: float,
        tif: str = "GTC",
        timeout: float = 300.0,
    ) -> float:
        ib = self.ib
        if not ib.isConnected():
            raise ConnectionError("IB not connected in limit entry")

        action = "BUY" if side.upper() == "LONG" else "SELL"
        order = Order(
            action=action,
            orderType="LMT",
            totalQuantity=quantity,
            lmtPrice=limit_price,
            tif=tif,
        )

        trade = ib.placeOrder(contract, order)
        with self._limit_order_lock:
            self._active_limit_order = order
            self._active_limit_trade = trade

        start_time = time.time()
        try:
            logging.info(
                "Limit entry order placed: %s %s @ %s (qty=%s, tif=%s)",
                action,
                contract.localSymbol or contract.symbol,
                limit_price,
                quantity,
                tif,
            )

            while not trade.isDone():
                await asyncio.sleep(0.5)
                if timeout > 0 and time.time() - start_time >= timeout:
                    logging.warning(
                        "Limit order timed out after %.1fs, cancelling...", timeout
                    )
                    ib.cancelOrder(order)
                    raise TimeoutError(
                        f"Limit entry did not fill within {timeout} seconds"
                    )

            final_status = trade.orderStatus.status
            fill_price = float(trade.orderStatus.avgFillPrice or 0.0)
            if fill_price <= 0.0:
                last_fill = trade.fills[-1] if trade.fills else None
                if last_fill:
                    fill_price = float(last_fill.execution.price or 0.0)
                if fill_price <= 0.0:
                    fill_price = limit_price
                    logging.warning(
                        "Limit entry fill price was not provided; falling back to limit price (%s)",
                        fill_price,
                    )

            if final_status != "Filled":
                raise RuntimeError(
                    f"Limit entry ended with status={final_status} fillPrice={fill_price}"
                )

            self._safe_notify(
                f"✅ Limit entry filled: {action} {quantity} "
                f"{contract.localSymbol or contract.symbol} @ {fill_price}"
            )
            logging.info(
                "Limit entry filled: %s %s qty=%s @ %s",
                action,
                contract.localSymbol or contract.symbol,
                quantity,
                fill_price,
            )

            return fill_price
        finally:
            with self._limit_order_lock:
                self._active_limit_order = None
                self._active_limit_trade = None

    def place_limit_entry(
        self,
        contract: Contract,
        side: str,
        quantity: int,
        limit_price: float,
        tif: str = "GTC",
        timeout: float = 300.0,
    ) -> float:
        if not self._loop:
            raise RuntimeError("IB event loop is not available for limit_entry")

        future = asyncio.run_coroutine_threadsafe(
            self._limit_entry_impl(contract, side, quantity, limit_price, tif, timeout),
            self._loop,
        )
        return future.result()

    def cancel_limit_order(self) -> None:
        if self._loop is None:
            logging.warning("cancel_limit_order: IB loop unavailable")
            return

        def _do_cancel():
            with self._limit_order_lock:
                order = self._active_limit_order
                trade = self._active_limit_trade
                self._active_limit_order = None
                self._active_limit_trade = None
            if order is None:
                logging.info("cancel_limit_order: no active limit order")
                return
            try:
                self.ib.cancelOrder(order)
                logging.info("Active limit order cancelled (orderId=%s)", order.orderId)
                self._safe_notify("⚠️ Limit order cancelled.")
            except Exception as exc:
                logging.warning("cancel_limit_order: failed to cancel order: %s", exc)

        self._loop.call_soon_threadsafe(_do_cancel)
        for attempt in range(max_retries):
            # Проверяем соединение перед попыткой
            if not self.ib.isConnected():
                if attempt < max_retries - 1:
                    logging.warning(
                        f"IB not connected, waiting {retry_delay}s before retry "
                        f"({attempt + 1}/{max_retries})..."
                    )
                    time.sleep(retry_delay)
                    # Пытаемся переподключиться
                    try:
                        self.connect()
                    except Exception as exc:
                        logging.warning(f"Reconnect attempt failed: {exc}")
                        continue
                else:
                    msg = "❌ Cannot place market entry: IB is not connected after retries."
                    logging.error(msg)
                    self._safe_notify(msg)
                    raise ConnectionError("IB not connected in market_entry after retries")
            
            # Ждем завершения переподключения, если оно идет
            if self._reconnecting:
                logging.info("Waiting for reconnection to complete...")
                wait_time = 0
                while self._reconnecting and wait_time < 30:
                    time.sleep(1)
                    wait_time += 1
                if self._reconnecting:
                    logging.warning("Reconnection timeout, proceeding anyway...")
            
            action = "BUY" if side.upper() == "LONG" else "SELL"
            order = Order(
                action=action,
                orderType="MKT",
                totalQuantity=quantity,
            )
            
            try:
                trade = self.ib.placeOrder(contract, order)
                logging.info("Market order sent: %s %s (attempt %d/%d)", action, quantity, attempt + 1, max_retries)

                # Wait for fill
                while not trade.isDone():
                    time.sleep(0.1)

                    # Проверяем соединение во время ожидания
                    if not self.ib.isConnected():
                        status = trade.orderStatus.status
                        logging.error(
                            f"Connection lost while waiting for order fill. "
                            f"Order status: {status}"
                        )
                        if attempt < max_retries - 1:
                            logging.info(f"Will retry after reconnection...")
                            break  # Выходим из цикла ожидания для retry
                        else:
                            raise ConnectionError(
                                f"IB connection lost during order execution. "
                                f"Order status: {status}"
                            )

                fill_price = float(trade.orderStatus.avgFillPrice or 0.0)
                final_status = trade.orderStatus.status
                
                logging.info(
                    "Market order status: %s avgFillPrice=%s",
                    final_status,
                    fill_price,
                )
                
                # Обработка ApiCancelled (ордер отменен из-за потери соединения)
                if final_status == "ApiCancelled":
                    if attempt < max_retries - 1:
                        logging.warning(
                            f"Order cancelled due to connection loss. "
                            f"Retrying in {retry_delay}s ({attempt + 1}/{max_retries})..."
                        )
                        self._safe_notify(
                            f"⚠️ Order cancelled due to connection loss. "
                            f"Retrying in {retry_delay}s..."
                        )
                        time.sleep(retry_delay)
                        # Пытаемся переподключиться перед retry
                        try:
                            if not self.ib.isConnected():
                                self.connect()
                        except Exception as exc:
                            logging.warning(f"Reconnect attempt failed: {exc}")
                        continue  # Retry
                    else:
                        error_msg = (
                            f"❌ Entry order {action} {quantity} "
                            f"{contract.localSymbol or contract.symbol} "
                            f"was cancelled due to connection loss after {max_retries} attempts. "
                            f"Please check connection and retry manually."
                        )
                        logging.error(error_msg)
                        self._safe_notify(error_msg)
                        raise ConnectionError(
                            f"Order cancelled due to connection loss after {max_retries} attempts: {final_status}"
                        )

                if fill_price > 0:
                    self._safe_notify(
                        f"✅ Entry filled: {action} {quantity} "
                        f"{contract.localSymbol or contract.symbol} @ {fill_price}"
                    )
                    return fill_price
                else:
                    self._safe_notify(
                        f"⚠️ Entry order {action} {quantity} "
                        f"{contract.localSymbol or contract.symbol} "
                        f"finished with status={final_status}, no fill price."
                    )
                    return fill_price
                    
            except ConnectionError:
                # Пробрасываем ConnectionError дальше после всех retry
                if attempt == max_retries - 1:
                    raise
                logging.warning(f"Connection error, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                continue
            except Exception as exc:
                # Для других ошибок не делаем retry
                logging.exception(f"Error placing market order: {exc}")
                raise
        
        # Не должно сюда дойти, но на всякий случай
        raise RuntimeError("Market entry failed after all retries")

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
        Place TP/SL bracket orders for an open position.
        Returns: (tp_price, sl_price)
        """
        if not self.ib.isConnected():
            msg = "❌ Cannot place exit bracket: IB is not connected."
            logging.error(msg)
            self._safe_notify(msg)
            raise ConnectionError("IB not connected in place_exit_bracket")

        # Проверяем актуальную позицию перед установкой TP/SL (НЕ из кеша)
        try:
            logging.info("place_exit_bracket: requesting fresh positions from broker (not from cache)...")
            positions = self.get_positions_from_broker()
            self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", "place_exit_bracket()")
            current_position = None
            for pos in positions:
                pos_contract = pos.contract
                if (getattr(pos_contract, "localSymbol", "") == getattr(contract, "localSymbol", "") or
                    getattr(pos_contract, "symbol", "") == getattr(contract, "symbol", "")):
                    current_position = pos
                    break
            
            if current_position:
                actual_qty = abs(float(current_position.position))
                if actual_qty != quantity:
                    logging.warning(
                        f"⚠️ Position quantity mismatch: config={quantity}, actual={actual_qty}. "
                        f"Using actual quantity for TP/SL."
                    )
                    quantity = int(actual_qty)
                    self._safe_notify(
                        f"⚠️ TP/SL quantity adjusted to match position: {quantity}"
                    )
        except Exception as exc:
            logging.warning(f"Failed to check current position before placing bracket: {exc}")

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
        Core logic for closing all positions via market orders.
        """
        ib = self.ib
        if not ib.isConnected():
            raise ConnectionError("IB not connected in close_all_positions")
        
        try:
            # Получаем актуальные позиции напрямую с брокера (НЕ из кеша)
            logging.info("CLOSE ALL: requesting fresh positions from broker (not from cache)...")
            positions = self.get_positions_from_broker()
            self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", "_close_all_positions_core()")
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

        logging.info("Closing all open positions via market orders (tracking via socket events)...")
        self._safe_notify("⛔ CLOSE ALL: sending market orders to close all positions (tracking via events).")

        summary_lines: List[str] = []
        trades_to_track: List[Trade] = []  # Список трейдов для отслеживания

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
                elif contract.localSymbol and contract.localSymbol.startswith('ES'):
                    contract.exchange = 'CME'
                    logging.info(f"Set exchange to CME (fallback for ES) for {symbol}")
                else:
                    try:
                        logging.info(f"Qualifying contract {symbol} to get exchange...")
                        qualified = self._run_in_loop(ib.qualifyContracts, contract)
                        if qualified and qualified[0].exchange:
                            contract.exchange = qualified[0].exchange
                            logging.info(f"Set exchange to {contract.exchange} (from qualification) for {symbol}")
                    except Exception as exc:
                        logging.warning(f"Failed to qualify contract {symbol}: {exc}")
            
            if not contract.exchange and (symbol.startswith('ES') or (contract.localSymbol and contract.localSymbol.startswith('ES'))):
                contract.exchange = 'CME'
                logging.info(f"Set exchange to CME (default for ES) for {symbol}")
            
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
                outsideRth=True,
            )

            try:
                logging.info(f"Placing CLOSE order: {action} {abs(qty)} {symbol} on exchange {contract.exchange}")
                
                if not ib.isConnected():
                    raise ConnectionError("IB is not connected, cannot place order")
                
                trade = ib.placeOrder(contract, order)
                trades_to_track.append(trade)  # Добавляем в список для отслеживания
                
                logging.info(
                    "Closing position (tracking via events): %s %s qty=%s orderId=%s exchange=%s",
                    action,
                    symbol,
                    qty,
                    trade.order.orderId,
                    contract.exchange,
                )
                
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

        # Ждем заполнения всех ордеров через события (socket-based)
        if trades_to_track:
            logging.info(f"Waiting for {len(trades_to_track)} orders to fill (tracking via socket events)...")
            max_wait = 15.0  # Максимальное время ожидания
            start_time = time.time()
            
            while trades_to_track and (time.time() - start_time) < max_wait:
                # Проверяем статус через события (они приходят автоматически через сокет)
                for trade in trades_to_track[:]:  # Копируем список для безопасной итерации
                    if trade.isDone():
                        trades_to_track.remove(trade)
                        final_status = trade.orderStatus.status
                        fill_price = trade.orderStatus.avgFillPrice
                        contract = trade.contract
                        symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
                        action = trade.order.action
                        qty = trade.order.totalQuantity
                        
                        if final_status == "Filled":
                            logging.info(f"✅ Order {trade.order.orderId} FILLED via socket event: {action} {qty} {symbol} @ {fill_price}")
                            line = f"{action} {qty} {symbol} ✅ FILLED @ {fill_price} (orderId={trade.order.orderId})"
                        elif final_status in ["Cancelled", "Inactive"]:
                            logging.warning(f"⚠️ Order {trade.order.orderId} was {final_status}")
                            line = f"{action} {qty} {symbol} ⚠️ {final_status} (orderId={trade.order.orderId})"
                        else:
                            line = f"{action} {qty} {symbol} ⏳ {final_status} (orderId={trade.order.orderId})"
                        
                        summary_lines.append(line)
                
                # Небольшая задержка для обработки событий
                if trades_to_track:
                    ib.sleep(0.5)
            
            # Если остались незаполненные ордера
            for trade in trades_to_track:
                contract = trade.contract
                symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
                action = trade.order.action
                qty = trade.order.totalQuantity
                status = trade.orderStatus.status
                line = f"{action} {qty} {symbol} ⏳ {status} (may fill later, orderId={trade.order.orderId})"
                summary_lines.append(line)

        if summary_lines:
            self._safe_notify(
                "✅ CLOSE ALL orders sent (tracked via socket events):\n" + "\n".join(summary_lines)
            )
        else:
            self._safe_notify(
                "ℹ️ CLOSE ALL: nothing was closed (no positions or all sends failed)."
            )

    # ---- event handlers ----
    
    def _check_position_closed(self, contract: Contract, current_qty: float, source: str) -> bool:
        """
        Централизованная проверка закрытия позиции.
        Отправляет уведомление только один раз для каждой позиции.
        
        Args:
            contract: Контракт позиции
            current_qty: Текущее количество позиции
            source: Источник события (для логирования)
        
        Returns:
            True если позиция закрыта и уведомление отправлено, False иначе
        """
        con_id = contract.conId
        symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
        expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
        
        # Получаем предыдущее количество позиции
        old_qty = self._last_positions.get(con_id, 0.0)
        
        logging.info(
            f"_check_position_closed: {symbol} {expiry} conId={con_id} "
            f"old_qty={old_qty} current_qty={current_qty} source={source} "
            f"notified={con_id in self._position_closed_notified}"
        )
        
        # Обновляем состояние позиции
        if abs(current_qty) > 0.001:
            # Позиция открыта или изменилась
            # Сохраняем предыдущее состояние перед обновлением
            if con_id in self._last_positions and abs(self._last_positions[con_id]) < 0.001:
                # Позиция была закрыта и теперь открылась снова
                logging.info(f"Position reopened: {symbol} {expiry}, clearing notification flag")
                self._position_closed_notified.discard(con_id)
            self._last_positions[con_id] = current_qty
            self._position_contracts[con_id] = contract
            return False
        else:
            # Позиция закрыта (qty = 0)
            # Проверяем, была ли позиция открыта раньше
            if abs(old_qty) > 0.001:
                # Позиция была открыта и теперь закрыта
                # Проверяем, не отправляли ли уже уведомление
                if con_id not in self._position_closed_notified:
                    # Отправляем уведомление только один раз
                    self._position_closed_notified.add(con_id)
                    logging.info(
                        f"✅ Position closed detected via {source}: {symbol} {expiry} "
                        f"(was {old_qty}, now {current_qty})"
                    )
                    self._safe_notify(
                        f"✅ Position closed ({source}): {symbol} {expiry}\n"
                        f"Previous qty: {old_qty}"
                    )
                    # Сохраняем состояние закрытия (qty=0) чтобы знать что позиция была открыта
                    self._last_positions[con_id] = 0.0
                    # Сохраняем контракт для возможности получения деталей после закрытия
                    self._position_contracts[con_id] = contract
                    return True
                else:
                    # Уведомление уже отправлено, просто логируем
                    logging.debug(
                        f"Position closure already notified for {symbol} {expiry} "
                        f"(source: {source})"
                    )
                    # Обновляем состояние закрытия
                    self._last_positions[con_id] = 0.0
                    # Сохраняем контракт для возможности получения деталей после закрытия
                    self._position_contracts[con_id] = contract
                    return False
            else:
                # Позиция уже была закрыта или никогда не была открыта
                # Если old_qty был 0, но позиция есть в _last_positions, значит она была закрыта ранее
                if con_id in self._last_positions:
                    # Позиция уже была закрыта ранее, просто обновляем состояние
                    self._last_positions[con_id] = 0.0
                    # Сохраняем контракт для возможности получения деталей после закрытия
                    self._position_contracts[con_id] = contract
                else:
                    # Позиция никогда не была открыта (или была удалена из _last_positions)
                    # Это может быть начальное состояние - не отправляем уведомление
                    # Если это positionEvent с qty=0 и old_qty=0, значит позиция уже была закрыта до инициализации
                    # или это просто обновление состояния - не нужно проверять через get_positions_from_broker()
                    # чтобы избежать рекурсии
                    if source == "positionEvent":
                        logging.debug(
                            f"PositionEvent qty=0 and old_qty=0 for {symbol} {expiry}. "
                            f"Position was not tracked (bot may have restarted after position was opened). "
                            f"Skipping verification to avoid recursion."
                        )
                return False

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
            
            # Проверяем, полностью ли заполнен ордер
            order_qty = order.totalQuantity
            filled_qty = exec_data.shares
            
            logging.info(
                f"Bracket exit fill: {action} {filled_qty}/{order_qty} @ {price} "
                f"(OCA group: {oca_group})"
            )
            
            # Если частичное заполнение - предупреждаем
            if filled_qty < order_qty:
                logging.warning(
                    f"⚠️ Partial fill: {filled_qty}/{order_qty} filled. "
                    f"Position may not be fully closed."
                )
            
            msg = (
                f"✅ Bracket exit filled: {contract.localSymbol or contract.symbol} "
                f"{action} {filled_qty} @ {price}.\n"
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
            
            # После заполнения TP/SL ордера принудительно синхронизируем кеш позиций
            # и проверяем, что позиция действительно закрыта
            logging.info("Bracket exit filled, syncing positions cache to reflect closed position...")
            try:
                ib_loop = self._loop
                if ib_loop is not None and not ib_loop.is_closed() and self.ib.isConnected():
                    import threading
                    
                    # Делаем несколько попыток синхронизации с увеличивающимся временем ожидания
                    for sync_attempt in range(3):
                        position_synced = threading.Event()
                        self._schedule_req_positions_with_event(
                            position_synced,
                            f"_on_exec_details sync attempt {sync_attempt+1}",
                        )
                        
                        # Ждем синхронизации
                        if position_synced.wait(timeout=2.0):
                            # Даем больше времени для обновления кеша через positionEvent
                            # Используем ib.waitOnUpdate() если возможно
                            wait_time = 3.0 + (sync_attempt * 1.0)  # Увеличиваем время с каждой попыткой
                            logging.info(f"Waiting {wait_time}s for positionEvent to update cache (attempt {sync_attempt+1}/3)...")
                            
                            try:
                                # Пробуем использовать ib.waitOnUpdate() для ожидания обновления
                                if threading.current_thread() is threading.main_thread():
                                    self.ib.waitOnUpdate(timeout=wait_time)
                                else:
                                    time.sleep(wait_time)
                            except Exception:
                                time.sleep(wait_time)
                            
                            # Проверяем, что позиция действительно закрыта
                            positions = self.get_positions_from_broker()
                            self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", f"_on_exec_details() sync attempt {sync_attempt+1}")
                            open_positions = [p for p in positions if abs(float(p.position)) > 0.001]
                            
                            # Ищем позицию по этому контракту
                            contract_positions = [
                                p for p in open_positions 
                                if (getattr(p.contract, "localSymbol", "") == getattr(contract, "localSymbol", "") or
                                    getattr(p.contract, "symbol", "") == getattr(contract, "symbol", ""))
                            ]
                            
                            if not contract_positions:
                                logging.info(f"✅ Position fully closed confirmed after bracket exit fill (attempt {sync_attempt+1})")
                                # Используем централизованную проверку для отправки уведомления
                                # (позиция уже закрыта, поэтому передаем qty=0)
                                self._check_position_closed(contract, 0.0, "execDetails (TP/SL fill)")
                                break  # Позиция закрыта, выходим из цикла
                            else:
                                remaining_qty = sum(abs(float(p.position)) for p in contract_positions)
                                logging.info(f"Position still open: {remaining_qty} remaining (attempt {sync_attempt+1}/3)")
                                if sync_attempt < 2:  # Пробуем еще раз
                                    continue
                                else:
                                    # После всех попыток позиция все еще открыта
                                    logging.warning(
                                        f"⚠️ Position not fully closed after TP/SL fill! "
                                        f"Remaining: {remaining_qty} {getattr(contract, 'localSymbol', '')}"
                                    )
                                    self._safe_notify(
                                        f"⚠️ Position may not be fully closed after TP/SL. "
                                        f"Remaining: {remaining_qty}. "
                                        f"Please check manually or use CLOSE ALL."
                                    )
                        else:
                            logging.warning(f"Position sync timeout (attempt {sync_attempt+1}/3)")
                else:
                    logging.debug("Cannot sync positions after bracket exit: event loop not available")
            except Exception as sync_exc:
                logging.warning(f"Failed to sync positions after bracket exit fill: {sync_exc}")

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
            elif status == "ApiCancelled":
                logging.error(
                    f"Order {order_id} cancelled due to connection loss (ApiCancelled)"
                )
                self._safe_notify(
                    f"❌ Order {order_id} cancelled due to connection loss. "
                    f"Please check IB connection."
                )
            elif status == "Filled":
                fill_price = trade.orderStatus.avgFillPrice
                filled_qty = trade.orderStatus.filled
                contract = trade.contract
                symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
                expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
                action = order.action
                
                logging.info(
                    f"Order {order_id} filled: {filled_qty} @ {fill_price} "
                    f"({action} {symbol} {expiry})"
                )
                
                # Если это TP/SL ордер (OCA group), проверяем закрытие позиции
                oca_group = getattr(order, "ocaGroup", "") or ""
                if oca_group.startswith("BRACKET_"):
                    # Согласно TWS API: когда исполняется TP/SL, позиция закрывается
                    logging.info("Bracket order (TP/SL) filled, checking if position is closed...")
                    
                    # Отправляем уведомление о заполнении TP/SL ордера
                    order_type = "TP" if order.orderType == "LMT" else "SL"
                    self._safe_notify(
                        f"✅ {order_type} order filled: {action} {filled_qty} {symbol} {expiry} @ {fill_price}\n"
                        f"Checking if position is closed..."
                    )
                    
                    try:
                        ib_loop = self._loop
                        if ib_loop is not None and not ib_loop.is_closed() and self.ib.isConnected():
                            import threading
                            position_synced = threading.Event()
                            self._schedule_req_positions_with_event(
                                position_synced, "_on_order_status TP/SL fill"
                            )
                            if position_synced.wait(timeout=2.0):
                                # Даем время для обновления кеша через positionEvent
                                time.sleep(3.0)
                                
                                # Проверяем позиции согласно TWS API документации
                                positions = self.get_positions_from_broker()
                                self._log_positions_source(positions, "BROKER (via get_positions_from_broker)", "_on_order_status() TP/SL fill check")
                                open_positions = [p for p in positions if abs(float(p.position)) > 0.001]
                                
                                # Ищем позицию по этому контракту
                                contract_positions = [
                                    p for p in open_positions 
                                    if (getattr(p.contract, "localSymbol", "") == symbol or
                                        getattr(p.contract, "conId", 0) == contract.conId)
                                ]
                                
                                if not contract_positions:
                                    # Позиция закрыта согласно TWS API
                                    logging.info(
                                        f"✅ Position closed confirmed via orderStatus(): {symbol} {expiry} "
                                        f"(TP/SL order {order_id} filled)"
                                    )
                                    # Используем централизованную проверку для отправки уведомления
                                    self._check_position_closed(contract, 0.0, f"orderStatus ({order_type} fill)")
                                else:
                                    remaining_qty = sum(abs(float(p.position)) for p in contract_positions)
                                    logging.info(
                                        f"Position partially closed: {symbol} {expiry} "
                                        f"remaining={remaining_qty}"
                                    )
                                    if remaining_qty < abs(filled_qty):
                                        self._safe_notify(
                                            f"⚠️ Position partially closed: {symbol} {expiry}\n"
                                            f"Remaining: {remaining_qty}"
                                        )
                    except Exception as sync_exc:
                        logging.debug(f"Failed to sync positions after order fill: {sync_exc}")
                elif order.orderType == "LMT" and not oca_group:
                    direction = "LONG" if action == "BUY" else "SHORT"
                    self._safe_notify(
                        f"✅ Limit entry filled: {direction} {filled_qty} {symbol} {expiry} @ {fill_price}"
                    )
            elif status in ["PendingSubmit", "PreSubmitted", "Submitted"]:
                logging.debug(f"Order {order_id} in progress: {status}")
        except Exception as exc:
            logging.exception("Error in _on_order_status: %s", exc)

    def _on_position_change(self, position):
        """
        Handler для positionEvent - вызывается автоматически при изменении позиций через сокет.
        Это основной механизм мониторинга через WebSocket (IB API использует TCP сокет).
        Использует централизованную проверку закрытия для предотвращения дублирования уведомлений.
        """
        symbol = position.contract.localSymbol or position.contract.symbol
        expiry = getattr(position.contract, "lastTradeDateOrContractMonth", "")
        current_qty = float(position.position)
        
        con_id = position.contract.conId
        old_qty = self._last_positions.get(con_id, 0.0)
        
        # Логируем источник события
        source_type = "EVENT (positionEvent socket)"
        logging.info(
            f"🔌 PositionEvent (socket update): {symbol} {expiry} "
            f"qty={current_qty} (was {old_qty}) avgCost={position.avgCost} "
            f"conId={con_id} SOURCE={source_type}"
        )
        
        # Логируем позицию через вспомогательную функцию
        self._log_positions_source([position], source_type, "_on_position_change()")
        
        # Логируем текущее состояние отслеживания для отладки
        if con_id in self._last_positions:
            logging.debug(
                f"Position tracking state: conId={con_id} in _last_positions={True} "
                f"value={self._last_positions[con_id]} notified={con_id in self._position_closed_notified}"
            )
        else:
            logging.debug(
                f"Position tracking state: conId={con_id} NOT in _last_positions "
                f"(will be added if qty != 0)"
            )
        
        # Явная проверка закрытия для реального времени
        if abs(old_qty) > 0.001 and abs(current_qty) < 0.001:
            # Позиция закрылась (была открыта, теперь закрыта)
            logging.warning(
                f"🚨 REAL-TIME CLOSURE DETECTED via positionEvent: {symbol} {expiry} "
                f"conId={con_id} was {old_qty}, now {current_qty}"
            )
        
        # Используем централизованную проверку закрытия
        closed = self._check_position_closed(position.contract, current_qty, "positionEvent")
        if closed:
            logging.info(f"✅ Position closure notification sent via positionEvent for {symbol} {expiry}")
        elif abs(current_qty) < 0.001 and abs(old_qty) < 0.001:
            logging.debug(
                f"PositionEvent qty=0 but old_qty=0 for {symbol} {expiry}. "
                f"Position may not have been tracked or was already closed."
            )
        elif abs(current_qty) < 0.001 and abs(old_qty) > 0.001:
            # Позиция закрыта, но уведомление не отправилось
            if con_id in self._position_closed_notified:
                logging.debug(
                    f"PositionEvent: position {symbol} {expiry} closure already notified earlier"
                )
            else:
                logging.warning(
                    f"PositionEvent: position {symbol} {expiry} closed but notification not sent "
                    f"(conId={con_id} not in _position_closed_notified)"
                )

    def _on_portfolio_update(self, item) -> None:
        """
        Handler для updatePortfolioEvent - вызывается автоматически при обновлении портфеля через сокет.
        Это аналог updatePortfolio() callback из IB API.
        
        Согласно документации TWS API:
        - Когда позиция полностью закрыта, вы получите обновление, показывающее размер позиции как ноль.
        - Отправляет уведомление при закрытии позиции (position = 0).
        """
        try:
            contract = item.contract
            con_id = contract.conId
            market_price = item.marketPrice
            position = float(item.position)
            
            # Сохраняем цену в кеш (если есть)
            if market_price and market_price > 0:
                self._portfolio_prices[con_id] = float(market_price)
            
            symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
            expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
            
            old_qty = self._last_positions.get(con_id, 0.0)
            
            # Логируем источник события
            source_type = "EVENT (updatePortfolioEvent socket)"
            logging.info(
                f"📊 Portfolio update (socket): {symbol} {expiry} conId={con_id} "
                f"marketPrice={market_price} position={position} (was {old_qty}) "
                f"unrealizedPNL={item.unrealizedPNL} SOURCE={source_type}"
            )
            
            # Явная проверка закрытия позиции для реального времени
            # Согласно TWS API: когда позиция полностью закрыта, position = 0
            if abs(old_qty) > 0.001 and abs(position) < 0.001:
                # Позиция закрылась (была открыта, теперь закрыта)
                logging.warning(
                    f"🚨 REAL-TIME CLOSURE DETECTED via updatePortfolio: {symbol} {expiry} "
                    f"conId={con_id} was {old_qty}, now {position}"
                )
            
            # Создаем объект Position для логирования
            pos_obj = Position(contract=contract, position=position, avgCost=item.averageCost)
            self._log_positions_source([pos_obj], source_type, "_on_portfolio_update()")
            
            # Используем централизованную проверку закрытия
            # Согласно TWS API: когда позиция полностью закрыта, position = 0
            closed = self._check_position_closed(contract, position, "updatePortfolio")
            if closed:
                logging.info(f"✅ Position closure notification sent via updatePortfolio for {symbol} {expiry}")
            else:
                # Логируем почему уведомление не отправилось
                if abs(position) < 0.001:
                    if abs(old_qty) < 0.001:
                        logging.debug(
                            f"updatePortfolio: position {symbol} {expiry} qty=0 but old_qty=0 "
                            f"(already closed or not tracked)"
                        )
                    elif con_id in self._position_closed_notified:
                        logging.debug(
                            f"updatePortfolio: position {symbol} {expiry} closure already notified"
                        )
        except Exception as exc:
            logging.debug(f"Error in _on_portfolio_update: {exc}")

    def _on_error(self, reqId: int, errorCode: int, errorString: str, contract: Optional[Contract] = None) -> None:
        """Handle IB API errors."""
        # Skip informational messages (errorCode < 1000)
        if errorCode < 1000:
            return
        
        # Error 200 - это нормальная ошибка "контракт не найден" при попытках квалификации
        # Не логируем как ERROR, только как DEBUG чтобы не засорять логи
        if errorCode == 200:
            if "No security definition" in errorString or "Invalid value" in errorString:
                logging.debug(f"Contract qualification attempt failed (Error 200): {contract}")
                return
        
        # Errors 2157/2158: Sec-def data farm connection status (informational, not critical)
        if errorCode in [2157, 2158]:
            # 2157 = broken, 2158 = OK
            status = "broken" if errorCode == 2157 else "OK"
            logging.info(f"IB data farm status: {status} (code={errorCode}) - {errorString}")
            return
        
        # Error 1100: Connectivity between IBKR and Trader Workstation has been lost
        if errorCode == 1100:
            logging.error(
                f"🔌 IB connection lost (Error 1100): {errorString}. "
                f"Attempting to reconnect..."
            )
            self._notify_connection_error(
                f"⚠️ IB connection lost (Error 1100). Attempting to reconnect..."
            )
            self._trigger_reconnect("Error 1100")
            return
        
        # Error 10328: Connection lost, order data could not be resolved
        if errorCode == 10328:
            logging.error(
                f"🔌 Connection lost during order (Error 10328): {errorString}. "
                f"Order data may be lost."
            )
            self._notify_connection_error(
                f"⚠️ Connection lost during order (Error 10328). "
                f"Order may have been cancelled."
            )
            # Пытаемся переподключиться
            self._trigger_reconnect("Error 10328")
            return
        
        # Информационные сообщения о соединении - логируем как INFO/WARNING, не ERROR
        if errorCode in [2104, 2105, 2106]:
            logging.info(f"IB info: reqId={reqId} code={errorCode} msg={errorString}")
            return
        if errorCode == 2107:
            logging.info(f"IB info: reqId={reqId} code={errorCode} msg={errorString}")
            return
        
        # Все остальные ошибки логируем как ERROR
        logging.error(f"IB error: reqId={reqId} code={errorCode} msg={errorString}")
        if contract:
            logging.error(f"  Contract: {contract}")
        
        # Уведомляем только о критических ошибках (не информационных)
        if errorCode >= 2000:
            self._safe_notify(
                f"❌ IB error: code={errorCode} msg={errorString}"
            )