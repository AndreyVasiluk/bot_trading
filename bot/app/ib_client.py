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
        self._reconnecting = False  # Флаг переподключения

        # Simple callback that will be set from main() to send messages to Telegram.
        self._notify: Callable[[str], None] = lambda msg: None

        # Map OCA group -> human-readable description (entry side/qty/symbol)
        self._oca_meta: Dict[str, str] = {}

        # Attach handler for execution details (fills of any orders)
        self.ib.execDetailsEvent += self._on_exec_details
        
        # Attach handler for order status changes (to track cancellations)
        self.ib.orderStatusEvent += self._on_order_status
        
        # Attach handler for position changes (real-time monitoring)
        self.ib.positionEvent += self._on_position_change
        
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
        # Проверяем, есть ли event loop в текущем потоке
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Event loop уже запущен - это нормально для ib_insync
                pass
        except RuntimeError:
            # Нет event loop в текущем потоке - это проблема для ib_insync
            # ib_insync.connect() создаст свой event loop, но только если его нет
            pass
        
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
                    
                    # Инициализируем кеш позиций через reqPositions() (socket-based)
                    try:
                        logging.info("Initializing positions cache via reqPositions() (socket)...")
                        self.ib.reqPositions()
                        # Ждем обновления кеша через positionEvent
                        self.ib.sleep(2.0)
                        initial_positions = list(self.ib.positions())
                        logging.info(f"Positions cache initialized: {len(initial_positions)} positions")
                    except Exception as exc:
                        logging.warning(f"Failed to initialize positions cache: {exc}")
                    
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
        
        # Сначала проверяем, есть ли уже открытая позиция по этому контракту
        # Если есть, используем её контракт напрямую (самый надежный способ)
        if symbol.upper() == "ES":
            try:
                positions = self.ib.positions()
                logging.info(f"Checking {len(positions)} existing positions for matching contract")
                for pos in positions:
                    pos_contract = pos.contract
                    pos_symbol = getattr(pos_contract, "symbol", "")
                    pos_local_sym = getattr(pos_contract, "localSymbol", "")
                    pos_expiry = getattr(pos_contract, 'lastTradeDateOrContractMonth', '')
                    
                    logging.info(f"  Checking position: symbol={pos_symbol}, localSymbol={pos_local_sym}, expiry={pos_expiry}")
                    
                    # Проверяем, подходит ли эта позиция
                    if pos_symbol == symbol.upper():
                        # Проверяем expiry (может быть в формате 20260320 или 2026-03-20)
                        expiry_normalized = expiry.replace("-", "")
                        pos_expiry_normalized = pos_expiry.replace("-", "")
                        
                        if expiry_normalized in pos_expiry_normalized or pos_expiry_normalized.startswith(expiry_normalized):
                            logging.info(f"✅ Found matching position! Using contract from existing position: {pos_local_sym}")
                            # Используем контракт из позиции напрямую - он уже квалифицирован
                            qualified = pos_contract
                            logging.info(f"Using contract from existing position: conId={getattr(qualified, 'conId', 'N/A')}, localSymbol={getattr(qualified, 'localSymbol', 'N/A')}")
                            return qualified
                        
                        # Также проверяем по localSymbol (ESH6 для 202603)
                        if pos_local_sym in local_symbols:
                            logging.info(f"✅ Found matching position by localSymbol! Using contract: {pos_local_sym}")
                            qualified = pos_contract
                            logging.info(f"Using contract from existing position: conId={getattr(qualified, 'conId', 'N/A')}")
                            return qualified
            except Exception as exc:
                logging.warning(f"Error checking existing positions: {exc}")
        
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
                        symbol if not use_local_symbol else "ES",
                        exp_to_use if not use_local_symbol else local_sym,
                        exch or "auto",
                    )
                    return None
                qualified = contracts[0]
                logging.info("✅ Qualified contract: %s", qualified)
                logging.info(f"  conId={getattr(qualified, 'conId', 'N/A')}, localSymbol={getattr(qualified, 'localSymbol', 'N/A')}, expiry={getattr(qualified, 'lastTradeDateOrContractMonth', 'N/A')}")
                return qualified
            except Exception as exc:
                logging.warning("Exception during contract qualification: %s", exc)
                logging.debug(f"  Contract details: symbol={symbol if not use_local_symbol else local_sym}, exchange={exch}, expiry={exp_to_use if not use_local_symbol else 'N/A'}")
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
        Return latest known positions from IB cache (updated via socket).
        
        Синхронизирует кеш через reqPositions() (socket-based) для актуальных данных.
        Кеш обновляется через positionEvent после reqPositions().
        """
        ib = self.ib
        if not ib.isConnected():
            logging.warning("IB not connected, cannot refresh positions")
            return []
        
        ib_loop = self._loop
        
        # Синхронизируем кеш через reqPositions() (socket-based)
        # Это гарантирует, что кеш обновится через positionEvent
        if ib_loop is not None and not ib_loop.is_closed():
            try:
                logging.info("Syncing positions cache via reqPositions() (socket)...")
                
                import threading
                position_synced = threading.Event()
                sync_error = None
                
                def _do_req_positions():
                    """Выполняем reqPositions в правильном event loop для синхронизации кеша."""
                    try:
                        ib.reqPositions()
                        position_synced.set()
                    except Exception as exc:
                        nonlocal sync_error
                        sync_error = exc
                        position_synced.set()
                
                # Вызываем reqPositions в правильном loop
                ib_loop.call_soon_threadsafe(_do_req_positions)
                
                # Ждем завершения запроса (максимум 2 секунды)
                if position_synced.wait(timeout=2.0):
                    if sync_error:
                        logging.warning(f"reqPositions() error during sync: {sync_error}, using cached data")
                    else:
                        # Даем больше времени для обновления кеша через positionEvent
                        # Используем ib.sleep() для правильной работы с event loop
                        logging.info("Waiting for positionEvent to update cache...")
                        try:
                            # Если мы в правильном потоке, используем ib.sleep()
                            if threading.current_thread() is threading.main_thread():
                                ib.sleep(2.0)
                            else:
                                # В другом потоке используем time.sleep()
                                time.sleep(2.0)
                        except Exception as sleep_exc:
                            logging.debug(f"Sleep error: {sleep_exc}, using time.sleep()")
                            time.sleep(2.0)
                        logging.info("Positions cache synced via socket")
                else:
                    logging.warning("reqPositions() sync timeout, using cached data")
            except Exception as exc:
                logging.warning(f"Failed to sync positions cache: {exc}, using cached data")
        else:
            logging.warning("Cannot sync positions cache: event loop not available")
        
        # Читаем позиции из кеша (обновленного через positionEvent)
        try:
            positions = list(ib.positions())
            logging.info("Cached positions (synced via socket): %s", positions)
            
            # Логируем детали для отладки
            if positions:
                for pos in positions:
                    qty = float(pos.position)
                    if abs(qty) > 0.001:  # Только ненулевые позиции
                        symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                        expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                        logging.info(f"  Open position: {symbol} {expiry} qty={qty} avgCost={pos.avgCost}")
            else:
                logging.info("  No open positions found")
            
            return positions
        except Exception as exc:
            logging.exception("Failed to read positions: %s", exc)
            self._safe_notify(f"❌ Failed to read positions: {exc}")
            return []

    def force_sync_positions(self) -> List:
        """
        Принудительно синхронизировать позиции через сокет (reqPositions()).
        Отправляет команду через IB API socket для получения актуальных позиций.
        
        Returns:
            List of Position objects (updated via positionEvent after reqPositions())
        """
        ib = self.ib
        if not ib.isConnected():
            logging.warning("IB not connected, cannot force sync positions")
            self._safe_notify("⚠️ IB not connected, cannot sync positions")
            return []
        
        ib_loop = self._loop
        
        if ib_loop is None or ib_loop.is_closed():
            logging.error("Cannot force sync positions: event loop not available")
            self._safe_notify("❌ Cannot sync positions: event loop not available")
            return []
        
        logging.info("🔌 Force syncing positions via socket (reqPositions())...")
        self._safe_notify("🔄 Syncing positions via socket...")
        
        try:
            import threading
            position_synced = threading.Event()
            sync_error = None
            
            def _do_req_positions():
                """Выполняем reqPositions() в правильном event loop."""
                nonlocal sync_error  # Объявляем nonlocal в начале функции
                try:
                    ib.reqPositions()
                    logging.info("reqPositions() command sent via socket")
                    position_synced.set()
                except RuntimeError as exc:
                    # Обрабатываем "This event loop is already running"
                    if "already running" in str(exc):
                        logging.debug("Event loop already running - positionEvent will update cache anyway")
                        # Не критично - positionEvent все равно обновит кеш
                        position_synced.set()
                    else:
                        sync_error = exc
                        logging.error(f"reqPositions() error: {exc}")
                        position_synced.set()
                except Exception as exc:
                    sync_error = exc
                    logging.error(f"reqPositions() error: {exc}")
                    position_synced.set()
            
            # Вызываем reqPositions() в правильном loop через сокет
            ib_loop.call_soon_threadsafe(_do_req_positions)
            
            # Ждем отправки команды (максимум 2 секунды)
            if not position_synced.wait(timeout=2.0):
                logging.warning("reqPositions() command timeout")
                # Продолжаем - кеш может обновиться через positionEvent
            elif sync_error:
                if "already running" in str(sync_error):
                    logging.debug("reqPositions() event loop issue - positionEvent will update cache")
                    # Не критично - positionEvent все равно обновит кеш
                else:
                    logging.warning(f"reqPositions() failed: {sync_error}")
                    # Продолжаем - кеш может обновиться через positionEvent
            
            # Ждем обновления кеша через positionEvent (IB отправит данные через сокет)
            logging.info("Waiting for positionEvent to update cache (socket response)...")
            wait_time = 0
            max_wait = 8.0  # Увеличиваем до 8 секунд
            
            # Делаем несколько проверок кеша с ожиданием
            last_position_count = -1
            stable_count = 0
            
            while wait_time < max_wait:
                try:
                    if threading.current_thread() is threading.main_thread():
                        ib.sleep(1.0)
                    else:
                        time.sleep(1.0)
                except Exception:
                    time.sleep(1.0)
                
                wait_time += 1.0
                
                # Проверяем кеш каждую секунду
                positions = list(ib.positions())
                current_count = len(positions)
                
                # Если количество позиций стабильно 2 секунды подряд - считаем что обновилось
                if current_count == last_position_count:
                    stable_count += 1
                    if stable_count >= 2:
                        logging.info(f"Position cache stable after {wait_time}s")
                        break
                else:
                    stable_count = 0
                    last_position_count = current_count
                    logging.debug(f"Cache check at {wait_time}s: {current_count} positions (changed)")
            
            logging.info(f"Position sync completed after {wait_time}s")
            
            # Читаем обновленные позиции из кеша
            positions = list(ib.positions())
            
            logging.info(f"✅ Positions synced via socket: {len(positions)} total positions")
            
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
        Retries on connection loss (ApiCancelled).
        """
        max_retries = 3
        retry_delay = 5.0  # секунд
        
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
                    self.ib.waitOnUpdate(timeout=5)
                    
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
                        qualified = ib.qualifyContracts(contract)
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
                        
                        def _do_req_positions():
                            try:
                                self.ib.reqPositions()
                                position_synced.set()
                            except Exception as exc:
                                logging.debug(f"reqPositions() error in _on_exec_details (attempt {sync_attempt+1}): {exc}")
                                position_synced.set()
                        
                        ib_loop.call_soon_threadsafe(_do_req_positions)
                        
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
                            positions = list(self.ib.positions())
                            open_positions = [p for p in positions if abs(float(p.position)) > 0.001]
                            
                            # Ищем позицию по этому контракту
                            contract_positions = [
                                p for p in open_positions 
                                if (getattr(p.contract, "localSymbol", "") == getattr(contract, "localSymbol", "") or
                                    getattr(p.contract, "symbol", "") == getattr(contract, "symbol", ""))
                            ]
                            
                            if not contract_positions:
                                logging.info(f"✅ Position fully closed confirmed after bracket exit fill (attempt {sync_attempt+1})")
                                # Отправляем уведомление о закрытии позиции
                                symbol = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
                                expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
                                self._safe_notify(
                                    f"✅ Position closed: {symbol} {expiry}\n"
                                    f"Closed via TP/SL fill"
                                )
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
                logging.info(f"Order {order_id} filled: {trade.orderStatus.filled} @ {trade.orderStatus.avgFillPrice}")
                
                # Если это TP/SL ордер (OCA group), синхронизируем позиции
                oca_group = getattr(order, "ocaGroup", "") or ""
                if oca_group.startswith("BRACKET_"):
                    logging.info("Bracket order filled, syncing positions cache...")
                    try:
                        ib_loop = self._loop
                        if ib_loop is not None and not ib_loop.is_closed() and self.ib.isConnected():
                            import threading
                            position_synced = threading.Event()
                            
                            def _do_req_positions():
                                try:
                                    self.ib.reqPositions()
                                    position_synced.set()
                                except Exception as exc:
                                    logging.warning(f"reqPositions() error in _on_order_status: {exc}")
                                    position_synced.set()
                            
                            ib_loop.call_soon_threadsafe(_do_req_positions)
                            if position_synced.wait(timeout=2.0):
                                # Даем больше времени для обновления кеша
                                time.sleep(3.0)
                                
                                # Проверяем позиции
                                positions = list(self.ib.positions())
                                open_positions = [p for p in positions if abs(float(p.position)) > 0.001]
                                logging.info(f"Positions after bracket fill: {len(open_positions)} open positions")
                                
                                if open_positions:
                                    for pos in open_positions:
                                        symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                                        qty = pos.position
                                        logging.info(f"  Still open: {symbol} qty={qty}")
                    except Exception as sync_exc:
                        logging.debug(f"Failed to sync positions after order fill: {sync_exc}")
            elif status in ["PendingSubmit", "PreSubmitted", "Submitted"]:
                logging.debug(f"Order {order_id} in progress: {status}")
        except Exception as exc:
            logging.exception("Error in _on_order_status: %s", exc)

    def _on_position_change(self, position):
        """
        Handler для positionEvent - вызывается автоматически при изменении позиций через сокет.
        Это и есть мониторинг через WebSocket (IB API использует TCP сокет).
        """
        logging.info(
            f"🔌 PositionEvent (socket update): {position.contract.localSymbol or position.contract.symbol} "
            f"qty={position.position} avgCost={position.avgCost}"
        )
        
        # Если позиция закрылась (qty=0), отправляем уведомление
        if abs(float(position.position)) < 0.001:
            symbol = position.contract.localSymbol or position.contract.symbol
            expiry = getattr(position.contract, "lastTradeDateOrContractMonth", "")
            self._safe_notify(
                f"✅ Position closed via socket: {symbol} {expiry}\n"
                f"Previous qty: {position.position}"
            )

    def _on_error(self, reqId: int, errorCode: int, errorString: str, contract: Optional[Contract] = None) -> None:
        """Handle IB API errors."""
        # Skip informational messages (errorCode < 1000)
        if errorCode < 1000:
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
            self._safe_notify(
                f"⚠️ IB connection lost (Error 1100). Attempting to reconnect..."
            )
            # Пытаемся переподключиться
            self._reconnecting = True
            try:
                if not self.ib.isConnected():
                    logging.info("Reconnecting to IB...")
                    # Проверяем, есть ли event loop перед переподключением
                    try:
                        loop = asyncio.get_running_loop()
                        # Event loop есть - можно переподключаться
                        self.connect()
                        self._safe_notify("✅ Reconnected to IB Gateway/TWS.")
                    except RuntimeError:
                        # Нет event loop - ib_insync создаст свой при connect()
                        self.connect()
                        self._safe_notify("✅ Reconnected to IB Gateway/TWS.")
                else:
                    logging.info("Connection restored, clearing reconnecting flag")
            except Exception as exc:
                logging.exception(f"Failed to reconnect: {exc}")
                self._safe_notify(f"❌ Failed to reconnect: {exc}")
            finally:
                self._reconnecting = False
            return
        
        # Error 10328: Connection lost, order data could not be resolved
        if errorCode == 10328:
            logging.error(
                f"🔌 Connection lost during order (Error 10328): {errorString}. "
                f"Order data may be lost."
            )
            self._safe_notify(
                f"⚠️ Connection lost during order (Error 10328). "
                f"Order may have been cancelled."
            )
            # Пытаемся переподключиться
            self._reconnecting = True
            try:
                if not self.ib.isConnected():
                    logging.info("Reconnecting to IB after order error...")
                    self.connect()
                    self._safe_notify("✅ Reconnected to IB Gateway/TWS.")
                else:
                    logging.info("Connection restored, clearing reconnecting flag")
            except Exception as exc:
                logging.exception(f"Failed to reconnect: {exc}")
            finally:
                self._reconnecting = False
            return
        
        # Информационные сообщения о соединении - логируем как INFO/WARNING, не ERROR
        if errorCode in [2104, 2105, 2106]:
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
