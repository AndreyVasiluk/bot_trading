import logging
import time
from datetime import datetime, timezone
import threading
import os
from typing import Optional, Dict, List, Callable

from .config import load_trading_config, load_env_config
from .ib_client import IBClient
from .notifier import TelegramNotifier, telegram_command_loop
from .strategy import TimeEntryBracketStrategy
from .scheduler import DailyScheduler


def setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)

    log_dir = "/app/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "bot.log")

    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.StreamHandler(),        # console (docker logs)
            logging.FileHandler(log_file),  # file /app/logs/bot.log
        ],
    )


class MultiNotifier:
    """Send the same message to multiple Telegram notifiers."""

    def __init__(self, *notifiers: Optional[TelegramNotifier]) -> None:
        # Filter out None (in case second bot is not configured)
        self._notifiers = [n for n in notifiers if n is not None]
        logging.info(f"MultiNotifier initialized with {len(self._notifiers)} notifier(s)")
        for i, n in enumerate(self._notifiers, 1):
            logging.info(f"  Notifier {i}: chat_id={getattr(n, 'chat_id', 'unknown')[:15]}...")

    def send(self, text: str, keyboard=None) -> None:
        for i, n in enumerate(self._notifiers, 1):
            try:
                # We ignore keyboard for now in main(), but support it for future use
                if keyboard is not None:
                    n.send(text, keyboard=keyboard)
                else:
                    n.send(text)
                logging.debug(f"Message sent to notifier {i} (chat_id={n.chat_id[:10]}...)")
            except Exception as exc:
                logging.exception(
                    f"Failed to send Telegram message to notifier {i} "
                    f"(chat_id={getattr(n, 'chat_id', 'unknown')[:10]}...): {exc}"
                )


def position_monitor_loop(ib_client: IBClient, notifier: MultiNotifier) -> None:
    """
    Background thread that monitors positions via positionEvent (socket-based).
    Uses centralized handler from IBClient to avoid duplication.
    Falls back to polling if event-based monitoring fails.
    """
    logging.info("Position monitor thread started (event-based)")
    
    # НЕ создаем свой обработчик - используем централизованный из ib_client
    # ib_client._on_position_change уже подписан на positionEvent и использует
    # централизованную проверку _check_position_closed 
    
    
    # Инициализируем начальное состояние через get_positions_from_broker
    # (она сама обновит _last_positions в ib_client)
    try:
        if ib_client.ib.isConnected():
            logging.info("Initializing position tracking from broker...")
            initial_positions = ib_client.get_positions_from_broker()
            logging.info(f"Position tracking initialized: {len(initial_positions)} positions")
            
            # Логируем детали позиций для отладки
            for pos in initial_positions:
                symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                qty = float(pos.position)
                if abs(qty) > 0.001:
                    logging.info(f"position_monitor_loop: INITIALIZED position from BROKER: {symbol} {expiry} qty={qty}")
        
        # Комбинированный режим: события + периодический polling для надежности
        # События от IB API могут иногда не приходить, поэтому делаем активные проверки
        # Улучшено: проверяем закрытие позиций в реальном времени
        while True:
            try:
                # Используем time.sleep() так как это отдельный поток, но периодически даем IB обработать события
                time.sleep(10)  # Уменьшено до 10 секунд для более частых проверок
                
                if not ib_client.ib.isConnected():
                    logging.warning("Position monitor: IB disconnected, will retry when reconnected")
                    continue
                
                # Периодически проверяем позиции напрямую с брокера
                # Это гарантирует обнаружение закрытия позиций, даже если события не пришли
                logging.info("Position monitor: periodic check of positions from broker (checking for closures)...")
                
                # Сохраняем предыдущее состояние позиций для сравнения
                previous_con_ids = set(ib_client._last_positions.keys())
                previous_open_positions = {
                    con_id: qty for con_id, qty in ib_client._last_positions.items() 
                    if abs(qty) > 0.001
                }
                
                current_positions = ib_client.get_positions_from_broker()
                current_con_ids = {}
                for p in current_positions:
                    try:
                        qty = float(p.position)
                        if abs(qty) > 0.001:
                            current_con_ids[p.contract.conId] = p
                    except (AttributeError, ValueError) as exc:
                        logging.warning(f"Position monitor: error parsing position object: {exc}")
                        continue
                
                # Проверяем, какие позиции закрылись
                for con_id, old_qty in previous_open_positions.items():
                    if con_id not in current_con_ids:
                        # Позиция была открыта, но теперь закрыта - проверяем через _check_position_closed
                        logging.warning(
                            f"Position monitor: detected potential closure - conId={con_id} "
                            f"was {old_qty}, not found in current positions"
                        )
                        
                        # Пытаемся найти контракт из сохраненных контрактов (_position_contracts)
                        logging.info(f"Position monitor: searching for contract conId={con_id} (was qty={old_qty})")
                        contract = ib_client._position_contracts.get(con_id)
                        if contract:
                            logging.info(f"Position monitor: found contract in _position_contracts for conId={con_id}")
                        
                        # Если не нашли в сохраненных, пытаемся найти в текущих позициях
                        if contract is None:
                            logging.debug(f"Position monitor: contract not in _position_contracts, checking current_positions...")
                            for pos in current_positions:
                                if pos.contract.conId == con_id:
                                    contract = pos.contract
                                    logging.info(f"Position monitor: found contract in current positions for conId={con_id}")
                                    break
                        
                        # Если все еще не нашли, пытаемся получить из ib.positions()
                        if contract is None:
                            logging.debug(f"Position monitor: contract not in current_positions, checking ib.positions()...")
                            try:
                                all_positions = list(ib_client.ib.positions())
                                for pos in all_positions:
                                    if pos.contract.conId == con_id:
                                        contract = pos.contract
                                        logging.info(f"Position monitor: found contract in ib.positions() for conId={con_id}")
                                        break
                            except Exception as exc:
                                logging.debug(f"Could not get contract from ib.positions() for conId={con_id}: {exc}")
                        
                        if contract:
                            # Вызываем проверку закрытия
                            logging.info(f"Position monitor: checking closure for conId={con_id} via _check_position_closed")
                            closed = ib_client._check_position_closed(contract, 0.0, "position_monitor_loop")
                            if closed:
                                logging.info(f"Position monitor: closure notification sent for conId={con_id}")
                        else:
                            logging.warning(
                                f"Position monitor: could not find contract for closed position conId={con_id} "
                                f"(checked _position_contracts={len(ib_client._position_contracts)} contracts, "
                                f"current_positions={len(current_positions)} positions, ib.positions())"
                            )
                
                # Логируем текущие открытые позиции
                logging.info(f"Position monitor: found {len(current_con_ids)} open positions")
                for pos in current_positions:
                    symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                    expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                    qty = float(pos.position)
                    if abs(qty) > 0.001:
                        logging.info(f"position_monitor_loop: MONITORING position from BROKER: {symbol} {expiry} qty={qty}")
                    else:
                        logging.debug(f"position_monitor_loop: found closed position: {symbol} {expiry} qty={qty}")
                
            except Exception as exc:
                logging.exception("Error in position monitor loop: %s", exc)
                time.sleep(10)
                
    except Exception as exc:
        logging.exception("Error in event-based position monitor, falling back to polling: %s", exc)
        # Fallback к polling - используем get_positions_from_broker которая сама обновит состояние
        while True:
            try:
                time.sleep(10)  # Уменьшено до 10 секунд
                
                if not ib_client.ib.isConnected():
                    logging.debug("Position monitor (fallback): IB not connected, skipping position check")
                    continue
                
                # Получаем актуальные позиции напрямую с брокера
                # get_positions_from_broker сама обновит _last_positions и отправит уведомления
                logging.info("Position monitor (fallback): requesting fresh positions from broker...")
                current_positions = ib_client.get_positions_from_broker()
                logging.info(f"Position monitor (fallback): got {len(current_positions)} positions")
                
                # Логируем детали позиций для отладки
                for pos in current_positions:
                    symbol = getattr(pos.contract, "localSymbol", "") or getattr(pos.contract, "symbol", "")
                    expiry = getattr(pos.contract, "lastTradeDateOrContractMonth", "")
                    qty = float(pos.position)
                    if abs(qty) > 0.001:
                        logging.info(f"position_monitor_loop (fallback): CHECKING position from BROKER: {symbol} {expiry} qty={qty}")
                    else:
                        logging.debug(f"position_monitor_loop (fallback): found closed position: {symbol} {expiry} qty={qty}")
                
            except Exception as exc:
                logging.exception("Error in position monitor loop (fallback): %s", exc)
                time.sleep(10)


def connection_status_monitor(ib_client: IBClient) -> None:
    """
    Background thread that logs IB socket connection status every minute.
    """
    logging.info("Connection status monitor thread started")
    
    while True:
        try:
            time.sleep(60)  # Проверка раз в минуту
            
            is_connected = ib_client.ib.isConnected()
            host = ib_client.host
            port = ib_client.port
            
            if is_connected:
                # Проверяем, есть ли event loop
                ib_loop = ib_client._loop
                loop_status = "available" if ib_loop is not None and not ib_loop.is_closed() else "unavailable"
                
                logging.info(
                    f"🔌 IB Socket Status: CONNECTED to {host}:{port} "
                    f"(event loop: {loop_status})"
                )
            else:
                logging.warning(
                    f"🔌 IB Socket Status: DISCONNECTED from {host}:{port}"
                )
                
        except Exception as exc:
            logging.exception("Error in connection status monitor: %s", exc)
            time.sleep(60)  # Продолжаем после ошибки


def main() -> None:
    # Load configs
    trading_cfg = load_trading_config()
    env_cfg = load_env_config()

    setup_logging(env_cfg.log_level)

    logging.info("Starting IBKR trading bot with config: %s", trading_cfg)

    # Connect IB Gateway (in background thread)
    ib_client = IBClient(env_cfg.ib_host, env_cfg.ib_port, env_cfg.ib_client_id)
    threading.Thread(target=ib_client.connect, daemon=True).start()

    # --- Telegram notifiers (two bots) ---

    # Primary bot: with commands (/positions, /config, CLOSE ALL, etc.)
    notifier1 = TelegramNotifier(
        env_cfg.telegram_bot_token,
        env_cfg.telegram_chat_id,
    )

    # Optional second bot: only for notifications (no command loop here)
    bot2_token = getattr(env_cfg, "telegram_bot2_token", None)
    bot2_chat_id = getattr(env_cfg, "telegram_chat2_id", None)
    notifier2: Optional[TelegramNotifier] = None
    if bot2_token and bot2_chat_id:
        notifier2 = TelegramNotifier(bot2_token, bot2_chat_id)
        logging.info(f"✅ Second Telegram bot configured (chat_id={bot2_chat_id[:15]}...)")
    else:
        logging.warning(
            "⚠️ Second Telegram bot NOT configured: "
            f"TELEGRAM_BOT2_TOKEN={'✅ set' if bot2_token else '❌ NOT SET'}, "
            f"TELEGRAM_CHAT2_ID={'✅ set' if bot2_chat_id else '❌ NOT SET'}"
        )

    # Unified notifier that broadcasts to both bots
    notifier = MultiNotifier(notifier1, notifier2)

    # Attach TP/SL/CLOSE ALL / PnL-style notifications from IB client to Telegram
    ib_client.set_notify_callback(lambda text: notifier.send(text))

    notifier.send(
        f"✅ IBKR bot started.\n"
        f"Symbol: {trading_cfg.symbol} {trading_cfg.expiry}\n"
        f"Side: {trading_cfg.side} qty={trading_cfg.quantity}\n"
        f"Entry time (UTC): {trading_cfg.entry_time_utc}"
    )

    # Trading job executed at scheduled time
    def job() -> None:
        now = datetime.now(timezone.utc).isoformat()
        logging.info("Executing scheduled trade job at %s", now)

        # 1) Check IB connection before running the strategy
        try:
            if not ib_client.ib.isConnected():
                logging.warning(
                    "IB is not connected, trying to reconnect before running strategy..."
                )

                try:
                    ib_client.connect()
                except Exception as exc:
                    logging.exception("Reconnect to IB failed: %s", exc)
                    notifier.send(
                        "❌ IB Gateway is not connected — cannot execute scheduled entry.\n"
                        "Please check TWS / IB Gateway and Internet connection."
                    )
                    return

                # If still not connected after reconnect attempt — skip this run
                if not ib_client.ib.isConnected():
                    logging.error(
                        "Still not connected to IB after reconnect attempt, skipping run"
                    )
                    notifier.send(
                        "❌ After reconnect attempt IB API is still not connected.\n"
                        "This run is skipped, next attempt will be at the next scheduled time."
                    )
                    return
            
            # Ждем завершения переподключения, если оно идет
            if ib_client._reconnecting:
                logging.info("Waiting for reconnection to complete before running strategy...")
                wait_time = 0
                while ib_client._reconnecting and wait_time < 30:
                    time.sleep(1)
                    wait_time += 1
                if ib_client._reconnecting:
                    logging.warning("Reconnection timeout, proceeding anyway...")
                
                # Проверяем соединение еще раз после ожидания
                if not ib_client.ib.isConnected():
                    logging.error("Still not connected after waiting for reconnection, skipping run")
                    notifier.send(
                        "❌ IB API is not connected after reconnection wait.\n"
                        "This run is skipped."
                    )
                    return

        except Exception as exc:
            # Fallback if something goes wrong even while checking the connection
            logging.exception("Error while checking IB connection before job: %s", exc)
            notifier.send(f"❌ Error while checking IB connection: `{exc}`")
            return

        # 2) Connection is OK — run the strategy
        strategy = TimeEntryBracketStrategy(ib_client, trading_cfg)

        try:
            res = strategy.run()
            msg = (
                f"✅ Trade executed:\n"
                f"{res.side} {res.quantity} {trading_cfg.symbol} {trading_cfg.expiry}\n"
                f"Entry: {res.entry_price}\n"
                f"TP: {res.take_profit_price}\n"
                f"SL: {res.stop_loss_price}"
            )
            notifier.send(msg)
        except Exception as exc:
            logging.exception("Trade job failed: %s", exc)
            notifier.send(f"❌ Trade job failed: {exc}")

    # Daily scheduler (runs job at cfg.entry_time_utc)
    scheduler = DailyScheduler(trading_cfg.entry_time_utc, job)

    # Start Telegram command loop (buttons: TP, SL, TIME, /positions, /config, CLOSE ALL)
    # Command loop is only for the primary bot (notifier1)
    if env_cfg.telegram_bot_token and env_cfg.telegram_chat_id:
        cmd_thread = threading.Thread(
            target=telegram_command_loop,
            args=(
                env_cfg.telegram_bot_token,
                env_cfg.telegram_chat_id,
                trading_cfg,
                ib_client,
                scheduler,
            ),
            daemon=True,
        )
        cmd_thread.start()

    # Start position monitor thread (checks positions every minute)
    monitor_thread = threading.Thread(
        target=position_monitor_loop,
        args=(ib_client, notifier),
        daemon=True,
    )
    monitor_thread.start()
    logging.info("Position monitor thread started (checks every minute)")
    
    # Start connection status monitor thread (logs socket status every minute)
    status_thread = threading.Thread(
        target=connection_status_monitor,
        args=(ib_client,),
        daemon=True,
    )
    status_thread.start()
    logging.info("Connection status monitor thread started (logs every minute)")

    try:
        scheduler.run_forever()
    finally:
        ib_client.disconnect()


if __name__ == "__main__":
    main()