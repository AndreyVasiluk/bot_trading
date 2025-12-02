import logging
import time
from datetime import datetime, timedelta, time as dtime, timezone
from typing import Callable
import threading


class DailyScheduler:
    """
    Простий daily scheduler:
    - запускає job щодня в зазначений час (UTC)
    - дозволяє on-the-fly змінити час через set_time()
    """

    def __init__(self, entry_time_utc: dtime, job: Callable[[], None]) -> None:
        self._entry_time = entry_time_utc
        self._job = job
        self._lock = threading.Lock()
        self._next_run = self._calc_next_run()

    def _calc_next_run(self) -> datetime:
        now = datetime.now(timezone.utc)
        candidate = now.replace(
            hour=self._entry_time.hour,
            minute=self._entry_time.minute,
            second=self._entry_time.second,
            microsecond=0,
        )
        if candidate <= now:
            candidate = candidate + timedelta(days=1)
        logging.info("Next run scheduled at %s", candidate.isoformat())
        return candidate

    def set_time(self, new_time: dtime) -> None:
        """Оновити час входу (UTC) і перерахувати наступний запуск."""
        with self._lock:
            self._entry_time = new_time
            self._next_run = self._calc_next_run()

    def run_forever(self) -> None:
        """
        Основний цикл:
        - чекає до next_run
        - виконує job
        - рахує наступний next_run
        """
        while True:
            with self._lock:
                next_run = self._next_run

            now = datetime.now(timezone.utc)
            delta = (next_run - now).total_seconds()

            if delta <= 0:
                try:
                    self._job()
                except Exception:
                    logging.exception("Scheduled job error")
                with self._lock:
                    self._next_run = self._calc_next_run()
            else:
                time.sleep(min(delta, 60))