import logging
import os
import queue
from logging.handlers import TimedRotatingFileHandler
from queue import Queue
from threading import Thread, Event, Lock

from bot import send_file
from yadisk_api import YDApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    filename='logs/workers.log',
    when='midnight'
)
handler.setFormatter(
    logging.Formatter(
        '[%(asctime)s] [%(levelname)s] "%(message)s"'
    )
)
logger.addHandler(handler)


class Workers:
    def __init__(self, amount_of_workers: int, download_requests: Queue,
                 token: str):
        self._stop: Event = Event()
        self._file_lock: Lock = Lock()
        self.workers: list[Thread] = []

        for i in range(amount_of_workers):
            self.workers.append(Thread(target=self._worker))

        self.yd_api: YDApi = YDApi(token)
        self.requests: Queue = download_requests

    def start(self):
        for w in self.workers:
            w.start()

    def stop(self):
        self._stop.set()

        for w in self.workers:
            w.join()

    def _worker(self):
        while not self._stop.is_set():
            try:
                user_id, public_key, path = self.requests.get_nowait()
            except queue.Empty:
                continue

            # TODO: Add data collection about downloading
            # TODO: Add caching

            name: str = self.yd_api.save(public_key, path)
            link: str = self.yd_api.get_download_link(name)

            data = self.yd_api.download(link)
            self.yd_api.delete(f'/Загрузки/{name}')

            with self._file_lock:
                with open(rf'temp\{name}', 'wb') as file:
                    file.write(data)

                # os.system(f'zip -s 1983m {name}.zip {name}')

                send_file(user_id, name)

                os.remove(rf'temp\{name}')
