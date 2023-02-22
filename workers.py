import logging
import os
import queue
from hashlib import md5
import time

from requests import HTTPError
from logging.handlers import TimedRotatingFileHandler
from sqlite3 import connect, Connection, Cursor
from threading import Thread, Event, Lock
from zipfile import ZipFile, ZIP_DEFLATED

from bot import YDBot
from cache import Cache
from tokens import get
from yadisk_api import YDApi, YDResource

logger = logging.getLogger(__name__)
handler = TimedRotatingFileHandler(
    filename='logs/workers.log',
    when='midnight'
)
handler.setFormatter(
    logging.Formatter(
        '[%(asctime)s] [%(levelname)s] "%(message)s"',
        datefmt='%d.%m.%Y %H:%M:%S'
    )
)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

bot: YDBot = YDBot(get('tg_token'))


class Workers:
    def __init__(self, workers: int, download_requests: queue.Queue,
                 token: str, volume_size: int, buffer_size: int, db_path: str):
        self._stop: Event = Event()
        self._file_lock: Lock = Lock()
        self.workers: list[Thread] = []
        self.cache: Cache = Cache(self._file_lock)

        self.VOL_SIZE: int = int(volume_size)
        self.BUF_SIZE: int = int(buffer_size)
        self.PATH: str = f'temp{os.sep}'

        connection: Connection = connect(db_path)
        connection.cursor().execute(
            """
            CREATE TABLE IF NOT EXISTS Statistics(
                ID INT PRIMARY KEY AUTOINCREMENT,
                PublicKey TEXT,
                Path TEXT,
                Size INT,
                StartTime INT,
                EndTime INT
            )
            """
        )

        connection.close()

        for i in range(workers):
            self.workers.append(
                Thread(
                    target=self.worker,
                    args=(db_path,),
                    name=f'Worker-{i+1:0>2}'
                )
            )

        self.yd_api: YDApi = YDApi(token)
        self.requests: queue.Queue = download_requests

    def start(self):
        for w in self.workers:
            w.start()

    def stop(self):
        self._stop.set()

        for w in self.workers:
            w.join()

    def worker(self, db_path: str):
        user_id: int
        public_key: str
        path: str
        size: int
        start_time: int

        con: Connection = connect(db_path)
        cursor: Cursor = con.cursor()

        while not self._stop.is_set():
            try:
                user_id, public_key, path = self.requests.get_nowait()
            except queue.Empty:
                continue

            start_time = round(time.time())
            try:
                size = self._handle_task(user_id, public_key, path)
            except TypeError as e:
                logger.error(f'TypeError (probably in cache): {e}')
            except ValueError as e:
                logger.error(f'ValueError (probably while zipping): {e}')

            except HTTPError as e:
                logger.error(f'HTTPError: {e}')
                time.sleep(10)
                self.requests.task_done()
                logger.error(f'Putting ({public_key}, {path}) back in queue...')
                self.requests.put((user_id, public_key, path))
                continue

            except Exception as e:
                logger.critical(
                    'Unexpected error!',
                    exc_info=e
                )
                bot.send_message(
                    user_id,
                    'Some unexpected error has occurred... '
                    'Please provide us with more info via /feedback.'
                )

            else:
                cursor.execute(
                    f"""
                    INSERT INTO Statistics(
                        PublicKey, Path, Size,
                        StartTime, EndTime
                    )
                    VALUES (
                        '{public_key}', '{path}', {size},
                        {start_time}, {round(time.time())}
                    )
                    """
                )
                with self._file_lock:
                    con.commit()

            finally:
                self.requests.task_done()

    def _handle_task(self, user_id: int, public_key: str, path: str) -> int:
        size: int
        hash_key: str = md5(
            (public_key + path).encode(errors='replace'),
            usedforsecurity=False
        ).hexdigest()

        if self._check_hash(path, public_key):
            logger.info(f'File {path} ({public_key}) is cached.')
            self._send_files(user_id, self.cache[hash_key]["files"])
            return 0

        name, link = self._save_file(public_key, path)
        download_path: str = self._download_file(name, link)

        with self._file_lock:
            size = os.path.getsize(download_path)
            files: list[str, ...] = split_file(
                zip_file(download_path),
                self.VOL_SIZE,
                self.BUF_SIZE
            )

        self.cache[hash_key] = {
            "time": YDResource(public_key).get_modified(path),
            "files": self._send_files(user_id, files)
        }

        return size

    def _save_file(self, public_key: str, path: str) -> tuple[str, str]:
        """:return: Name and link."""

        logger.debug(f'Started saving {path} ({public_key})...')
        name: str = self.yd_api.save(public_key, path)
        logger.info(f'Saved {path} ({public_key}).')

        link: str = self.yd_api.get_download_link(name)
        logger.debug(f'Got the download link ({link}).')

        return name, link

    def _download_file(self, name: str, link: str) -> str:
        """Downloads file and deletes it from YD.

        :returns: Path to downloaded file."""

        download_path: str = f'{self.PATH}{name}'

        if os.path.exists(download_path):
            logger.warning(f'File "{download_path}" already exists!')
            open(download_path, 'w').close()

        logger.debug(f'Started downloading from {link}...')
        for chunk in self.yd_api.download(link, self.BUF_SIZE):
            with self._file_lock:
                with open(download_path, 'ab') as file:
                    file.write(chunk)
        logger.info(f'Downloaded {name} from {link}.')

        self.yd_api.delete(f'/Загрузки/{name}')
        logger.debug('Deleted.')

        return download_path

    def _send_files(self, user_id: int, files: list[str, ...]) -> list[str, ...]:
        """Sends files and deletes them from computer.

        :returns: Sent file IDs."""

        logger.debug(f'Sending files ({files})...')

        with self._file_lock:
            file_ids: list[str, ...] = bot.send_files(user_id, files)

        logger.info('Files sent.')

        for file in files:
            with self._file_lock:
                if not os.path.exists(file):
                    logger.debug(f'File "{file}" does not exist.')
                    continue

                logger.debug(f'Removing {file}...')
                os.remove(file)

        logger.debug('Files removed.')

        return file_ids

    def _check_hash(self, path: str, public_key: str) -> bool:
        hash_key: str = md5(
            (public_key + path).encode(errors='replace'),
            usedforsecurity=False
        ).hexdigest()

        if not self.cache[hash_key]:
            logger.debug(f'File {path} ({public_key}) is not cached.')
            return False

        logger.debug(f'File {path} ({public_key}) is cached.')
        if self.cache[hash_key]["time"] < YDResource(public_key).get_modified(path):
            logger.debug(f'File {path} ({public_key}) is outdated.')
            return False

        logger.debug(f'File {path} ({public_key}) is up to date.')
        return True


def split_file(file: str,
               volume_size: int, max_buff: int = float('inf')
               ) -> list[str, ...]:
    """Splits file and deletes it.

    :returns: List of split files names."""
    part: int = 0

    part_names: list[str, ...] = []
    part_name: str

    file_size: int = os.stat(file).st_size
    if file_size < volume_size:
        return [file]

    name, ext = os.path.splitext(file)

    with open(file, 'rb') as src:
        while src.tell() < file_size:
            part += 1
            part_name = f'{name}{ext}.part{part:0>2}'
            part_names.append(part_name)

            with open(part_name, 'wb') as tgt:
                while (tgt.tell() < file_size - (volume_size * (part - 1))
                       and tgt.tell() < volume_size):
                    tgt.write(
                        src.read(
                            min(max_buff, volume_size - tgt.tell())
                        )
                    )

    os.remove(file)

    return part_names


def zip_file(file: str, name: str = None) -> str:
    """Zips file and deletes it.

    :returns: Name of the created archive."""
    if name is None:
        name = f'{file}.zip'

    with ZipFile(name, 'w', ZIP_DEFLATED) as archive:
        archive.write(file)

    os.remove(file)

    return name
