import time
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from threading import Lock
from math import ceil

logger = logging.getLogger(__name__)
handler = TimedRotatingFileHandler(
    filename='logs/cache.log',
    when='midnight'
)
handler.setLevel(logging.DEBUG)
handler.setFormatter(
    logging.Formatter(
        '[%(asctime)s] [%(levelname)s] "%(message)s"',
        datefmt='%d.%m.%Y %H:%M:%S'
    )
)
logger.addHandler(handler)


class Cache:
    def __init__(self, lock: Lock = Lock(), cache_file: str = 'data/cache.json'):
        self._file_lock: Lock = lock
        self.cache_file = cache_file
        self.cache: dict[str: dict[list | float]]

        try:
            with open(self.cache_file) as f:
                self.cache = json.load(f)
        except FileNotFoundError:
            logger.warning('Cache file not found, creating new one.')
            self.cache = {}
            self.save()
        except json.JSONDecodeError as JDE:
            logger.critical(
                f'The file "{self.cache_file}" is not JSON!',
                exc_info=JDE
            )
            raise

    def save(self):
        with self._file_lock and open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def __contains__(self, item):
        return item in self.cache

    def __len__(self):
        return len(self.cache)

    def __iter__(self):
        return iter(self.cache)

    def __getitem__(self, item):
        return self.cache.get(
            item,
            {}
        )

    def __setitem__(self, key: str, value: dict[str: list | float]):
        if key not in self.cache:
            if not isinstance(key, str):
                logger.error(f'Key must be a string, not {type(key)}.')
                raise TypeError(f'Key must be a string, not {type(key)}.')

            logger.info(f'New cache entry: {key}')

        if not isinstance(value, dict):
            logger.error(f'Value must be a dict, not {type(value)}.')
            raise TypeError(f'Value must be a dict, not {type(value)}.')
        if "files" not in value:
            logger.error('File IDs should be specified.')
            value["files"] = []
        if "time" not in value:
            logger.warning('Time not specified, using current time.')
            value["time"] = time.time()

        self.cache[key] = {
            "files": list(value["files"]),
            "time": ceil(value["time"])
        }
        self.save()

    def __delitem__(self, key):
        self.cache.pop(key, None)
        self.save()
