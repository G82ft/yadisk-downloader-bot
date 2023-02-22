import logging
from logging.handlers import TimedRotatingFileHandler
from math import ceil
from threading import Lock
from time import sleep
from time import strptime, mktime
from typing import Iterator

import requests
from requests import Session, Response
from requests.exceptions import ConnectionError
from urllib3.exceptions import MaxRetryError

URL: str = 'https://cloud-api.yandex.net/v1/disk/'

logger = logging.getLogger(__name__)
handler = TimedRotatingFileHandler(
    filename='logs/api.log',
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


class LimitedRPPSession(Session):
    """Session with limited requests per period (secs)"""

    def __init__(self, amount: int, period: float = 1.0):
        self._lock = Lock()
        self._period: float = period / amount

        super().__init__()

    def request(self, *args, **kwargs) -> Response:
        with self._lock:
            sleep(self._period)
            try:
                resp: Response = super().request(*args, **kwargs)
            except (MaxRetryError, ConnectionError) as e:
                logger.error(str(e))
                raise

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.error(str(e))
            raise

        return resp


class YDResource:
    def __init__(self, public_key: str):
        self.session: LimitedRPPSession = LimitedRPPSession(35)

        self.files: dict[str: int | dict] = {
            "/": {}
        }
        self.path: list[str] = ['/']

        data: dict = self._fetch_metadata(public_key, self.cwd)
        self.name: str = data["name"]
        self.public_key: str = data["public_key"]

        self.ll()

    def __getitem__(self, index: int):
        return tuple(self.ll())[index]

    @property
    def cwd(self) -> str:
        return f"/{'/'.join(self.path[1:])}"

    def index(self, item: str):
        return list(self.ll()).index(item)

    def ll(self) -> dict[str: int | dict]:
        files: dict[str: int | dict] = self.files

        for depth, folder in enumerate(self.path, start=1):
            if files.get(folder, False):
                files = files[folder]
                continue

            data: dict = self._fetch_metadata(self.public_key, self.cwd)

            if "_embedded" not in data:
                files[folder][data["name"]] = data["size"]
                continue

            items: list[dict] = data["_embedded"]["items"]
            for item in items:
                name: str = item["name"]
                if item["type"] == 'dir':
                    files[folder][name] = {}
                    continue

                files[folder][name] = item["size"]

            files = files[folder]

        return files

    def get_modified(self, path: str) -> int:
        data: dict = self._fetch_metadata(self.public_key, path)

        return ceil(
            mktime(
                strptime(
                    data["modified"],
                    '%Y-%m-%dT%H:%M:%S%z'
                )
            )
        )

    def up(self):
        self.path.pop()

    def goto(self, location: str):
        if location in self.ll():
            self.path.append(location)
        else:
            raise FileNotFoundError(f"No such directory: '{location}'")

    def _fetch_metadata(self, public_key: str, path: str):
        r = self.session.get(
            f'{URL}public/resources',
            params={
                "public_key": public_key,
                "path": path
            }
        )

        return r.json()


class YDApi:
    def __init__(self, token: str):
        self.session: LimitedRPPSession = LimitedRPPSession(35)
        self.session.headers.update(
            {
                "Authorization": f'OAuth {token}' if token else None,
                "Accept": 'application/json'
            }
        )

    def save(self, public_key: str, path: str) -> str:
        r = self.session.post(
            f'{URL}public/resources/save-to-disk',
            params={
                "public_key": public_key,
                "path": path,
                "force_async": False
            }
        )

        link: str = r.json()["href"]

        if r.status_code == 203:
            if self._get_operation_result(link) == 'failed':
                logger.error(
                    'Operation '
                    f'{link.removeprefix("https://cloud-api.yandex.net/v1/disk/operations/")}'
                    ' failed!'
                )

        return path.split('/')[-1]

    def _get_operation_result(self, link: str):
        r: Response = self.session.get(
            link
        )

        while r.json()["status"] == 'in-progress':
            sleep(10)
            r = self.session.get(link)

        return r.json()["status"]

    def get_download_link(self, name: str) -> str:
        r: Response = self.session.get(
            f'{URL}resources/download',
            params={
                "path": f'/Загрузки/{name}',
                "fields": 'href'
            }
        )

        return r.json()["href"]

    def download(self, link: str, buffer: int) -> Iterator[bytes]:
        r: Response = self.session.get(link)

        return r.iter_content(buffer)

    def delete(self, path):
        r: Response = self.session.delete(
            f'{URL}resources',
            params={
                "path": path,
                "force_async": False,
                "permanently": True
            }
        )

        return r
