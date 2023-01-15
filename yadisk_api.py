import logging
from logging.handlers import TimedRotatingFileHandler
from threading import Lock
from time import sleep

from requests import Session, Response, get

session: 'LimitedRPPSession'
URL: str = 'https://cloud-api.yandex.net/v1/disk/'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    filename='logs/yadisk_api.log',
    when='midnight'
)
handler.setFormatter(
    logging.Formatter(
        '[%(asctime)s] [%(levelname)s] "%(message)s"'
    )
)
logger.addHandler(handler)


class LimitedRPPSession(Session):
    """Session with limited requests per period (secs)"""

    def __init__(self, amount: int, period: float = 1.0):
        self._lock = Lock()
        self._period: float = period / amount

        super().__init__()

    def request(self, *args, **kwargs) -> Response:
        # TODO: Add max retries error handling
        with self._lock:
            sleep(self._period)
            resp: Response = super().request(*args, **kwargs)

        return resp


def _fetch_metadata(public_key: str, path: str):
    r = get(
        f'{URL}public/resources',
        params={
            "public_key": public_key,
            "path": path
        }
    )

    # TODO: Add logging
    r.raise_for_status()

    return r.json()


class YDResource:
    def __init__(self, public_key: str):

        self.files: dict[str, int | dict] = {
            "/": {}
        }
        self.path: list[str] = ['/']

        data: dict = _fetch_metadata(public_key, self.cwd)
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

    def ll(self) -> dict[str, int | dict]:
        files: dict[str, int | dict] = self.files

        for depth, folder in enumerate(self.path, start=1):
            if files.get(folder, False):
                files = files[folder]
                continue

            data: dict = _fetch_metadata(self.public_key, self.cwd)

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

    def up(self):
        self.path.pop()

    def goto(self, location: str):
        if location in self.ll():
            self.path.append(location)
        else:
            raise FileNotFoundError(f"No such directory: '{location}'")


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

        # TODO: Add logging
        r.raise_for_status()

        link: str = r.json()["href"]

        match r.status_code:
            case 201:
                return path.split('/')[-1]
            case 203:
                r = session.get(link)
                status = r.json()["status"]

                while status == 'in-progress':
                    sleep(10)
                    r = session.get(link)
                    status = r.json()["status"]

                if status == 'success':
                    return path.split('/')[-1]
            case _:
                # TODO: Add logging
                pass

    def get_download_link(self, name: str) -> str:
        r: Response = self.session.get(
            f'{URL}resources/download',
            params={
                "path": f'/Загрузки/{name}',
                "fields": 'href'
            }
        )

        # TODO: Add logging
        r.raise_for_status()

        return r.json()["href"]

    def download(self, link: str) -> bytes:
        r: Response = self.session.get(link)

        # TODO: Add logging
        r.raise_for_status()

        return r.content

    def delete(self, path):
        r: Response = self.session.delete(
            f'{URL}resources',
            params={
                "path": path,
                "force_async": False
            }
        )

        # TODO: Add logging
        r.raise_for_status()

        return None
