import logging
import os
from json import load, JSONDecodeError
from queue import Queue
from threading import Thread

try:
    with open(f'config{os.sep}config.json') as f:
        config: dict = load(f)
except FileNotFoundError:
    logging.critical(f'File "config{os.sep}config.json" not found!')
    raise FileNotFoundError(f'File "config{os.sep}config.json" not found!')
except JSONDecodeError as JDE:
    logging.critical('The file is not JSON!', exc_info=JDE)
    raise

try:
    logging.basicConfig(
        level=config.pop("log_level", 'INFO')
    )
except ValueError:
    logging.critical('Invalid log level!')
    raise

from bot import main
from workers import Workers
import tokens

dr: Queue = Queue()

config.update(
    {
        "download_requests": dr,
        "token": tokens.get("ya_token")
    }
)

Thread(
    target=os.system,
    args=(
        f'{config.pop("server_path", "./telegram-bot-api")} '
        f'--api-id={tokens.get("tg_api-id")} '
        f'--api-hash={tokens.get("tg_api-hash")}',
    ),
    daemon=True
).start()

wrk = Workers(**config)
wrk.start()

main(dr, config["volume_size"])

wrk.stop()
