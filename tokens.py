import json
import logging
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger(__name__)
handler = TimedRotatingFileHandler(
    filename='logs/token-access.log',
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


def get(token_name: str):
    logger.warning(f'Requested access: {token_name}.')

    try:
        with open('config/tokens.json') as f:
            data: dict[str: str] = json.load(f)
    except FileNotFoundError:
        logger.critical('File with tokens not found!')
        return None
    except json.JSONDecodeError as JDE:
        logger.critical('The file is not JSON!', exc_info=JDE)
        return None

    if token_name not in data:
        logger.error(f'There is no such token ({token_name})!')
        return None

    return data[token_name]
