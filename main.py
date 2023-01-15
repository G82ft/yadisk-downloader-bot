from json import load
from queue import Queue

from bot import main
from workers import Workers


with open('config.json') as f:
    config: dict = load(f)

dr = Queue()

wrk = Workers(
    config["workers"],
    dr,
    config["ya_token"]
)
wrk.start()

main(dr)

wrk.stop()
