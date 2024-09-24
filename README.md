# yadisk-downloader-bot
This is Telegram bot for downloading files from Yandex Disk

## Dependencies
- [Telegram Bot Api Server](https://github.com/tdlib/telegram-bot-api)
- Others are specified in `requirements.txt`

## Installing

[Telegram Bot API server build instructions generator](https://tdlib.github.io/telegram-bot-api/build.html)

```shell
pip install -r requirements.txt
```

Then simply start `main.py` and you are good to go!

## Run via docker

```shell
docker build . -t yadisk-downloader-bot
docker run yadisk-downloader-bot
```
