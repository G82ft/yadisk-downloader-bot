import logging
from asyncio import get_event_loop, run_coroutine_threadsafe
from json import load
from logging.handlers import TimedRotatingFileHandler
from queue import Queue

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from yadisk_api import YDResource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    filename='logs/bot.log',
    when='midnight'
)
handler.setFormatter(
    logging.Formatter(
        '[%(asctime)s] [%(levelname)s] "%(message)s"'
    )
)
logger.addHandler(handler)

with open('config.json') as f:
    config: dict = load(f)
    bot = Bot(token=config["tg_token"])

dp = Dispatcher(
    bot,
    storage=MemoryStorage()
)

loop = get_event_loop()
download_requests: Queue = None  # type: ignore


def format_size(size: int):
    if size > 1 << 30:
        return f'{size / (1 << 30):.1f} GB'
    elif size > 1 << 20:
        return f'{size / (1 << 20):.1f} MB'
    elif size > 1 << 10:
        return f'{size / (1 << 10):.1f} KB'
    else:
        return f'{size} B'


class FileMenu:
    def __init__(self, user_id: int, resource: YDResource, rows_on_page: int = 5,
                 dr: Queue = Queue()):
        self.resource: YDResource = resource
        self.page: int = 0
        self.rows: int = rows_on_page

        async def menu_handler(q: types.CallbackQuery):
            command: str = q.data.removeprefix('fm:').split(':')[0]
            match command:
                case 'prev':
                    return await self.prev_page(q)
                case 'next':
                    return await self.next_page(q)
                case 'up':
                    return await self.up(q)
                case 'gt':
                    return await self.goto(q)
                case 'dl':
                    sub_command: str = q.data.removeprefix('fm:dl:'
                                                           ).split(':')[0]
                    match sub_command:
                        case '?':
                            return await self.ask_download(q)
                        case '.':
                            return await self.accept_download(q, dr)
                        case '!':
                            return await self.update_message(q.message)
                        case _:
                            return None

                case 'x':
                    sub_command: str = q.data.removeprefix('fm:x:'
                                                           ).split(':')[0]
                    match sub_command:
                        case '?':
                            return await self.ask_close(q.message)
                        case '!':
                            return await self.update_message(q.message)
                        case '.':
                            return await self.close(q.message)
                case _:
                    return None

        self.handler = menu_handler

        dp.register_callback_query_handler(
            self.handler,
            lambda q: q.data.startswith('fm:') and q.from_user.id == user_id,
            state='browsing'
        )

    def get_rows(self) -> list[list[types.InlineKeyboardButton]]:
        rows: list[list[types.InlineKeyboardButton]] = [
            [
                types.InlineKeyboardButton(
                    text='🚫',
                    callback_data='fm:x:?'
                )
            ]
        ]

        if self.resource.cwd != '/':
            rows[0].insert(
                0,
                types.InlineKeyboardButton(
                    text='⤴️',
                    callback_data='fm:up'
                )
            )

        offset: int = 0
        requires_paging: bool = self.requires_paging()
        if requires_paging:
            offset = self.page * self.rows
        for name, info in tuple(self.resource.ll().items())[offset:offset + self.rows]:
            index: int = self.resource.index(name)

            if isinstance(info, int):
                rows.append(
                    [
                        types.InlineKeyboardButton(
                            text=f'📄 [{format_size(info)}] {name}',
                            callback_data=f'fm:dl:?:{index}'
                        )
                    ]
                )
            else:
                rows.append(
                    [
                        types.InlineKeyboardButton(
                            text=f'📁 {name}',
                            callback_data=f'fm:gt:{index}'
                        )
                    ]
                )

        if requires_paging:
            rows += [
                [
                    types.InlineKeyboardButton(
                        text='◀️',
                        callback_data='fm:prev'
                    ),
                    types.InlineKeyboardButton(
                        text=str(self.page + 1),
                        callback_data=' '
                    ),
                    types.InlineKeyboardButton(
                        text='▶️',
                        callback_data='fm:next'
                    )
                ]
            ]

        return rows

    def requires_paging(self) -> bool:
        return len(self.resource.ll()) > self.rows

    async def next_page(self, q: types.CallbackQuery):
        if (self.page + 1) * self.rows < len(self.resource.ll()):
            self.page += 1
            await q.answer('Going to next page...')
        else:
            return await q.answer('This is the last page!')

        return await self.update_message(q.message)

    async def prev_page(self, q: types.CallbackQuery):
        if self.page > 0:
            self.page -= 1
            await q.answer('Going to previous page...')
        else:
            return await q.answer('This is the first page!')

        return await self.update_message(q.message)

    async def update_message(self, msg: types.Message):
        return await msg.edit_text(
            f'Path: {self.resource.name}{self.resource.cwd}',
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=self.get_rows()
            )
        )

    async def up(self, q: types.CallbackQuery):
        await q.answer(
            'Going up...'
        )

        self.resource.up()

        return await self.update_message(q.message)

    async def goto(self, q: types.CallbackQuery):
        location: str = self.resource[int(q.data.split(':')[-1])]

        await q.answer(
            f'Going to {location}...'
        )

        self.resource.goto(location)
        self.page = 0

        return await self.update_message(q.message)

    async def ask_download(self, q: types.CallbackQuery):
        index: int = int(q.data.split(':')[-1])
        file: str = self.resource[index]
        if file not in self.resource.ll():
            # TODO: Add logging
            return await q.answer(
                'There is no such file in current directory.\n'
                'Maybe you should wait a bit.',
                show_alert=True
            )

        await q.message.edit_text(
            'Are you sure you want to download '
            f'{self.resource.name}{self.resource.cwd}/{file}?\n'
            f'It is impossible to cancel it.',
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        types.InlineKeyboardButton(
                            '✅',
                            callback_data=f'fm:dl:.:{index}'
                        ),
                        types.InlineKeyboardButton(
                            '❌',
                            callback_data='fm:dl:!'
                        )
                    ]
                ]
            )
        )

    async def accept_download(self, q: types.CallbackQuery, dr: Queue):
        path: str = (
            f'{self.resource.cwd}/'
            f'{self.resource[(int(q.data.split(":")[-1]))]}'
        )

        dr.put((q.from_user.id, self.resource.public_key, path))

        await self.close(q.message)

        # TODO: Add logging
        return await q.message.edit_text(
            'Your request was putted in the queue.\n'
            f'Your approximate position is: {download_requests.qsize() + 1}'
        )

    @staticmethod
    async def ask_close(msg: types.Message):
        await msg.edit_text(
            'Are you sure you want to exit?',
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        types.InlineKeyboardButton(
                            '✅',
                            callback_data=f'fm:x:.'
                        ),
                        types.InlineKeyboardButton(
                            '❌',
                            callback_data='fm:x:!'
                        )
                    ]
                ]
            )
        )

    async def close(self, msg: types.Message):
        dp.callback_query_handlers.unregister(self.handler)
        await dp.current_state().set_state('idle')

        return await msg.edit_text(
            'File menu is closed.'
        )


@dp.message_handler(commands=['start'], state='*')
async def start(msg: types.Message):
    match await dp.current_state().get_state():
        case None:
            await msg.reply(
                'Hello!\n'
                'This is Telegram bot that allows you to download files from '
                'Yandex Disk without registration.\n'
                'Use /help for commands.'
            )
        case _:
            await msg.reply(
                'Restarting the bot...'
            )

    return await dp.current_state().set_state('idle')


@dp.message_handler(commands=['help'])
async def help_(msg: types.Message):
    await msg.reply(
        '/start - Start the bot\n'
        '/help - Show this message\n'
        '/fetch  - Get files\n',
        parse_mode='HTML'
    )


@dp.message_handler(commands=['fetch'], state='*')
async def fetch(msg: types.Message):
    match await dp.current_state().get_state():
        case 'browsing':
            return await msg.reply('Please close file explorer.')
        case 'idle':
            link: str = _get_link(msg.text)
            if not link:
                return await msg.reply(
                    'The link is missing or not valid.\n'
                    'The bot accepts links which start with '
                    'https://disk.yandex.ru/d/'
                )

            await dp.current_state().set_state('fetching')

            fm = FileMenu(
                msg.from_user.id,
                YDResource(link),
                5,
                download_requests
            )

            msg: types.Message = await msg.reply('Please wait, fetching...')

            await fm.update_message(msg)
            return await dp.current_state().set_state('browsing')
        case _:
            # TODO: Add logging
            return await msg.reply(
                'Something went wrong... Please restart the bot: /start.'
            )


def send_file(user_id: int, filename: str):
    async def _send_file_with_caption():
        msg: types.Message = await bot.send_document(
            user_id,
            types.InputFile(rf'temp\\{filename}')
        )
        await msg.edit_text(
            f'Here is your {filename}!'
        )

    run_coroutine_threadsafe(
        _send_file_with_caption(),
        loop
    ).result()


def _get_link(text: str) -> str | bool:
    if ' https://disk.yandex.ru/d/' not in text:
        return False

    return text.split()[1]


def main(queue: Queue):
    global download_requests, dp
    logger.warning('bot started')
    download_requests = queue
    executor.start_polling(dp)
