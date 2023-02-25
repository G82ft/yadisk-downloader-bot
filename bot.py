import logging
import os
from asyncio import get_event_loop, run_coroutine_threadsafe
from logging.handlers import TimedRotatingFileHandler
from queue import Queue

from aiogram import Bot, Dispatcher, executor, types
from aiogram.bot.api import TelegramAPIServer
from aiogram.contrib.fsm_storage.files import JSONStorage

import tokens
from yadisk_api import YDResource

logger = logging.getLogger(__name__)
handler = TimedRotatingFileHandler(
    filename='logs/bot.log',
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

loop = get_event_loop()


def _format_size(size: int):
    if size > 1 << 30:
        return f'{size / (1 << 30):.1f} GB'
    elif size > 1 << 20:
        return f'{size / (1 << 20):.1f} MB'
    elif size > 1 << 10:
        return f'{size / (1 << 10):.1f} KB'
    else:
        return f'{size} B'


class FileMenu:
    def __init__(self, dp: Dispatcher, user_id: int, resource: YDResource,
                 vol_size: int,
                 rows_on_page: int = 5, download_requests: Queue = Queue()):
        self.VOL_SIZE = vol_size
        self.resource: YDResource = resource
        self.page: int = 0
        self.rows: int = rows_on_page

        async def menu_handler(q: types.CallbackQuery):
            command: str = q.data.removeprefix('fm:').split(':')[0]
            match command:
                case 'upd':
                    return await self.update_message(q.message)
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
                        case '??':
                            return await self.ask_download(q, True)
                        case '.':
                            await self.accept_download(q, download_requests)
                            return await self.close(q.message, dp)
                        case 'i':
                            return await self.show_info(q.message)
                        case _:
                            return None

                case 'x':
                    sub_command: str = q.data.removeprefix('fm:x:'
                                                           ).split(':')[0]
                    match sub_command:
                        case '?':
                            return await self.ask_close(q.message)
                        case '.':
                            return await self.close(q.message, dp)
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
                icon: str
                data: str

                if info < self.VOL_SIZE:
                    icon = '📄'
                    data = f'fm:dl:?:{index}'
                elif info < 10_000_000_000:
                    icon = '📑'
                    data = f'fm:dl:??:{index}'
                else:
                    icon = '⚠️'
                    data = 'fm:dl:i'

                rows.append(
                    [
                        types.InlineKeyboardButton(
                            text=f'{icon} [{_format_size(info)}] {name}',
                            callback_data=data
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
        else:
            return await q.answer('This is the last page!')

        return await self.update_message(q.message)

    async def prev_page(self, q: types.CallbackQuery):
        if self.page > 0:
            self.page -= 1
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
        self.resource.up()

        return await self.update_message(q.message)

    async def goto(self, q: types.CallbackQuery):
        location: str = self.resource[int(q.data.split(':')[-1])]

        self.resource.goto(location)
        self.page = 0

        return await self.update_message(q.message)

    async def ask_download(self, q: types.CallbackQuery, warn_size: bool = False):
        index: int = int(q.data.split(':')[-1])
        file: str = self.resource[index]
        if file not in self.resource.ll():
            logger.error(f'There is no {file} in {self.resource.cwd}.')
            return await q.answer(
                'There is no such file in current directory.\n'
                'Maybe you should wait a bit.',
                show_alert=True
            )

        msg: str = (
            'Are you sure you want to download '
            f'{self.resource.name}{self.resource.cwd}/{file}?\n'
            f'It is impossible to cancel it.'
        )

        if warn_size:
            msg += (
                '\n⚠️ Due to telegram limitations on file upload size, '
                'it can be split into several archives.'
            )

        await q.message.edit_text(
            msg,
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        types.InlineKeyboardButton(
                            '✅',
                            callback_data=f'fm:dl:.:{index}'
                        ),
                        types.InlineKeyboardButton(
                            '❌',
                            callback_data='fm:upd'
                        )
                    ]
                ]
            )
        )

    async def accept_download(self,
                              q: types.CallbackQuery,
                              download_requests: Queue):
        path: str = (
            f'{self.resource.cwd}/'
            f'{self.resource[(int(q.data.split(":")[-1]))]}'
        )

        download_requests.put((q.from_user.id, self.resource.public_key, path))

        return await q.message.reply(
            'Your request was putted in the queue.\n'
            f'Your approximate position is: {download_requests.qsize() + 1}',
            reply=False
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
                            callback_data='fm:x:.'
                        ),
                        types.InlineKeyboardButton(
                            '❌',
                            callback_data='fm:upd'
                        )
                    ]
                ]
            )
        )

    @staticmethod
    async def show_info(msg: types.Message):
        return await msg.edit_text(
            'Sorry, but currently we can\'t download files bigger than 10 GB.',
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        types.InlineKeyboardButton(
                            text='✅',
                            callback_data='fm:upd'
                        )
                    ]
                ]
            )
        )

    async def close(self, msg: types.Message, dp: Dispatcher):
        dp.callback_query_handlers.unregister(self.handler)
        await dp.current_state().set_state('idle')

        return await msg.edit_text(
            'File menu is closed.'
        )


class YDBot:
    def __init__(self,
                 token: str, download_requests: Queue = Queue(),
                 vol_size: int = 1_983_000):
        self.bot = Bot(
            token=token,
            server=TelegramAPIServer.from_base('http://localhost:8081')
        )
        self.dp = Dispatcher(
            self.bot,
            storage=JSONStorage(f'data{os.sep}users.json')
        )

        self.download_requests: Queue = download_requests

        self.menu_handlers: dict[int: FileMenu] = {}

        @self.dp.message_handler(state='*')
        async def message_handler(msg: types.Message):
            if await self.dp.current_state().get_state() == 'feedback':
                return await self.feedback(msg)

            match msg.text.split()[0]:
                case '/start':
                    return await self.start(msg)
                case '/fetch':
                    return await self.fetch(msg, vol_size)
                case '/commands':
                    return await self.commands(msg)
                case '/about':
                    return await self.about(msg)
                case '/help':
                    return await self.help(msg)
                case '/feedback':
                    return await self.feedback(msg)
                case _:
                    return await msg.reply('Use /commands for commands.')

        logger.info('Bot initialized.')

    def start_polling(self):
        logger.info('Bot started.')
        executor.start_polling(self.dp)

    async def start(self, msg: types.Message):
        match await self.dp.current_state().get_state():
            case None:
                await msg.reply(
                    'Hello!\n'
                    'This is Telegram bot that allows you to download files '
                    'from Yandex Disk without registration.\n'
                    'Use /commands for commands.'
                )
            case _:
                bot_msg: types.Message = await msg.reply(
                    'Restarting the bot...'
                )

                if msg.from_user.id in self.menu_handlers:
                    try:
                        self.dp.callback_query_handlers.unregister(
                            self.menu_handlers.pop(msg.from_user.id, None)
                        )
                    except ValueError:
                        logger.warning('Unregistering error.')

                state = self.dp.current_state()
                await state.reset_data()
                await state.set_state('idle')

                return await bot_msg.edit_text(
                    'Bot restarted!'
                )

        return await self.dp.current_state().set_state('idle')

    async def fetch(self, msg: types.Message, vol_size: int):
        match await self.dp.current_state().get_state():
            case 'browsing':
                return await msg.reply(
                    'Please close the file explorer or restart the bot.'
                )
            case 'fetching':
                return await msg.reply(
                    'Please, wait, fetching...'
                )
            case 'idle':
                link: str = _get_link(msg.text)
                if not link:
                    return await msg.reply(
                        'The link is missing or not valid.\n'
                        'The bot accepts links which start with '
                        'https://disk.yandex.ru/d/'
                    )

                await self.dp.current_state().set_state('fetching')

                bot_msg: types.Message = await msg.reply(
                    'Please wait, fetching...'
                )

                fm = FileMenu(
                    self.dp,
                    msg.from_user.id,
                    YDResource(link),
                    vol_size,
                    5,
                    self.download_requests
                )

                self.menu_handlers[msg.from_id] = fm.handler

                await fm.update_message(bot_msg)
                return await self.dp.current_state().set_state('browsing')
            case unknown:
                logger.warning(
                    f'Unknown state for /fetch command: "{unknown}"'
                )
                return await msg.reply(
                    'Something went wrong... Please restart the bot: /start.'
                )

    @staticmethod
    async def commands(msg: types.Message):
        return await msg.reply(
            '/start - Start the bot.\n'
            '/fetch <link> - Get file (pass link without <>).\n'
            '/help - How to join split files?\n'
            '/about - Show info about the bot.\n'
            '/commands - Show this message.\n'
        )

    @staticmethod
    async def help(msg: types.Message):
        return await msg.reply(
            '*How to join files?*\n'
            '1. Open the terminal/command prompt.\n'
            '2. Go to the folder with files (`cd <path>`).\n'
            '3. Run the command to concatenate files.\n\n'
            '*Windows*:\n'
            'Press Windows + R, type `cmd` and press Enter.\n'
            '*Linux*:\n'
            '_Why are you even here, if you have Linux?_\n'
            'Press Ctrl + Alt + T to open Terminal.\n'
            '*Android*:\n'
            'You can download [terminal emulator]'
            '(https://play.google.com/store/apps/details?id=jackpal.androidterm)'
            ' and use command for _Linux_.\n',
            parse_mode='Markdown',
            disable_web_page_preview=True
        )

    @staticmethod
    async def about(msg: types.Message):
        return await msg.reply(
            'I was made by G4m3-80ft.\n'
            '• [Github](https://github.com/G4m3-80ft)\n'
            '• [Telegram Channel](https://t.me/blockofnonsense)\n\n'
            "Author of the idea: Daniel'.\n"
            '• [Telegram Channel](https://t.me/EGORxGG_channel)',
            parse_mode='Markdown',
            disable_web_page_preview=True
        )

    def send_message(self, user_id: int, text: str) -> None:
        run_coroutine_threadsafe(
            self.bot.send_message(
                user_id, text
            ),
            loop
        )

    def send_files(self, user_id: int, files: list[str]) -> list[str, ...]:
        async def _send_files() -> list[str]:
            file_ids: list[str] = []
            filenames: list[str] = []

            await self.bot.send_message(
                user_id,
                'Uploading files...'
            )

            logger.debug(f'Started files uploading ({files})...')

            for file in files:
                logger.debug(f'Sending {file}...')
                document: types.Document = (
                    await self.bot.send_document(
                        user_id,
                        types.InputFile(file) if os.path.exists(file) else file
                    )
                ).document
                file_ids.append(document.file_id)
                filenames.append(document.file_name)

            logger.info(f'Files sent ({files}).')

            await self.bot.send_message(
                user_id,
                'Done!'
            )
            if len(filenames) > 1:
                original_name: str = filenames[0][:-7]  # .part01
                await self.bot.send_message(
                    user_id,
                    'Windows:\n'
                    f'`copy /b {"+".join(filenames)} {original_name} /b`\n'
                    'Linux:\n'
                    f'`cat {" ".join(filenames)} > {original_name}`\n'
                    'For more info: /help.',
                    parse_mode='Markdown'
                )

            await self.dp.current_state(user=user_id).set_state('idle')

            return file_ids

        return run_coroutine_threadsafe(
            _send_files(),
            loop
        ).result()

    async def feedback(self, msg: types.Message):
        match await self.dp.current_state().get_state():
            case 'feedback':
                await msg.reply('Thank you for your feedback!')
                await self.bot.send_message(
                    1496610352,
                    msg.text
                )

                with open(f'data{os.sep}feedback.txt', 'a') as fb:
                    fb.write(f'{msg.text}\n\n')

                return await self.dp.current_state().set_state('idle')
            case _:
                await self.dp.current_state().set_state('feedback')
                return await msg.reply(
                    'Write your bug report/review here. You also can add your '
                    'username so we can contact you.'
                )


def _get_link(text: str) -> str | bool:
    if ' https://disk.yandex.ru/d/' not in text:
        return False

    return text.split()[1]


def main(queue: Queue, vol_size: int):
    bot: YDBot = YDBot(tokens.get("tg_token"), queue, vol_size)

    bot.start_polling()
