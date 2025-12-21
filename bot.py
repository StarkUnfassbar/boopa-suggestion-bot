import asyncio
import logging
import os
import re
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from collections import defaultdict
from dataclasses import dataclass
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import *
import requests

load_dotenv()

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SHEETS_KEY_PATH')
SPREADSHEET_ID = os.getenv('GOOGLE_SPREADSHEET_ID')
SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME', 'Sheet1')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# Константа для ссылки на канал Бупы
BUPA_CHANNEL_LINK = "t.me/boopablup"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ------------------ Безопасные структуры данных ------------------
@dataclass
class PendingVideo:
    """Безопасная структура для хранения ожидающих видео"""
    video_url: str
    user_name: str
    user_id: int
    chat_id: int
    timestamp: float
    message_id: Optional[int] = None

class SafeVideoStorage:
    """Потокобезопасное хранилище для ожидающих видео"""
    def __init__(self):
        self._storage: Dict[str, PendingVideo] = {}
        self._lock = asyncio.Lock()
    
    async def add(self, video_key: str, video: PendingVideo):
        async with self._lock:
            self._storage[video_key] = video
    
    async def get(self, video_key: str) -> Optional[PendingVideo]:
        async with self._lock:
            return self._storage.get(video_key)
    
    async def remove(self, video_key: str) -> Optional[PendingVideo]:
        async with self._lock:
            return self._storage.pop(video_key, None)
    
    async def cleanup_old(self, max_age_seconds: int = 300):
        """Удаляет старые записи (старше 5 минут)"""
        async with self._lock:
            current_time = datetime.now().timestamp()
            old_keys = [
                key for key, video in self._storage.items()
                if current_time - video.timestamp > max_age_seconds
            ]
            for key in old_keys:
                del self._storage[key]
            return len(old_keys)

class WriteQueue:
    """Очередь для записи в Google Sheets с ограничением скорости"""
    def __init__(self, max_concurrent: int = 1, delay_seconds: float = 2.0):
        self._queue = asyncio.Queue()
        self._max_concurrent = max_concurrent
        self._delay = delay_seconds
        self._current_tasks = 0
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._worker_task = None
    
    async def start(self):
        """Запуск обработчика очереди"""
        self._worker_task = asyncio.create_task(self._process_queue())
    
    async def stop(self):
        """Остановка обработчика очереди"""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
    
    async def add_write_task(self, task_func, *args, **kwargs):
        """Добавление задачи в очередь на запись"""
        future = asyncio.get_event_loop().create_future()
        await self._queue.put((future, task_func, args, kwargs))
        return await future
    
    async def _process_queue(self):
        """Обработчик очереди записей"""
        while True:
            try:
                future, task_func, args, kwargs = await self._queue.get()
                
                async with self._semaphore:
                    try:
                        result = await task_func(*args, **kwargs)
                        future.set_result(result)
                    except Exception as e:
                        future.set_exception(e)
                    
                    # Задержка между записями
                    await asyncio.sleep(self._delay)
                    
                self._queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в обработчике очереди: {e}")

# Глобальные экземпляры безопасных структур
pending_videos = SafeVideoStorage()
write_queue = WriteQueue(max_concurrent=1, delay_seconds=2.0)

# Словари для хранения статистики (оставляем простыми, т.к. данные не критичны)
user_submissions = defaultdict(int)

# Множество для отслеживания пользователей, которые уже использовали /start
# Ключ: (chat_id, user_id)
started_users = set()

YOUTUBE_REGEX = r'(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/(watch\?v=|embed/|v/|.+\?v=)?([^&=%\?]{11})'

COLUMN_A_WIDTH = 120
COLUMN_B_WIDTH = 300
COLUMN_C_WIDTH = 300
COLUMN_D_WIDTH = 200
COLUMN_E_WIDTH = 200
ROW_HEIGHT = 100

# Обновленные заголовки с новым столбцом
EXPECTED_HEADERS = ['Превью', 'Название видео', 'Ссылка', 'Кем предложено', 'Дата добавления']

# Московское время (UTC+3)
MOSCOW_UTC_OFFSET = 3

# ------------------ Инициализация Google Sheets ------------------
_sheet_instance = None
_sheet_lock = asyncio.Lock()

async def get_google_sheet():
    """Потокобезопасное получение экземпляра Google Sheets"""
    global _sheet_instance
    async with _sheet_lock:
        if _sheet_instance is None:
            _sheet_instance = await asyncio.to_thread(init_google_sheets_sync)
        return _sheet_instance

def init_google_sheets_sync():
    """Синхронная инициализация Google Sheets (выполняется в отдельном потоке)"""
    try:
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        try:
            sheet = spreadsheet.worksheet(SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=5)
        
        logger.info("Подключение к Google Sheets установлено.")
        return sheet
    except Exception as e:
        logger.error(f"Ошибка при инициализации Google Sheets: {e}")
        return None

async def ensure_headers_and_formatting(worksheet):
    """Асинхронная проверка заголовков и форматирование таблицы"""
    try:
        loop = asyncio.get_event_loop()
        
        # Проверяем и обновляем заголовки если нужно
        current_headers = await loop.run_in_executor(None, worksheet.row_values, 1)
        
        needs_update = False
        
        if len(current_headers) < len(EXPECTED_HEADERS):
            needs_update = True
        elif current_headers != EXPECTED_HEADERS:
            needs_update = True
        
        if needs_update:
            if current_headers:
                await loop.run_in_executor(
                    None,
                    lambda: worksheet.update(values=[['' for _ in range(5)]], range_name='A1:E1')
                )
            
            await loop.run_in_executor(
                None,
                lambda: worksheet.update(values=[EXPECTED_HEADERS], range_name='A1:E1')
            )
            
            await loop.run_in_executor(
                None,
                lambda: worksheet.format('A1:E1', {'textFormat': {'bold': True}})
            )
            
            logger.info("Заголовки таблицы обновлены")
        else:
            await loop.run_in_executor(
                None,
                lambda: worksheet.format('A1:E1', {'textFormat': {'bold': True}})
            )
            logger.info("Заголовки таблицы уже правильные")
        
        # Применяем форматирование
        await loop.run_in_executor(
            None,
            lambda: (
                set_column_width(worksheet, 'A', COLUMN_A_WIDTH),
                set_column_width(worksheet, 'B', COLUMN_B_WIDTH),
                set_column_width(worksheet, 'C', COLUMN_C_WIDTH),
                set_column_width(worksheet, 'D', COLUMN_D_WIDTH),
                set_column_width(worksheet, 'E', COLUMN_E_WIDTH),
                set_row_height(worksheet, '2:', ROW_HEIGHT)
            )
        )
        
        fmt = cellFormat(
            verticalAlignment='MIDDLE',
            wrapStrategy='WRAP'
        )
        
        await loop.run_in_executor(
            None,
            lambda: format_cell_range(worksheet, 'A2:E1000', fmt)
        )
        
        logger.info("Форматирование таблицы применено.")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке заголовков и форматировании: {e}")

# ------------------ Вспомогательные функции ------------------
def get_moscow_datetime():
    """Возвращает текущую дату и время по московскому времени"""
    utc_now = datetime.now(timezone.utc)
    moscow_offset = timedelta(hours=MOSCOW_UTC_OFFSET)
    return utc_now + moscow_offset

def format_moscow_date(moscow_dt):
    """Форматирует дату в человеческом виде для Москвы"""
    month_names = {
        1: 'янв', 2: 'фев', 3: 'мар', 4: 'апр', 5: 'май', 6: 'июн',
        7: 'июл', 8: 'авг', 9: 'сен', 10: 'окт', 11: 'ноя', 12: 'дек'
    }
    
    day = moscow_dt.day
    month = month_names[moscow_dt.month]
    year = moscow_dt.year
    hour = moscow_dt.hour
    minute = moscow_dt.minute
    
    return f"{day} {month} {year}, {hour:02d}:{minute:02d} (МСК)"

async def fetch_video_info(video_id):
    """Асинхронное получение информации о видео"""
    if not YOUTUBE_API_KEY:
        return None, None
    
    try:
        url = f'https://www.googleapis.com/youtube/v3/videos'
        params = {
            'id': video_id,
            'key': YOUTUBE_API_KEY,
            'part': 'snippet'
        }
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, 
            lambda: requests.get(url, params=params, timeout=10)
        )
        data = response.json()
        
        if 'items' in data and len(data['items']) > 0:
            snippet = data['items'][0]['snippet']
            video_title = snippet['title']
            
            thumbnails = snippet.get('thumbnails', {})
            thumbnail_url = thumbnails.get('high', {}).get('url', 
                         thumbnails.get('medium', {}).get('url',
                         thumbnails.get('standard', {}).get('url',
                         f'https://img.youtube.com/vi/{video_id}/hqdefault.jpg')))
            
            return video_title, thumbnail_url
            
    except Exception as e:
        logger.error(f"Ошибка при получении информации о видео: {e}")
    
    return None, None

async def get_video_count_from_sheet():
    """Асинхронное получение количества видео (без форматирования таблицы)"""
    try:
        sheet = await get_google_sheet()
        if sheet is None:
            return 0
        
        loop = asyncio.get_event_loop()
        all_values = await loop.run_in_executor(None, sheet.get_all_values)
        return len(all_values) - 1 if len(all_values) > 1 else 0
    except Exception as e:
        logger.error(f"Ошибка при получении количества видео из таблицы: {e}")
        return 0

async def is_video_already_in_sheet(video_url):
    """Асинхронная проверка наличия видео в таблице"""
    try:
        sheet = await get_google_sheet()
        if sheet is None:
            return False
        
        video_id = extract_youtube_id(video_url)
        if not video_id:
            return False
        
        short_link = f"https://youtu.be/{video_id}"
        
        loop = asyncio.get_event_loop()
        all_values = await loop.run_in_executor(None, sheet.get_all_values)
        
        for row in all_values[1:]:
            if len(row) >= 3:
                cell_link = row[2]
                if short_link in cell_link or video_url in cell_link:
                    return True
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке наличия видео в таблице: {e}")
        return False

async def write_to_google_sheets_async(video_url: str, user_name: str, is_anonymous: bool):
    """Асинхронная запись в Google Sheets (используется в очереди)"""
    try:
        sheet = await get_google_sheet()
        if sheet is None:
            raise Exception("Отсутствует подключение к Google Sheets")
        
        # Проверяем и применяем форматирование таблицы перед записью
        await ensure_headers_and_formatting(sheet)
        
        video_id = extract_youtube_id(video_url)
        if not video_id:
            logger.error(f"Не удалось извлечь ID видео из ссылки: {video_url}")
            return False
        
        # Получаем информацию о видео асинхронно
        video_title, thumbnail_url = await fetch_video_info(video_id)
        short_link = f"https://youtu.be/{video_id}"
        
        if not video_title:
            video_title = f"Видео от {user_name if not is_anonymous else 'Анонима'}"
        
        if thumbnail_url:
            preview_formula = f'=IMAGE("{thumbnail_url}"; 2)'
        else:
            preview_formula = f'=IMAGE("https://img.youtube.com/vi/{video_id}/mqdefault.jpg"; 2)'
        
        # Определяем имя для записи
        author_name = "Аноним" if is_anonymous else user_name
        
        # Получаем текущую дату и время по Москве
        moscow_now = get_moscow_datetime()
        formatted_date = format_moscow_date(moscow_now)
        
        # Данные для записи
        row_data = [
            preview_formula,
            video_title,
            short_link,
            author_name,
            formatted_date
        ]
        
        # Выполняем синхронные операции Google Sheets в отдельном потоке
        loop = asyncio.get_event_loop()
        
        # Добавляем строку
        await loop.run_in_executor(
            None, 
            lambda: sheet.append_row(row_data, value_input_option='USER_ENTERED')
        )
        
        # Получаем номер последней строки
        all_values = await loop.run_in_executor(None, sheet.get_all_values)
        last_row = len(all_values)
        
        # Применяем форматирование для новой строки
        fmt = cellFormat(
            verticalAlignment='MIDDLE',
            wrapStrategy='WRAP'
        )
        
        await loop.run_in_executor(
            None,
            lambda: format_cell_range(sheet, f'A{last_row}:E{last_row}', fmt)
        )
        
        await loop.run_in_executor(
            None,
            lambda: set_row_height(sheet, f'{last_row}:{last_row}', ROW_HEIGHT)
        )
        
        logger.info(f"Данные записаны в строку {last_row}: {video_title} от {author_name} в {formatted_date}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при записи в Google Sheets: {e}")
        return False

def is_youtube_link(url: str) -> bool:
    match = re.match(YOUTUBE_REGEX, url)
    if match:
        # Проверяем, не является ли ссылка на YouTube Shorts
        url_lower = url.lower()
        # Исключаем ссылки на shorts
        if '/shorts/' in url_lower:
            return False
        return True
    return False

def extract_youtube_id(url: str) -> str:
    match = re.match(YOUTUBE_REGEX, url)
    if match:
        return match.group(6)
    return None

def create_video_key(video_url: str, user_id: int) -> str:
    """Создает уникальный ключ для видео"""
    video_id = extract_youtube_id(video_url)
    if video_id:
        return f"{user_id}_{video_id}"
    return f"{user_id}_{hashlib.md5(video_url.encode()).hexdigest()[:16]}"

# ------------------ Обработчики команд ------------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    chat_id = update.effective_chat.id
    
    # Создаем уникальный ключ для комбинации чат+пользователь
    user_chat_key = f"{chat_id}_{user_id}"
    
    # Проверяем, использовал ли этот пользователь уже /start в этом чате
    if user_chat_key in started_users:
        # Короткая версия для повторного использования
        short_text = "Просто отправь мне ссылку на YouTube видео, и я добавлю её в предложку.\n\nЕсли нужна помощь, используй /help"
        await update.message.reply_text(short_text)
        return
    
    # Первое использование /start этим пользователем в этом чате
    started_users.add(user_chat_key)
    
    welcome_text_part1 = (
        f"Привет, {user.first_name}! 👋\n\n"
        f"Я помогаю собирать предложки полнометражных YouTube видео, чтобы [Бупа]({BUPA_CHANNEL_LINK}) посмотрела их на стриме. \n\n"
        "📋 *Доступные команды:*\n"
        "• /start - Начало работы с ботом\n"
        "• /list - Показать все предложенные видео\n"
        "• /info - Подробная информация о боте\n"
        "• /help - Показать список команд"
    )
    
    welcome_text_part2 = (
        "🚀 Для того, чтобы начать, просто пришли мне ссылку на YouTube и я добавлю ее в предложку"
    )
    
    await update.message.reply_text(welcome_text_part1, parse_mode='Markdown')
    await update.message.reply_text(welcome_text_part2)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "*📋 Доступные команды:*\n\n"
        "/start - Начало работы с ботом\n"
        "/list - Показать все предложенные видео\n"
        "/info - Подробная информация о боте\n"
        "/help - Показать этот список команд\n\n"
        "*📹 Как пользоваться ботом:*\n"
        "Просто отправь мне ссылку на YouTube видео"
    )
    
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    info_text = (
        f"*Бот для предложки YouTube видео*\n\n"
        f"*Назначение:*\n"
        f"Этот бот создан для сбора предложок с YouTube. "
        f"Он помогает [Бупе]({BUPA_CHANNEL_LINK}) собирать предлагаемые видео для их просмотра и обсуждения.\n\n"
        f"*Как работает:*\n"
        f"1. Вы отправляете ссылку на полнометражное YouTube видео\n"
        f"2. Бот проверяет ссылку и предлагает выбрать: добавить анонимно или с вашим именем\n"
        f"3. После подтверждения видео добавляется в общую таблицу\n"
        f"p.s. Посмотреть полный список видео можно по команде /list\n\n"
        f"*Поддерживаемые форматы YouTube ссылок:*\n"
        f"• https://youtube.com/watch?v=ID\n"
        f"• https://youtu.be/ID\n"
        f"• https://www.youtube.com/v/ID\n"
        f"И другие форматы YouTube\n\n"
        f"*📋 Доступные команды:*\n"
        f"• /start - Начало работы с ботом\n"
        f"• /list - Показать все предложенные видео\n"
        f"• /info - Подробная информация о боте\n"
        f"• /help - Показать список команд\n\n"
    )
    
    await update.message.reply_text(info_text, parse_mode='Markdown')

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Получаем количество видео из таблицы (без форматирования таблицы)
        video_count = await get_video_count_from_sheet()
        
        if video_count == 0:
            await update.message.reply_text(
                "📭 В таблице пока нет видео.\n\n"
                "Будьте первым, кто предложит видео! 🎬\n"
                "Просто отправьте ссылку на YouTube."
            )
            return
        
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        
        list_text = (
            f"🎯 Всего видео в таблице: {video_count}\n\n"
            f"📋 Полный список доступен по ссылке:\n"
            f"{spreadsheet_url}"
        )
        
        await update.message.reply_text(list_text)
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении команды /list: {e}")
        await update.message.reply_text(
            f"❌ Произошла ошибка при получении информации из таблицы. Попробуйте позже."
        )

async def ask_anonymous_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, video_url: str, user_name: str):
    """Спрашивает пользователя, хочет ли он добавить видео анонимно"""
    user = update.effective_user
    user_id = user.id
    chat_id = update.effective_chat.id
    
    # Создаем уникальный ключ для этого видео
    video_key = create_video_key(video_url, user_id)
    
    # Сохраняем информацию о видео в безопасное хранилище
    video_data = PendingVideo(
        video_url=video_url,
        user_name=user_name,
        user_id=user_id,
        chat_id=chat_id,
        timestamp=datetime.now().timestamp()
    )
    
    await pending_videos.add(video_key, video_data)
    
    # Создаем кнопки
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, с моим именем", callback_data=f"name_{video_key}"),
            InlineKeyboardButton("🚫 Нет, анонимно", callback_data=f"anon_{video_key}")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    video_id = extract_youtube_id(video_url)
    short_link = f"https://youtu.be/{video_id}" if video_id else video_url
    
    message = await update.message.reply_text(
        f"Хотите, чтобы в таблице было указано ваше имя?\n"
        f"👤 Ваше имя: {user_name}\n\n"
        f"По умолчанию: 🚫 Анонимно",
        reply_markup=reply_markup
    )
    
    # Сохраняем ID сообщения
    video_data.message_id = message.message_id
    await pending_videos.add(video_key, video_data)

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия на кнопки"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    user_id = user.id
    
    # Извлекаем данные из callback
    callback_data = query.data
    
    if callback_data.startswith("name_"):
        video_key = callback_data.replace("name_", "")
        is_anonymous = False
    elif callback_data.startswith("anon_"):
        video_key = callback_data.replace("anon_", "")
        is_anonymous = True
    else:
        await query.edit_message_text("❌ Ошибка обработки запроса. Попробуйте еще раз.")
        return
    
    # Получаем информацию о видео из безопасного хранилища
    video_data = await pending_videos.get(video_key)
    if not video_data:
        await query.edit_message_text("❌ Ссылка устарела. Пожалуйста, отправьте видео еще раз.")
        return
    
    video_url = video_data.video_url
    user_name = video_data.user_name
    
    # Проверяем, что пользователь совпадает
    if user_id != video_data.user_id:
        await query.edit_message_text("❌ Это не ваше видео!")
        return
    
    # Удаляем видео из временного хранилища
    await pending_videos.remove(video_key)
    
    # Показываем сообщение "ожидайте"
    await query.edit_message_text(
        "⏳ Видео добавляется в таблицу, пожалуйста, ожидайте..."
    )
    
    # Проверяем, не добавили ли видео уже в таблицу
    if await is_video_already_in_sheet(video_url):
        await query.edit_message_text(
            "⚠️ Это видео уже есть в предложке!\n\n"
            "Проверьте полный список через /list"
        )
        return
    
    try:
        # Добавляем задачу в очередь на запись
        success = await write_queue.add_write_task(
            write_to_google_sheets_async,
            video_url,
            user_name,
            is_anonymous
        )
        
        # Получаем информацию о видео для отображения
        video_id = extract_youtube_id(video_url)
        short_link = f"https://youtu.be/{video_id}" if video_id else video_url
        
        if success:
            # Получаем текущую дату для отображения в сообщении
            moscow_now = get_moscow_datetime()
            formatted_date = format_moscow_date(moscow_now)
            
            author_text = f"👤 От: {user_name}" if not is_anonymous else "👤 От: Анонимно"
            
            success_message = (
                f"✅ Видео успешно добавлено!\n\n"
                f"📹 Ссылка: {short_link}\n"
                f"{author_text}\n"
                f"🕐 Дата: {formatted_date}\n\n"
                f"🎬 Спасибо за предложку!"
            )
            
            # Обновляем сообщение с результатом
            await query.edit_message_text(
                success_message
            )
            
            # Обновляем статистику
            user_submissions[user_id] += 1
            
            logger.info(f"Пользователь {user.id} добавил видео: {short_link} в {formatted_date}")
        else:
            error_message = (
                f"❌ Не удалось добавить видео\n\n"
                f"📹 Ссылка: {short_link}\n\n"
                f"⚠️ Попробуйте еще раз или свяжитесь с администратором."
            )
            
            await query.edit_message_text(
                error_message
            )
            
            logger.error(f"Ошибка при добавлении видео пользователем {user.id}: {video_url}")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке callback: {e}")
        await query.edit_message_text(
            "❌ Произошла ошибка при добавлении видео. Попробуйте еще раз."
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = update.message.text
    user = update.effective_user
    chat_id = update.effective_chat.id
    
    if message_text.startswith('/'):
        await update.message.reply_text(
            "Неизвестная команда. Используйте /help для списка команд."
        )
        return
    
    if 'http' in message_text.lower() or 'youtu' in message_text.lower():
        if is_youtube_link(message_text):
            # Проверяем, есть ли видео уже в таблице
            if await is_video_already_in_sheet(message_text):
                await update.message.reply_text(
                    "⚠️ Это видео уже есть в предложке!\n"
                    "Проверьте полный список через /list"
                )
                return
            
            # Сохраняем информацию о видео
            video_id = extract_youtube_id(message_text)
            user_name = user.first_name
            if user.username:
                user_name = f"{user_name} (@{user.username})"
            
            # Сразу спрашиваем пользователя о выборе анонимности
            await ask_anonymous_choice(update, context, message_text, user_name)
            
        else:
            await update.message.reply_text(
                "❌ Это ссылка не на YouTube видео или формата shorts!\n\n"
                "Я принимаю только ссылки на полнометражные видео YouTube.\n\n"
                "Примеры правильных ссылок:\n"
                "• https://youtube.com/watch?v=dQw4w9WgXcQ\n"
                "• https://youtu.be/dQw4w9WgXcQ\n"
                "• https://www.youtube.com/v/dQw4w9WgXcQ\n\n"
                "Попробуйте отправить другую ссылку"
            )
    else:
        # Проверяем, использовал ли пользователь уже /start в этом чате
        user_chat_key = f"{chat_id}_{user.id}"
        if user_chat_key in started_users:
            response = f"Привет, {user.first_name}! Я жду от тебя ссылку на YouTube видео. Просто отправь её мне, и я добавлю в предложку.\n\nЕсли нужна помощь, используй /help"
        else:
            response = f"Привет, {user.first_name}! 👋\n\nЯ бот для предложки YouTube видео.\nИспользуй /start для начала работы или отправь мне ссылку на YouTube видео."
        
        await update.message.reply_text(response)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Ошибка при обработке сообщения: {context.error}", exc_info=True)
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "😕 Произошла ошибка при обработке запроса.\n"
            "Попробуйте еще раз или свяжитесь с разработчиком."
        )

async def periodic_cleanup():
    """Периодическая очистка устаревших данных"""
    while True:
        try:
            # Очищаем старые записи каждые 5 минут
            cleaned = await pending_videos.cleanup_old()
            if cleaned > 0:
                logger.info(f"Очищено {cleaned} устаревших записей")
        except Exception as e:
            logger.error(f"Ошибка при очистке устаревших данных: {e}")
        
        await asyncio.sleep(300)  # 5 минут

async def startup(app: Application):
    """Действия при запуске бота"""
    logger.info("Бот запускается...")
    await write_queue.start()
    
    # Запускаем периодическую очистку
    asyncio.create_task(periodic_cleanup())
    logger.info("Фоновые задачи запущены")

async def shutdown(app: Application):
    """Действия при остановке бота"""
    logger.info("Бот останавливается...")
    await write_queue.stop()
    logger.info("Очередь записи остановлена")

def check_config():
    if not TOKEN:
        logger.error("Токен Telegram бота не найден.")
        return False
    if not SERVICE_ACCOUNT_FILE:
        logger.error("Путь к JSON-ключу не найден.")
        return False
    if not SPREADSHEET_ID:
        logger.error("ID таблицы не найден.")
        return False
    if "ваш_токен" in TOKEN or "example" in TOKEN.lower():
        logger.error("В файле .env указан неверный токен.")
        return False
    logger.info("Конфигурация загружена.")
    return True

def main():
    if not check_config():
        return
    
    try:
        # Создаем приложение с использованием современных методов
        app = Application.builder().token(TOKEN).build()
        
        # Регистрируем обработчики
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("list", list_command))
        app.add_handler(CommandHandler("info", info_command))
        app.add_handler(CommandHandler("help", help_command))
        
        # Добавляем обработчик callback запросов
        app.add_handler(CallbackQueryHandler(handle_callback_query))
        
        # Обработчик текстовых сообщений
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        # Обработчик ошибок
        app.add_error_handler(error_handler)
        
        # Регистрируем обработчики запуска и остановки
        app.post_init = startup
        app.post_stop = shutdown
        
        logger.info("Бот запускается...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}", exc_info=True)

if __name__ == '__main__':
    main()