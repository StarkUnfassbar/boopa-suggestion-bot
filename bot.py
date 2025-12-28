import asyncio
import logging
import os
import re
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List, Any
from collections import defaultdict
from dataclasses import dataclass
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import requests

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SHEETS_KEY_PATH')
SPREADSHEET_ID = os.getenv('GOOGLE_SPREADSHEET_ID')
SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME', 'Sheet1')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

BUPA_CHANNEL_LINK = "t.me/boopablup"

# ------------------ Настройка логгирования в файл ------------------

def setup_logging():
    """Настройка логгирования в файл"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    current_time = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"bot_{current_time}.log")
    
    class CustomFormatter(logging.Formatter):
        def format(self, record):
            record.asctime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return super().format(record)
    
    class HttpxNoiseFilter(logging.Filter):
        def __init__(self):
            super().__init__()
            self.noise_patterns = [
                "HTTP Request:.*/getUpdates.*HTTP/1.1 200 OK",
            ]
        
        def filter(self, record):
            if record.name != "httpx" or record.levelno != logging.INFO:
                return True
            
            message = record.getMessage()
            
            for pattern in self.noise_patterns:
                import re
                if re.search(pattern, message, re.IGNORECASE):
                    return False
            
            return True
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    logger.handlers.clear()
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    file_handler.addFilter(HttpxNoiseFilter())
    
    file_formatter = CustomFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

# ------------------ Глобальные переменные и структуры данных ------------------

AUTO_CLEANUP_TIME = "03:00"  # Формат "ЧЧ:ММ" по московскому времени

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
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._worker_task = None
        self._is_running = False
    
    async def start(self):
        """Запуск обработчика очереди"""
        if self._is_running:
            return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._process_queue())
    
    async def stop(self):
        """Остановка обработчика очереди"""
        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
    
    async def add_write_task(self, task_func, *args, **kwargs):
        """Добавление задачи в очередь на запись"""
        if not self._is_running:
            raise Exception("Очередь записи не запущена")
        
        future = asyncio.get_event_loop().create_future()
        await self._queue.put((future, task_func, args, kwargs))
        return await future
    
    async def _process_queue(self):
        """Обработчик очереди записей"""
        while self._is_running:
            try:
                future, task_func, args, kwargs = await self._queue.get()
                
                async with self._semaphore:
                    try:
                        result = await task_func(*args, **kwargs)
                        future.set_result(result)
                    except Exception as e:
                        future.set_exception(e)
                    
                    await asyncio.sleep(self._delay)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в обработчике очереди: {e}")
                if not future.done():
                    future.set_exception(e)
            finally:
                if not self._queue.empty():
                    self._queue.task_done()

pending_videos = SafeVideoStorage()
write_queue = WriteQueue(max_concurrent=1, delay_seconds=2.0)

user_submissions = defaultdict(int)

started_users = set()

YOUTUBE_REGEX = r'(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/(watch\?v=|embed/|v/|.+\?v=)?([^&=%\?]{11})'

COLUMN_A_WIDTH = 150
COLUMN_B_WIDTH = 300
COLUMN_C_WIDTH = 110
COLUMN_D_WIDTH = 220
COLUMN_E_WIDTH = 170
COLUMN_F_WIDTH = 200
COLUMN_G_WIDTH = 130
ROW_HEIGHT = 115
FIRST_ROW_HEIGHT = 40
SECOND_ROW_HEIGHT = 40

EXPECTED_HEADERS = ['Превью', 'Название видео', 'Длительность', 'Ссылка', 'Кем предложено', 'Дата добавления', 'Статус']

MOSCOW_UTC_OFFSET = 3

# ------------------ Глобальный сервис Google Sheets ------------------

_sheets_service = None
_sheets_lock = asyncio.Lock()
_sheet_id = None
_is_initialized = False

async def get_sheets_service():
    """Потокобезопасное получение экземпляра Google Sheets API"""
    global _sheets_service
    async with _sheets_lock:
        if _sheets_service is None:
            _sheets_service = await asyncio.to_thread(init_sheets_service_sync)
        return _sheets_service

def init_sheets_service_sync():
    """Синхронная инициализация Google Sheets API (выполняется в отдельном потоке)"""
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, 
            scopes=SCOPES
        )
        
        service = build('sheets', 'v4', credentials=creds)
        
        logger.info("Подключение к Google Sheets API установлено.")
        return service
    except Exception as e:
        logger.error(f"Ошибка при инициализации Google Sheets API: {e}")
        return None

async def get_sheet_id_cached(service):
    """Получение ID листа с кэшированием"""
    global _sheet_id
    if _sheet_id is not None:
        return _sheet_id
    
    try:
        spreadsheet = service.spreadsheets()
        result = spreadsheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        
        for sheet in result.get('sheets', []):
            if sheet['properties']['title'] == SHEET_NAME:
                _sheet_id = sheet['properties']['sheetId']
                return _sheet_id
        
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении ID листа: {e}")
        return None

async def ensure_sheet_exists(service):
    """Проверка существования листа и его создание при необходимости"""
    try:
        spreadsheet = service.spreadsheets()
        result = spreadsheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        
        for sheet in result.get('sheets', []):
            if sheet['properties']['title'] == SHEET_NAME:
                return True
        
        requests = [{
            'addSheet': {
                'properties': {
                    'title': SHEET_NAME,
                    'gridProperties': {
                        'rowCount': 1000,
                        'columnCount': len(EXPECTED_HEADERS)
                    }
                }
            }
        }]
        
        body = {'requests': requests}
        spreadsheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при проверке/создании листа: {e}")
        return False

async def initialize_google_sheets():
    """Инициализация Google Sheets при запуске бота"""
    global _is_initialized
    
    if _is_initialized:
        return True
    
    try:
        service = await get_sheets_service()
        if service is None:
            logger.error("Не удалось получить сервис Google Sheets")
            return False
        
        if not await ensure_sheet_exists(service):
            logger.error(f"Не удалось создать лист {SHEET_NAME}")
            return False
        
        await get_sheet_id_cached(service)
        
        await ensure_headers_and_formatting()
        
        _is_initialized = True
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации Google Sheets: {e}")
        return False

async def get_total_rows():
    """Получение общего количества строк с данными в таблице"""
    try:
        service = await get_sheets_service()
        if service is None:
            return 0
        
        spreadsheet = service.spreadsheets()
        
        result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:{chr(65 + len(EXPECTED_HEADERS) - 1)}"
        ).execute()
        
        values = result.get('values', [])
        return len(values)
        
    except Exception as e:
        logger.error(f"Ошибка при получении количества строк: {e}")
        return 0

async def apply_formatting_after_add(total_rows: int):
    """Применяет форматирование после добавления новой строки"""
    try:
        service = await get_sheets_service()
        if service is None:
            return False
        
        spreadsheet = service.spreadsheets()
        
        sheet_id = await get_sheet_id_cached(service)
        if sheet_id is None:
            return False
        
        BORDER_STYLE = {
            'style': 'SOLID',
            'width': 1,
            'color': {'red': 0.0, 'green': 0.0, 'blue': 0.0}
        }
        
        WHITE_BACKGROUND = {
            'red': 1.0,
            'green': 1.0,
            'blue': 1.0
        }
        
        num_columns = len(EXPECTED_HEADERS)
        
        formatting_requests = []
        
        formatting_requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'endRowIndex': total_rows,
                    'startColumnIndex': 0,
                    'endColumnIndex': num_columns
                },
                'cell': {
                    'userEnteredFormat': {
                        'verticalAlignment': 'MIDDLE',
                        'wrapStrategy': 'WRAP',
                        'backgroundColor': WHITE_BACKGROUND,
                        'borders': {
                            'top': BORDER_STYLE,
                            'bottom': BORDER_STYLE,
                            'left': BORDER_STYLE,
                            'right': BORDER_STYLE
                        }
                    }
                },
                'fields': 'userEnteredFormat(verticalAlignment,wrapStrategy,backgroundColor,borders)'
            }
        })
        
        text_columns = [0, 1, 2, 4, 5, 6]
        
        for col_idx in text_columns:
            if col_idx < num_columns:
                formatting_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': total_rows,
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': False,
                                    'fontSize': 10
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat)'
                    }
                })
        
        row_requests = []
        if total_rows > 2:
            row_requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': 2,
                        'endIndex': total_rows
                    },
                    'properties': {
                        'pixelSize': ROW_HEIGHT
                    },
                    'fields': 'pixelSize'
                }
            })
        
        if formatting_requests or row_requests:
            batch_update_request = {
                'requests': formatting_requests + row_requests
            }
            
            spreadsheet.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=batch_update_request
            ).execute()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при применении форматирования: {e}")
        return False

async def create_dropdown_for_new_row(row_index: int):
    """Создает раскрывающийся список для новой строки в стиле Чип"""
    try:
        service = await get_sheets_service()
        if service is None:
            return False
        
        spreadsheet = service.spreadsheets()
        
        sheet_id = await get_sheet_id_cached(service)
        if sheet_id is None:
            return False
        
        dropdown_request = {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': row_index - 1,
                    'endRowIndex': row_index,
                    'startColumnIndex': len(EXPECTED_HEADERS) - 1,
                    'endColumnIndex': len(EXPECTED_HEADERS)
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [
                            {'userEnteredValue': 'не просмотрено'},
                            {'userEnteredValue': 'просмотрено'},
                            {'userEnteredValue': 'удалить'}
                        ]
                    },
                    'strict': True,
                    'showCustomUi': True,
                    'inputMessage': 'Выберите статус просмотра'
                }
            }
        }
        
        batch_update_request = {
            'requests': [dropdown_request]
        }
        
        spreadsheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=batch_update_request
        ).execute()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при создании выпадающего списка: {e}")
        return False

async def ensure_headers_and_formatting():
    """Асинхронное форматирование таблицы через Google Sheets API v4"""
    try:
        service = await get_sheets_service()
        if service is None:
            return False
        
        spreadsheet = service.spreadsheets()
        
        result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1:{chr(65 + len(EXPECTED_HEADERS) - 1)}2"
        ).execute()
        
        values = result.get('values', [])
        
        needs_restructure = False
        
        if len(values) < 2:
            needs_restructure = True
        else:
            first_row = values[0] if len(values) > 0 else []
            second_row = values[1] if len(values) > 1 else []
            
            if second_row != EXPECTED_HEADERS:
                needs_restructure = True
        
        if needs_restructure:
            all_result = spreadsheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A:{chr(65 + len(EXPECTED_HEADERS) - 1)}"
            ).execute()
            
            all_values = all_result.get('values', [])
            
            new_values = []
            
            first_row_text = f"Предложить видео сюда → @BoopaSuggestionBot"
            new_values.append([first_row_text])
            
            new_values.append(EXPECTED_HEADERS)
            
            if len(all_values) > 0 and all_values[0] == EXPECTED_HEADERS:
                for row in all_values[1:]:
                    new_values.append(row)
            else:
                for row in all_values:
                    if row:
                        new_values.append(row)
            
            spreadsheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A:{chr(65 + len(EXPECTED_HEADERS) - 1)}"
            ).execute()
            
            body = {
                'values': new_values
            }
            spreadsheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
        
        sheet_id = await get_sheet_id_cached(service)
        if sheet_id is None:
            return False
        
        total_rows = await get_total_rows()
        if total_rows < 2:
            total_rows = 2
        
        num_columns = len(EXPECTED_HEADERS)
        
        merge_request = [{
            'mergeCells': {
                'mergeType': 'MERGE_ALL',
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': num_columns
                }
            }
        }]
        
        BACKGROUND_COLOR = {
            'red': 1.0,
            'green': 0.804,
            'blue': 0.929
        }
        
        BORDER_STYLE = {
            'style': 'SOLID',
            'width': 1,
            'color': {'red': 0.0, 'green': 0.0, 'blue': 0.0}
        }
        
        formatting_requests = [
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': num_columns
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': BACKGROUND_COLOR,
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE',
                            'borders': {
                                'top': BORDER_STYLE,
                                'bottom': BORDER_STYLE,
                                'left': BORDER_STYLE,
                                'right': BORDER_STYLE
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,borders)'
                }
            },
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 1,
                        'endRowIndex': 2,
                        'startColumnIndex': 0,
                        'endColumnIndex': num_columns
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': BACKGROUND_COLOR,
                            'textFormat': {
                                'bold': True,
                                'underline': False
                            },
                            'borders': {
                                'top': BORDER_STYLE,
                                'bottom': BORDER_STYLE,
                                'left': BORDER_STYLE,
                                'right': BORDER_STYLE
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat.bold,textFormat.underline,borders)'
                }
            }
        ]
        
        column_widths = [COLUMN_A_WIDTH, COLUMN_B_WIDTH, COLUMN_C_WIDTH, COLUMN_D_WIDTH, COLUMN_E_WIDTH, COLUMN_F_WIDTH, COLUMN_G_WIDTH]
        column_requests = []
        
        for i in range(min(num_columns, len(column_widths))):
            column_requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': i,
                        'endIndex': i + 1
                    },
                    'properties': {
                        'pixelSize': column_widths[i] if i < len(column_widths) else 100
                    },
                    'fields': 'pixelSize'
                }
            })
        
        for i in range(7, num_columns):
            column_requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': i,
                        'endIndex': i + 1
                    },
                    'properties': {
                        'pixelSize': 100
                    },
                    'fields': 'pixelSize'
                }
            })
        
        row_requests = [
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': 0,
                        'endIndex': 1
                    },
                    'properties': {
                        'pixelSize': FIRST_ROW_HEIGHT
                    },
                    'fields': 'pixelSize'
                }
            },
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': 1,
                        'endIndex': 2
                    },
                    'properties': {
                        'pixelSize': SECOND_ROW_HEIGHT
                    },
                    'fields': 'pixelSize'
                }
            }
        ]
        
        batch_update_request = {
            'requests': merge_request + formatting_requests + column_requests + row_requests
        }
        
        spreadsheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=batch_update_request
        ).execute()
        
        result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1"
        ).execute()
        
        if not result.get('values') or len(result.get('values', [])) == 0:
            first_row_text = f"Предложить видео сюда → @BoopaSuggestionBot"
            
            body = {
                'values': [[first_row_text]]
            }
            
            spreadsheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
        
        format_update_request = {
            'requests': [{
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE',
                            'textFormat': {
                                'bold': True,
                                'fontSize': 10
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)'
                }
            }]
        }
        
        spreadsheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=format_update_request
        ).execute()
        
        if total_rows > 2:
            for row_index in range(3, total_rows + 1):
                await create_dropdown_for_new_row(row_index)
        
        await apply_formatting_after_add(total_rows)
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при проверке заголовков и форматировании: {e}")
        return False

# ------------------ Функция удаления ------------------

async def cleanup_videos():
    """Удаляет видео со статусами 'просмотрено' и 'удалить'"""
    try:
        service = await get_sheets_service()
        if service is None:
            logger.error("Не удалось получить сервис Google Sheets для очистки")
            return 0
        
        spreadsheet = service.spreadsheets()
        
        result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A3:{chr(65 + len(EXPECTED_HEADERS) - 1)}",
            majorDimension="ROWS"
        ).execute()
        
        values = result.get('values', [])
        if not values:
            logger.info("Нет данных для очистки")
            return 0
        
        status_column_index = len(EXPECTED_HEADERS) - 1
        
        videos_to_keep = []
        deleted_count = 0
        
        for row in values:
            if len(row) > status_column_index:
                status = row[status_column_index].strip().lower()
                if status in ['просмотрено', 'удалить']:
                    deleted_count += 1
                else:
                    videos_to_keep.append(row)
            else:
                videos_to_keep.append(row)
        
        if deleted_count == 0:
            logger.info("Не найдено видео для удаления")
            return 0
        
        logger.info(f"Начало удаления: всего видео {len(values)}, удалится {deleted_count}")
        
        total_rows_result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:{chr(65 + len(EXPECTED_HEADERS) - 1)}"
        ).execute()
        
        total_rows = len(total_rows_result.get('values', []))
        
        if total_rows <= 2:
            logger.info("В таблице только заголовки, удаление не требуется")
            return 0
        
        sheet_id = await get_sheet_id_cached(service)
        if sheet_id is None:
            logger.error("Не удалось получить ID листа")
            return 0
        
        try:
            delete_request = {
                'deleteDimension': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': 2,
                        'endIndex': total_rows
                    }
                }
            }
            
            batch_request = {
                'requests': [delete_request]
            }
            
            spreadsheet.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=batch_request
            ).execute()
            
        except Exception as delete_error:
            logger.error(f"Ошибка при удалении строк: {delete_error}")
            return 0
        
        if videos_to_keep:
            rows_to_insert = []
            
            for row in videos_to_keep:
                while len(row) < len(EXPECTED_HEADERS):
                    row.append("")
                
                new_row = row.copy()
                
                if len(new_row) > status_column_index:
                    current_status = new_row[status_column_index].strip().lower()
                    if current_status not in ['просмотрено', 'удалить', 'не просмотрено']:
                        new_row[status_column_index] = 'не просмотрено'
                
                rows_to_insert.append(new_row)
            
            if rows_to_insert:
                body = {
                    'values': rows_to_insert
                }
                
                spreadsheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_NAME}!A3",
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
        
        if videos_to_keep:
            await asyncio.sleep(1)
            
            result = spreadsheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A3:{chr(65 + len(EXPECTED_HEADERS) - 1)}",
                majorDimension="ROWS"
            ).execute()
            
            values = result.get('values', [])
            
            if values:
                updated_rows = []
                
                for row in values:
                    while len(row) < len(EXPECTED_HEADERS):
                        row.append("")
                    
                    new_row = row.copy()
                    
                    if len(new_row) > 2 and new_row[2]:
                        duration = new_row[2]
                        if not duration.startswith("'"):
                            new_row[2] = f"'{duration}"
                    
                    if len(new_row) > 3 and new_row[3]:
                        video_link = new_row[3]
                        video_id = extract_youtube_id(video_link)
                        
                        if video_id:
                            preview_formula = f'=IMAGE("https://img.youtube.com/vi/{video_id}/hqdefault.jpg"; 2)'
                            new_row[0] = preview_formula
                    
                    updated_rows.append(new_row)
                
                clear_range = f"{SHEET_NAME}!A3:{chr(65 + len(EXPECTED_HEADERS) - 1)}"
                spreadsheet.values().clear(
                    spreadsheetId=SPREADSHEET_ID,
                    range=clear_range,
                    body={}
                ).execute()
                
                await asyncio.sleep(1)
                
                if updated_rows:
                    body = {
                        'values': updated_rows
                    }
                    
                    spreadsheet.values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"{SHEET_NAME}!A3",
                        valueInputOption='USER_ENTERED',
                        body=body
                    ).execute()
        
        await asyncio.sleep(1)
        
        new_total_rows = await get_total_rows()
        logger.info(f"Удаление завершено: всего видео {new_total_rows - 2}")
        
        await apply_formatting_after_add(new_total_rows)
        
        if new_total_rows > 2:
            for row_index in range(3, new_total_rows + 1):
                try:
                    await create_dropdown_for_new_row(row_index)
                    if row_index % 10 == 0:
                        await asyncio.sleep(0.5)
                except Exception as e:
                    continue
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"Ошибка при удалении видео: {e}", exc_info=True)
        return 0

# ------------------ Автоматическая очистка по времени ------------------

async def check_and_run_auto_cleanup():
    """Проверяет время и запускает автоматическую очистку если нужно"""
    try:
        moscow_now = get_moscow_datetime()
        current_time_str = moscow_now.strftime("%H:%M")
        
        if current_time_str == AUTO_CLEANUP_TIME:
            logger.info(f"Время для автоматической очистки: {AUTO_CLEANUP_TIME} МСК")
            
            start_time = datetime.now()
            deleted_count = await cleanup_videos()
            end_time = datetime.now()
            
            duration = (end_time - start_time).total_seconds()
            
            if deleted_count > 0:
                logger.info(f"Автоматическая очистка завершена за {duration:.2f} сек. Удалено строк: {deleted_count}")
            else:
                logger.info(f"Автоматическая очистка завершена за {duration:.2f} сек. Нечего удалять")
                
            return True
        return False
        
    except Exception as e:
        logger.error(f"Ошибка при проверке времени автоматической очистки: {e}")
        return False

async def auto_cleanup_scheduler():
    """Планировщик автоматической очистки по времени"""
    logger.info(f"Автоматическая очистка настроена на время {AUTO_CLEANUP_TIME} МСК")
    
    while True:
        try:
            await asyncio.sleep(60)
            
            await check_and_run_auto_cleanup()
            
        except Exception as e:
            logger.error(f"Ошибка в планировщике автоматической очистки: {e}")
            await asyncio.sleep(300)

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
    
    return f"{day} {month} {year}, {hour:02d}:{minute:02d}\u00A0(МСК)"

def format_duration(seconds):
    """Форматирует продолжительность в формат '0:00:00'"""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours}:{minutes:02d}:{seconds:02d}"

async def fetch_video_info(video_id):
    """Асинхронное получение информации о видео"""
    if not YOUTUBE_API_KEY:
        return None, None, None
    
    try:
        url = f'https://www.googleapis.com/youtube/v3/videos'
        params = {
            'id': video_id,
            'key': YOUTUBE_API_KEY,
            'part': 'snippet,contentDetails'
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
            
            duration_str = data['items'][0]['contentDetails']['duration']
            duration_seconds = parse_youtube_duration(duration_str)
            formatted_duration = format_duration(duration_seconds)
            
            return video_title, thumbnail_url, formatted_duration
            
    except Exception as e:
        logger.error(f"Ошибка при получении информации о видео: {e}")
    
    return None, None, None

def parse_youtube_duration(duration_str):
    """Парсит продолжительность из формата YouTube ISO 8601 в секунды"""
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    if not match:
        return 0
    
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    
    return hours * 3600 + minutes * 60 + seconds

async def get_video_count_from_sheet():
    """Асинхронное получение количества видео"""
    try:
        service = await get_sheets_service()
        if service is None:
            return 0
        
        spreadsheet = service.spreadsheets()
        
        result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:{chr(65 + len(EXPECTED_HEADERS) - 1)}"
        ).execute()
        
        values = result.get('values', [])
        return len(values) - 2 if len(values) > 2 else 0
    except Exception as e:
        logger.error(f"Ошибка при получении количества видео из таблица: {e}")
        return 0

async def is_video_already_in_sheet(video_url):
    """Асинхронная проверка наличия видео в таблице"""
    try:
        service = await get_sheets_service()
        if service is None:
            return False
        
        video_id = extract_youtube_id(video_url)
        if not video_id:
            return False
        
        short_link = f"https://youtu.be/{video_id}"
        
        spreadsheet = service.spreadsheets()
        result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!D:D"
        ).execute()
        
        values = result.get('values', [])
        
        for row in values[2:]:
            if row and len(row) > 0:
                cell_link = row[0]
                if short_link in cell_link or video_url in cell_link:
                    return True
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке наличия видео в таблице: {e}")
        return False

async def write_to_google_sheets_async(video_url: str, user_name: str, is_anonymous: bool):
    """Асинхронная запись в Google Sheets (используется в очереди)"""
    try:
        service = await get_sheets_service()
        if service is None:
            raise Exception("Отсутствует подключение к Google Sheets API")
        
        video_id = extract_youtube_id(video_url)
        if not video_id:
            logger.error(f"Не удалось извлечь ID видео из ссылки: {video_url}")
            return False
        
        video_title, thumbnail_url, duration = await fetch_video_info(video_id)
        short_link = f"https://youtu.be/{video_id}"
        
        if not video_title:
            video_title = f"Видео от {user_name if not is_anonymous else 'Анонима'}"
        
        if not duration:
            duration = "0:00:00"
        
        if thumbnail_url:
            preview_formula = f'=IMAGE("{thumbnail_url}"; 2)'
        else:
            preview_formula = f'=IMAGE("https://img.youtube.com/vi/{video_id}/mqdefault.jpg"; 2)'
        
        author_name = "Аноним" if is_anonymous else user_name
        
        moscow_now = get_moscow_datetime()
        formatted_date = format_moscow_date(moscow_now)
        
        row_data = []
        num_columns = len(EXPECTED_HEADERS)
        
        if num_columns >= 1:
            row_data.append(preview_formula)
        if num_columns >= 2:
            row_data.append(video_title)
        if num_columns >= 3:
            row_data.append(f"'{duration}")
        if num_columns >= 4:
            row_data.append(short_link)
        if num_columns >= 5:
            row_data.append(author_name)
        if num_columns >= 6:
            row_data.append(formatted_date)
        if num_columns >= 7:
            row_data.append("не просмотрено")
        
        for i in range(7, num_columns):
            row_data.append("")
        
        spreadsheet = service.spreadsheets()
        
        body = {
            'values': [row_data]
        }
        
        result = spreadsheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:{chr(65 + len(EXPECTED_HEADERS) - 1)}"
        ).execute()
        
        values = result.get('values', [])
        insert_row = len(values) + 1
        
        spreadsheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A{insert_row}:{chr(65 + len(EXPECTED_HEADERS) - 1)}{insert_row}",
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        await create_dropdown_for_new_row(insert_row)
        
        new_total_rows = await get_total_rows()
        await apply_formatting_after_add(new_total_rows)
        
        video_id = extract_youtube_id(video_url)
        logger.info(f"Добавлено видео \"{short_link}\" от пользователя \"{author_name}\"")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при записи в Google Sheets: {e}")
        return False

def is_youtube_link(url: str) -> bool:
    match = re.match(YOUTUBE_REGEX, url)
    if match:
        url_lower = url.lower()
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
    
    user_chat_key = f"{chat_id}_{user_id}"
    
    if user_chat_key in started_users:
        short_text = "Просто отправь мне ссылку на YouTube видео, и я добавлю её в предложку.\n\nЕсли нужна помощь, используй /help"
        await update.message.reply_text(short_text)
        return
    
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
        f"3. После подтверждения видео добавляется в общую таблице\n"
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
            f"❌ Произошла ошибка при получении информации из таблицы. Попробуйте позже или свяжитесь с разработчиком @NoirBane"
        )

async def ask_anonymous_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, video_url: str, user_name: str):
    """Спрашивает пользователя, хочет ли он добавить видео анонимно"""
    user = update.effective_user
    user_id = user.id
    chat_id = update.effective_chat.id
    
    video_key = create_video_key(video_url, user_id)
    
    video_data = PendingVideo(
        video_url=video_url,
        user_name=user_name,
        user_id=user_id,
        chat_id=chat_id,
        timestamp=datetime.now().timestamp()
    )
    
    await pending_videos.add(video_key, video_data)
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, с моим именем", callback_data=f"name_{video_key}"),
            InlineKeyboardButton("🚫 Нет, анонимно", callback_data=f"anon_{video_key}")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        message = await update.message.reply_text(
            f"Хотите, чтобы в таблице было указано ваше имя и тег?\n"
            f"👤 Ваше имя: {user_name}",
            reply_markup=reply_markup
        )
        video_data.message_id = message.message_id
        await pending_videos.add(video_key, video_data)
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение с выбором анонимности: {e}")
        try:
            await update.message.reply_text(
                f"Хотите, чтобы в таблице было указано ваше имя и тег?\n"
                f"👤 Ваше имя: {user_name}\n\n"
                f"К сожалению, произошла ошибка при создании кнопок. "
                f"Пожалуйста, отправьте ссылку на видео еще раз или свяжитесь с разработчиком @NoirBane"
            )
        except:
            pass

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оптимизированный обработчик callback-запросов с упрощенным UX"""
    query = update.callback_query
    
    try:
        await query.edit_message_text("⏳ Видео добавляется в таблицу, пожалуйста, ожидайте...")
    except Exception as e:
        logger.warning(f"Не удалось обновить сообщение: {e}")
        try:
            if update.effective_chat:
                await update.effective_chat.send_message("⏳ Видео добавляется в таблицу, пожалуйста, ожидайте...")
        except Exception as send_error:
            logger.error(f"Не удалось отправить сообщение: {send_error}")
    
    async def answer_telegram():
        try:
            await query.answer()
        except Exception as e:
            logger.debug(f"Не ответили на callback: {e}")
    
    asyncio.create_task(answer_telegram())
    
    callback_data = query.data
    
    if callback_data.startswith("name_"):
        video_key = callback_data[5:]
        is_anonymous = False
    elif callback_data.startswith("anon_"):
        video_key = callback_data[5:]
        is_anonymous = True
    else:
        error_msg = "❌ Ошибка обработки запроса. Попробуйте еще раз или свяжитесь с разработчиком @NoirBane"
        try:
            await query.edit_message_text(error_msg)
        except:
            if update.effective_chat:
                await update.effective_chat.send_message(error_msg)
        return
    
    user_id = query.from_user.id
    
    video_data = await pending_videos.get(video_key)
    if not video_data:
        error_msg = "❌ Ссылка устарела. Пожалуйста, отправьте видео еще раз или свяжитесь с разработчиком @NoirBane"
        try:
            await query.edit_message_text(error_msg)
        except:
            if update.effective_chat:
                await update.effective_chat.send_message(error_msg)
        return
    
    if user_id != video_data.user_id:
        error_msg = "❌ Это не ваше видео! Если возникли проблемы, свяжитесь с разработчиком @NoirBane"
        try:
            await query.edit_message_text(error_msg)
        except:
            if update.effective_chat:
                await update.effective_chat.send_message(error_msg)
        return
    
    asyncio.create_task(pending_videos.remove(video_key))
    
    video_url = video_data.video_url
    user_name = video_data.user_name
    
    async def add_video_task():
        try:
            if await is_video_already_in_sheet(video_url):
                error_msg = "⚠️ Это видео уже есть в предложке!\n\nПроверьте полный список через /list"
                try:
                    await query.edit_message_text(error_msg)
                except:
                    if update.effective_chat:
                        await update.effective_chat.send_message(error_msg)
                return
            
            success = await write_queue.add_write_task(
                write_to_google_sheets_async,
                video_url,
                user_name,
                is_anonymous
            )
            
            video_id = extract_youtube_id(video_url)
            short_link = f"https://youtu.be/{video_id}" if video_id else video_url
            
            if success:
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
                
                user_submissions[user_id] += 1
                
            else:
                success_message = (
                    f"❌ Не удалось добавить видео\n\n"
                    f"📹 Ссылка: {short_link}\n\n"
                    f"⚠️ Попробуйте еще раз или свяжитесь с разработчиком @NoirBane"
                )
            
            try:
                await query.edit_message_text(success_message)
            except Exception as e:
                logger.warning(f"Не удалось обновить сообщение: {e}")
                if update.effective_chat:
                    try:
                        await update.effective_chat.send_message(success_message)
                    except Exception as send_error:
                        logger.error(f"Не удалось отправить сообщение пользователю: {send_error}")
                        
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче добавления видео: {e}")
            
            error_message = "❌ Произошла ошибка при добавлении видео. Попробуйте еще раз или свяжитесь с разработчиком @NoirBane"
            try:
                await query.edit_message_text(error_message)
            except:
                if update.effective_chat:
                    await update.effective_chat.send_message(error_message)
    
    asyncio.create_task(add_video_task())

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
            if await is_video_already_in_sheet(message_text):
                await update.message.reply_text(
                    "⚠️ Это видео уже есть в предложке!\n"
                    "Проверьте полный список через /list"
                )
                return
            
            video_id = extract_youtube_id(message_text)
            user_name = user.first_name
            if user.username:
                user_name = f"{user_name} (@{user.username})"
            
            await ask_anonymous_choice(update, context, message_text, user_name)
            
        else:
            await update.message.reply_text(
                "❌ Это ссылка не на YouTube видео или она формата shorts!\n\n"
                "Я принимаю только ссылки на полнометражные видео YouTube.\n\n"
                "Примеры правильных ссылок:\n"
                "• https://youtube.com/watch?v=dQw4w9WgXcQ\n"
                "• https://youtu.be/dQw4w9WgXcQ\n"
                "• https://www.youtube.com/v/dQw4w9WgXcQ\n\n"
                "Попробуйте отправить другую ссылку"
            )
    else:
        user_chat_key = f"{chat_id}_{user.id}"
        if user_chat_key in started_users:
            response = f"Привет, {user.first_name}! Я жду от тебя ссылку на YouTube видео. Просто отправь её мне, и я добавлю в предложку.\n\nЕсли нужна помощь, используй /help"
        else:
            response = f"Привет, {user.first_name}! 👋\n\nЯ бот для предложки YouTube видео.\nИспользуй /start для начала работы или отправь мне ссылку на YouTube видео."
        
        await update.message.reply_text(response)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Ошибка при обработке сообщения: {context.error}", exc_info=True)
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "😕 Произошла ошибка при обработке запроса.\n"
                "Попробуйте еще раз или свяжитесь с разработчиком @NoirBane"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке: {e}")

async def periodic_cleanup():
    """Периодическая очистка устаревших данных"""
    while True:
        try:
            cleaned = await pending_videos.cleanup_old()
            if cleaned > 0:
                logger.info(f"Очищено {cleaned} устаревших записей")
        except Exception as e:
            logger.error(f"Ошибка при очистке устаревших данных: {e}")
        
        await asyncio.sleep(300)

async def startup(app: Application):
    """Действия при запуске бота"""
    logger.info("Бот запускается...")
    
    logger.info("Инициализация Google Sheets...")
    if await initialize_google_sheets():
        logger.info("Google Sheets успешно инициализирован")
    else:
        logger.error("Не удалось инициализировать Google Sheets")
    
    await write_queue.start()
    logger.info("Очередь записи запущена")
    
    logger.info("Запуск очистки завершенных видео...")
    deleted_count = await cleanup_videos()
    if deleted_count > 0:
        logger.info(f"Удалено завершенных видео: {deleted_count}")
    else:
        logger.info("Завершенных видео для удаления не найдено")
    
    # Запускаем все фоновые задачи
    asyncio.create_task(periodic_cleanup())
    asyncio.create_task(auto_cleanup_scheduler())
    
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
    logger.info("Конфигурация загружена успешно.")
    return True

def main():
    if not check_config():
        return
    
    try:
        app = Application.builder().token(TOKEN).build()
        
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("list", list_command))
        app.add_handler(CommandHandler("info", info_command))
        app.add_handler(CommandHandler("help", help_command))
        
        app.add_handler(CallbackQueryHandler(handle_callback_query))
        
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        app.add_error_handler(error_handler)
        
        app.post_init = startup
        app.post_stop = shutdown
        
        logger.info("Запуск бота...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}", exc_info=True)

if __name__ == '__main__':
    main()