import asyncio
import pandas as pd
from telethon.sync import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.photos import GetUserPhotosRequest
from telethon.tl.functions.messages import GetPeerDialogsRequest, GetHistoryRequest
from telethon.tl.types import PeerChannel
from telethon.errors import FloodWaitError
from typing import Tuple, List, Union, Optional, Dict, Any


class TelegramParser:
    """
    Класс для парсинга данных из Telegram (контакты и сообщения).
    
    Examples:
        parser = TelegramParser()
        await parser.run()
        
    Attributes:
        client (TelegramClient): Авторизованный клиент Telegram
        api_id (str): ID приложения Telegram API
        api_hash (str): Хэш приложения Telegram API
        session_name (str): Имя файла сессии
    """
    
    def __init__(self):
        """Инициализация парсера."""
        self.client = None
        self.api_id = None
        self.api_hash = None
        self.session_name = None
    
    async def get_user_input(self) -> Tuple[str, str, str]:
        """
        Запрашивает у пользователя данные для подключения к Telegram API.

        Returns:
            Tuple[str, str, str]: Кортеж из (api_id, api_hash, session_name)
        """
        print("Введите данные для подключения к Telegram API:")
        self.api_id = input("API ID: ")
        self.api_hash = input("API Hash: ")
        self.session_name = input("Имя сессии (например: 'session_name'): ")
        return self.api_id, self.api_hash, self.session_name
    
    async def authorize(self) -> TelegramClient:
        """
        Авторизует клиент Telegram с предоставленными учетными данными.

        Returns:
            TelegramClient: Авторизованный клиент Telegram

        Raises:
            ConnectionError: Если не удалось установить соединение
        """
        print("\nАвторизация в Telegram...")
        self.client = TelegramClient(
            self.session_name, 
            int(self.api_id), 
            self.api_hash, 
            system_version='4.16.30-vxCUSTOM'
        )
        
        await self.client.start()
        print("Авторизация успешна!")
        return self.client
    
    async def choose_parse_mode(self) -> str:
        """
        Предлагает пользователю выбрать режим парсинга.

        Returns:
            str: Выбранный режим ('1', '2' или '3')
        """
        print("\nВыберите режим парсинга:")
        print("1. Парсинг контактов участников групп")
        print("2. Парсинг сообщений из групп/чатов")
        print("3. Выход")
        while True:
            choice = input("Введите номер выбора (1-3): ")
            if choice in ('1', '2', '3'):
                return choice
            print("Неверный ввод. Пожалуйста, выберите 1, 2 или 3.")
    
    async def list_user_groups(self) -> List[int]:
        """
        Получает список доступных групп/чатов пользователя.

        Returns:
            List[int]: Список ID выбранных групп
        """
        print("\nПолучаем список ваших групп/чатов...")
        groups = []
        
        async for dialog in self.client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                groups.append((dialog.id, dialog.name))
        
        if not groups:
            print("Не найдено ни одной группы/чата.")
            return []
        
        print("\nДоступные группы/чаты:")
        for i, (group_id, group_name) in enumerate(groups, 1):
            print(f"{i}. {group_name} (ID: {group_id})")
        
        while True:
            selected = input("\nВведите номера групп через запятую (например: 1,3,5) или 'all' для всех: ").strip()
            
            if selected.lower() == 'all':
                return [group[0] for group in groups]
            
            try:
                selected_indices = [int(x.strip()) - 1 for x in selected.split(",") if x.strip().isdigit()]
                selected_groups = [groups[i][0] for i in selected_indices if 0 <= i < len(groups)]
                if selected_groups:
                    return selected_groups
                print("Не выбрано ни одной группы. Попробуйте снова.")
            except ValueError:
                print("Некорректный ввод. Попробуйте снова.")
    
    async def get_message_params(self) -> Tuple[int, int, Union[int, float]]:
        """
        Запрашивает параметры для парсинга сообщений.

        Returns:
            Tuple[int, int, Union[int, float]]: (год, месяц, максимальное количество сообщений)
        """
        print("\nУкажите параметры для парсинга сообщений:")
        while True:
            try:
                year = int(input("Год, с которого начинать сбор сообщений (например 2023): "))
                if 2000 <= year <= 2100:
                    break
                print("Введите год между 2000 и 2100")
            except ValueError:
                print("Некорректный год. Попробуйте снова.")
        
        while True:
            try:
                stop_month = int(input("Месяц, на котором остановиться (1-12): "))
                if 1 <= stop_month <= 12:
                    break
                print("Введите месяц от 1 до 12")
            except ValueError:
                print("Некорректный месяц. Попробуйте снова.")
        
        while True:
            max_messages = input("Максимальное количество сообщений (оставьте пустым для без ограничений): ")
            if not max_messages:
                return year, stop_month, float("inf")
            try:
                return year, stop_month, int(max_messages)
            except ValueError:
                print("Некорректное число. Попробуйте снова.")
    
    async def fetch_contacts(self, chat_id: int) -> List[Dict[str, Any]]:
        """
        Собирает информацию о участниках указанной группы.

        Args:
            chat_id: ID группы для парсинга

        Returns:
            List[Dict[str, Any]]: Список словарей с информацией о пользователях
        """
        data = []
        try:
            chat_entity = await self.client.get_entity(chat_id)
            chat_title = chat_entity.title

            async for user in self.client.iter_participants(chat_id):
                user_data = await self._process_user(user, chat_id, chat_title)
                data.append(user_data)

        except Exception as e:
            print(f"Ошибка при обработке группы {chat_id}: {e}")

        return data
    
    async def _process_user(self, user, chat_id: int, chat_title: str) -> Dict[str, Any]:
        """
        Обрабатывает данные одного пользователя.

        Args:
            user: Объект пользователя Telethon
            chat_id: ID группы
            chat_title: Название группы

        Returns:
            Dict[str, Any]: Словарь с данными пользователя
        """
        user_id = user.id
        first_name = user.first_name or 'Нет информации'
        last_name = user.last_name or 'Нет информации'
        username = user.username or 'Нет информации'
        phone = user.phone or 'Нет информации'

        about, channel_title, channel_link = await self._get_user_about_and_channel(user_id)
        has_photo = await self._check_user_photo(user)
        last_seen = self._get_last_seen(user)
        
        return {
            'ID группы': str(chat_id),
            'Название группы': chat_title,
            'ID пользователя': user_id,
            'Имя': first_name,
            'Фамилия': last_name,
            'Username': username,
            'Телефон': phone,
            'Фото': has_photo,
            'Дата последнего визита (UTC)': last_seen,
            'Описание профиля': about,
            'Привязан канал': channel_title,
            'Ссылка на канал': channel_link,
            'Бот': getattr(user, 'bot', 'Нет информации'),
            'Подтверждён': getattr(user, 'verified', 'Нет информации'),
            'Удалён': getattr(user, 'deleted', 'Нет информации'),
            'Мошенник': getattr(user, 'scam', 'Нет информации'),
            'Фейк': getattr(user, 'fake', 'Нет информация')
        }
    
    async def _get_user_about_and_channel(self, user_id: int) -> Tuple[str, str, str]:
        """
        Получает информацию "О себе" и привязанный канал пользователя.

        Args:
            user_id: ID пользователя

        Returns:
            Tuple[str, str, str]: (about, channel_title, channel_link)
        """
        about = 'Нет описания'
        channel_title = 'Нет'
        channel_link = 'Нет'
        
        try:
            full_user = await self.client(GetFullUserRequest(user_id))
            about = full_user.full_user.about or 'Нет описания'
            channel_id = full_user.full_user.personal_channel_id

            if channel_id:
                try:
                    peer = PeerChannel(channel_id)
                    result = await self.client(GetPeerDialogsRequest(peers=[peer]))
                    channel = result.chats[0]
                    channel_title = channel.title
                    channel_link = f"https://t.me/{channel.username}" if channel.username else 'Нет username'
                except Exception as e:
                    channel_title = 'Ошибка'
                    channel_link = f'Ошибка: {e}'
        except Exception:
            pass
        
        return about, channel_title, channel_link
    
    async def _check_user_photo(self, user) -> str:
        """
        Проверяет, есть ли у пользователя фото профиля.

        Args:
            user: Объект пользователя Telethon

        Returns:
            str: 'Есть' если фото есть, 'Нет' если нет
        """
        try:
            photos = await self.client(GetUserPhotosRequest(user_id=user.id, offset=0, max_id=0, limit=1))
            return 'Есть' if photos.photos else 'Нет'
        except Exception:
            return 'Нет'
    
    def _get_last_seen(self, user) -> str:
        """
        Получает дату последнего посещения пользователя.

        Args:
            user: Объект пользователя Telethon

        Returns:
            str: Строка с датой или 'Неизвестно'
        """
        if hasattr(user.status, 'was_online'):
            return user.status.was_online.strftime('%Y-%m-%d %H:%M:%S')
        return 'Неизвестно'
    
    async def fetch_messages(self, chat_id: int, year: int, 
                           stop_month: int, max_messages: Union[int, float]) -> List[Dict[str, Any]]:
        """
        Собирает сообщения из указанного чата.

        Args:
            chat_id: ID чата для парсинга
            year: Год, с которого начинать сбор
            stop_month: Месяц, на котором остановиться
            max_messages: Максимальное количество сообщений

        Returns:
            List[Dict[str, Any]]: Список сообщений с метаданными
        """
        data = []
        try:
            entity = await self.client.get_entity(chat_id)
        except Exception as e:
            print(f"Не удалось получить чат {chat_id}: {e}")
            return data

        chat_name = entity.title or 'Нет названия'
        offset_id = 0
        message_count = 0

        while True:
            try:
                messages = await self.client(GetHistoryRequest(
                    peer=entity,
                    offset_id=offset_id,
                    offset_date=None,
                    add_offset=0,
                    limit=100,
                    max_id=0,
                    min_id=0,
                    hash=0
                ))

                if not messages.messages:
                    break

                for message in messages.messages:
                    if self._should_stop_parsing(message.date, year, stop_month):
                        return data

                    if not message.message:
                        continue

                    message_data = await self._process_message(message, chat_id, chat_name)
                    data.append(message_data)
                    message_count += 1

                    if message_count % 500 == 0:
                        print(f"Собрано {message_count} сообщений")
                    if message_count >= max_messages:
                        return data

                offset_id = messages.messages[-1].id

            except FloodWaitError as e:
                print(f"FloodWait: жду {e.seconds} секунд")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                print(f"Ошибка: {e}")
                break

        return data
    
    def _should_stop_parsing(self, msg_date, year: int, stop_month: int) -> bool:
        """
        Определяет, нужно ли прекратить парсинг по дате сообщения.

        Args:
            msg_date: Дата сообщения
            year: Целевой год
            stop_month: Целевой месяц

        Returns:
            bool: True если парсинг следует прекратить
        """
        return msg_date.year < year or (msg_date.year == year and msg_date.month < stop_month)
    
    async def _process_message(self, message, chat_id: int, chat_name: str) -> Dict[str, Any]:
        """
        Обрабатывает одно сообщение и извлекает нужные данные.

        Args:
            message: Объект сообщения Telethon
            chat_id: ID чата
            chat_name: Название чата

        Returns:
            Dict[str, Any]: Словарь с данными сообщения
        """
        user_id, username, phone = await self._get_message_sender(message)
        fwd_from = await self._get_forward_source(message)
        
        return {
            'ID группы': chat_id,
            'Имя пользователя': username,
            'ID пользователя': user_id,
            'Телефон': phone,
            'Дата': message.date.strftime("%Y-%m-%d %H:%M:%S"),
            'Текст сообщения': message.message,
            'Имя группы': chat_name,
            'Переслано от': fwd_from,
            'Медиа (есть/нет)': 1 if message.media else 0
        }
    
    async def _get_message_sender(self, message) -> Tuple[Optional[int], str, str]:
        """
        Получает информацию об отправителе сообщения.

        Args:
            message: Объект сообщения Telethon

        Returns:
            Tuple[Optional[int], str, str]: (user_id, username, phone)
        """
        if message.from_id is None:
            return None, 'Нет', 'Нет'
        
        try:
            user = await self.client.get_entity(message.from_id)
            return user.id, getattr(user, 'username', 'Нет'), getattr(user, 'phone', 'Нет')
        except Exception:
            return None, 'Нет', 'Нет'
    
    async def _get_forward_source(self, message) -> str:
        """
        Получает источник пересланного сообщения.

        Args:
            message: Объект сообщения Telethon

        Returns:
            str: Имя источника или 'Нет'
        """
        if not message.fwd_from:
            return 'Нет'
        
        try:
            if message.fwd_from.from_id:
                user = await self.client.get_entity(message.fwd_from.from_id)
                return getattr(user, 'username', 'Неизвестный источник')
            return 'Нет'
        except Exception:
            return 'Неизвестный источник'
    
    async def parse_contacts(self) -> None:
        """Основная функция парсинга контактов из выбранных групп."""
        group_ids = await self.list_user_groups()
        if not group_ids:
            return
        
        all_data = []
        for chat_id in group_ids:
            group_data = await self.fetch_contacts(chat_id)
            all_data.extend(group_data)

            chat_df = pd.DataFrame(group_data)
            chat_df.to_excel(f'{chat_id}_contacts.xlsx', index=False, engine='openpyxl')
            print(f"Сохранены контакты для группы {chat_id}")
            await asyncio.sleep(2)

        all_data_df = pd.DataFrame(all_data)
        all_data_df.to_excel('all_telegram_contacts.xlsx', index=False, engine='openpyxl')
        print("Общий файл с контактами сохранён.")
    
    async def parse_messages(self) -> None:
        """Основная функция парсинга сообщений из выбранных групп."""
        group_ids = await self.list_user_groups()
        if not group_ids:
            return
        
        year, stop_month, max_messages = await self.get_message_params()
        
        all_data = []
        for chat_id in group_ids:
            chat_data = await self.fetch_messages(chat_id, year, stop_month, max_messages)
            all_data.extend(chat_data)
            
            chat_df = pd.DataFrame(chat_data)
            chat_df.to_excel(f'{chat_id}_messages.xlsx', index=False, engine='openpyxl')
            print(f"Сохранены сообщения для группы {chat_id}")
            await asyncio.sleep(5)

        all_data_df = pd.DataFrame(all_data)
        all_data_df.to_excel('all_telegram_messages.xlsx', index=False, engine='openpyxl')
        print("Общий файл с сообщениями сохранён.")
    
    async def run(self) -> None:
        """Основной метод запуска парсера."""
        await self.get_user_input()
        await self.authorize()
        
        try:
            while True:
                parse_mode = await self.choose_parse_mode()
                
                if parse_mode == '1':
                    await self.parse_contacts()
                elif parse_mode == '2':
                    await self.parse_messages()
                elif parse_mode == '3':
                    print("Выход из программы.")
                    break
                
                print("\n" + "="*50 + "\n")
        finally:
            await self.client.disconnect()
