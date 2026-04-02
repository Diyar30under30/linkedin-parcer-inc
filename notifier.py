"""
notifier.py — Telegram publishing layer.

Depends on: config.Config, database.Database, requests, stdlib
"""

import time

import requests

from config import Config
from database import Database


class TelegramPublisher:
    """Formats and sends vacancy messages to a Telegram channel."""

    def __init__(self, token="", channel_id=""):
        self.token = (token or Config.TELEGRAM_TOKEN or "").strip()
        self.channel_id = (channel_id or Config.TELEGRAM_CHANNEL or "").strip()
        self.db = Database()
        self.session = requests.Session()
        self.session.timeout = 30

    # ── Channel helpers ───────────────────────────────────────────────────

    def _normalize_channel_id(self, channel_id):
        """Return the channel_id in the form the Bot API expects."""
        if not channel_id:
            return None
        if channel_id.startswith('@'):
            channel_id = channel_id[1:]
        if channel_id and not channel_id.startswith('-100'):
            return f"@{channel_id}"
        return channel_id

    # ── Connection test ───────────────────────────────────────────────────

    def test_connection(self):
        """Verify bot token and attempt to send a test message. Returns (ok, message)."""
        if not self.token:
            return False, "Не указан токен бота"

        try:
            api_url = f"https://api.telegram.org/bot{self.token}/getMe"
            response = self.session.get(api_url, timeout=10)

            if response.status_code != 200:
                return False, f"Ошибка HTTP: {response.status_code}"

            data = response.json()
            if not data.get('ok'):
                return False, f"Ошибка API: {data.get('description', 'Неизвестно')}"

            bot_info = data['result']
            normalized_channel = self._normalize_channel_id(self.channel_id)

            if not normalized_channel:
                return True, f"Бот: @{bot_info['username']} (ID: {bot_info['id']})\n⚠ ID канала не указан"

            send_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            send_response = self.session.post(send_url, json={
                'chat_id': normalized_channel,
                'text': "✅ Бот подключен успешно! Парсер готов к работе.",
                'parse_mode': 'HTML',
            }, timeout=10)

            if send_response.status_code == 200:
                return True, f"Бот: @{bot_info['username']} (ID: {bot_info['id']})\n✅ Канал доступен для публикации"

            error_desc = send_response.json().get('description', 'Неизвестная ошибка')
            if "chat not found" in error_desc.lower():
                return False, f"Бот: @{bot_info['username']}\n❌ Бот не добавлен в канал или канал не существует"
            elif "forbidden" in error_desc.lower():
                return False, f"Бот: @{bot_info['username']}\n❌ У бота нет прав на публикацию в канал"
            return False, f"Бот: @{bot_info['username']}\n❌ Ошибка отправки: {error_desc}"

        except Exception as e:
            return False, f"Ошибка подключения: {e}"

    # ── Formatting ────────────────────────────────────────────────────────

    @staticmethod
    def _escape_html(text: str) -> str:
        """Escape characters that have special meaning in Telegram HTML mode."""
        return (
            text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
        )

    def _clean(self, text: str) -> str:
        """Collapse whitespace and HTML-escape a string."""
        if not text:
            return ""
        return self._escape_html(' '.join(text.split()))

    def format_vacancy_message(self, vacancy: dict) -> str:
        """Build a Telegram HTML message for *vacancy*."""
        try:
            title = self._clean(vacancy.get('title', 'Вакансия'))
            company = self._clean(vacancy.get('company_name', ''))
            location = self._clean(vacancy.get('location', 'Не указано'))
            salary = self._clean(vacancy.get('salary', 'не указана'))
            url = vacancy.get('source_url', '')

            description = vacancy.get('description', '')
            if description and description != "Описание не найдено":
                description = self._clean(description)
                if len(description) > Config.MAX_DESCRIPTION_LENGTH:
                    description = description[:Config.MAX_DESCRIPTION_LENGTH - 3] + "..."
            else:
                description = "Описание не указано"

            msg = f"<b>🏢 {title}</b>\n\n"
            if company:
                msg += f"<b>🏭 Компания:</b> {company}\n"
            if location and location != "Не указана":
                msg += f"<b>📍 Локация:</b> {location}\n"
            if salary and salary != "не указана":
                msg += f"<b>💰 Зарплата:</b> {salary}\n"
            msg += f"\n<b>📝 Описание:</b>\n{description}\n\n"
            if url:
                msg += f'<a href="{url}">🔗 Ссылка на вакансию</a>\n\n'
            msg += "#вакансия #работа #linkedin"
            return msg

        except Exception as e:
            print(f"Ошибка форматирования: {e}")
            return f"<b>Вакансия</b>\n\n{vacancy.get('source_url', '')}\n\n#вакансия"

    # ── Publishing ────────────────────────────────────────────────────────

    def publish_vacancy_sync(self, vacancy: dict) -> bool:
        """Send one vacancy to Telegram; mark it published on success."""
        if not self.token:
            print("❌ Токен бота не указан")
            return False
        if not self.channel_id:
            print("❌ ID канала не указан")
            return False

        normalized_channel = self._normalize_channel_id(self.channel_id)
        if not normalized_channel:
            print("❌ Неверный формат ID канала")
            return False

        message = self.format_vacancy_message(vacancy)
        if len(message) > 4000:
            print("⚠ Сообщение слишком длинное, обрезаем...")
            message = message[:4000]

        api_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = {
            'chat_id': normalized_channel,
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': False,
        }

        for attempt in range(3):
            try:
                print(f"📤 Попытка {attempt + 1} отправки вакансии #{vacancy.get('id')}...")
                response = self.session.post(api_url, json=data, timeout=30)

                if response.status_code == 200 and response.json().get('ok'):
                    self.db.mark_as_published(vacancy['id'])
                    print(f"✅ Успешно опубликовано: {vacancy.get('title', '')[:50]}...")
                    return True

                if response.status_code == 400:
                    error_desc = response.json().get('description', '')
                    print(f"❌ Ошибка 400: {error_desc}")
                    if "can't parse entities" in error_desc.lower():
                        print("⚠ Повторяем без HTML разметки...")
                        data['parse_mode'] = None
                        data['text'] = data['text'].replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
                        continue
                else:
                    print(f"⚠ Ошибка {response.status_code}: {response.text[:100]}")

                time.sleep(2)

            except requests.exceptions.Timeout:
                print(f"⚠ Таймаут при отправке, попытка {attempt + 1}")
                time.sleep(3)
            except Exception as e:
                print(f"⚠ Ошибка при отправке: {e}")
                time.sleep(2)

        print(f"❌ Не удалось опубликовать вакансию #{vacancy.get('id')}")
        return False

    def publish_all_unpublished(self) -> int:
        """Send all un-published vacancies; return count successfully sent."""
        if not self.token:
            print("❌ Не указан токен Telegram бота")
            return 0
        if not self.channel_id:
            print("❌ Не указан ID канала Telegram")
            return 0

        vacancies = self.db.get_unpublished_vacancies()
        if not vacancies:
            print("ℹ Нет неопубликованных вакансий")
            return 0

        print(f"📊 Начинаем публикацию {len(vacancies)} вакансий...")
        published_count = 0
        failed_count = 0

        for i, vacancy in enumerate(vacancies, 1):
            print(f"\n[{i}/{len(vacancies)}] Публикация: {vacancy.get('title', '')[:50]}...")
            if self.publish_vacancy_sync(vacancy):
                published_count += 1
            else:
                failed_count += 1
            if i < len(vacancies):
                print("⏳ Ждем 3 секунды...")
                time.sleep(3)

        print(f"\n{'='*50}")
        print(f"📊 ПУБЛИКАЦИЯ ЗАВЕРШЕНА")
        print(f"✅ Успешно: {published_count}")
        print(f"❌ Не удалось: {failed_count}")
        print(f"📈 Всего: {len(vacancies)}")
        print(f"{'='*50}")

        return published_count
