"""
postgres.py — PostgreSQL integration layer.

Depends only on: config.Config, stdlib, psycopg2
"""

import re
import hashlib
import time
from datetime import datetime

import psycopg2

from config import Config


class PostgresDBFixedColumns:
    """Manages writes to the remote job_posting_sources PostgreSQL table."""

    def __init__(self):
        self.host = Config.POSTGRES_HOST
        self.port = Config.POSTGRES_PORT
        self.database = Config.POSTGRES_DB
        self.user = Config.POSTGRES_USER
        self.password = Config.POSTGRES_PASSWORD
        self.connection = None
        self.cursor = None
        self.enabled = Config.POSTGRES_ENABLED

    # ── Connection ────────────────────────────────────────────────────────

    def connect(self):
        """Open a connection to PostgreSQL and verify the required table exists."""
        if not self.enabled:
            print("⚠ PostgreSQL интеграция отключена в конфигурации")
            return False

        try:
            print(f"🔄 Попытка подключения к PostgreSQL: {self.host}:{self.port}/{self.database}")

            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()

            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()

            if result:
                print(f"✅ Подключено к PostgreSQL: {self.host}:{self.port}/{self.database}")
                if self.check_table_exists():
                    print("✅ Таблица 'job_posting_sources' существует")
                    self.get_table_columns()
                    return True
                else:
                    print("❌ Таблица 'job_posting_sources' не существует")
                    return False
            else:
                print("⚠ Подключение не установлено")
                return False

        except Exception as e:
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
            return False

    def disconnect(self):
        """Close the PostgreSQL connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                print("✅ Отключено от PostgreSQL")
        except Exception:
            pass

    # ── Schema helpers ────────────────────────────────────────────────────

    def check_table_exists(self):
        """Return True if job_posting_sources exists in the public schema."""
        try:
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'job_posting_sources'
            );
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result[0] if result else False
        except Exception as e:
            print(f"❌ Ошибка проверки таблицы: {e}")
            return False

    def get_table_columns(self):
        """Print the column list of job_posting_sources for debugging."""
        try:
            query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'job_posting_sources'
            ORDER BY ordinal_position;
            """
            self.cursor.execute(query)
            columns = self.cursor.fetchall()
            print("📋 Структура таблицы 'job_posting_sources':")
            for column in columns:
                print(f"   • {column[0]} ({column[1]}) {'NULL' if column[2] == 'YES' else 'NOT NULL'}")
            return columns
        except Exception as e:
            print(f"⚠ Ошибка получения структуры таблицы: {e}")
            return []

    def check_column_exists(self, column_name):
        """Return True if *column_name* exists in job_posting_sources."""
        try:
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = 'job_posting_sources'
                AND column_name = %s
            );
            """
            self.cursor.execute(query, (column_name,))
            result = self.cursor.fetchone()
            return result[0] if result else False
        except Exception as e:
            print(f"⚠ Ошибка проверки колонки {column_name}: {e}")
            return False

    # ── Query execution ───────────────────────────────────────────────────

    def execute_query(self, query, params=None):
        """Run an arbitrary SQL statement; returns rows for SELECT, True for DML."""
        if not self.enabled or not self.connection:
            print("⚠ PostgreSQL не подключен или отключен")
            return None

        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return True

        except Exception as e:
            print(f"❌ Ошибка выполнения запроса PostgreSQL: {e}")
            print(f"   Запрос: {query[:200]}")
            if params:
                print(f"   Параметры: {params}")
            return None

    # ── Data extraction helpers ───────────────────────────────────────────

    def extract_contacts(self, description, url):
        """Parse contact info (name, phone, social links) from a job description."""
        try:
            if not description:
                return ""

            contact_parts = []

            name_patterns = [
                r'контактное лицо[:\s]+([^\n]+)',
                r'по вопросам[:\s]+([^\n]+)',
                r'обращаться[:\s]+([^\n]+)',
                r'сотрудник[:\s]+([^\n]+)',
                r'менеджер[:\s]+([^\n]+)',
                r'рекрутер[:\s]+([^\n]+)',
                r'hr[:\s]+([^\n]+)',
                r'обращайтесь[:\s]+([^\n]+)',
            ]
            for pattern in name_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    contact_parts.append(f"Имя: {match.group(1).strip()}")
                    break

            phone_patterns = [
                r'\+7\s?\(?\d{3}\)?\s?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
                r'8\s?\(?\d{3}\)?\s?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
                r'тел[\.:\s]+([+\d\s\-\(\)]{7,})',
                r'телефон[:\s]+([+\d\s\-\(\)]{7,})',
                r'\+?\d[\d\s\-\(\)]{7,}\d',
            ]
            for pattern in phone_patterns:
                matches = re.findall(pattern, description)
                if matches:
                    contact_parts.append(f"Телефон: {', '.join(matches[:3])}")
                    break

            social_patterns = {
                'linkedin': r'linkedin\.com/[^\s]+',
                'telegram': r't\.me/[^\s]+',
                'whatsapp': r'wa\.me/[^\s]+',
                'vk': r'vk\.com/[^\s]+',
                'facebook': r'facebook\.com/[^\s]+',
                'instagram': r'instagram\.com/[^\s]+',
                'twitter': r'twitter\.com/[^\s]+',
                'x.com': r'x\.com/[^\s]+',
            }
            social_links = []
            for _, pattern in social_patterns.items():
                matches = re.findall(pattern, description, re.IGNORECASE)
                if matches:
                    social_links.extend(matches[:2])

            email_matches = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', description)
            if email_matches:
                social_links.extend(email_matches[:3])

            if social_links:
                contact_parts.append(f"Соцсети: {', '.join(social_links[:5])}")

            if url:
                contact_parts.append(f"URL: {url}")

            return (' | '.join(contact_parts))[:500]

        except Exception as e:
            print(f"⚠ Ошибка извлечения контактов: {e}")
            return ""

    def extract_salary_info(self, salary_text):
        """Parse min/max salary and currency from a free-text salary string."""
        try:
            salary_min = None
            salary_max = None
            salary_currency = "KZT"

            if not salary_text or salary_text.lower() in ['не указана', 'договорная']:
                return salary_min, salary_max, salary_currency

            original_text = salary_text
            salary_text = salary_text.lower().strip()

            currency_patterns = {
                'RUB': [r'₽', r'руб\.?', r'rur', r'rub', r'р\.'],
                'USD': [r'\$', r'usd', r'доллар'],
                'EUR': [r'€', r'eur', r'euro', r'евро'],
                'KZT': [r'₸', r'kzt', r'тенге', r'тг'],
                'GBP': [r'£', r'gbp', r'фунт'],
            }

            detected_currency = "KZT"
            for currency, patterns in currency_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, salary_text, re.IGNORECASE):
                        detected_currency = currency
                        break
                if detected_currency != "KZT":
                    break

            salary_currency = detected_currency

            patterns = [
                r'(?:от\s*)?(\d[\d\s]*[\d])(?:\s*[-–—]\s*|\s*(?:до|по)\s*)(\d[\d\s]*[\d])',
                r'(\d[\d\s]*[\d])\s*[-–—]\s*(\d[\d\s]*[\d])',
                r'от\s*(\d[\d\s]*[\d])',
                r'до\s*(\d[\d\s]*[\d])',
                r'^(\d[\d\s]*[\d])(?![^\s]*\d)',
            ]

            for pattern in patterns:
                match = re.search(pattern, salary_text, re.IGNORECASE)
                if match:
                    numbers = []
                    for group in match.groups():
                        if group:
                            clean_num = re.sub(r'[^\d]', '', group)
                            if clean_num:
                                num = int(clean_num)
                                if 'к' in salary_text or 'k' in salary_text or 'тыс' in salary_text:
                                    num *= 1000
                                numbers.append(num)

                    if len(numbers) == 2:
                        salary_min = min(numbers[0], numbers[1])
                        salary_max = max(numbers[0], numbers[1])
                    elif len(numbers) == 1:
                        salary_min = numbers[0]
                        salary_max = numbers[0]

                    if salary_min is not None:
                        break

            print(f"💰 Обработка зарплаты: '{original_text}' -> мин: {salary_min}, макс: {salary_max}, валюта: {salary_currency}")
            return salary_min, salary_max, salary_currency

        except Exception as e:
            print(f"⚠ Ошибка извлечения зарплаты '{salary_text}': {e}")
            return None, None, "KZT"

    def normalize_company_name(self, company_name_raw):
        """Normalise a raw company name string."""
        if not company_name_raw:
            return ""

        normalized = re.sub(r'\s+', ' ', company_name_raw.strip()).lower()

        for form in ['ооо', 'зао', 'оао', 'ао', 'ип', 'тнв', 'пк', 'чп']:
            normalized = re.sub(rf'\b{form}\b\.?\s*', '', normalized)

        normalized = re.sub(r'[\"«»].*?[\"«»]', '', normalized)
        normalized = re.sub(r'\(.*?\)', '', normalized)
        normalized = re.sub(r'[^\w\sа-яА-ЯёЁ-]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        words = normalized.split()
        if words:
            words[0] = words[0].capitalize()
            for i in range(1, len(words)):
                if len(words[i]) > 2:
                    words[i] = words[i].capitalize()
            normalized = ' '.join(words)

        return normalized[:255]

    # ── ID helpers ────────────────────────────────────────────────────────

    def generate_source_id(self, title, company, location):
        """Derive a stable 32-char SHA-256 ID from title/company/location."""
        try:
            base = f"{title[:50].strip().lower()}_{(company or '')[:50].strip().lower()}_{(location or '')[:50].strip().lower()}"
            return hashlib.sha256(base.encode('utf-8')).hexdigest()[:32]
        except Exception:
            return hashlib.sha256(str(time.time()).encode()).hexdigest()[:32]

    def check_vacancy_exists(self, source_id):
        """Return True if source_id already exists in job_posting_sources."""
        try:
            result = self.execute_query(
                "SELECT id FROM job_posting_sources WHERE source_id = %s LIMIT 1",
                (source_id,),
            )
            return result is not None and len(result) > 0
        except Exception:
            return False

    # ── Write ─────────────────────────────────────────────────────────────

    def save_vacancy_to_postgres(self, vacancy_data):
        """Insert a vacancy dict into job_posting_sources; skip if duplicate."""
        if not self.enabled:
            print("⚠ PostgreSQL интеграция отключена")
            return False

        try:
            if not self.connection or self.connection.closed:
                print("🔄 Переподключение к PostgreSQL...")
                if not self.connect():
                    print("❌ Не удалось подключиться к PostgreSQL")
                    return False

            position_name = vacancy_data.get('title', '')
            company_name_raw = vacancy_data.get('company_name', '') or "Не указана"
            vacancy_description = vacancy_data.get('description', '')
            location = vacancy_data.get('location', 'Не указана')
            salary_text = vacancy_data.get('salary', 'не указана')
            source_url = vacancy_data.get('source_url', '')
            org_url = source_url

            if not position_name or position_name == 'Не указано':
                print("⚠ Пропущена вакансия без названия должности")
                return False

            source_id = self.generate_source_id(position_name, company_name_raw, location)

            if self.check_vacancy_exists(source_id):
                print(f"⚠ Вакансия уже существует в PostgreSQL: {position_name[:50]}...")
                return True

            contact_social = self.extract_contacts(vacancy_description, source_url)
            salary_min, salary_max, salary_currency = self.extract_salary_info(salary_text)
            company_name_normalized = self.normalize_company_name(company_name_raw)
            created_at = datetime.now()

            org_url_column_exists = self.check_column_exists(Config.ORG_URL_COLUMN)

            if org_url_column_exists:
                print(f"✅ Колонка '{Config.ORG_URL_COLUMN}' существует в таблице")
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id, position_name, vacancy_description, company_name_raw,
                    source_type, location, contact_social, created_at, created_by,
                    salary_min, salary_max, salary_currency, is_published,
                    company_name_normalized, org_url
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                insert_params = (
                    source_id,
                    position_name[:200] or "Не указано",
                    vacancy_description[:5000] or "Описание не указано",
                    company_name_raw[:200],
                    'linkedin',
                    location[:100] or "Не указана",
                    contact_social[:500] if contact_social else None,
                    created_at,
                    Config.SYSTEM_USER_UUID,
                    salary_min,
                    salary_max,
                    salary_currency or None,
                    False,
                    company_name_normalized[:255] if company_name_normalized else None,
                    org_url[:500] if org_url else None,
                )
            else:
                print(f"⚠ Колонка '{Config.ORG_URL_COLUMN}' не существует, сохраняем без неё")
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id, position_name, vacancy_description, company_name_raw,
                    source_type, location, contact_social, created_at, created_by,
                    salary_min, salary_max, salary_currency, is_published,
                    company_name_normalized
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                insert_params = (
                    source_id,
                    position_name[:200] or "Не указано",
                    vacancy_description[:5000] or "Описание не указано",
                    company_name_raw[:200],
                    'linkedin',
                    location[:100] or "Не указана",
                    contact_social[:500] if contact_social else None,
                    created_at,
                    Config.SYSTEM_USER_UUID,
                    salary_min,
                    salary_max,
                    salary_currency or None,
                    False,
                    company_name_normalized[:255] if company_name_normalized else None,
                )

            print(f"🔄 Сохранение в PostgreSQL: {position_name[:50]}...")
            result = self.execute_query(insert_query, insert_params)

            if result is not None:
                print(f"✅ Вакансия сохранена в PostgreSQL: {position_name[:50]}...")
                return True
            else:
                print(f"❌ Ошибка сохранения в PostgreSQL: {position_name[:50]}...")
                return self._simple_insert(source_id, position_name, company_name_raw, vacancy_description, org_url)

        except Exception as e:
            print(f"❌ Критическая ошибка сохранения в PostgreSQL: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _simple_insert(self, source_id, position_name, company_name_raw, vacancy_description, org_url=None):
        """Fallback insert with only the mandatory fields."""
        try:
            if not vacancy_description:
                vacancy_description = "Описание не указано"

            org_url_column_exists = self.check_column_exists(Config.ORG_URL_COLUMN)

            if org_url_column_exists and org_url:
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id, position_name, vacancy_description, company_name_raw,
                    source_type, created_at, created_by, is_published, org_url
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                insert_params = (
                    source_id,
                    position_name[:200],
                    vacancy_description[:5000],
                    company_name_raw[:200],
                    'linkedin',
                    datetime.now(),
                    Config.SYSTEM_USER_UUID,
                    False,
                    org_url[:500],
                )
            else:
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id, position_name, vacancy_description, company_name_raw,
                    source_type, created_at, created_by, is_published
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """
                insert_params = (
                    source_id,
                    position_name[:200],
                    vacancy_description[:5000],
                    company_name_raw[:200],
                    'linkedin',
                    datetime.now(),
                    Config.SYSTEM_USER_UUID,
                    False,
                )

            print(f"🔄 Попытка упрощённой вставки: {position_name[:50]}...")
            result = self.execute_query(insert_query, insert_params)

            if result:
                print(f"✅ Упрощённая вставка успешна: {position_name[:50]}...")
                return True
            else:
                print(f"❌ Упрощённая вставка не удалась: {position_name[:50]}...")
                return False

        except Exception as e:
            print(f"❌ Ошибка упрощённой вставки: {e}")
            return False
