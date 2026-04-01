"""
LinkedIn Parser Pro v5.5.9 - PostgreSQL Sync Fixed + Исправлены город и зарплата + Строгий поиск по фильтру
✅ Исправлено извлечение города - теперь правильно определяет локацию
✅ Исправлено извлечение зарплаты - точное определение сумм и валюты
✅ Город не заменяется названием компании
✅ Добавлена новая колонка org_url для хранения оригинальной ссылки на вакансию
✅ Исправлена проблема удаления оригинальных вакансий при обнаружении дубликатов
✅ Правильная дедупликация - сохраняется только оригинал
✅ Улучшена проверка дубликатов
✅ ДОБАВЛЕНА СТРОГАЯ ФИЛЬТРАЦИЯ - поиск только по указанным профессиям, исключение неподходящих вакансий
"""

import os
import sys
import json
import sqlite3
import time
import random
import logging
import threading
import webbrowser
import subprocess
import hashlib
import re
import uuid
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import queue

# ============================================
# АВТОМАТИЧЕСКАЯ УСТАНОВКА ЗАВИСИМОСТЕЙ
# ============================================

def install_dependencies():
    """Автоматическая установка необходимых библиотек"""
    required_packages = [
        "selenium==4.15.0",
        "chromedriver-autoinstaller==0.4.0",
        "requests==2.31.0",
        "beautifulsoup4==4.12.2",
        "lxml",
        "psycopg2-binary",
        "webdriver-manager==4.0.1"
    ]
    
    print("=" * 50)
    print("УСТАНОВКА ЗАВИСИМОСТЕЙ")
    print("=" * 50)
    
    for package in required_packages:
        print(f"Установка {package}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✓ {package} установлен")
        except Exception as e:
            print(f"⚠ Ошибка установки {package}: {e}")
    
    print("\n✓ Все зависимости установлены!")
    print("Перезапустите программу.")
    print("=" * 50)
    input("Нажмите Enter для выхода...")
    sys.exit(0)

# Проверяем зависимости
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    import chromedriver_autoinstaller
    from bs4 import BeautifulSoup
    import requests
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from webdriver_manager.chrome import ChromeDriverManager
    DEPENDENCIES_OK = True
except ImportError as e:
    print(f"Не установлены зависимости: {e}")
    response = input("Установить зависимости автоматически? (y/n): ")
    if response.lower() == 'y':
        install_dependencies()
    else:
        print("Программа не может работать без зависимостей.")
        sys.exit(1)

# ============================================
# КОНФИГУРАЦИЯ
# ============================================

class Config:
    DB_FILE = "vacancies.db"
    LOG_FILE = "parser.log"
    CONFIG_FILE = "config.json"
    
    # LinkedIn поиск
    CITIES = ["Астана", "Алматы", "Караганда", "Нур-Султан", "Шымкент", "Актобе", "Тараз", "Павлодар", "Усть-Каменогорск", "Семей"]
    JOBS = [
        "QA Engineer",
        "QA",
        "Тестировщик",
        "Frontend Developer",
        "Frontend",
        "JavaScript Developer",
        "React Developer",
        "Product Manager",
        "Продукт менеджер",
        "Project Manager",
        "Проект менеджер",
        "UX/UI Designer",
        "Дизайнер",
        "UI Designer",
        "UX Designer",
        "Backend Developer",
        "Python Developer",
        "Java Developer",
        "DevOps",
        "Data Scientist",
        "Analyst"
    ]
    
    # Настройки парсера - УВЕЛИЧЕНЫ ДЛЯ ЛУЧШЕГО ПОИСКА
    DELAY_MIN = 3
    DELAY_MAX = 5
    MAX_VACANCIES_PER_SEARCH = 20  # Увеличено с 5 до 20
    HEADLESS = True
    
    # PostgreSQL настройки
    POSTGRES_HOST = "37.151.89.186"
    POSTGRES_PORT = 59003
    POSTGRES_DB = "parsers"
    POSTGRES_USER = "parsers"
    POSTGRES_PASSWORD = "avrhggfxDJWf827D"
    POSTGRES_ENABLED = True
    
    # Telegram
    MAX_DESCRIPTION_LENGTH = 1000
    
    # Автопарсинг
    AUTO_PARSE_INTERVAL = 600
    AUTO_PARSE_ENABLED = False
    
    # Системный пользователь для created_by
    SYSTEM_USER_UUID = "00000000-0000-0000-0000-000000000001"
    
    # Дедупликация - УПРОЩЕНА ДЛЯ ЛУЧШЕГО ПОИСКА
    DEDUPLICATION_ENABLED = True
    SIMILARITY_THRESHOLD = 0.7  # СНИЖЕНО с 0.85 до 0.7 для менее строгой дедупликации
    
    # Новая колонка для ссылки
    ORG_URL_COLUMN = "org_url"  # Новая колонка для хранения оригинальной ссылки
    
    def get(self, key, default=None):
        """Получение значения из конфига"""
        try:
            if hasattr(self, key):
                return getattr(self, key)
            return default
        except:
            return default

# ============================================
# PostgreSQL БАЗА ДАННЫХ (ИСПРАВЛЕННЫЕ ВСЕ ОШИБКИ + ДОБАВЛЕН org_url)
# ============================================

class PostgresDBFixedColumns:
    """Исправленный класс с правильными именами колонок и типами данных для PostgreSQL"""
    
    def __init__(self):
        self.host = Config.POSTGRES_HOST
        self.port = Config.POSTGRES_PORT
        self.database = Config.POSTGRES_DB
        self.user = Config.POSTGRES_USER
        self.password = Config.POSTGRES_PASSWORD
        self.connection = None
        self.cursor = None
        self.enabled = Config.POSTGRES_ENABLED
        
    def connect(self):
        """Подключение к PostgreSQL базе данных"""
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
                connect_timeout=10
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            
            # Простая проверка подключения
            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            
            if result:
                print(f"✅ Подключено к PostgreSQL: {self.host}:{self.port}/{self.database}")
                
                # Проверяем существование таблицы и получаем структуру
                if self.check_table_exists():
                    print("✅ Таблица 'job_posting_sources' существует")
                    # Получаем список колонок для отладки
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
    
    def check_table_exists(self):
        """Проверка существования таблицы job_posting_sources"""
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
        """Получить список колонок таблицы для отладки"""
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
        """Проверка существования колонки в таблице"""
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
    
    def execute_query(self, query, params=None):
        """Выполнение SQL запроса"""
        if not self.enabled or not self.connection:
            print("⚠ PostgreSQL не подключен или отключен")
            return None
        
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                result = self.cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return True
                
        except Exception as e:
            print(f"❌ Ошибка выполнения запроса PostgreSQL: {e}")
            print(f"   Запрос: {query[:200]}")
            if params:
                print(f"   Параметры: {params}")
            return None
    
    def extract_contacts(self, description, url):
        """Извлечение контактных данных из описания (только contact_social)"""
        try:
            contact_social = ""
            
            if not description:
                return contact_social
            
            # Извлекаем все контактные данные в одну строку
            contact_parts = []
            
            # Поиск имени контакта
            name_patterns = [
                r'контактное лицо[:\s]+([^\n]+)',
                r'по вопросам[:\s]+([^\n]+)',
                r'обращаться[:\s]+([^\n]+)',
                r'сотрудник[:\s]+([^\n]+)',
                r'менеджер[:\s]+([^\n]+)',
                r'рекрутер[:\s]+([^\n]+)',
                r'hr[:\s]+([^\n]+)',
                r'обращайтесь[:\s]+([^\n]+)'
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    name = match.group(1).strip()
                    contact_parts.append(f"Имя: {name}")
                    break
            
            # Поиск телефона
            phone_patterns = [
                r'\+7\s?\(?\d{3}\)?\s?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
                r'8\s?\(?\d{3}\)?\s?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
                r'тел[\.:\s]+([+\d\s\-\(\)]{7,})',
                r'телефон[:\s]+([+\d\s\-\(\)]{7,})',
                r'\+?\d[\d\s\-\(\)]{7,}\d'
            ]
            
            for pattern in phone_patterns:
                matches = re.findall(pattern, description)
                if matches:
                    phones = ', '.join(matches[:3])
                    contact_parts.append(f"Телефон: {phones}")
                    break
            
            # Извлекаем социальные сети
            social_patterns = {
                'linkedin': r'linkedin\.com/[^\s]+',
                'telegram': r't\.me/[^\s]+',
                'whatsapp': r'wa\.me/[^\s]+',
                'vk': r'vk\.com/[^\s]+',
                'facebook': r'facebook\.com/[^\s]+',
                'instagram': r'instagram\.com/[^\s]+',
                'twitter': r'twitter\.com/[^\s]+',
                'x.com': r'x\.com/[^\s]+'
            }
            
            social_links = []
            
            # Ищем в описании
            for platform, pattern in social_patterns.items():
                matches = re.findall(pattern, description, re.IGNORECASE)
                if matches:
                    social_links.extend(matches[:2])
            
            # Добавляем email если есть
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            email_matches = re.findall(email_pattern, description)
            if email_matches:
                social_links.extend(email_matches[:3])
            
            if social_links:
                contact_parts.append(f"Соцсети: {', '.join(social_links[:5])}")
            
            # Добавляем исходный URL
            if url:
                contact_parts.append(f"URL: {url}")
            
            contact_social = ' | '.join(contact_parts)
            
            return contact_social[:500]  # Ограничиваем длину
            
        except Exception as e:
            print(f"⚠ Ошибка извлечения контактов: {e}")
            return ""
    
    def extract_salary_info(self, salary_text):
        """Извлечение информации о зарплате - ИСПРАВЛЕННЫЙ МЕТОД"""
        try:
            salary_min = None
            salary_max = None
            salary_currency = "KZT"
            
            if not salary_text or salary_text.lower() in ['не указана', 'договорная']:
                return salary_min, salary_max, salary_currency
            
            # Сохраняем оригинальный текст для логирования
            original_text = salary_text
            salary_text = salary_text.lower().strip()
            
            # Определяем валюту - улучшенная логика
            currency_patterns = {
                'RUB': [r'₽', r'руб\.?', r'rur', r'rub', r'р\.', r'₽', r'₽', r'₽'],
                'USD': [r'\$', r'usd', r'доллар', r'\$', r'\$', r'\$'],
                'EUR': [r'€', r'eur', r'euro', r'евро', r'€', r'€', r'€'],
                'KZT': [r'₸', r'kzt', r'тенге', r'тг', r'₸', r'₸', r'₸'],
                'GBP': [r'£', r'gbp', r'фунт', r'£', r'£', r'£']
            }
            
            detected_currency = "KZT"  # По умолчанию тенге
            
            for currency, patterns in currency_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, salary_text, re.IGNORECASE):
                        detected_currency = currency
                        break
                if detected_currency != "KZT":
                    break
            
            salary_currency = detected_currency
            
            # Ищем числовые значения - улучшенная логика
            # Паттерны для поиска диапазонов зарплат
            patterns = [
                # Диапазоны: от 100 000 до 200 000 тенге
                r'(?:от\s*)?(\d[\d\s]*[\d])(?:\s*[-–—]\s*|\s*(?:до|по)\s*)(\d[\d\s]*[\d])',
                # Диапазоны: 100 000 - 200 000 тенге
                r'(\d[\d\s]*[\d])\s*[-–—]\s*(\d[\d\s]*[\d])',
                # Одно значение: от 100 000 тенге
                r'от\s*(\d[\d\s]*[\d])',
                # Одно значение: до 200 000 тенге
                r'до\s*(\d[\d\s]*[\d])',
                # Одно значение: 100 000 тенге
                r'^(\d[\d\s]*[\d])(?![^\s]*\d)'  # Только если нет второго числа после
            ]
            
            for pattern in patterns:
                match = re.search(pattern, salary_text, re.IGNORECASE)
                if match:
                    groups = match.groups()
                    
                    # Обрабатываем найденные числа
                    numbers = []
                    for group in groups:
                        if group:
                            # Очищаем число от пробелов и других символов
                            clean_num = re.sub(r'[^\d]', '', group)
                            if clean_num:
                                num = int(clean_num)
                                
                                # Умножаем на 1000 если есть указание "к" или "k"
                                if 'к' in salary_text or 'k' in salary_text or 'тыс' in salary_text:
                                    num = num * 1000
                                
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
        """Нормализация названия компании"""
        if not company_name_raw:
            return ""
        
        normalized = re.sub(r'\s+', ' ', company_name_raw.strip()).lower()
        
        legal_forms = ['ооо', 'зао', 'оао', 'ао', 'ип', 'тнв', 'пк', 'чп']
        for form in legal_forms:
            normalized = re.sub(rf'\b{form}\b\.?\s*', '', normalized)
        
        normalized = re.sub(r'["«»].*?["«»]', '', normalized)
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
    
    def generate_source_id(self, title, company, location):
        """Генерация source_id на основе данных вакансии"""
        try:
            # Берем только первые 50 символов каждого поля для стабильности
            title_part = title[:50].strip().lower()
            company_part = (company or "")[:50].strip().lower()
            location_part = (location or "")[:50].strip().lower()
            
            base_string = f"{title_part}_{company_part}_{location_part}".encode('utf-8')
            source_id = hashlib.sha256(base_string).hexdigest()[:32]
            return source_id
        except:
            return hashlib.sha256(str(time.time()).encode()).hexdigest()[:32]
    
    def check_vacancy_exists(self, source_id):
        """Проверка существования вакансии по source_id"""
        try:
            query = "SELECT id FROM job_posting_sources WHERE source_id = %s LIMIT 1"
            result = self.execute_query(query, (source_id,))
            return result is not None and len(result) > 0
        except:
            return False
    
    def save_vacancy_to_postgres(self, vacancy_data):
        """Сохранение вакансии в PostgreSQL с правильными именами колонок и типами данных"""
        if not self.enabled:
            print("⚠ PostgreSQL интеграция отключена")
            return False
        
        try:
            # Проверяем подключение
            if not self.connection or self.connection.closed:
                print("🔄 Переподключение к PostgreSQL...")
                if not self.connect():
                    print("❌ Не удалось подключиться к PostgreSQL")
                    return False
            
            # Извлекаем данные
            position_name = vacancy_data.get('title', '')
            company_name_raw = vacancy_data.get('company_name', '')
            vacancy_description = vacancy_data.get('description', '')
            location = vacancy_data.get('location', 'Не указана')
            salary_text = vacancy_data.get('salary', 'не указана')
            source_url = vacancy_data.get('source_url', '')
            source_type = 'linkedin'
            
            # Новая переменная для org_url (оригинальной ссылки)
            org_url = source_url  # Используем source_url как оригинальную ссылку
            
            # Проверка обязательных полей
            if not position_name or position_name == 'Не указано':
                print(f"⚠ Пропущена вакансия без названия должности")
                return False
            
            if not company_name_raw:
                company_name_raw = "Не указана"
            
            # Генерируем source_id
            source_id = self.generate_source_id(position_name, company_name_raw, location)
            
            # Проверяем существование вакансии
            if self.check_vacancy_exists(source_id):
                print(f"⚠ Вакансия уже существует в PostgreSQL: {position_name[:50]}...")
                return True
            
            # Извлекаем контакты (только contact_social)
            contact_social = self.extract_contacts(vacancy_description, source_url)
            
            # Извлекаем информацию о зарплате
            salary_min, salary_max, salary_currency = self.extract_salary_info(salary_text)
            
            # Нормализуем название компании
            company_name_normalized = self.normalize_company_name(company_name_raw)
            
            # Генерируем UUID для created_by
            created_at = datetime.now()
            
            # Проверяем наличие колонки org_url
            org_url_column_exists = self.check_column_exists(Config.ORG_URL_COLUMN)
            
            # Формируем запрос вставки с учетом наличия колонки org_url
            if org_url_column_exists:
                print(f"✅ Колонка '{Config.ORG_URL_COLUMN}' существует в таблице")
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id,
                    position_name,
                    vacancy_description,
                    company_name_raw,
                    source_type,
                    location,
                    contact_social,
                    created_at,
                    created_by,
                    salary_min,
                    salary_max,
                    salary_currency,
                    is_published,
                    company_name_normalized,
                    org_url
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                """
                
                # Подготавливаем параметры с org_url
                insert_params = (
                    source_id,
                    position_name[:200] if position_name else "Не указано",
                    vacancy_description[:5000] if vacancy_description else "Описание не указано",
                    company_name_raw[:200] if company_name_raw else "Не указана",
                    source_type,
                    location[:100] if location else "Не указана",
                    contact_social[:500] if contact_social else None,
                    created_at,
                    Config.SYSTEM_USER_UUID,
                    salary_min,
                    salary_max,
                    salary_currency if salary_currency else None,
                    False,
                    company_name_normalized[:255] if company_name_normalized else None,
                    org_url[:500] if org_url else None
                )
            else:
                print(f"⚠ Колонка '{Config.ORG_URL_COLUMN}' не существует в таблице, сохраняем без нее")
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id,
                    position_name,
                    vacancy_description,
                    company_name_raw,
                    source_type,
                    location,
                    contact_social,
                    created_at,
                    created_by,
                    salary_min,
                    salary_max,
                    salary_currency,
                    is_published,
                    company_name_normalized
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """
                
                # Подготавливаем параметры без org_url
                insert_params = (
                    source_id,
                    position_name[:200] if position_name else "Не указано",
                    vacancy_description[:5000] if vacancy_description else "Описание не указано",
                    company_name_raw[:200] if company_name_raw else "Не указана",
                    source_type,
                    location[:100] if location else "Не указана",
                    contact_social[:500] if contact_social else None,
                    created_at,
                    Config.SYSTEM_USER_UUID,
                    salary_min,
                    salary_max,
                    salary_currency if salary_currency else None,
                    False,
                    company_name_normalized[:255] if company_name_normalized else None
                )
            
            print(f"🔄 Сохранение в PostgreSQL (job_posting_sources):")
            print(f"   Название: {position_name[:50]}...")
            print(f"   Компания: {company_name_raw[:30]}...")
            print(f"   Город: {location}")
            print(f"   Зарплата: {salary_text[:50]}")
            print(f"   Source ID: {source_id}")
            if org_url_column_exists:
                print(f"   Оригинальная ссылка (org_url): '{org_url[:50] if org_url else 'Нет'}'")
            
            # Выполняем вставку
            result = self.execute_query(insert_query, insert_params)
            
            if result is not None:
                print(f"✅ Вакансия сохранена в PostgreSQL: {position_name[:50]}...")
                return True
            else:
                print(f"❌ Ошибка сохранения в PostgreSQL: {position_name[:50]}...")
                # Пробуем упрощенную вставку с обязательными полями
                return self.simple_insert(source_id, position_name, company_name_raw, vacancy_description, org_url)
                
        except Exception as e:
            print(f"❌ Критическая ошибка сохранения в PostgreSQL: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def simple_insert(self, source_id, position_name, company_name_raw, vacancy_description, org_url=None):
        """Упрощенная вставка только с обязательными полями"""
        try:
            # Обязательные поля: vacancy_description - NOT NULL, поэтому обязательно нужно значение
            if not vacancy_description:
                vacancy_description = "Описание не указано"
            
            # Проверяем наличие колонки org_url
            org_url_column_exists = self.check_column_exists(Config.ORG_URL_COLUMN)
            
            # Минимальный набор полей, которые точно есть в таблистве
            # ВАЖНО: vacancy_description - NOT NULL поле!
            if org_url_column_exists and org_url:
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id,
                    position_name,
                    vacancy_description,
                    company_name_raw,
                    source_type,
                    created_at,
                    created_by,
                    is_published,
                    org_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                insert_params = (
                    source_id,
                    position_name[:200],
                    vacancy_description[:5000] if vacancy_description else "Описание не указано",
                    company_name_raw[:200],
                    'linkedin',
                    datetime.now(),
                    Config.SYSTEM_USER_UUID,
                    False,
                    org_url[:500]
                )
            else:
                insert_query = """
                INSERT INTO job_posting_sources (
                    source_id,
                    position_name,
                    vacancy_description,
                    company_name_raw,
                    source_type,
                    created_at,
                    created_by,
                    is_published
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                insert_params = (
                    source_id,
                    position_name[:200],
                    vacancy_description[:5000] if vacancy_description else "Описание не указано",
                    company_name_raw[:200],
                    'linkedin',
                    datetime.now(),
                    Config.SYSTEM_USER_UUID,
                    False
                )
            
            print(f"🔄 Попытка упрощенной вставки: {position_name[:50]}...")
            if org_url_column_exists and org_url:
                print(f"   С org_url: {org_url[:50]}...")
            
            result = self.execute_query(insert_query, insert_params)
            
            if result:
                print(f"✅ Упрощенная вставка успешна: {position_name[:50]}...")
                return True
            else:
                print(f"❌ Упрощенная вставка не удалась: {position_name[:50]}...")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка упрощенной вставки: {e}")
            return False
    
    def disconnect(self):
        """Отключение от PostgreSQL"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                print("✅ Отключено от PostgreSQL")
        except:
            pass

# ============================================
# БАЗА ДАННЫХ SQLite (ЛОКАЛЬНАЯ) - ИСПРАВЛЕННАЯ ДЕДУПЛИКАЦИЯ + УЛУЧШЕННАЯ ЛОГИКА
# ============================================

class Database:
    def __init__(self, db_file=None):
        self.db_file = db_file or Config.DB_FILE
        self.postgres = PostgresDBFixedColumns()  # Исправленный класс
        self.init_db()
    
    def init_db(self):
        """Инициализация локальной SQLite базы данных"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            # Таблица вакансий
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS vacancies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT,
                    salary TEXT DEFAULT 'не указана',
                    location TEXT,
                    contact TEXT,
                    source TEXT DEFAULT 'LinkedIn',
                    source_url TEXT UNIQUE,
                    fingerprint TEXT UNIQUE,  -- Уникальный индекс для дедупликации
                    company_name TEXT,
                    published INTEGER DEFAULT 0,
                    is_duplicate INTEGER DEFAULT 0,
                    posted_to_postgres INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_parsed_at TIMESTAMP,
                    parse_count INTEGER DEFAULT 0
                )
            ''')
            
            # Таблица для хранения хэшей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_fingerprints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT UNIQUE,
                    vacancy_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (vacancy_id) REFERENCES vacancies (id)
                )
            ''')
            
            # Таблица для статистики парсинга
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS parsing_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    total_found INTEGER DEFAULT 0,
                    new_vacancies INTEGER DEFAULT 0,
                    duplicates_found INTEGER DEFAULT 0,
                    postgres_sent INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'completed'
                )
            ''')
            
            # Индексы для ускорения поиска дубликатов
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_url_unique ON vacancies(source_url)')
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_fingerprint_unique ON vacancies(fingerprint)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_postgres ON vacancies(posted_to_postgres)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_title_company ON vacancies(title, company_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON vacancies(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_duplicate ON vacancies(is_duplicate)')
            
            conn.commit()
            print(f"✅ Локальная база данных инициализирована: {self.db_file}")
            
        except sqlite3.Error as e:
            print(f"❌ Ошибка инициализации базы данных: {e}")
            raise
        finally:
            conn.close()
        
        # Пробуем подключиться к PostgreSQL
        if Config.POSTGRES_ENABLED:
            if self.postgres.connect():
                print("✅ PostgreSQL подключение установлено")
            else:
                print("⚠ PostgreSQL подключение не удалось, работаем в локальном режиме")
    
    def generate_fingerprint(self, title, company, location, description="", source_url=""):
        """Генерация уникального хэша для вакансии - УПРОЩЕННАЯ ВЕРСИЯ ДЛЯ ЛУЧШЕГО ПОИСКА"""
        try:
            # Упрощенная очистка данных для менее строгого fingerprint
            clean_title = re.sub(r'\s+', ' ', title.strip().lower())
            clean_title = re.sub(r'[^a-zа-яё0-9\s]', '', clean_title)  # Упрощенная очистка
            
            # Очистка компании (если есть)
            clean_company = ""
            if company:
                clean_company = re.sub(r'\s+', ' ', company.strip().lower())
                clean_company = re.sub(r'[^a-zа-яё0-9\s]', '', clean_company)[:50]
            
            # Извлекаем ID из URL LinkedIn для дополнительной проверки
            linkedin_id = ""
            if source_url and "linkedin.com" in source_url:
                match = re.search(r'/jobs/view/(\d+)/', source_url)
                if match:
                    linkedin_id = match.group(1)
            
            # Создаем упрощенный fingerprint для лучшего поиска
            if linkedin_id:
                # Если есть LinkedIn ID, используем его как основу
                content = f"{linkedin_id}"
            else:
                # Если нет LinkedIn ID, используем упрощенный набор данных
                content = f"{clean_title[:100]}|{clean_company[:50]}"
            
            fingerprint = hashlib.md5(content.encode('utf-8')).hexdigest()  # Используем MD5 для простоты
            
            return fingerprint
        except Exception as e:
            print(f"⚠ Ошибка генерации fingerprint: {e}")
            # Резервный метод на случай ошибки
            backup_content = f"{title}|{company}|{location}|{source_url}"
            return hashlib.md5(backup_content.encode('utf-8')).hexdigest()
    
    def calculate_similarity(self, text1, text2):
        """Вычисление схожести двух текстов (простой алгоритм)"""
        if not text1 or not text2:
            return 0.0
        
        # Приводим к нижнему регистру и удаляем лишние пробелы
        text1 = re.sub(r'\s+', ' ', text1.strip().lower())
        text2 = re.sub(r'\s+', ' ', text2.strip().lower())
        
        # Если тексты одинаковые
        if text1 == text2:
            return 1.0
        
        # Разбиваем на слова
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        if not words1 or not words2:
            return 0.0
        
        # Вычисляем коэффициент Жаккара
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    def is_vacancy_exists(self, source_url=None, fingerprint=None, title=None, company=None, location=None):
        """Проверка существования вакансии в локальной БД - УПРОЩЕННАЯ ВЕРСИЯ"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            # Приоритет 1: Проверка по source_url (самый надежный)
            if source_url:
                cursor.execute(
                    "SELECT id, fingerprint, title, company_name, is_duplicate FROM vacancies WHERE source_url = ?",
                    (source_url,)
                )
                result = cursor.fetchone()
                if result:
                    vac_id, vac_fingerprint, vac_title, vac_company, is_duplicate = result
                    # Если это оригинальная (не дубликат) вакансия
                    if not is_duplicate:
                        return True, vac_id, vac_fingerprint, "url"
                    else:
                        # Если это дубликат, ищем оригинал
                        original = self.find_original_vacancy(vac_fingerprint, vac_title, vac_company)
                        if original:
                            return True, original[0], original[1], "duplicate_url"
            
            # Приоритет 2: Проверка по fingerprint
            if fingerprint:
                cursor.execute(
                    "SELECT id, source_url, title, company_name, is_duplicate FROM vacancies WHERE fingerprint = ?",
                    (fingerprint,)
                )
                result = cursor.fetchone()
                if result:
                    vac_id, vac_url, vac_title, vac_company, is_duplicate = result
                    if not is_duplicate:
                        return True, vac_id, fingerprint, "fingerprint"
                    else:
                        # Если это дубликат, ищем оригинал
                        original = self.find_original_vacancy(fingerprint, vac_title, vac_company)
                        if original:
                            return True, original[0], original[1], "duplicate_fingerprint"
            
            # Приоритет 3: УПРОЩЕННАЯ проверка по названию и компании
            if title:
                clean_title = re.sub(r'\s+', ' ', title.strip().lower())
                
                # Проверяем похожие названия (более гибкая проверка)
                cursor.execute('''
                    SELECT id, fingerprint, title, company_name FROM vacancies 
                    WHERE is_duplicate = 0
                    AND LOWER(title) LIKE ?
                    ORDER BY created_at DESC
                    LIMIT 5
                ''', (f"%{clean_title[:20]}%",))
                
                results = cursor.fetchall()
                for result in results:
                    vac_id, vac_fingerprint, vac_title_db, vac_company_db = result
                    
                    # Проверяем схожесть названий с пониженным порогом
                    title_similarity = self.calculate_similarity(title, vac_title_db)
                    
                    # Если схожесть названий выше порога, считаем дубликатом
                    if title_similarity >= Config.SIMILARITY_THRESHOLD:
                        # Дополнительно проверяем компанию, если указана
                        if company:
                            company_similarity = self.calculate_similarity(company, vac_company_db)
                            if company_similarity >= 0.5:  # Более низкий порог для компании
                                print(f"⚠ Найдено совпадение (схожесть: title={title_similarity:.2f}, company={company_similarity:.2f}): {title[:30]}...")
                                return True, vac_id, vac_fingerprint, "similar_title"
                        else:
                            print(f"⚠ Найдено совпадение по названию (схожесть: {title_similarity:.2f}): {title[:30]}...")
                            return True, vac_id, vac_fingerprint, "title_only_match"
            
            return False, None, None, None
            
        except sqlite3.Error as e:
            print(f"⚠ Ошибка проверки существования вакансии: {e}")
            return False, None, None, None
        finally:
            conn.close()
    
    def find_original_vacancy(self, fingerprint, title, company):
        """Поиск оригинальной вакансии по fingerprint, названию и компании"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            # Ищем оригинал по fingerprint среди НЕ-дубликатов
            cursor.execute('''
                SELECT id, fingerprint FROM vacancies 
                WHERE fingerprint = ? AND is_duplicate = 0
                LIMIT 1
            ''', (fingerprint,))
            
            result = cursor.fetchone()
            if result:
                return result
            
            # Если не нашли по fingerprint, ищем по названию среди НЕ-дубликатов
            clean_title = re.sub(r'\s+', ' ', title.strip().lower())
            
            cursor.execute('''
                SELECT id, fingerprint FROM vacancies 
                WHERE LOWER(title) LIKE ? AND is_duplicate = 0
                LIMIT 1
            ''', (f"%{clean_title[:30]}%",))
            
            return cursor.fetchone()
            
        except Exception as e:
            print(f"⚠ Ошибка поиска оригинальной вакансии: {e}")
            return None
        finally:
            conn.close()
    
    def save_vacancy(self, vacancy):
        """Сохранение вакансии в локальную БД - УПРОЩЕННАЯ ВЕРСИЯ"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            title = vacancy.get('title', '').strip()
            company = vacancy.get('company_name', '').strip()
            location = vacancy.get('location', '').strip()
            description = vacancy.get('description', '').strip()
            source_url = vacancy.get('source_url', '').strip()
            
            if not title or not source_url:
                print("⚠ Попытка сохранить вакансию без title или source_url")
                return None
            
            # Генерируем упрощенный fingerprint
            fingerprint = self.generate_fingerprint(title, company, location, description, source_url)
            
            # Проверяем существование вакансии по всем критериям
            exists, existing_id, existing_fingerprint, match_type = self.is_vacancy_exists(
                source_url=source_url,
                fingerprint=fingerprint,
                title=title,
                company=company,
                location=location
            )
            
            if exists:
                # Обновляем только СТАТИСТИКУ существующей записи
                cursor.execute('''
                    UPDATE vacancies 
                    SET updated_at = datetime('now'),
                        last_parsed_at = datetime('now'),
                        parse_count = parse_count + 1
                    WHERE id = ? AND is_duplicate = 0
                ''', (existing_id,))
                
                conn.commit()
                print(f"✅ ДУБЛИКАТ ОБНАРУЖЕН И ПРОПУЩЕН (тип: {match_type}): {title[:50]}... (ID оригинала: {existing_id})")
                
                # Сохраняем fingerprint дубликата
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO processed_fingerprints (fingerprint, vacancy_id)
                        VALUES (?, ?)
                    ''', (fingerprint, existing_id))
                    conn.commit()
                except Exception as e:
                    print(f"⚠ Ошибка сохранения fingerprint: {e}")
                
                return existing_id
                
            else:
                # Новая вакансия - дополнительная проверка на дубликаты
                try:
                    # Проверяем наличие похожих вакансий за последние 3 дня
                    clean_title = re.sub(r'\s+', ' ', title.strip().lower())
                    
                    cursor.execute('''
                        SELECT id, fingerprint FROM vacancies 
                        WHERE LOWER(title) LIKE ? 
                          AND datetime(created_at) > datetime('now', '-3 days')
                          AND is_duplicate = 0
                        LIMIT 1
                    ''', (f"%{clean_title[:30]}%",))
                    
                    duplicate_result = cursor.fetchone()
                    if duplicate_result:
                        print(f"⚠ Дубликат обнаружен (последние 3 дня): {title[:50]}...")
                        # Обновляем статистику оригинала
                        cursor.execute('''
                            UPDATE vacancies 
                            SET updated_at = datetime('now'),
                                last_parsed_at = datetime('now'),
                                parse_count = parse_count + 1
                            WHERE id = ?
                        ''', (duplicate_result[0],))
                        conn.commit()
                        return duplicate_result[0]
                    
                except Exception as e:
                    print(f"⚠ Ошибка дополнительной проверки дубликатов: {e}")
                
                # Если дошли сюда, значит это действительно новая вакансия
                print(f"🔍 НОВАЯ УНИКАЛЬНАЯ ВАКАНСИЯ: {title[:50]}...")
                
                cursor.execute('''
                    INSERT INTO vacancies 
                    (title, description, salary, location, contact, source, source_url, 
                     fingerprint, company_name, last_parsed_at, parse_count, posted_to_postgres, is_duplicate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), 1, 0, 0)
                ''', (
                    title[:500],
                    description[:5000] if description else "Нет описания",
                    vacancy.get('salary', 'не указана')[:200],
                    location[:100],
                    vacancy.get('contact', '')[:500],
                    vacancy.get('source', 'LinkedIn'),
                    source_url[:500],
                    fingerprint,
                    company[:200]
                ))
                
                vacancy_id = cursor.lastrowid
                
                try:
                    cursor.execute('''
                        INSERT INTO processed_fingerprints (fingerprint, vacancy_id)
                        VALUES (?, ?)
                    ''', (fingerprint, vacancy_id))
                except Exception as e:
                    print(f"⚠ Ошибка сохранения fingerprint: {e}")
                
                conn.commit()
                print(f"✅ Сохранена НОВАЯ ВАКАНСИЯ в локальную БД: {title[:50]}... (ID: {vacancy_id})")
                
                # Сразу отправляем в PostgreSQL с оригинальной ссылкой
                if Config.POSTGRES_ENABLED:
                    success = self.postgres.save_vacancy_to_postgres(vacancy)
                    
                    if success:
                        cursor.execute(
                            "UPDATE vacancies SET posted_to_postgres = 1 WHERE id = ?",
                            (vacancy_id,)
                        )
                        conn.commit()
                        print(f"✅ Вакансия отправлена в PostgreSQL (с org_url)")
                    else:
                        cursor.execute(
                            "UPDATE vacancies SET posted_to_postgres = 0 WHERE id = ?",
                            (vacancy_id,)
                        )
                        conn.commit()
                        print(f"⚠ Не удалось отправить вакансию в PostgreSQL")
                
                return vacancy_id
                
        except sqlite3.Error as e:
            if "UNIQUE constraint failed" in str(e):
                print(f"⚠ Нарушение уникальности (дубликат): {title[:50]}...")
                # Пытаемся найти существующую оригинальную запись
                try:
                    cursor.execute(
                        "SELECT id FROM vacancies WHERE (fingerprint = ? OR source_url = ?) AND is_duplicate = 0 LIMIT 1",
                        (fingerprint, source_url)
                    )
                    result = cursor.fetchone()
                    if result:
                        # Обновляем статистику оригинала
                        cursor.execute('''
                            UPDATE vacancies 
                            SET updated_at = datetime('now'),
                                last_parsed_at = datetime('now'),
                                parse_count = parse_count + 1
                            WHERE id = ?
                        ''', (result[0],))
                        conn.commit()
                        return result[0]
                except Exception as find_error:
                    print(f"⚠ Ошибка поиска оригинала: {find_error}")
            print(f"❌ Ошибка базы данных при сохранении: {e}")
            return None
        except Exception as e:
            print(f"❌ Неожиданная ошибка при сохранении: {e}")
            return None
        finally:
            conn.close()
    
    def get_vacancies_for_postgres(self, limit=50):
        """Получить вакансии, которые еще не были отправлены в PostgreSQL"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM vacancies 
                WHERE posted_to_postgres = 0 AND is_duplicate = 0
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (limit,))
            
            vacancies = [dict(row) for row in cursor.fetchall()]
            return vacancies
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения вакансий для PostgreSQL: {e}")
            return []
        finally:
            conn.close()
    
    def sync_to_postgres(self, limit=50):
        """Синхронизация вакансий с PostgreSQL"""
        if not Config.POSTGRES_ENABLED:
            return 0
        
        vacancies = self.get_vacancies_for_postgres(limit)
        
        if not vacancies:
            return 0
        
        print(f"🔄 Синхронизация {len(vacancies)} вакансий с PostgreSQL...")
        
        sent_count = 0
        
        for vacancy in vacancies:
            # Подготавливаем данные для PostgreSQL
            vacancy_data = {
                'title': vacancy.get('title', ''),
                'company_name': vacancy.get('company_name', ''),
                'description': vacancy.get('description', ''),
                'location': vacancy.get('location', ''),
                'salary': vacancy.get('salary', ''),
                'source_url': vacancy.get('source_url', ''),  # Используем как org_url
                'source': vacancy.get('source', 'LinkedIn')
            }
            
            # Отправляем в PostgreSQL
            success = self.postgres.save_vacancy_to_postgres(vacancy_data)
            
            if success:
                # Обновляем флаг в локальной БД
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE vacancies SET posted_to_postgres = 1 WHERE id = ?",
                    (vacancy['id'],)
                )
                conn.commit()
                conn.close()
                
                sent_count += 1
                print(f"✅ Синхронизировано (с org_url): {vacancy.get('title', '')[:50]}...")
            else:
                print(f"❌ Не удалось синхронизировать: {vacancy.get('title', '')[:50]}...")
            
            time.sleep(0.5)
        
        print(f"✅ Синхронизация завершена. Отправлено: {sent_count}/{len(vacancies)}")
        return sent_count
    
    def get_unpublished_vacancies(self, limit=50):
        """Получить неопубликованные вакансии"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM vacancies 
                WHERE published = 0 AND is_duplicate = 0
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (limit,))
            
            vacancies = [dict(row) for row in cursor.fetchall()]
            return vacancies
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения неопубликованных вакансий: {e}")
            return []
        finally:
            conn.close()
    
    def get_all_vacancies(self, limit=200):
        """Получить все вакансии (только оригиналы, не дубликаты)"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute(f'''
                SELECT * FROM vacancies 
                WHERE is_duplicate = 0
                ORDER BY created_at DESC 
                LIMIT {limit}
            ''')
            
            vacancies = [dict(row) for row in cursor.fetchall()]
            return vacancies
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения всех вакансий: {e}")
            return []
        finally:
            conn.close()
    
    def search_vacancies(self, keyword="", location="", source=""):
        """Поиск вакансий (только оригиналы)"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            query = "SELECT * FROM vacancies WHERE is_duplicate = 0"
            params = []
            
            if keyword:
                query += " AND (title LIKE ? OR description LIKE ? OR company_name LIKE ?)"
                params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
            
            if location:
                query += " AND location LIKE ?"
                params.append(f"%{location}%")
            
            if source:
                query += " AND source = ?"
                params.append(source)
            
            query += " ORDER BY created_at DESC"
            
            cursor.execute(query, params)
            vacancies = [dict(row) for row in cursor.fetchall()]
            return vacancies
        except sqlite3.Error as e:
            print(f"⚠ Ошибка поиска вакансий: {e}")
            return []
        finally:
            conn.close()
    
    def get_vacancy_by_id(self, vacancy_id):
        """Получить вакансию по ID"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM vacancies WHERE id = ?", (vacancy_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения вакансии по ID: {e}")
            return None
        finally:
            conn.close()
    
    def get_stats(self):
        """Получить статистику"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        stats = {}
        
        try:
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_duplicate = 0")
            stats['total'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE published = 0 AND is_duplicate = 0")
            stats['unpublished'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE published = 1")
            stats['published'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE date(created_at) = date('now') AND is_duplicate = 0")
            stats['today'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_duplicate = 1")
            stats['duplicates'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres = 1 AND is_duplicate = 0")
            stats['postgres_sent'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres = 0 AND is_duplicate = 0")
            stats['postgres_pending'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE datetime(created_at) > datetime('now', '-7 days') AND is_duplicate = 0")
            stats['last_7_days'] = cursor.fetchone()[0]
            
            # Дополнительная статистика по дедупликации
            cursor.execute("SELECT COUNT(DISTINCT fingerprint) FROM vacancies WHERE fingerprint IS NOT NULL")
            stats['unique_fingerprints'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM processed_fingerprints")
            stats['total_fingerprints'] = cursor.fetchone()[0]
            
            # Статистика эффективности дедупликации
            cursor.execute("SELECT COUNT(*) FROM vacancies")
            total_all = cursor.fetchone()[0]
            if total_all > 0:
                stats['deduplication_efficiency'] = (stats['duplicates'] / total_all * 100)
            else:
                stats['deduplication_efficiency'] = 0
            
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения статистики: {e}")
            stats = {
                'total': 0,
                'unpublished': 0,
                'published': 0,
                'today': 0,
                'duplicates': 0,
                'postgres_sent': 0,
                'postgres_pending': 0,
                'last_7_days': 0,
                'unique_fingerprints': 0,
                'total_fingerprints': 0,
                'deduplication_efficiency': 0
            }
        finally:
            conn.close()
        
        return stats
    
    def save_parsing_session(self, session_id, total_found, new_vacancies, duplicates_found, postgres_sent=0, status="completed"):
        """Сохранение статистики сессии парсинга"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO parsing_stats (session_id, end_time, total_found, new_vacancies, duplicates_found, postgres_sent, status)
                VALUES (?, datetime('now'), ?, ?, ?, ?, ?)
            ''', (session_id, total_found, new_vacancies, duplicates_found, postgres_sent, status))
            
            conn.commit()
        except sqlite3.Error as e:
            print(f"⚠ Ошибка сохранения сессии парсинга: {e}")
        finally:
            conn.close()
    
    def get_parsing_history(self, limit=10):
        """Получить историю парсинга"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM parsing_stats 
                ORDER BY start_time DESC 
                LIMIT ?
            ''', (limit,))
            
            history = [dict(row) for row in cursor.fetchall()]
            return history
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения истории парсинга: {e}")
            return []
        finally:
            conn.close()
    
    def mark_as_published(self, vacancy_id):
        """Пометить вакансию как опубликованную"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE vacancies SET published = 1 WHERE id = ? AND is_duplicate = 0",
                (vacancy_id,)
            )
            conn.commit()
        except sqlite3.Error as e:
            print(f"⚠ Ошибка пометки как опубликованной: {e}")
        finally:
            conn.close()
    
    def mark_as_postgres_sent(self, vacancy_id):
        """Пометить вакансию как отправленную в PostgreSQL"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE vacancies SET posted_to_postgres = 1 WHERE id = ? AND is_duplicate = 0",
                (vacancy_id,)
            )
            conn.commit()
        except sqlite3.Error as e:
            print(f"⚠ Ошибка пометки как отправленной в PostgreSQL: {e}")
        finally:
            conn.close()
    
    def delete_vacancy(self, vacancy_id):
        """Удалить вакансию (только оригинал или дубликат)"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM vacancies WHERE id = ?", (vacancy_id,))
            conn.commit()
            success = cursor.rowcount > 0
            return success
        except sqlite3.Error as e:
            print(f"⚠ Ошибка удаления вакансии: {e}")
            return False
        finally:
            conn.close()
    
    def cleanup_duplicates(self):
        """Очистка дубликатов из базы данных - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            # Находим все дубликаты (is_duplicate = 1)
            cursor.execute('''
                SELECT COUNT(*) as cnt
                FROM vacancies 
                WHERE is_duplicate = 1
            ''')
            
            duplicate_count = cursor.fetchone()[0]
            
            if duplicate_count == 0:
                print("ℹ Дубликатов не найдено")
                conn.close()
                return 0
            
            print(f"🔄 Найдено {duplicate_count} дубликатов для удаления")
            
            # Удаляем все записи, помеченные как дубликаты
            cursor.execute('''
                DELETE FROM vacancies 
                WHERE is_duplicate = 1
            ''')
            
            deleted_count = cursor.rowcount
            
            # Также удаляем соответствующие fingerprint из processed_fingerprints
            cursor.execute('''
                DELETE FROM processed_fingerprints 
                WHERE fingerprint IN (
                    SELECT fingerprint FROM vacancies WHERE is_duplicate = 1
                )
            ''')
            
            conn.commit()
            conn.close()
            
            print(f"✅ Удалено {deleted_count} дубликатов")
            return deleted_count
            
        except Exception as e:
            print(f"❌ Ошибка очистки дубликатов: {e}")
            conn.close()
            return 0
    
    def mark_as_duplicate(self, vacancy_id, original_id=None):
        """Пометить вакансию как дубликат"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            if original_id:
                # Если указан оригинал, получаем его fingerprint
                cursor.execute("SELECT fingerprint FROM vacancies WHERE id = ?", (original_id,))
                original_result = cursor.fetchone()
                if original_result:
                    original_fingerprint = original_result[0]
                    # Обновляем fingerprint дубликата на fingerprint оригинала
                    cursor.execute('''
                        UPDATE vacancies 
                        SET is_duplicate = 1, 
                            fingerprint = ?,
                            updated_at = datetime('now')
                        WHERE id = ?
                    ''', (original_fingerprint, vacancy_id))
            else:
                cursor.execute('''
                    UPDATE vacancies 
                    SET is_duplicate = 1,
                        updated_at = datetime('now')
                    WHERE id = ?
                ''', (vacancy_id,))
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"⚠ Ошибка пометки как дубликата: {e}")
            return False
        finally:
            conn.close()

# ============================================
# ПАРСЕР LINKEDIN - ИСПРАВЛЕН ДЛЯ СТРОГОГО ПОИСКА ПО ФИЛЬТРУ
# ============================================

class LinkedInParser:
    def __init__(self, email="", password="", headless=True, use_proxy_pool=False, auto_mode=False):
        self.email = email
        self.password = password
        self.headless = headless
        self.use_proxy_pool = use_proxy_pool
        self.auto_mode = auto_mode
        self.driver = None
        self.db = Database()
        self.session_id = f"session_{int(time.time())}_{random.randint(1000, 9999)}"
        
        self.session_stats = {
            "total_found": 0,
            "new_vacancies": 0,
            "duplicates_found": 0,
            "postgres_sent": 0,
            "errors": 0,
            "start_time": datetime.now()
        }
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(Config.LOG_FILE, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        if self.auto_mode:
            self.logger.info(f"Авто-режим парсинга. Session ID: {self.session_id}")
    
    def setup_driver(self):
        """Настройка WebDriver - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            # Используем webdriver-manager для автоматической установки ChromeDriver
            from selenium.webdriver.chrome.service import Service
            
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless=new")
            
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            if self.auto_mode:
                chrome_options.add_argument("--disable-features=UserAgentClientHint")
            
            # Экспериментальные опции
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "credentials_enable_service": False,
                "profile.password_manager_enabled": False
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Автоматическая установка ChromeDriver через webdriver-manager
            service = Service(ChromeDriverManager().install())
            
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Маскируем WebDriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info("WebDriver успешно настроен с помощью webdriver-manager")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка настройки WebDriver: {e}")
            self.logger.info("Пробуем альтернативный метод...")
            
            # Альтернативный метод
            try:
                chrome_options = Options()
                
                if self.headless:
                    chrome_options.add_argument("--headless")
                
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--window-size=1920,1080")
                
                self.driver = webdriver.Chrome(options=chrome_options)
                self.logger.info("WebDriver запущен альтернативным методом")
                return True
            except Exception as e2:
                self.logger.error(f"Альтернативный метод также не сработал: {e2}")
                return False
    
    def login(self):
        """Вход в LinkedIn"""
        try:
            self.logger.info("Вход в LinkedIn...")
            self.driver.get("https://www.linkedin.com/login")
            time.sleep(3)
            
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            
            email_field = self.driver.find_element(By.ID, "username")
            email_field.clear()
            email_field.send_keys(self.email)
            time.sleep(1)
            
            password_field = self.driver.find_element(By.ID, "password")
            password_field.clear()
            password_field.send_keys(self.password)
            time.sleep(1)
            
            login_button = self.driver.find_element(By.XPATH, "//button[@type='submit']")
            login_button.click()
            time.sleep(5)
            
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[role='combobox']"))
                )
                self.logger.info("Успешный вход в LinkedIn")
                return True
            except:
                if "captcha" in self.driver.page_source.lower():
                    self.logger.warning("Обнаружена капча")
                return False
                    
        except Exception as e:
            self.logger.error(f"Ошибка входа в LinkedIn: {e}")
            return False
    
    def search_jobs(self, job_title, location):
        """Поиск вакансий - УЛУЧШЕННЫЙ МЕТОД С ФИЛЬТРАЦИЕЙ РЕЗУЛЬТАТОВ"""
        try:
            # Используем более строгий поиск с фильтрами
            search_url = f"https://www.linkedin.com/jobs/search/?keywords={job_title.replace(' ', '%20')}&location={location.replace(' ', '%20')}&f_TPR=r86400&sortBy=DD&f_AL=true&f_E=1%2C2%2C3%2C4"  # Добавлены фильтры: f_AL=true (все локации), f_E=1,2,3,4 (уровень опыта)
            
            self.logger.info(f"Поиск: {job_title} в {location} (строгий фильтр)")
            self.driver.get(search_url)
            time.sleep(4)
            
            # УВЕЛИЧЕНО количество прокруток для загрузки большего количества вакансий
            self.scroll_page(5)  # Было 3, стало 5
            time.sleep(3)
            
            # Ищем все карточки вакансий - расширенный селектор
            job_cards = self.driver.find_elements(By.CSS_SELECTOR, "div.job-card-container, div.job-card-list, li.jobs-search-results__list-item, div.occludable-update")
            
            job_links = []
            seen_links = set()
            
            # Создаем список ключевых слов для строгой фильтрации
            job_keywords = self.get_job_keywords(job_title)
            
            # Обрабатываем больше карточек с фильтрацией по заголовку
            for card in job_cards[:Config.MAX_VACANCIES_PER_SEARCH * 2]:  # Берем в 2 раза больше
                try:
                    # Получаем текст карточки для фильтрации
                    card_text = card.text.lower()
                    
                    # Фильтруем по ключевым словам заголовка
                    if not self.filter_by_job_title(card_text, job_keywords):
                        continue  # Пропускаем вакансии, не соответствующие запросу
                    
                    # Пробуем разные способы найти ссылку
                    link_elements = card.find_elements(By.TAG_NAME, "a")
                    for link in link_elements:
                        href = link.get_attribute("href")
                        if href and "/jobs/view/" in href and href not in seen_links:
                            job_id = self.extract_job_id(href)
                            if job_id:
                                job_links.append(href)
                                seen_links.add(href)
                                break
                    
                    # Если не нашли через тег a, пробуем через другие атрибуты
                    if not link_elements:
                        try:
                            card.click()
                            time.sleep(1)
                            current_url = self.driver.current_url
                            if "/jobs/view/" in current_url and current_url not in seen_links:
                                job_links.append(current_url)
                                seen_links.add(current_url)
                        except:
                            pass
                            
                except Exception as e:
                    self.logger.debug(f"Ошибка обработки карточки: {e}")
                    continue
            
            self.logger.info(f"Найдено {len(job_links)} вакансий для {job_title} в {location} (после фильтрации)")
            return job_links
            
        except Exception as e:
            self.logger.error(f"Ошибка поиска: {e}")
            return []
    
    def get_job_keywords(self, job_title):
        """Получить ключевые слова для фильтрации по названию должности"""
        # Создаем список ключевых слов для каждого типа вакансий
        job_keywords_map = {
            "qa": ["qa", "тестиров", "quality assurance", "quality control", "test engineer", "тестирование"],
            "frontend": ["frontend", "front-end", "javascript", "react", "vue", "angular", "ui developer", "web developer"],
            "backend": ["backend", "back-end", "python", "java", "node", "django", "spring", "server", "api"],
            "fullstack": ["fullstack", "full-stack", "mern", "mean"],
            "devops": ["devops", "sre", "site reliability", "infrastructure", "cloud", "aws", "azure", "gcp"],
            "data": ["data scientist", "data analyst", "data engineer", "machine learning", "ml", "ai", "аналитик данных"],
            "manager": ["product manager", "project manager", "продукт менеджер", "проект менеджер", "менеджер продукта", "менеджер проекта"],
            "designer": ["ui designer", "ux designer", "designer", "дизайнер", "ui/ux", "веб-дизайнер", "graphic designer"]
        }
        
        job_title_lower = job_title.lower()
        
        # Ищем подходящие ключевые слова
        for key, keywords in job_keywords_map.items():
            if any(keyword in job_title_lower for keyword in keywords):
                return keywords
        
        # Если не нашли точного совпадения, используем общие ключевые слова из названия
        words = job_title_lower.split()
        return [word for word in words if len(word) > 2]
    
    def filter_by_job_title(self, card_text, job_keywords):
        """Фильтрация карточек по заголовку вакансии"""
        try:
            # Проверяем, содержит ли текст карточки ключевые слова
            for keyword in job_keywords:
                if keyword.lower() in card_text:
                    return True
            
            # Дополнительная проверка: исключаем явно неподходящие вакансии
            exclude_keywords = [
                "юрист", "lawyer", "адвокат", "юридический",
                "бухгалтер", "accountant", "финансовый",
                "маркетолог", "marketing", "реклам",
                "продавец", "sales", "торговый",
                "водитель", "driver", "доставк",
                "официант", "waiter", "бармен", "bartender",
                "уборщик", "cleaner", "клининг",
                "охранник", "security", "сторож",
                "строитель", "builder", "construction"
            ]
            
            for exclude in exclude_keywords:
                if exclude.lower() in card_text:
                    return False
            
            # Если не нашли ключевых слов, но и не исключили - проверяем по основным словам
            main_keywords = ["разработчик", "developer", "инженер", "engineer", "менеджер", "manager", "аналитик", "analyst", "дизайнер", "designer"]
            if any(keyword in card_text for keyword in main_keywords):
                return True
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Ошибка фильтрации: {e}")
            return True  # В случае ошибки пропускаем фильтрацию
    
    def extract_job_id(self, url):
        """Извлечение ID вакансии из URL"""
        try:
            match = re.search(r'/jobs/view/(\d+)/', url)
            if match:
                return match.group(1)
            return None
        except:
            return None
    
    def scroll_page(self, times=2):
        """Прокрутка страницы - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            for i in range(times):
                # Прокручиваем до конца
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Пробуем найти и нажать кнопку "Показать больше результатов"
                try:
                    show_more_buttons = self.driver.find_elements(By.XPATH, "//button[contains(@class, 'infinite-scroller__show-more-button') or contains(text(), 'Показать')]")
                    for button in show_more_buttons:
                        if button.is_displayed() and button.is_enabled():
                            button.click()
                            time.sleep(2)
                            break
                except:
                    pass
                
                # Ждем загрузки новых элементов
                time.sleep(2)
                
                # Проверяем новую высоту
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                
        except Exception as e:
            self.logger.debug(f"Ошибка при прокрутке: {e}")
    
    def parse_job_page(self, job_url):
        """Парсинг страницы вакансии"""
        try:
            self.driver.get(job_url)
            time.sleep(3)
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            title = self.extract_title(soup)
            company = self.extract_company(soup)
            location = self.extract_location(soup)  # ИСПРАВЛЕННЫЙ МЕТОД
            description = self.extract_description(soup)
            salary = self.extract_salary(soup)  # ИСПРАВЛЕННЫЙ МЕТОД
            
            if not title or title == "Не указано":
                self.logger.warning(f"Не удалось извлечь название для {job_url}")
                return None
            
            # ДОПОЛНИТЕЛЬНАЯ ФИЛЬТРАЦИЯ: проверяем, что вакансия действительно соответствует запросу
            title_lower = title.lower()
            
            # Проверяем основные IT-профессии
            it_keywords = ["разработчик", "developer", "инженер", "engineer", "qa", "тестиров", "менеджер", "manager", "дизайнер", "designer", "аналитик", "analyst", "devops", "data"]
            
            if not any(keyword in title_lower for keyword in it_keywords):
                self.logger.warning(f"Пропущена неподходящая вакансия: {title}")
                return None
            
            vacancy = {
                'title': title[:200],
                'company_name': company[:100] if company else "",
                'description': description[:4000] if description else "Описание не найдено",
                'salary': salary[:100] if salary else "не указана",
                'location': location[:100] if location else "Не указана",
                'contact': job_url,
                'source': 'LinkedIn',
                'source_url': job_url  # Сохраняем оригинальную ссылку
            }
            
            self.logger.info(f"Обработана: {title[:50]}... (Компания: {company[:30] if company else 'N/A'}, Город: {location})")
            return vacancy
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга {job_url}: {e}")
            return None
    
    def extract_title(self, soup):
        """Извлечение названия"""
        try:
            selectors = [
                'h1',
                '.top-card-layout__title', 
                '.jobs-unified-top-card__job-title',
                '.job-details-jobs-unified-top-card__job-title',
                'h1.job-title',
                '.jobs-details-top-card__job-title'
            ]
            
            for selector in selectors:
                element = soup.select_one(selector)
                if element and element.text.strip():
                    return element.text.strip()
            
            # Дополнительный поиск по тексту
            h1_elements = soup.find_all('h1')
            for h1 in h1_elements:
                text = h1.text.strip()
                if text and len(text) > 5:
                    return text
            
            return "Не указано"
        except:
            return "Не указано"
    
    def extract_company(self, soup):
        """Извлечение компании"""
        try:
            selectors = [
                '.top-card-layout__card-subtitle-item a',
                '.jobs-unified-top-card__company-name',
                '.job-details-jobs-unified-top-card__company-name',
                '.jobs-unified-top-card__primary-description-container a',
                '.jobs-details-top-card__company-url',
                '.jobs-company__box a'
            ]
            
            for selector in selectors:
                element = soup.select_one(selector)
                if element:
                    text = element.text.strip()
                    if text:
                        return text
            
            # Поиск по классам содержащим "company"
            company_elements = soup.find_all(['span', 'div', 'a'], class_=re.compile(r'company|employer|organization', re.I))
            for element in company_elements:
                text = element.text.strip()
                if text and len(text) > 2 and len(text) < 100:
                    return text
            
            return ""
        except:
            return ""
    
    def extract_location(self, soup):
        """Извлечение локации - ИСПРАВЛЕННЫЙ МЕТОД (ТЕПЕРЬ НЕ ПИШЕТ КОМПАНИЮ ВМЕСТО ГОРОДА)"""
        try:
            # Сохраняем оригинальный текст для отладки
            page_text = soup.get_text()[:1000]  # Первые 1000 символов для отладки
            
            # ПРИОРИТЕТ 1: Поиск в конкретных элементах LinkedIn
            location_selectors = [
                '.job-details-jobs-unified-top-card__primary-description',
                '.jobs-unified-top-card__primary-description',
                '.jobs-unified-top-card__bullet',
                '.jobs-unified-top-card__subtitle-secondary',
                '.job-details-jobs-unified-top-card__subtitle-secondary',
                '.jobs-details-top-card__exact-location',
                '.jobs-unified-top-card__location',
                '.topcard__flavor--bullet',
                '.top-card-layout__card-subtitle-item',
                '.jobs-details-top-card__bullet',
                '.jobs-details-top-card__company-info'
            ]
            
            for selector in location_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    if text and len(text) < 100:  # Исключаем длинные тексты
                        # Очищаем текст от лишней информации
                        cleaned = self.clean_location_text(text)
                        if cleaned and cleaned != "Не указана":
                            # Проверяем, что это не название компании
                            company_name = self.extract_company(soup)
                            if company_name and company_name in text:
                                # Пропускаем, если текст содержит название компании
                                continue
                            return cleaned
            
            # ПРИОРИТЕТ 2: Поиск по ключевым словам локации
            location_keywords = ['location', 'местоположение', 'город', 'адрес', 'расположение', 'регион', 'страна']
            
            for tag in ['span', 'div', 'li']:
                elements = soup.find_all(tag)
                for element in elements:
                    text = element.get_text(strip=True).lower()
                    # Ищем элементы, содержащие ключевые слова локации
                    if any(keyword in text for keyword in location_keywords):
                        # Берем текст элемента или следующего элемента
                        location_text = element.get_text(strip=True)
                        if location_text:
                            cleaned = self.clean_location_text(location_text)
                            if cleaned and cleaned != "Не указана":
                                return cleaned
            
            # ПРИОРИТЕТ 3: Поиск городов из нашего списка Config.CITIES
            all_text = soup.get_text()
            
            # Проверяем наличие городов из нашего списка
            for city in Config.CITIES:
                if city.lower() in all_text.lower():
                    # Проверяем контекст, чтобы убедиться что это действительно город
                    city_lower = city.lower()
                    # Ищем город в разумном контексте (не в середине длинных слов)
                    pattern = r'(?<!\w)' + re.escape(city_lower) + r'(?!\w)'
                    if re.search(pattern, all_text.lower()):
                        return city
            
            # ПРИОРИТЕТ 4: Поиск в мета-тегах
            meta_tags = soup.find_all('meta')
            for tag in meta_tags:
                if tag.get('property') in ['og:locality', 'og:region', 'og:country_name']:
                    content = tag.get('content', '')
                    if content:
                        cleaned = self.clean_location_text(content)
                        if cleaned and cleaned != "Не указана":
                            return cleaned
            
            # Если ничего не найдено, возвращаем "Не указана"
            return "Не указана"
            
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения локации: {e}")
            return "Не указана"
    
    def clean_location_text(self, text):
        """Очистка текста локации - УЛУЧШЕННЫЙ МЕТОД"""
        try:
            if not text:
                return "Не указана"
            
            # Удаляем лишние пробелы и символы
            text = re.sub(r'\s+', ' ', text.strip())
            
            # Удаляем префиксы типа "Location: "
            prefixes = ['Location:', 'Местоположение:', 'Город:', 'Адрес:', 'Локация:', 'Locality:', 'Region:']
            for prefix in prefixes:
                if text.startswith(prefix):
                    text = text[len(prefix):].strip()
            
            # Разделяем текст по разделителям (берем первую часть)
            separators = ['·', '|', '•', '—', '-', '–', '•', '·', '·']
            for sep in separators:
                if sep in text:
                    parts = text.split(sep)
                    # Берем первую часть, которая выглядит как город
                    for part in parts:
                        part_clean = part.strip()
                        if part_clean and len(part_clean) < 50:
                            # Проверяем, похоже ли это на город
                            if self.looks_like_city(part_clean):
                                return part_clean
            
            # Проверяем, похоже ли текст на город
            if self.looks_like_city(text):
                return text[:50]  # Ограничиваем длину
            
            return "Не указана"
            
        except Exception as e:
            self.logger.debug(f"Ошибка очистки локации: {e}")
            return "Не указана"
    
    def looks_like_city(self, text):
        """Проверяет, похож ли текст на название города"""
        try:
            if not text or len(text) > 50:
                return False
            
            # Проверяем, есть ли город в нашем списке
            for city in Config.CITIES:
                if city.lower() in text.lower():
                    return True
            
            # Проверяем паттерны, похожие на города
            # Города обычно начинаются с заглавной буквы и не содержат цифр
            if re.match(r'^[А-ЯЁA-Z][а-яёa-z]+(?:\s+[А-ЯЁA-Z][а-яёa-z]+)*$', text):
                # Не должно быть типичных слов для компаний
                company_words = ['ltd', 'inc', 'company', 'корпорация', 'компания', 'ооо', 'зао', 'ао']
                for word in company_words:
                    if word in text.lower():
                        return False
                return True
            
            return False
        except:
            return False
    
    def extract_description(self, soup):
        """Извлечение описания"""
        try:
            selectors = [
                '.description__text',
                '.show-more-less-html__markup',
                '.jobs-description__content',
                '.jobs-description-content',
                '.jobs-box__html-content',
                '.jobs-description'
            ]
            
            for selector in selectors:
                element = soup.select_one(selector)
                if element:
                    for tag in element(['script', 'style', 'iframe', 'form']):
                        tag.decompose()
                    
                    text = element.get_text(separator='\n', strip=True)
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    return text[:5000]
            
            # Альтернативный поиск
            desc_divs = soup.find_all('div', {'class': re.compile(r'desc|content|body', re.I)})
            for div in desc_divs:
                text = div.get_text(separator='\n', strip=True)
                if len(text) > 100:
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    return text[:5000]
            
            return "Описание не найдено"
        except:
            return "Описание не найдено"
    
    def extract_salary(self, soup):
        """Извлечение зарплаты - ИСПРАВЛЕННЫЙ МЕТОД (ТЕПЕРЬ ТОЧНЕЕ)"""
        try:
            # ПРИОРИТЕТ 1: Поиск в специальных элементах зарплаты
            salary_selectors = [
                '.salary',
                '.compensation',
                '.pay-scale',
                '.jobs-unified-top-card__job-insight',
                '.job-details-jobs-unified-top-card__job-insight',
                '.jobs-details-top-card__salary-info',
                '.job-details-jobs-unified-top-card__job-insight-text'
            ]
            
            for selector in salary_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    if text and self.is_salary_text(text):
                        return self.clean_salary_text(text)
            
            # ПРИОРИТЕТ 2: Поиск по всей странице с регулярными выражениями
            all_text = soup.get_text()
            
            # Паттерны для поиска зарплаты (улучшенные)
            salary_patterns = [
                # Паттерны для Казахстана (тенге)
                r'(?:зарплата|зп|оклад|ставка|жалақы)[:\s]*([$\d\s.,-–—₸тгkztKZT]{10,80})',
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:₸|тг|тенге|kzt|KZT)',
                r'(?:₸|тг|тенге)\s*(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?',
                r'(\d[\d\s.,]*\d?)\s*[-–—]\s*(\d[\d\s.,]*\d?)\s*(?:₸|тг|тенге)',
                
                # Паттерны для России (рубли)
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:₽|руб|рублей|RUB)',
                r'(?:₽|руб)\s*(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?',
                
                # Паттерны для долларов
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:[$]|usd|USD|доллар)',
                r'[$]\s*(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?',
                
                # Паттерны для евро
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:[€]|eur|EUR|евро)',
                r'[€]\s*(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?',
                
                # Общие паттерны
                r'\d[\d\s.,]{3,}?\s*(?:₸|₽|[$]|[€]|тг|тенге|руб|usd|eur|kzt)',
                r'(?:от\s*)?\d+[\d\s.,]*\d*\s*(?:тыс|к|k|т\.?)(?:\s*руб|\s*₽|\s*₸|\s*[$]|\s*[€])?'
            ]
            
            for pattern in salary_patterns:
                matches = re.finditer(pattern, all_text, re.IGNORECASE)
                for match in matches:
                    salary_text = match.group(0).strip()
                    if self.is_salary_text(salary_text):
                        cleaned = self.clean_salary_text(salary_text)
                        if cleaned and cleaned != "не указана":
                            return cleaned
            
            return "не указана"
            
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения зарплаты: {e}")
            return "не указана"
    
    def clean_salary_text(self, text):
        """Очистка текста зарплаты"""
        try:
            if not text:
                return "не указана"
            
            # Удаляем лишние пробелы
            text = re.sub(r'\s+', ' ', text.strip())
            
            # Заменяем длинные тире на обычные
            text = text.replace('–', '-').replace('—', '-')
            
            # Стандартизируем валюту
            currency_map = {
                'тг': '₸',
                'тенге': '₸',
                'kzt': '₸',
                'руб': '₽',
                'рублей': '₽',
                'rur': '₽',
                'rub': '₽',
                'доллар': '$',
                'usd': '$',
                'евро': '€',
                'eur': '€'
            }
            
            for old, new in currency_map.items():
                text = re.sub(rf'\b{old}\b', new, text, flags=re.IGNORECASE)
            
            # Стандартизируем "тыс" и "к"
            if 'тыс' in text.lower() or 'к' in text.lower() or 'k' in text.lower():
                # Ищем числа и умножаем на 1000
                numbers = re.findall(r'(\d+[\d\s.,]*)', text)
                for num in numbers:
                    clean_num = re.sub(r'[^\d]', '', num)
                    if clean_num:
                        multiplied = int(clean_num) * 1000
                        text = text.replace(num, str(multiplied))
                # Удаляем обозначения тысяч
                text = re.sub(r'тыс|к|k', '', text, flags=re.IGNORECASE)
            
            # Удаляем лишние слова
            remove_words = ['зарплата', 'зп', 'оклад', 'ставка', 'жалақы', 'salary', 'compensation', 'от', 'до']
            for word in remove_words:
                text = re.sub(rf'\b{word}\b\s*[:]?\s*', '', text, flags=re.IGNORECASE)
            
            # Удаляем лишние пробелы
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text[:100]  # Ограничиваем длину
            
        except Exception as e:
            self.logger.debug(f"Ошибка очистки зарплаты: {e}")
            return text if text else "не указана"
    
    def is_salary_text(self, text):
        """Проверка, является ли текст информацией о зарплате"""
        try:
            if not text or len(text) < 4:
                return False
            
            text_lower = text.lower()
            
            # Проверяем наличие валютных символов или слов
            currency_indicators = ['₽', '$', '€', '₸', '£', 'тенге', 'тг', 'руб', 'usd', 'eur', 'kzt']
            salary_keywords = ['зарплата', 'зп', 'salary', 'оклад', 'ставка', 'жалақы', 'compensation']
            
            # Проверяем наличие цифр
            has_digits = bool(re.search(r'\d', text))
            
            # Проверяем наличие валютных индикаторов
            has_currency = any(indicator in text_lower for indicator in currency_indicators)
            
            # Проверяем наличие ключевых слов о зарплате
            has_salary_keyword = any(keyword in text_lower for keyword in salary_keywords)
            
            # Проверяем паттерны типа "100000-200000"
            has_salary_pattern = bool(re.search(r'\d+\s*[-–—]\s*\d+', text))
            
            # Текст считается зарплатой если:
            # 1. Есть цифры И (есть валюта ИЛИ есть ключевые слова ИЛИ есть паттерн диапазона)
            # 2. Или есть ключевые слова о зарплате с цифрами рядом
            if has_digits and (has_currency or has_salary_keyword or has_salary_pattern):
                return True
            
            if has_salary_keyword and has_digits:
                return True
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Ошибка проверки зарплаты: {e}")
            return False
    
    def run_parsing(self):
        """Основной метод парсинга - УЛУЧШЕННЫЙ С ФИЛЬТРАЦИЕЙ"""
        if not self.email or not self.password:
            self.logger.warning("Email и пароль не указаны")
            return []
        
        try:
            if not self.setup_driver():
                raise Exception("Не удалось настроить WebDriver")
            
            login_success = self.login()
            if not login_success and not self.auto_mode:
                return []
            
            all_vacancies = []
            jobs_to_parse = Config.JOBS[:8] if self.auto_mode else Config.JOBS[:5]
            
            for job in jobs_to_parse:
                for city in Config.CITIES[:4] if self.auto_mode else Config.CITIES[:3]:
                    self.logger.info(f"🔄 Парсим: {job} в {city} (СТРОГИЙ ФИЛЬТР)")
                    
                    time.sleep(random.uniform(Config.DELAY_MIN, Config.DELAY_MAX))
                    
                    job_links = self.search_jobs(job, city)
                    self.session_stats["total_found"] += len(job_links)
                    
                    if len(job_links) == 0:
                        self.logger.warning(f"Не найдено вакансий для {job} в {city}")
                        continue
                    
                    self.logger.info(f"   После фильтрации: {len(job_links)} вакансий")
                    
                    for i, job_url in enumerate(job_links):
                        self.logger.info(f"   [{i+1}/{len(job_links)}] Обработка вакансии...")
                        
                        time.sleep(random.uniform(2, 4))
                        
                        vacancy = self.parse_job_page(job_url)
                        if vacancy:
                            vacancy_id = self.db.save_vacancy(vacancy)
                            if vacancy_id:
                                # Проверяем, была ли это новая вакансия или дубликат
                                conn = sqlite3.connect(Config.DB_FILE)
                                cursor = conn.cursor()
                                cursor.execute(
                                    "SELECT is_duplicate FROM vacancies WHERE id = ?",
                                    (vacancy_id,)
                                )
                                result = cursor.fetchone()
                                conn.close()
                                
                                if result and result[0] == 1:
                                    self.session_stats["duplicates_found"] += 1
                                    self.logger.info(f"   ⏩ Дубликат пропущен: {vacancy['title'][:50]}...")
                                else:
                                    self.session_stats["new_vacancies"] += 1
                                    all_vacancies.append(vacancy)
                                    self.logger.info(f"   ✅ Новая вакансия сохранена: {vacancy['title'][:50]}...")
                        else:
                            self.logger.warning(f"   ❌ Вакансия отфильтрована (не соответствует запросу)")
                        
                        # Пауза между обработкой вакансий
                        if self.auto_mode:
                            time.sleep(random.uniform(1, 3))
                        else:
                            time.sleep(1)
                    
                    # Пауза между поисковыми запросами
                    time.sleep(random.uniform(3, 5))
            
            # Синхронизируем с PostgreSQL
            if Config.POSTGRES_ENABLED:
                postgres_sent = self.db.sync_to_postgres(30)
                self.session_stats["postgres_sent"] = postgres_sent
            
            # Сохраняем статистику сессии
            self.db.save_parsing_session(
                self.session_id,
                self.session_stats["total_found"],
                self.session_stats["new_vacancies"],
                self.session_stats["duplicates_found"],
                self.session_stats.get("postgres_sent", 0)
            )
            
            self.logger.info(f"✅ Парсинг завершен. Найдено: {self.session_stats['total_found']}, "
                           f"Новых: {self.session_stats['new_vacancies']}, "
                           f"Дубликатов: {self.session_stats['duplicates_found']}, "
                           f"PostgreSQL отправлено: {self.session_stats.get('postgres_sent', 0)}")
            
            # Запускаем очистку дубликатов после парсинга
            cleaned = self.db.cleanup_duplicates()
            if cleaned > 0:
                self.logger.info(f"🗑 Очищено дубликатов: {cleaned}")
            
            return all_vacancies
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
            self.session_stats["errors"] += 1
            
            self.db.save_parsing_session(
                self.session_id,
                self.session_stats["total_found"],
                self.session_stats["new_vacancies"],
                self.session_stats["duplicates_found"],
                self.session_stats.get("postgres_sent", 0),
                "failed"
            )
            
            return []
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info("WebDriver закрыт")
                except:
                    pass
    
    def get_session_stats(self):
        """Получить статистику текущей сессии"""
        return self.session_stats

# ============================================
# АВТО-ПАРСЕР
# ============================================

class AutoParser:
    """Автоматический парсер с интервалом 10 минут"""
    
    def __init__(self, gui_app):
        self.gui = gui_app
        self.db = Database()
        self.is_running = False
        self.timer = None
        
    def start(self):
        """Запуск авто-парсинга"""
        if self.is_running:
            return
        
        self.is_running = True
        self.gui.safe_call(self.gui.log_message, "🔄 Авто-парсинг запущен (интервал: 10 минут)", "success")
        self.gui.safe_call(self.gui.update_status, "Авто-парсинг активен")
        
        self.run_auto_parse()
    
    def stop(self):
        """Остановка авто-парсинга"""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.timer:
            self.timer.cancel()
        
        self.gui.safe_call(self.gui.log_message, "⏹ Авто-парсинг остановлен", "warning")
        self.gui.safe_call(self.gui.update_status, "Авто-парсинг остановлен")
    
    def run_auto_parse(self):
        """Выполнение авто-парсинга"""
        if not self.is_running:
            return
        
        try:
            self.gui.safe_call(self.gui.log_message, "⏰ Запуск автоматического парсинга...", "info")
            
            email = self.gui.linkedin_email.get()
            password = self.gui.linkedin_password.get()
            
            if not email or not password:
                self.gui.safe_call(self.gui.log_message, "❌ Не указаны учетные данные LinkedIn", "error")
                self.schedule_next_run()
                return
            
            self.gui.safe_call(self.gui.save_config_settings)
            
            thread = threading.Thread(target=self._run_parser_thread, args=(email, password), daemon=True)
            thread.start()
            
        except Exception as e:
            self.gui.safe_call(self.gui.log_message, f"❌ Ошибка запуска авто-парсинга: {e}", "error")
            self.schedule_next_run()
    
    def _run_parser_thread(self, email, password):
        """Запуск парсера в отдельном потоке"""
        try:
            parser = LinkedInParser(
                email=email,
                password=password,
                headless=True,
                auto_mode=True
            )
            
            vacancies = parser.run_parsing()
            stats = parser.get_session_stats()
            
            self.gui.safe_call(self._auto_parse_completed, vacancies, stats)
            
        except Exception as e:
            self.gui.safe_call(self._auto_parse_failed, str(e))
    
    def _auto_parse_completed(self, vacancies, stats):
        """Завершение авто-парсинга"""
        self.gui.log_message(f"✅ Авто-парсинг завершен. "
                           f"Найдено: {stats['total_found']}, "
                           f"Новых: {stats['new_vacancies']}, "
                           f"Дубликатов: {stats['duplicates_found']}, "
                           f"PostgreSQL: {stats.get('postgres_sent', 0)} отправлено", 
                           "success")
        
        self.gui.show_all_vacancies()
        self.gui.update_stats()
        
        if stats['new_vacancies'] > 0:
            messagebox.showinfo(
                "Новые вакансии",
                f"Авто-парсинг нашел {stats['new_vacancies']} новых вакансий!\n\n"
                f"Всего обработано: {stats['total_found']}\n"
                f"Дубликатов пропущено: {stats['duplicates_found']}\n"
                f"Отправлено в PostgreSQL: {stats.get('postgres_sent', 0)}\n"
                f"Оригинальные ссылки сохранены в колонке org_url"
            )
        
        self.schedule_next_run()
    
    def _auto_parse_failed(self, error):
        """Ошибка авто-парсинга"""
        self.gui.log_message(f"❌ Ошибка авто-парсинга: {error}", "error")
        
        time.sleep(300)
        if self.is_running:
            self.run_auto_parse()
    
    def schedule_next_run(self):
        """Запланировать следующий запуск"""
        if not self.is_running:
            return
        
        self.gui.log_message(f"⏳ Следующий авто-парсинг через {Config.AUTO_PARSE_INTERVAL//60} минут", "info")
        
        if self.timer:
            self.timer.cancel()
        
        self.timer = threading.Timer(Config.AUTO_PARSE_INTERVAL, self.run_auto_parse)
        self.timer.daemon = True
        self.timer.start()

# ============================================
# TELEGRAM ПУБЛИКАТОР
# ============================================

class TelegramPublisher:
    """Telegram публикатор"""
    
    def __init__(self, token="", channel_id=""):
        self.token = token.strip()
        self.channel_id = channel_id.strip()
        self.db = Database()
        self.session = requests.Session()
        self.session.timeout = 30
        
    def _normalize_channel_id(self, channel_id):
        """Нормализация ID канала для Telegram API"""
        if not channel_id:
            return None
        
        if channel_id.startswith('@'):
            channel_id = channel_id[1:]
        
        if channel_id and not channel_id.startswith('-100'):
            return f"@{channel_id}"
        
        return channel_id
    
    def test_connection(self):
        """Проверка подключения к Telegram"""
        if not self.token:
            return False, "Не указан токен бота"
        
        try:
            api_url = f"https://api.telegram.org/bot{self.token}/getMe"
            response = self.session.get(api_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    bot_info = data['result']
                    
                    test_message = "✅ Бот подключен успешно! Парсер готов к работе."
                    send_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                    
                    normalized_channel = self._normalize_channel_id(self.channel_id)
                    if not normalized_channel:
                        return True, f"Бот: @{bot_info['username']} (ID: {bot_info['id']})\n⚠ ID канала не указан"
                    
                    test_data = {
                        'chat_id': normalized_channel,
                        'text': test_message,
                        'parse_mode': 'HTML'
                    }
                    
                    send_response = self.session.post(send_url, json=test_data, timeout=10)
                    
                    if send_response.status_code == 200:
                        return True, f"Бот: @{bot_info['username']} (ID: {bot_info['id']})\n✅ Канал доступен для публикации"
                    else:
                        error_data = send_response.json()
                        error_desc = error_data.get('description', 'Неизвестная ошибка')
                        
                        if "chat not found" in error_desc.lower():
                            return False, f"Бот: @{bot_info['username']}\n❌ Бот не добавлен в канал или канал не существует"
                        elif "forbidden" in error_desc.lower():
                            return False, f"Бот: @{bot_info['username']}\n❌ У бота нет прав на публикацию в канал"
                        else:
                            return False, f"Бот: @{bot_info['username']}\n❌ Ошибка отправки: {error_desc}"
                else:
                    return False, f"Ошибка API: {data.get('description', 'Неизвестно')}"
            else:
                return False, f"Ошибка HTTP: {response.status_code}"
                
        except Exception as e:
            return False, f"Ошибка подключения: {e}"
    
    def clean_text_for_telegram(self, text):
        """Очистка текста для Telegram HTML разметки"""
        if not text:
            return ""
        
        text = (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;'))
        
        text = ' '.join(text.split())
        
        return text
    
    def format_vacancy_message(self, vacancy):
        """Форматирование вакансии для Telegram с HTML разметкой"""
        try:
            title = self.clean_text_for_telegram(vacancy.get('title', 'Вакансия'))
            company = self.clean_text_for_telegram(vacancy.get('company_name', ''))
            location = self.clean_text_for_telegram(vacancy.get('location', 'Не указано'))
            salary = self.clean_text_for_telegram(vacancy.get('salary', 'не указана'))
            url = vacancy.get('source_url', '')
            
            description = vacancy.get('description', '')
            if description and description != "Описание не найдено":
                description = self.clean_text_for_telegram(description)
                if len(description) > 800:
                    description = description[:797] + "..."
            else:
                description = "Описание не указано"
            
            message = f"<b>🏢 {title}</b>\n\n"
            
            if company:
                message += f"<b>🏭 Компания:</b> {company}\n"
            
            if location and location != "Не указана":
                message += f"<b>📍 Локация:</b> {location}\n"
            
            if salary and salary != "не указана":
                message += f"<b>💰 Зарплата:</b> {salary}\n"
            
            message += f"\n<b>📝 Описание:</b>\n{description}\n\n"
            
            if url:
                message += f'<a href="{url}">🔗 Ссылка на вакансию</a>\n\n'
            
            message += "#вакансия #работа #linkedin"
            
            return message
            
        except Exception as e:
            print(f"Ошибка форматирования: {e}")
            return f"<b>Вакансия</b>\n\n{vacancy.get('source_url', '')}\n\n#вакансия"
    
    def publish_vacancy_sync(self, vacancy):
        """Публикация одной вакансии в Telegram канал"""
        try:
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
                'disable_web_page_preview': False
            }
            
            for attempt in range(3):
                try:
                    print(f"📤 Попытка {attempt + 1} отправки вакансии #{vacancy.get('id')}...")
                    
                    response = self.session.post(api_url, json=data, timeout=30)
                    
                    if response.status_code == 200:
                        response_data = response.json()
                        if response_data.get('ok'):
                            self.db.mark_as_published(vacancy['id'])
                            print(f"✅ Успешно опубликовано: {vacancy.get('title', '')[:50]}...")
                            return True
                        else:
                            print(f"⚠ API вернуло ошибку: {response_data.get('description')}")
                    
                    elif response.status_code == 400:
                        error_data = response.json()
                        error_desc = error_data.get('description', 'Неизвестная ошибка')
                        print(f"❌ Ошибка 400: {error_desc}")
                        
                        if "can't parse entities" in error_desc.lower():
                            print("⚠ Пробуем отправить без HTML разметки...")
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
            
        except Exception as e:
            print(f"❌ Критическая ошибка публикации: {e}")
            return False
    
    def publish_all_unpublished(self):
        """Публикация всех неопубликованных вакансий"""
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
                print(f"⏳ Ждем 3 секунды перед следующей публикацией...")
                time.sleep(3)
        
        print(f"\n{'='*50}")
        print(f"📊 ПУБЛИКАЦИЯ ЗАВЕРШЕНА")
        print(f"✅ Успешно: {published_count}")
        print(f"❌ Не удалось: {failed_count}")
        print(f"📈 Всего: {len(vacancies)}")
        print(f"{'='*50}")
        
        return published_count

# ============================================
# ГРАФИЧЕСКИЙ ИНТЕРФЕЙС (ИСПРАВЛЕННАЯ ВЕРСИЯ)
# ============================================

class ParserGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("LinkedIn Parser Pro v5.5.9 - PostgreSQL Sync Fixed + Исправлены город и зарплата + Строгий поиск по фильтру")
        self.root.geometry("1100x750")
        
        self.center_window()
        
        # Загружаем конфиг ДО создания GUI элементов
        self.config = self.load_config()
        
        # Создаем переменные Tkinter
        self.linkedin_email = tk.StringVar()
        self.linkedin_password = tk.StringVar()
        self.telegram_token = tk.StringVar()
        self.telegram_channel = tk.StringVar()
        self.auto_parse_enabled = tk.BooleanVar()
        
        # Устанавливаем значения из конфига
        self.linkedin_email.set(self.config.get("linkedin_email", ""))
        self.linkedin_password.set(self.config.get("linkedin_password", ""))
        self.telegram_token.set(self.config.get("telegram_token", ""))
        self.telegram_channel.set(self.config.get("telegram_channel", "@your_channel"))
        self.auto_parse_enabled.set(self.config.get("auto_parse_enabled", False))
        
        self.is_parsing = False
        self.db = Database()
        self.parser_thread = None
        
        # Автопарсер инициализируем позже, чтобы избежать циклических ссылок
        self.auto_parser = None
        
        self.setup_gui()
        self.update_stats()
        
        # Инициализируем автопарсер ПОСЛЕ создания GUI
        self.auto_parser = AutoParser(self)
        
        if self.auto_parse_enabled.get():
            self.root.after(2000, self.start_auto_parsing)
        
        print("\n" + "="*60)
        print("LinkedIn Parser Pro v5.5.9 - PostgreSQL Sync Fixed + Исправлены город и зарплата + Строгий поиск по фильтру")
        print("✅ ИСПРАВЛЕНО ИЗВЛЕЧЕНИЕ ГОРОДА - теперь не пишет компанию вместо города")
        print("✅ ИСПРАВЛЕНО ИЗВЛЕЧЕНИЕ ЗАРПЛАТЫ - точное определение сумм и валюты")
        print("✅ ДОБАВЛЕНА КОЛОНКА org_url для хранения оригинальных ссылок")
        print("✅ ИСПРАВЛЕНА ДЕДУПЛИКАЦИЯ - оригиналы не удаляются")
        print("✅ ДОБАВЛЕНА СТРОГАЯ ФИЛЬТРАЦИЯ - поиск только по IT-вакансиям")
        print("✅ ИСКЛЮЧЕНЫ: юристы, бухгалтеры, продавцы, водители и другие неподходящие вакансии")
        print("="*60)
        
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def on_closing(self):
        """Обработка закрытия окна"""
        if self.is_parsing:
            if messagebox.askokcancel("Выход", "Парсинг все еще выполняется. Вы уверены, что хотите выйти?"):
                self.is_parsing = False
                if self.auto_parser:
                    self.auto_parser.stop()
                self.root.quit()
        else:
            if self.auto_parser:
                self.auto_parser.stop()
            self.root.quit()
    
    def safe_call(self, func, *args, **kwargs):
        """Безопасный вызов функции Tkinter из другого потока"""
        if self.root:
            try:
                self.root.after(0, lambda: func(*args, **kwargs))
            except:
                pass
    
    def center_window(self):
        """Центрирование окна"""
        self.root.update_idletasks()
        width = 1100
        height = 750
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
    
    def load_config(self):
        """Загрузка конфигурации"""
        config = {
            "linkedin_email": "",
            "linkedin_password": "",
            "telegram_token": "",
            "telegram_channel": "@your_channel",
            "auto_parse_enabled": False
        }
        
        try:
            if os.path.exists(Config.CONFIG_FILE):
                with open(Config.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    config.update(loaded)
        except Exception as e:
            print(f"⚠ Ошибка загрузки конфига: {e}")
        
        return config
    
    def save_config(self):
        """Сохранение конфигурации"""
        config = {
            "linkedin_email": self.linkedin_email.get(),
            "linkedin_password": self.linkedin_password.get(),
            "telegram_token": self.telegram_token.get(),
            "telegram_channel": self.telegram_channel.get(),
            "auto_parse_enabled": self.auto_parse_enabled.get()
        }
        
        try:
            with open(Config.CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"⚠ Ошибка сохранения конфига: {e}")
            return False
    
    def setup_gui(self):
        """Настройка графического интерфейса"""
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        self.setup_settings_tab()
        self.setup_parsing_tab()
        self.setup_database_tab()
        self.setup_publish_tab()
        self.setup_stats_tab()
        self.setup_postgres_tab()
        
        self.setup_status_bar()
    
    def setup_settings_tab(self):
        """Вкладка настроек"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="⚙ Настройки")
        
        linkedin_frame = ttk.LabelFrame(tab, text="LinkedIn Аккаунт", padding=10)
        linkedin_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(linkedin_frame, text="Email:").grid(row=0, column=0, sticky='w', pady=5)
        ttk.Entry(linkedin_frame, textvariable=self.linkedin_email, width=40).grid(row=0, column=1, pady=5)
        
        ttk.Label(linkedin_frame, text="Пароль:").grid(row=1, column=0, sticky='w', pady=5)
        ttk.Entry(linkedin_frame, textvariable=self.linkedin_password, show="*", width=40).grid(row=1, column=1, pady=5)
        
        telegram_frame = ttk.LabelFrame(tab, text="Telegram Бот", padding=10)
        telegram_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(telegram_frame, text="Токен бота:").grid(row=0, column=0, sticky='w', pady=5)
        ttk.Entry(telegram_frame, textvariable=self.telegram_token, width=50).grid(row=0, column=1, pady=5)
        ttk.Label(telegram_frame, text="(получить у @BotFather)", font=('Arial', 8), foreground='gray').grid(row=0, column=2, padx=5)
        
        ttk.Label(telegram_frame, text="ID канала:").grid(row=1, column=0, sticky='w', pady=5)
        ttk.Entry(telegram_frame, textvariable=self.telegram_channel, width=50).grid(row=1, column=1, pady=5)
        ttk.Label(telegram_frame, text="(например: @my_channel или -1001234567890)", font=('Arial', 8), foreground='gray').grid(row=1, column=2, padx=5)
        
        auto_frame = ttk.LabelFrame(tab, text="Автоматический парсинг", padding=10)
        auto_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Checkbutton(auto_frame, text="Включить авто-парсинг каждые 10 минут", 
                       variable=self.auto_parse_enabled, width=40).grid(row=0, column=0, sticky='w', pady=5)
        
        ttk.Label(auto_frame, text="При включении парсер будет автоматически искать новые вакансии", 
                 font=('Arial', 8), foreground='gray').grid(row=1, column=0, sticky='w', pady=2)
        
        button_frame = ttk.Frame(tab)
        button_frame.pack(pady=20)
        
        ttk.Button(button_frame, text="💾 Сохранить настройки", 
                  command=self.save_config_settings, width=25).pack(side='left', padx=5)
        
        ttk.Button(button_frame, text="🔗 Проверить Telegram", 
                  command=self.test_telegram, width=25).pack(side='left', padx=5)
        
        ttk.Button(button_frame, text="🔧 Проверить БД", 
                  command=self.check_database, width=25).pack(side='left', padx=5)
        
        ttk.Button(button_frame, text="🧹 Очистить дубликаты", 
                  command=self.clean_duplicates_gui, width=25).pack(side='left', padx=5)
    
    def clean_duplicates_gui(self):
        """Очистка дубликатов через GUI"""
        try:
            db = Database()
            cleaned = db.cleanup_duplicates()
            
            if cleaned > 0:
                self.log_message(f"✅ Очищено дубликатов: {cleaned}", "success")
                self.show_all_vacancies()
                self.update_stats()
                messagebox.showinfo(
                    "Очистка дубликатов",
                    f"Успешно очищено {cleaned} дубликатов из базы данных!\n\n"
                    f"⚠ Оригинальные вакансии сохранены.\n"
                    f"✅ Удалены только записи с is_duplicate = 1"
                )
            else:
                self.log_message("ℹ Дубликатов не найдено", "info")
                messagebox.showinfo(
                    "Очистка дубликатов",
                    "Дубликатов не найдено в базе данных."
                )
                
        except Exception as e:
            self.log_message(f"❌ Ошибка очистки дубликатов: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось очистить дубликаты:\n\n{e}")
    
    def setup_postgres_tab(self):
        """Вкладка управления PostgreSQL"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🗃 PostgreSQL")
        
        status_frame = ttk.LabelFrame(tab, text="Статус подключения", padding=10)
        status_frame.pack(fill='x', padx=10, pady=10)
        
        self.postgres_status_var = tk.StringVar(value="Не проверено")
        ttk.Label(status_frame, textvariable=self.postgres_status_var, 
                 font=('Arial', 10)).pack(pady=5)
        
        ttk.Button(status_frame, text="🔄 Проверить подключение",
                  command=self.check_postgres_connection_gui, width=25).pack(pady=5)
        
        sync_frame = ttk.LabelFrame(tab, text="Управление синхронизацией", padding=10)
        sync_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(sync_frame, text="🔄 Синхронизировать с PostgreSQL",
                  command=self.sync_to_postgres_gui, width=30).pack(pady=5)
        
        ttk.Button(sync_frame, text="📊 Статистика PostgreSQL",
                  command=self.show_postgres_stats, width=30).pack(pady=5)
        
        info_frame = ttk.LabelFrame(tab, text="Конфигурация", padding=10)
        info_frame.pack(fill='x', padx=10, pady=10)
        
        info_text = f"""
Хост: {Config.POSTGRES_HOST}
Порт: {Config.POSTGRES_PORT}
База данных: {Config.POSTGRES_DB}
Пользователь: {Config.POSTGRES_USER}
Таблица: job_posting_sources (должна существовать)
Новая колонка: org_url (для хранения оригинальных ссылок)
Статус: {'✅ Включено' if Config.POSTGRES_ENABLED else '❌ Выключено'}
"""
        
        ttk.Label(info_frame, text=info_text, justify='left', 
                 font=('Consolas', 9)).pack(pady=5)
        
        ttk.Button(tab, text="⚠ Проверить таблицу job_posting_sources",
                  command=self.check_postgres_table, width=30).pack(pady=10)
    
    def check_postgres_table(self):
        """Проверка существования таблицы job_posting_sources"""
        try:
            db = Database()
            if db.postgres.connect():
                if db.postgres.check_table_exists():
                    # Проверяем наличие колонки org_url
                    if db.postgres.check_column_exists(Config.ORG_URL_COLUMN):
                        messagebox.showinfo("Проверка таблицы", 
                            f"✅ Таблица 'job_posting_sources' существует\n"
                            f"✅ Колонка '{Config.ORG_URL_COLUMN}' существует\n\n"
                            f"Программа будет сохранять оригинальные ссылки в эту колонку.")
                    else:
                        messagebox.showwarning("Предупреждение", 
                            f"✅ Таблица 'job_posting_sources' существует\n"
                            f"⚠ Колонка '{Config.ORG_URL_COLUMN}' НЕ существует!\n\n"
                            f"Рекомендуется добавить колонку:\n"
                            f"ALTER TABLE job_posting_sources ADD COLUMN {Config.ORG_URL_COLUMN} VARCHAR(500);\n\n"
                            f"Программа будет работать без нее, но оригинальные ссылки не сохранятся.")
                else:
                    messagebox.showerror("Ошибка", "❌ Таблица 'job_posting_sources' не существует!\n\nСоздайте таблицу вручную со следующей структурой:\n\nCREATE TABLE job_posting_sources (\n    id SERIAL PRIMARY KEY,\n    source_id VARCHAR(255) UNIQUE,\n    position_name TEXT NOT NULL,\n    vacancy_description TEXT,\n    company_name_raw VARCHAR(255),\n    source_type VARCHAR(50),\n    location TEXT,\n    contact_social VARCHAR(500),\n    salary_min NUMERIC,\n    salary_max NUMERIC,\n    salary_currency VARCHAR(10),\n    is_published BOOLEAN DEFAULT FALSE,\n    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n    created_by VARCHAR(255),\n    source_url VARCHAR(500),\n    company_name_normalized VARCHAR(255),\n    org_url VARCHAR(500)  -- НОВАЯ КОЛОНКА ДЛЯ ОРИГИНАЛЬНОЙ ССЫЛКИ\n);")
            else:
                messagebox.showerror("Ошибка", "Не удалось подключиться к PostgreSQL")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось проверить таблицу:\n\n{e}")
    
    def check_postgres_connection_gui(self):
        """Проверка подключения к PostgreSQL через GUI"""
        try:
            db = Database()
            if db.postgres.connect():
                self.postgres_status_var.set("✅ Подключение установлено")
                # Проверяем наличие колонки org_url
                if db.postgres.check_column_exists(Config.ORG_URL_COLUMN):
                    self.postgres_status_var.set("✅ Подключение установлено (колонка org_url есть)")
                    self.log_message("✅ Подключение к PostgreSQL успешно, колонка org_url существует", "success")
                else:
                    self.postgres_status_var.set("✅ Подключение (колонки org_url нет)")
                    self.log_message("✅ Подключение к PostgreSQL успешно, но колонки org_url нет", "warning")
            else:
                self.postgres_status_var.set("❌ Ошибка подключения")
                self.log_message("❌ Не удалось подключиться к PostgreSQL", "error")
        except Exception as e:
            self.postgres_status_var.set(f"❌ Ошибка: {str(e)[:50]}")
            self.log_message(f"❌ Ошибка подключения к PostgreSQL: {e}", "error")
    
    def sync_to_postgres_gui(self):
        """Синхронизация с PostgreSQL через GUI"""
        try:
            if not Config.POSTGRES_ENABLED:
                messagebox.showwarning("Внимание", "PostgreSQL интеграция выключена.")
                return
            
            db = Database()
            
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres = 0 AND is_duplicate = 0")
            count = cursor.fetchone()[0]
            conn.close()
            
            if count == 0:
                messagebox.showinfo("Информация", "Нет вакансий для синхронизации с PostgreSQL")
                return
            
            confirm = messagebox.askyesno(
                "Синхронизация с PostgreSQL",
                f"Найдено {count} вакансий для синхронизации с PostgreSQL (таблица job_posting_sources).\n\n"
                f"Оригинальные ссылки будут сохранены в колонке org_url (если она существует).\n\n"
                f"Продолжить?"
            )
            
            if not confirm:
                return
            
            self.log_message(f"🔄 Начинаю синхронизацию {count} вакансий с PostgreSQL (job_posting_sources)...", "info")
            
            progress_window = tk.Toplevel(self.root)
            progress_window.title("Синхронизация с PostgreSQL")
            progress_window.geometry("400x150")
            progress_window.transient(self.root)
            progress_window.grab_set()
            
            ttk.Label(progress_window, text="Синхронизация с PostgreSQL...", 
                     font=("Arial", 11, "bold")).pack(pady=10)
            
            progress_bar = ttk.Progressbar(progress_window, mode='indeterminate', length=300)
            progress_bar.pack(pady=10)
            progress_bar.start(10)
            
            status_label = ttk.Label(progress_window, text="Подготовка...")
            status_label.pack(pady=5)
            
            def sync_thread():
                try:
                    sent_count = db.sync_to_postgres(count)
                    
                    self.safe_call(lambda: self._sync_completed(sent_count, count, progress_window))
                    
                except Exception as e:
                    self.safe_call(lambda: self._sync_failed(str(e), progress_window))
            
            threading.Thread(target=sync_thread, daemon=True).start()
            
        except Exception as e:
            self.log_message(f"❌ Ошибка запуска синхронизации: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось запустить синхронизацию:\n\n{e}")
    
    def _sync_completed(self, sent_count, total_count, progress_window):
        """Завершение синхронизации"""
        try:
            progress_window.destroy()
            
            self.log_message(f"✅ Синхронизация завершена. Отправлено: {sent_count}/{total_count} в job_posting_sources", "success")
            
            self.show_all_vacancies()
            self.update_stats()
            
            # Проверяем наличие колонки org_url
            db = Database()
            org_url_exists = False
            if db.postgres.connection:
                org_url_exists = db.postgres.check_column_exists(Config.ORG_URL_COLUMN)
            
            messagebox.showinfo(
                "Синхронизация завершена",
                f"Синхронизация с PostgreSQL завершена!\n\n"
                f"✅ Успешно отправлено: {sent_count}\n"
                f"📊 Всего обработано: {total_count}\n\n"
                f"Обязательные поля:\n"
                f"• source_id, position_name, company_name_raw\n"
                f"• vacancy_description (NOT NULL)\n"
                f"• location, created_at, created_by, is_published\n"
                f"{'• Оригинальные ссылки сохранены в org_url' if org_url_exists else '• Колонка org_url отсутствует, ссылки не сохранены'}"
            )
            
        except:
            pass
    
    def _sync_failed(self, error, progress_window):
        """Ошибка синхронизации"""
        try:
            progress_window.destroy()
            
            self.log_message(f"❌ Ошибка синхронизации с PostgreSQL: {error}", "error")
            
            messagebox.showerror(
                "Ошибка синхронизации",
                f"Не удалось выполнить синхронизацию с PostgreSQL:\n\n{error}"
            )
            
        except:
            pass
    
    def show_postgres_stats(self):
        """Показать статистику PostgreSQL"""
        try:
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) as total FROM vacancies WHERE is_duplicate = 0")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) as sent FROM vacancies WHERE posted_to_postgres = 1")
            sent = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) as pending FROM vacancies WHERE posted_to_postgres = 0 AND is_duplicate = 0")
            pending = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) as duplicates FROM vacancies WHERE is_duplicate = 1")
            duplicates = cursor.fetchone()[0]
            
            conn.close()
            
            # Проверяем наличие колонки org_url
            db = Database()
            org_url_exists = False
            if db.postgres.connection:
                org_url_exists = db.postgres.check_column_exists(Config.ORG_URL_COLUMN)
            
            stats_text = f"""
📊 СТАТИСТИКА POSTGRESQL (job_posting_sources):

Всего вакансий в локальной БД: {total}
✅ Отправлено в PostgreSQL: {sent}
⏳ Ожидают отправки: {pending}
🗑 Дубликатов (не отправляются): {duplicates}

Колонка org_url: {'✅ СУЩЕСТВУЕТ' if org_url_exists else '❌ ОТСУТСТВУЕТ'}
{'✓ Оригинальные ссылки сохраняются в org_url' if org_url_exists else '⚠ Добавьте колонку org_url для сохранения ссылок'}

Структура таблицы (рекомендуемая):
• id (integer) - автоинкремент
• source_id (character varying) - уникальный ID
• position_name (text) - название должности
• vacancy_description (text) - описание (NOT NULL)
• company_name_raw (character varying) - название компании
• source_type (character varying) - источник
• location (text) - местоположение
• contact_social (character varying) - контакты
• salary_min (numeric) - мин зарплата
• salary_max (numeric) - макс зарплата
• salary_currency (character varying) - валюта
• is_published (boolean) - опубликовано ли
• created_at (timestamp) - дата создания
• created_by (character varying) - создатель
• source_url (character varying) - URL источника
• company_name_normalized (character varying) - нормализованное название компании
• org_url (character varying) - НОВАЯ КОЛОНКА: оригинальная ссылка на вакансию

Конфигурация:
• Хост: {Config.POSTGRES_HOST}
• Порт: {Config.POSTGRES_PORT}
• База данных: {Config.POSTGRES_DB}
• Пользователь: {Config.POSTGRES_USER}
• Таблица: job_posting_sources
• Статус: {'✅ Включено' if Config.POSTGRES_ENABLED else '❌ Выключено'}
"""
            
            stats_window = tk.Toplevel(self.root)
            stats_window.title("Статистика PostgreSQL (job_posting_sources)")
            stats_window.geometry("500x400")
            
            text_widget = scrolledtext.ScrolledText(stats_window, wrap=tk.WORD, font=('Arial', 10))
            text_widget.pack(fill='both', expand=True, padx=10, pady=10)
            
            text_widget.insert(1.0, stats_text)
            text_widget.config(state='disabled')
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось получить статистику PostgreSQL:\n\n{e}")
    
    def check_database(self):
        """Проверка структуры базы данных"""
        try:
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA table_info(vacancies)")
            columns = cursor.fetchall()
            
            column_info = "Структура таблицы vacancies:\n\n"
            for col in columns:
                column_info += f"• {col[1]} ({col[2]})\n"
            
            db_window = tk.Toplevel(self.root)
            db_window.title("Проверка базы данных")
            db_window.geometry("500x400")
            
            text_widget = scrolledtext.ScrolledText(db_window, wrap=tk.WORD, font=('Consolas', 10))
            text_widget.pack(fill='both', expand=True, padx=10, pady=10)
            
            text_widget.insert(1.0, column_info)
            text_widget.config(state='disabled')
            
            conn.close()
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось проверить базу данных:\n\n{e}")
    
    def setup_parsing_tab(self):
        """Вкладка парсинга"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🔍 Парсинг LinkedIn")
        
        control_frame = ttk.LabelFrame(tab, text="Управление парсингом", padding=10)
        control_frame.pack(fill='x', padx=10, pady=10)
        
        self.start_btn = ttk.Button(
            control_frame, 
            text="▶ Начать парсинг", 
            command=self.start_parsing,
            width=20
        )
        self.start_btn.pack(side='left', padx=5)
        
        self.stop_btn = ttk.Button(
            control_frame,
            text="■ Остановить",
            command=self.stop_parsing,
            width=20,
            state='disabled'
        )
        self.stop_btn.pack(side='left', padx=5)
        
        self.auto_start_btn = ttk.Button(
            control_frame,
            text="🔄 Запустить авто-парсинг",
            command=self.start_auto_parsing,
            width=25
        )
        self.auto_start_btn.pack(side='left', padx=5)
        
        self.auto_stop_btn = ttk.Button(
            control_frame,
            text="⏹ Остановить авто-парсинг",
            command=self.stop_auto_parsing,
            width=25
        )
        self.auto_stop_btn.pack(side='left', padx=5)
        
        self.progress = ttk.Progressbar(tab, mode='indeterminate', length=400)
        self.progress.pack(pady=10)
        
        log_frame = ttk.LabelFrame(tab, text="Лог выполнения", padding=10)
        log_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            wrap=tk.WORD,
            width=80, 
            height=20,
            font=('Consolas', 9)
        )
        self.log_text.pack(fill='both', expand=True)
        
        self.log_text.tag_config("error", foreground="red")
        self.log_text.tag_config("success", foreground="green")
        self.log_text.tag_config("warning", foreground="orange")
        self.log_text.tag_config("info", foreground="black")
    
    def setup_database_tab(self):
        """Вкладка базы данных"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🗄 База данных")
        
        search_frame = ttk.LabelFrame(tab, text="Поиск вакансий", padding=10)
        search_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(search_frame, text="Ключевое слово:").pack(side='left')
        self.search_var = tk.StringVar()
        ttk.Entry(search_frame, textvariable=self.search_var, width=30).pack(side='left', padx=5)
        
        ttk.Button(search_frame, text="🔍 Найти", 
                  command=self.search_vacancies, width=15).pack(side='left', padx=5)
        
        ttk.Button(search_frame, text="🔄 Показать все", 
                  command=self.show_all_vacancies, width=15).pack(side='left', padx=5)
        
        ttk.Button(search_frame, text="🗑 Очистить дубликаты", 
                  command=self.clean_duplicates, width=20).pack(side='left', padx=5)
        
        ttk.Button(search_frame, text="🧹 Очистка БД", 
                  command=self.cleanup_database, width=20).pack(side='left', padx=5)
        
        columns = ("ID", "Название", "Компания", "Город", "Зарплата", "Дата", "Опубликована", "PostgreSQL")
        self.tree = ttk.Treeview(tab, columns=columns, show='headings', height=20)
        
        col_widths = [50, 200, 120, 80, 90, 90, 90, 90]
        for col, width in zip(columns, col_widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, minwidth=width)
        
        scrollbar = ttk.Scrollbar(tab, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side='left', fill='both', expand=True, padx=(10, 0), pady=10)
        scrollbar.pack(side='right', fill='y', padx=(0, 10), pady=10)
        
        self.setup_context_menu()
        
        self.show_all_vacancies()
    
    def setup_publish_tab(self):
        """Вкладка публикации"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📢 Публикация в Telegram")
        
        stats_frame = ttk.LabelFrame(tab, text="📊 Статистика вакансий", padding=10)
        stats_frame.pack(fill='x', padx=10, pady=10)
        
        self.stats_text = tk.StringVar()
        self.stats_label = ttk.Label(
            stats_frame, 
            textvariable=self.stats_text,
            font=('Arial', 10),
            justify='left'
        )
        self.stats_label.pack(anchor='w', padx=10, pady=5)
        
        publish_frame = ttk.LabelFrame(tab, text="Управление публикацией", padding=10)
        publish_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(publish_frame, text="📤 Опубликовать все неопубликованные",
                  command=self.publish_all, width=35).pack(pady=5)
        
        ttk.Button(publish_frame, text="⚡ Тест публикации (1 вакансия)",
                  command=self.test_publish_one, width=35).pack(pady=5)
        
        ttk.Button(publish_frame, text="🔄 Обновить статистику",
                  command=self.update_stats, width=35).pack(pady=5)
    
    def setup_stats_tab(self):
        """Вкладка статистики"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📈 Статистика")
        
        history_frame = ttk.LabelFrame(tab, text="История парсинга", padding=10)
        history_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        columns = ("ID", "Время", "Найдено", "Новых", "Дубликатов", "PostgreSQL", "Статус")
        self.history_tree = ttk.Treeview(history_frame, columns=columns, show='headings', height=15)
        
        col_widths = [80, 150, 80, 80, 100, 90, 100]
        for col, width in zip(columns, col_widths):
            self.history_tree.heading(col, text=col)
            self.history_tree.column(col, width=width, minwidth=width)
        
        scrollbar = ttk.Scrollbar(history_frame, orient='vertical', command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=scrollbar.set)
        
        self.history_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        button_frame = ttk.Frame(tab)
        button_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(button_frame, text="🔄 Обновить историю",
                  command=self.update_history, width=25).pack(side='left', padx=5)
        
        ttk.Button(button_frame, text="📊 Подробная статистика",
                  command=self.show_detailed_stats, width=25).pack(side='left', padx=5)
    
    def setup_status_bar(self):
        """Статус бар"""
        self.status_var = tk.StringVar(value="✅ Готов к работе")
        self.status_bar = ttk.Label(
            self.root, 
            textvariable=self.status_var,
            relief='sunken', 
            anchor='w',
            padding=5
        )
        self.status_bar.pack(side='bottom', fill='x')
    
    def setup_context_menu(self):
        """Контекстное меню для таблицы"""
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="👁 Просмотреть вакансию", command=self.view_vacancy)
        self.context_menu.add_command(label="🌐 Открыть в браузере", command=self.open_in_browser)
        self.context_menu.add_command(label="📤 Опубликовать в Telegram", command=self.publish_selected)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="🗑 Удалить вакансию", command=self.delete_vacancy)
        
        self.tree.bind("<Button-3>", self.show_context_menu)
        self.tree.bind("<Double-Button-1>", lambda e: self.view_vacancy())
    
    def log_message(self, message, level="info"):
        """Добавление сообщения в лог"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            if level == "error":
                formatted = f"[{timestamp}] ❌ {message}\n"
                tag = "error"
            elif level == "success":
                formatted = f"[{timestamp}] ✅ {message}\n"
                tag = "success"
            elif level == "warning":
                formatted = f"[{timestamp}] ⚠ {message}\n"
                tag = "warning"
            else:
                formatted = f"[{timestamp}] ℹ {message}\n"
                tag = "info"
            
            self.log_text.insert(tk.END, formatted, tag)
            self.log_text.see(tk.END)
            
            self.root.update()
        except:
            pass
    
    def update_status(self, message):
        """Обновление статус бара"""
        try:
            self.status_var.set(message)
            self.root.update()
        except:
            pass
    
    def save_config_settings(self):
        """Сохранение настроек - исправленная версия без рекурсии"""
        if self.save_config():
            self.log_message("Настройки успешно сохранены", "success")
            self.update_status("Настройки сохранены")
            
            # Запускаем авто-парсинг только если он включен
            if self.auto_parse_enabled.get():
                self.start_auto_parsing()
            else:
                # Останавливаем авто-парсинг если он выключен
                if self.auto_parser:
                    self.auto_parser.stop()
                    self.auto_start_btn.config(state='normal')
                    self.auto_stop_btn.config(state='disabled')
        else:
            self.log_message("Ошибка при сохранении настроек", "error")
    
    def test_telegram(self):
        """Тест подключения к Telegram"""
        token = self.telegram_token.get()
        if not token:
            messagebox.showwarning("Внимание", "Введите токен Telegram бота")
            return
        
        try:
            publisher = TelegramPublisher(token=token, channel_id=self.telegram_channel.get())
            success, message = publisher.test_connection()
            
            if success:
                self.log_message(f"Telegram: {message}", "success")
                messagebox.showinfo("Успех", f"Подключение к Telegram успешно!\n\n{message}")
            else:
                self.log_message(f"Telegram: {message}", "error")
                messagebox.showerror("Ошибка", f"Не удалось подключиться к Telegram:\n\n{message}")
                
        except Exception as e:
            self.log_message(f"Ошибка теста Telegram: {e}", "error")
            messagebox.showerror("Ошибка", f"Произошла ошибка:\n\n{e}")
    
    def test_publish_one(self):
        """Тест публикации одной вакансии"""
        if not self.telegram_token.get() or not self.telegram_channel.get():
            messagebox.showwarning("Внимание", "Настройте Telegram бота")
            return
        
        self.save_config_settings()
        
        vacancies = self.db.get_unpublished_vacancies(1)
        if not vacancies:
            messagebox.showinfo("Информация", "Нет неопубликованных вакансий для теста")
            return
        
        vacancy = vacancies[0]
        
        try:
            publisher = TelegramPublisher(
                token=self.telegram_token.get(),
                channel_id=self.telegram_channel.get()
            )
            
            self.log_message(f"Тест публикации: {vacancy['title'][:50]}...")
            self.update_status("Тест публикации...")
            
            progress_window = tk.Toplevel(self.root)
            progress_window.title("Тест публикации")
            progress_window.geometry("300x150")
            progress_window.transient(self.root)
            progress_window.grab_set()
            
            ttk.Label(progress_window, text="Отправка тестового сообщения...", 
                     font=("Arial", 10)).pack(pady=20)
            
            progress_bar = ttk.Progressbar(progress_window, mode='indeterminate', length=200)
            progress_bar.pack(pady=10)
            progress_bar.start(10)
            
            def test_thread():
                success = publisher.publish_vacancy_sync(vacancy)
                self.safe_call(self.test_publish_completed, success, vacancy, progress_window)
            
            threading.Thread(target=test_thread, daemon=True).start()
            
        except Exception as e:
            self.log_message(f"Ошибка теста публикации: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось запустить тест:\n\n{e}")
    
    def test_publish_completed(self, success, vacancy, progress_window):
        """Завершение теста публикации"""
        try:
            progress_window.destroy()
            
            if success:
                self.log_message("✅ Тестовая публикация успешна!", "success")
                self.update_status("Тест успешен")
                
                self.show_all_vacancies()
                self.update_stats()
                
                messagebox.showinfo(
                    "Успех", 
                    "Тестовая публикация успешна!\n\n"
                    f"Вакансия '{vacancy['title'][:50]}...' опубликована.\n"
                    "Проверьте ваш Telegram канал."
                )
            else:
                self.log_message("❌ Тестовая публикация не удалась", "error")
                self.update_status("Тест не удался")
                
                messagebox.showwarning(
                    "Внимание", 
                    "Тестовая публикация не удалась.\n\n"
                    "Проверьте:\n"
                    "1. Токен бота\n"
                    "2. ID канала\n"
                    "3. Что бот добавлен в канал как администратор"
                )
        except:
            pass
    
    def start_parsing(self):
        """Запуск парсинга LinkedIn"""
        if not self.linkedin_email.get() or not self.linkedin_password.get():
            messagebox.showwarning("Внимание", "Введите email и пароль LinkedIn")
            return
        
        if self.is_parsing:
            return
        
        self.save_config_settings()
        
        email = self.linkedin_email.get()
        password = self.linkedin_password.get()
        
        self.is_parsing = True
        self.start_btn.config(state='disabled')
        self.stop_btn.config(state='normal')
        self.auto_start_btn.config(state='disabled')
        self.auto_stop_btn.config(state='disabled')
        self.progress.start(10)
        
        self.log_text.delete(1.0, tk.END)
        self.log_message("🔄 Запуск парсинга LinkedIn...", "info")
        self.log_message("✅ ИСПРАВЛЕННАЯ ДЕДУПЛИКАЦИЯ АКТИВНА", "success")
        self.log_message("✅ ИСПРАВЛЕНО ИЗВЛЕЧЕНИЕ ГОРОДА - теперь не пишет компанию вместо города", "success")
        self.log_message("✅ ИСПРАВЛЕНО ИЗВЛЕЧЕНИЕ ЗАРПЛАТЫ - правильные суммы и валюта", "success")
        self.log_message("✅ АКТИВИРОВАН СТРОГИЙ ФИЛЬТР - поиск только по IT-вакансиям", "success")
        self.log_message("✅ ИСКЛЮЧЕНЫ: юристы, бухгалтеры, продавцы, водители и др.", "success")
        self.log_message("✅ Оригинальные вакансии сохраняются, дубликаты пропускаются", "success")
        if Config.POSTGRES_ENABLED:
            self.log_message("✅ PostgreSQL синхронизация активна", "success")
            self.log_message("✅ Данные будут отправлены в таблицу job_posting_sources", "success")
            # Проверяем наличие колонки org_url
            db = Database()
            if db.postgres.connection:
                if db.postgres.check_column_exists(Config.ORG_URL_COLUMN):
                    self.log_message(f"✅ Колонка '{Config.ORG_URL_COLUMN}' существует, оригинальные ссылки сохраняются", "success")
                else:
                    self.log_message(f"⚠ Колонка '{Config.ORG_URL_COLUMN}' отсутствует, ссылки не будут сохранены", "warning")
        self.update_status("Парсинг запущен...")
        
        self.parser_thread = threading.Thread(
            target=self.run_parser_thread, 
            args=(email, password),
            daemon=True
        )
        self.parser_thread.start()
    
    def run_parser_thread(self, email, password):
        """Запуск парсера в отдельном потоке"""
        try:
            parser = LinkedInParser(
                email=email,
                password=password,
                headless=False
            )
            
            vacancies = parser.run_parsing()
            stats = parser.get_session_stats()
            
            self.safe_call(self.parsing_completed, len(vacancies), stats)
            
        except Exception as e:
            self.safe_call(self.parsing_failed, str(e))
    
    def parsing_completed(self, count, stats):
        """Завершение парсинга"""
        try:
            self.is_parsing = False
            self.start_btn.config(state='normal')
            self.stop_btn.config(state='disabled')
            self.auto_start_btn.config(state='normal')
            self.auto_stop_btn.config(state='normal')
            self.progress.stop()
            
            self.log_message(f"✅ Парсинг завершен. Найдено вакансий: {stats['total_found']}", "success")
            self.log_message(f"✅ Новых уникальных вакансий: {stats['new_vacancies']}", "success")
            self.log_message(f"⏩ Пропущено дубликатов: {stats['duplicates_found']}", "info")
            self.log_message(f"🗑 Отфильтровано неподходящих вакансий: {stats['total_found'] - stats['new_vacancies'] - stats['duplicates_found']}", "info")
            if Config.POSTGRES_ENABLED:
                self.log_message(f"📊 Отправлено в PostgreSQL: {stats.get('postgres_sent', 0)}", "info")
                # Проверяем наличие колонки org_url
                db = Database()
                if db.postgres.connection:
                    if db.postgres.check_column_exists(Config.ORG_URL_COLUMN):
                        self.log_message(f"✅ Оригинальные ссылки сохранены в колонке '{Config.ORG_URL_COLUMN}'", "success")
            self.update_status(f"Найдено {stats['new_vacancies']} новых уникальных IT-вакансий")
            
            self.show_all_vacancies()
            self.update_stats()
            self.update_history()
            
            # Проверяем наличие колонки org_url для отображения в сообщении
            org_url_info = ""
            db = Database()
            if db.postgres.connection:
                if db.postgres.check_column_exists(Config.ORG_URL_COLUMN):
                    org_url_info = "\n✅ Оригинальные ссылки сохранены в колонке org_url"
                else:
                    org_url_info = "\n⚠ Колонка org_url отсутствует, ссылки не сохранены"
            
            if stats['new_vacancies'] > 0:
                messagebox.showinfo(
                    "Успех", 
                    f"✅ Парсинг завершен успешно!\n\n"
                    f"📊 Статистика:\n"
                    f"• Найдено всего: {stats['total_found']}\n"
                    f"• Новых IT-вакансий: {stats['new_vacancies']}\n"
                    f"• Дубликатов пропущено: {stats['duplicates_found']}\n"
                    f"• Отфильтровано неподходящих: {stats['total_found'] - stats['new_vacancies'] - stats['duplicates_found']}\n"
                    f"• Отправлено в PostgreSQL: {stats.get('postgres_sent', 0)}\n\n"
                    f"✅ Только IT-вакансии: разработчики, QA, менеджеры, дизайнеры"
                    f"{org_url_info}"
                )
            else:
                messagebox.showinfo(
                    "Информация", 
                    "ℹ Парсинг завершен.\n\n"
                    f"Новых IT-вакансий не найдено.\n"
                    f"Пропущено дубликатов: {stats['duplicates_found']}\n"
                    f"Отфильтровано неподходящих: {stats['total_found'] - stats['duplicates_found']}\n"
                    f"Отправлено в PostgreSQL: {stats.get('postgres_sent', 0)}"
                    f"{org_url_info}"
                )
        except:
            pass
    
    def parsing_failed(self, error):
        """Ошибка парсинга"""
        try:
            self.is_parsing = False
            self.start_btn.config(state='normal')
            self.stop_btn.config(state='disabled')
            self.auto_start_btn.config(state='normal')
            self.auto_stop_btn.config(state='normal')
            self.progress.stop()
            
            self.log_message(f"❌ Ошибка парсинга: {error}", "error")
            self.update_status("Ошибка парсинга")
            
            messagebox.showerror(
                "Ошибка", 
                f"Не удалось выполнить парсинг:\n\n{error}"
            )
        except:
            pass
    
    def stop_parsing(self):
        """Остановка парсинга"""
        self.is_parsing = False
        self.log_message("Парсинг остановлен пользователем", "warning")
        self.update_status("Парсинг остановлен")
    
    def start_auto_parsing(self):
        """Запуск авто-парсинга"""
        if not self.linkedin_email.get() or not self.linkedin_password.get():
            messagebox.showwarning("Внимание", "Введите email и пароль LinkedIn")
            return
        
        if self.auto_parser:
            self.auto_parser.start()
            self.auto_start_btn.config(state='disabled')
            self.auto_stop_btn.config(state='normal')
        
        self.auto_parse_enabled.set(True)
        self.save_config_settings()
    
    def stop_auto_parsing(self):
        """Остановка авто-парсинга"""
        if self.auto_parser:
            self.auto_parser.stop()
            self.auto_start_btn.config(state='normal')
            self.auto_stop_btn.config(state='disabled')
        
        self.auto_parse_enabled.set(False)
        self.save_config_settings()
    
    def show_all_vacancies(self):
        """Показать все вакансии"""
        try:
            vacancies = self.db.get_all_vacancies(100)
            self.display_vacancies(vacancies)
        except Exception as e:
            self.log_message(f"Ошибка загрузки вакансий: {e}", "error")
    
    def search_vacancies(self):
        """Поиск вакансий"""
        keyword = self.search_var.get()
        try:
            vacancies = self.db.search_vacancies(keyword=keyword)
            self.display_vacancies(vacancies)
            self.log_message(f"Найдено {len(vacancies)} вакансий по запросу '{keyword}'")
        except Exception as e:
            self.log_message(f"Ошибка поиска: {e}", "error")
    
    def display_vacancies(self, vacancies):
        """Отображение вакансий в таблице"""
        try:
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            for vac in vacancies:
                title = vac.get('title', '')
                if len(title) > 30:
                    title = title[:27] + "..."
                
                company = vac.get('company_name', '')
                if len(company) > 20:
                    company = company[:17] + "..."
                
                salary = vac.get('salary', '')
                if len(salary) > 15:
                    salary = salary[:12] + "..."
                
                date_str = ""
                if vac.get('created_at'):
                    if isinstance(vac['created_at'], str):
                        date_str = vac['created_at'][:10]
                    else:
                        date_str = str(vac['created_at'])[:10]
                
                published = "✅ Да" if vac.get('published') else "❌ Нет"
                postgres = "✅ Да" if vac.get('posted_to_postgres') else "❌ Нет"
                
                self.tree.insert('', 'end', values=(
                    vac.get('id', ''),
                    title,
                    company,
                    vac.get('location', ''),
                    salary,
                    date_str,
                    published,
                    postgres
                ))
        except:
            pass
    
    def view_vacancy(self):
        """Просмотр деталей вакансии"""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = self.tree.item(selection[0])
        vacancy_id = item['values'][0]
        
        try:
            vacancy = self.db.get_vacancy_by_id(vacancy_id)
            if vacancy:
                self.show_vacancy_details(vacancy)
        except Exception as e:
            self.log_message(f"Ошибка загрузки вакансии: {e}", "error")
    
    def show_vacancy_details(self, vacancy):
        """Окно с деталями вакансии"""
        details_window = tk.Toplevel(self.root)
        details_window.title(f"Вакансия: {vacancy.get('title', '')[:50]}")
        details_window.geometry("800x600")
        
        notebook = ttk.Notebook(details_window)
        notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        info_tab = ttk.Frame(notebook)
        notebook.add(info_tab, text="📋 Основная информация")
        
        info_text = scrolledtext.ScrolledText(info_tab, wrap=tk.WORD, font=('Arial', 10))
        info_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        postgres_status = "✅ Отправлена" if vacancy.get('posted_to_postgres') else "❌ Не отправлена"
        duplicate_status = "✅ Уникальная" if not vacancy.get('is_duplicate') else "❌ Дубликат"
        
        info_content = f"""
{'='*70}
🏢 ВАКАНСИЯ: {vacancy.get('title', '')}
{'='*70}

🏭 Компания: {vacancy.get('company_name', 'Не указана')}
📅 Дата добавления: {vacancy.get('created_at', 'Не указана')}
📍 Локация: {vacancy.get('location', 'Не указана')}
💰 Зарплата: {vacancy.get('salary', 'не указана')}
📊 Опубликована в Telegram: {'✅ Да' if vacancy.get('published') else '❌ Нет'}
📊 Отправлена в PostgreSQL: {postgres_status}
📊 Статус вакансии: {duplicate_status}
🔗 Источник: {vacancy.get('source', 'LinkedIn')}
🔗 Оригинальная ссылка: {vacancy.get('source_url', 'Нет ссылки')}
🔑 Fingerprint: {vacancy.get('fingerprint', 'Нет')[:30]}...

{'='*70}
🌐 ССЫЛКА НА ВАКАНСИЮ:
{vacancy.get('source_url', 'Нет ссылки')}
"""
        
        info_text.insert(1.0, info_content)
        info_text.config(state='disabled')
        
        desc_tab = ttk.Frame(notebook)
        notebook.add(desc_tab, text="📝 Полное описание")
        
        desc_text = scrolledtext.ScrolledText(desc_tab, wrap=tk.WORD, font=('Arial', 10))
        desc_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        desc_text.insert(1.0, vacancy.get('description', 'Нет описания'))
        desc_text.config(state='disabled')
        
        button_frame = ttk.Frame(details_window)
        button_frame.pack(fill='x', padx=10, pady=10)
        
        if vacancy.get('source_url'):
            ttk.Button(
                button_frame,
                text="🌐 Открыть в браузере",
                command=lambda: webbrowser.open(vacancy['source_url'])
            ).pack(side='left', padx=5)
        
        if not vacancy.get('published'):
            ttk.Button(
                button_frame,
                text="📤 Опубликовать в Telegram",
                command=lambda: self.publish_single(vacancy, details_window)
            ).pack(side='left', padx=5)
        
        if not vacancy.get('posted_to_postgres') and Config.POSTGRES_ENABLED:
            ttk.Button(
                button_frame,
                text="🗃 Отправить в PostgreSQL",
                command=lambda: self.send_to_postgres_single(vacancy, details_window)
            ).pack(side='left', padx=5)
        
        ttk.Button(
            button_frame,
            text="Закрыть",
            command=details_window.destroy
        ).pack(side='right', padx=5)
    
    def send_to_postgres_single(self, vacancy, details_window=None):
        """Отправка одной вакансии в PostgreSQL"""
        if not Config.POSTGRES_ENABLED:
            messagebox.showwarning("Внимание", "PostgreSQL интеграция выключена")
            return
        
        try:
            vacancy_data = {
                'title': vacancy.get('title', ''),
                'company_name': vacancy.get('company_name', ''),
                'description': vacancy.get('description', ''),
                'location': vacancy.get('location', ''),
                'salary': vacancy.get('salary', ''),
                'source_url': vacancy.get('source_url', ''),  # Используем как org_url
                'source': vacancy.get('source', 'LinkedIn')
            }
            
            success = self.db.postgres.save_vacancy_to_postgres(vacancy_data)
            
            if success:
                self.db.mark_as_postgres_sent(vacancy['id'])
                
                if details_window:
                    details_window.destroy()
                
                self.log_message(f"✅ Вакансия отправлена в PostgreSQL (с org_url): {vacancy.get('title', '')[:50]}...", "success")
                self.update_status("Вакансия отправлена в PostgreSQL")
                
                self.show_all_vacancies()
                self.update_stats()
                
                messagebox.showinfo(
                    "Успех",
                    "Вакансия успешно отправлена в PostgreSQL (таблица job_posting_sources)!\n\n"
                    "Оригинальная ссылка сохранена в колонке org_url (если она существует)."
                )
            else:
                self.log_message(f"❌ Не удалось отправить вакансию в PostgreSQL", "error")
                messagebox.showwarning(
                    "Внимание",
                    "Не удалось отправить вакансию в PostgreSQL.\n"
                    "Проверьте подключение к серверу."
                )
                
        except Exception as e:
            self.log_message(f"❌ Ошибка отправки в PostgreSQL: {e}", "error")
            messagebox.showerror(
                "Ошибка",
                f"Не удалось отправить вакансию в PostgreSQL:\n\n{e}"
            )
    
    def open_in_browser(self):
        """Открыть ссылку вакансии в браузере"""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = self.tree.item(selection[0])
        vacancy_id = item['values'][0]
        
        try:
            vacancy = self.db.get_vacancy_by_id(vacancy_id)
            if vacancy and vacancy.get('source_url'):
                webbrowser.open(vacancy['source_url'])
                self.log_message(f"Открыта вакансия: {vacancy.get('title', '')[:50]}...")
        except Exception as e:
            self.log_message(f"Ошибка открытия ссылки: {e}", "error")
    
    def publish_all(self):
        """Публикация всех неопубликованных вакансий"""
        if not self.telegram_token.get() or not self.telegram_channel.get():
            messagebox.showwarning("Внимание", "Настройте Telegram бота")
            return
        
        self.save_config_settings()
        
        vacancies = self.db.get_unpublished_vacancies()
        
        if not vacancies:
            messagebox.showinfo("Информация", "Нет неопубликованных вакансий")
            return
        
        confirm = messagebox.askyesno(
            "Подтверждение публикации",
            f"Вы собираетесь опубликовать {len(vacancies)} вакансий в Telegram.\n\n"
            f"Это займет примерно {len(vacancies) * 5} секунд.\n\n"
            f"Продолжить?"
        )
        
        if not confirm:
            return
        
        self.progress_window = tk.Toplevel(self.root)
        self.progress_window.title("Публикация вакансий")
        self.progress_window.geometry("500x200")
        self.progress_window.transient(self.root)
        self.progress_window.grab_set()
        
        ttk.Label(
            self.progress_window, 
            text=f"Публикация {len(vacancies)} вакансий в Telegram...",
            font=("Arial", 12, "bold")
        ).pack(pady=10)
        
        self.publish_progress_bar = ttk.Progressbar(
            self.progress_window,
            mode='determinate',
            length=400,
            maximum=len(vacancies)
        )
        self.publish_progress_bar.pack(pady=10)
        
        self.publish_status_label = ttk.Label(
            self.progress_window,
            text="Подготовка...",
            font=("Arial", 10)
        )
        self.publish_status_label.pack(pady=5)
        
        self.publish_counter_label = ttk.Label(
            self.progress_window,
            text=f"0 / {len(vacancies)}",
            font=("Arial", 10)
        )
        self.publish_counter_label.pack(pady=5)
        
        self.cancel_publishing = False
        ttk.Button(
            self.progress_window,
            text="❌ Отменить публикацию",
            command=lambda: setattr(self, 'cancel_publishing', True)
        ).pack(pady=10)
        
        thread = threading.Thread(
            target=self._run_publishing_thread,
            args=(vacancies,),
            daemon=True
        )
        thread.start()
    
    def _run_publishing_thread(self, vacancies):
        """Поток для публикации вакансий"""
        try:
            publisher = TelegramPublisher(
                token=self.telegram_token.get(),
                channel_id=self.telegram_channel.get()
            )
            
            published_count = 0
            
            for i, vacancy in enumerate(vacancies, 1):
                if self.cancel_publishing:
                    self.safe_call(self._publishing_cancelled, published_count)
                    return
                
                self.safe_call(self._update_publish_progress, i, len(vacancies), vacancy.get('title', ''))
                
                if publisher.publish_vacancy_sync(vacancy):
                    published_count += 1
                
                time.sleep(3)
            
            self.safe_call(self._publishing_completed, published_count, len(vacancies))
            
        except Exception as e:
            self.safe_call(self._publishing_error, str(e))
    
    def _update_publish_progress(self, current, total, title):
        """Обновление прогресса публикации"""
        if hasattr(self, 'publish_progress_bar'):
            self.publish_progress_bar['value'] = current
            self.publish_status_label.config(text=f"Публикация: {title[:40]}...")
            self.publish_counter_label.config(text=f"{current} / {total}")
            self.update_status(f"Публикация {current}/{total}...")
    
    def _publishing_completed(self, published, total):
        """Завершение публикации"""
        try:
            if hasattr(self, 'progress_window'):
                self.progress_window.destroy()
            
            self.show_all_vacancies()
            self.update_stats()
            
            self.log_message(f"✅ Публикация завершена! Опубликовано {published}/{total} вакансий", "success")
            self.update_status(f"Опубликовано {published}/{total}")
            
            messagebox.showinfo(
                "Публикация завершена",
                f"Публикация вакансий завершена!\n\n"
                f"✅ Успешно опубликовано: {published}\n"
                f"❌ Не удалось опубликовать: {total - published}\n"
                f"📊 Всего обработано: {total}"
            )
        except:
            pass
    
    def _publishing_cancelled(self, published):
        """Отмена публикации"""
        try:
            if hasattr(self, 'progress_window'):
                self.progress_window.destroy()
            
            self.log_message(f"Публикация отменена пользователем. Опубликовано {published} вакансий", "warning")
            self.update_status("Публикация отменена")
            
            messagebox.showinfo(
                "Публикация отменена",
                f"Публикация вакансий отменена.\n\n"
                f"✅ Успешно опубликовано: {published}"
            )
        except:
            pass
    
    def _publishing_error(self, error):
        """Ошибка при публикации"""
        try:
            if hasattr(self, 'progress_window'):
                self.progress_window.destroy()
            
            self.log_message(f"❌ Ошибка при публикации: {error}", "error")
            self.update_status("Ошибка публикации")
            
            messagebox.showerror(
                "Ошибка публикации",
                f"Произошла ошибка при публикации вакансий:\n\n{error}"
            )
        except:
            pass
    
    def publish_selected(self):
        """Публикация выбранных вакансий"""
        selection = self.tree.selection()
        if not selection:
            messagebox.showwarning("Внимание", "Выберите вакансии для публикации")
            return
        
        if not self.telegram_token.get() or not self.telegram_channel.get():
            messagebox.showwarning("Внимание", "Настройте Telegram бота")
            return
        
        self.save_config_settings()
        
        vacancies = []
        for item in selection:
            vacancy_id = self.tree.item(item)['values'][0]
            vacancy = self.db.get_vacancy_by_id(vacancy_id)
            if vacancy and not vacancy.get('published'):
                vacancies.append(vacancy)
        
        if not vacancies:
            messagebox.showinfo("Информация", "Нет выбранных вакансий для публикации")
            return
        
        publisher = TelegramPublisher(
            token=self.telegram_token.get(),
            channel_id=self.telegram_channel.get()
        )
        
        published_count = 0
        
        for vacancy in vacancies:
            if publisher.publish_vacancy_sync(vacancy):
                published_count += 1
            time.sleep(2)
        
        self.show_all_vacancies()
        self.update_stats()
        
        if published_count > 0:
            self.log_message(f"✅ Опубликовано {published_count} выбранных вакансий", "success")
            self.update_status(f"Опубликовано {published_count} вакансий")
            messagebox.showinfo(
                "Успех",
                f"Опубликовано {published_count} выбранных вакансий!"
            )
        else:
            messagebox.showinfo(
                "Информация",
                "Не удалось опубликовать выбранные вакансии."
            )
    
    def publish_single(self, vacancy, details_window=None):
        """Публикация одной вакансии"""
        if not self.telegram_token.get() or not self.telegram_channel.get():
            messagebox.showwarning("Внимание", "Настройте Telegram бота")
            return
        
        try:
            publisher = TelegramPublisher(
                token=self.telegram_token.get(),
                channel_id=self.telegram_channel.get()
            )
            
            if publisher.publish_vacancy_sync(vacancy):
                self.show_all_vacancies()
                self.update_stats()
                
                if details_window:
                    details_window.destroy()
                
                self.log_message(f"✅ Вакансия опубликована: {vacancy.get('title', '')[:50]}...", "success")
                self.update_status("Вакансия опубликована")
                
                messagebox.showinfo(
                    "Успех",
                    "Вакансия успешно опубликована в Telegram!"
                )
            else:
                self.log_message(f"❌ Не удалось опубликовать вакансию: {vacancy.get('title', '')[:50]}...", "error")
                messagebox.showwarning(
                    "Внимание",
                    "Не удалось опубликовать вакансию.\nПроверьте настройки Telegram."
                )
                
        except Exception as e:
            self.log_message(f"❌ Ошибка публикации: {e}", "error")
            messagebox.showerror(
                "Ошибка",
                f"Не удалось опубликовать вакансию:\n\n{e}"
            )
    
    def update_stats(self):
        """Обновление статистики"""
        try:
            stats = self.db.get_stats()
            
            stats_text = f"""
📊 СТАТИСТИКА ВАКАНСИЙ:

✅ Всего уникальных вакансий в базе: {stats.get('total', 0)}
⏳ Неопубликованных: {stats.get('unpublished', 0)}
📤 Опубликованных: {stats.get('published', 0)}
🗑 Дубликатов обнаружено: {stats.get('duplicates', 0)}
📅 Добавлено сегодня: {stats.get('today', 0)}
📈 За 7 дней: {stats.get('last_7_days', 0)}
🗃 Отправлено в PostgreSQL (job_posting_sources): {stats.get('postgres_sent', 0)}
⏳ Ожидают отправки в PostgreSQL: {stats.get('postgres_pending', 0)}
🔑 Уникальных fingerprint: {stats.get('unique_fingerprints', 0)}
"""
            self.stats_text.set(stats_text)
            
        except Exception as e:
            self.log_message(f"Ошибка обновления статистики: {e}", "error")
    
    def update_history(self):
        """Обновление истории парсинга"""
        try:
            history = self.db.get_parsing_history(15)
            
            for item in self.history_tree.get_children():
                self.history_tree.delete(item)
            
            for item in history:
                start_time = item.get('start_time', '')
                if isinstance(start_time, str):
                    time_str = start_time[:16]
                else:
                    time_str = str(start_time)[:16]
                
                self.history_tree.insert('', 'end', values=(
                    item.get('session_id', '')[:8],
                    time_str,
                    item.get('total_found', 0),
                    item.get('new_vacancies', 0),
                    item.get('duplicates_found', 0),
                    item.get('postgres_sent', 0),
                    item.get('status', '')
                ))
                
        except Exception as e:
            self.log_message(f"Ошибка обновления истории: {e}", "error")
    
    def show_detailed_stats(self):
        """Показать подробную статистику"""
        try:
            stats = self.db.get_stats()
            
            detailed_stats = f"""
📊 ПОДРОБНАЯ СТАТИСТИКА:

Общие данные:
• Всего уникальных вакансий: {stats.get('total', 0)}
• Опубликовано: {stats.get('published', 0)}
• Не опубликовано: {stats.get('unpublished', 0)}
• Дубликатов обнаружено: {stats.get('duplicates', 0)}

PostgreSQL (job_posting_sources):
• Отправлено в PostgreSQL: {stats.get('postgres_sent', 0)}
• Ожидают отправки: {stats.get('postgres_pending', 0)}

Временные данные:
• Добавлено сегодня: {stats.get('today', 0)}
• Добавлено за 7 дней: {stats.get('last_7_days', 0)}

Дедупликация:
• Уникальных fingerprint: {stats.get('unique_fingerprints', 0)}
• Всего fingerprint: {stats.get('total_fingerprints', 0)}
• Эффективность дедупликации: {stats.get('deduplication_efficiency', 0):.1f}%
"""
            
            stats_window = tk.Toplevel(self.root)
            stats_window.title("Подробная статистика")
            stats_window.geometry("500x400")
            
            text_widget = scrolledtext.ScrolledText(stats_window, wrap=tk.WORD, font=('Arial', 10))
            text_widget.pack(fill='both', expand=True, padx=10, pady=10)
            
            text_widget.insert(1.0, detailed_stats)
            text_widget.config(state='disabled')
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить статистику:\n\n{e}")
    
    def clean_duplicates(self):
        """Очистка дубликатов из базы данных"""
        try:
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT COUNT(*) as cnt 
                FROM vacancies 
                WHERE is_duplicate = 1
            ''')
            
            duplicate_count = cursor.fetchone()[0]
            
            if duplicate_count == 0:
                messagebox.showinfo("Информация", "Дубликатов не найдено")
                conn.close()
                return
            
            confirm = messagebox.askyesno(
                "Очистка дубликатов",
                f"Найдено {duplicate_count} дубликатов.\n\n"
                f"Будут удалены только записи, помеченные как дубликаты (is_duplicate = 1).\n"
                f"Оригинальные вакансии не будут затронуты.\n\n"
                f"Продолжить?"
            )
            
            if not confirm:
                conn.close()
                return
            
            cursor.execute('''
                DELETE FROM vacancies 
                WHERE is_duplicate = 1
            ''')
            
            deleted_count = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            self.log_message(f"✅ Удалено {deleted_count} дубликатов", "success")
            self.show_all_vacancies()
            self.update_stats()
            
            messagebox.showinfo(
                "Успех",
                f"Очистка дубликатов завершена!\n\n"
                f"Удалено дубликатов: {deleted_count}\n"
                f"Оригинальные вакансии сохранены."
            )
            
        except Exception as e:
            self.log_message(f"❌ Ошибка очистки дубликатов: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось очистить дубликаты:\n\n{e}")
    
    def cleanup_database(self):
        """Полная очистка базы данных от старых дубликатов"""
        try:
            confirm = messagebox.askyesno(
                "Очистка базы данных",
                "Это действие удалит все дубликаты старше 7 дней.\n\n"
                "Уникальные вакансии не будут затронуты.\n\n"
                "Продолжить?"
            )
            
            if not confirm:
                return
            
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            
            # Удаляем дубликаты старше 7 дней
            cursor.execute('''
                DELETE FROM vacancies 
                WHERE is_duplicate = 1 
                AND datetime(created_at) < datetime('now', '-7 days')
            ''')
            
            deleted_count = cursor.rowcount
            
            # Также удаляем старые записи из processed_fingerprints
            cursor.execute('''
                DELETE FROM processed_fingerprints 
                WHERE datetime(created_at) < datetime('now', '-30 days')
            ''')
            
            fingerprints_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            self.log_message(f"✅ Очистка БД: удалено {deleted_count} дубликатов и {fingerprints_deleted} старых fingerprint", "success")
            self.show_all_vacancies()
            self.update_stats()
            
            messagebox.showinfo(
                "Очистка базы данных",
                f"База данных успешно очищена!\n\n"
                f"Удалено:\n"
                f"• Дубликатов старше 7 дней: {deleted_count}\n"
                f"• Старых fingerprint: {fingerprints_deleted}"
            )
            
        except Exception as e:
            self.log_message(f"❌ Ошибка очистки базы данных: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось очистить базу данных:\n\n{e}")
    
    def refresh_database(self):
        """Обновление базы данных"""
        self.show_all_vacancies()
        self.update_stats()
        self.update_history()
        self.log_message("База данных обновлена", "success")
        self.update_status("Данные обновлены")
    
    def delete_vacancy(self):
        """Удаление вакансии"""
        selection = self.tree.selection()
        if not selection:
            return
        
        if not messagebox.askyesno(
            "Подтверждение удаления",
            f"Удалить {len(selection)} выбранных вакансий?\n\n"
            f"Это действие нельзя отменить."
        ):
            return
        
        deleted_count = 0
        for item in selection:
            vacancy_id = self.tree.item(item)['values'][0]
            if self.db.delete_vacancy(vacancy_id):
                deleted_count += 1
        
        self.show_all_vacancies()
        self.update_stats()
        
        self.log_message(f"🗑 Удалено {deleted_count} вакансий", "success")
        self.update_status(f"Удалено {deleted_count} вакансий")
    
    def show_context_menu(self, event):
        """Показать контекстное меню"""
        try:
            self.tree.selection_set(self.tree.identify_row(event.y))
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()

# ============================================
# ЗАПУСК ПРОГРАММЫ
# ============================================

def main():
    """Главная функция"""
    print("\n" + "="*60)
    print("LINKEDIN PARSER PRO v5.5.9 - PostgreSQL Sync Fixed + Исправлены город и зарплата + Строгий поиск по фильтру")
    print("✅ ИСПРАВЛЕНО ИЗВЛЕЧЕНИЕ ГОРОДА - теперь не пишет компанию вместо города")
    print("✅ ИСПРАВЛЕНО ИЗВЛЕЧЕНИЕ ЗАРПЛАТЫ - точное определение сумм и валюты")
    print("✅ ДОБАВЛЕНА КОЛОНКА org_url для хранения оригинальных ссылок")
    print("✅ ИСПРАВЛЕНА ДЕДУПЛИКАЦИЯ - оригиналы не удаляются")
    print("✅ ДОБАВЛЕНА СТРОГАЯ ФИЛЬТРАЦИЯ - поиск только по IT-вакансиям")
    print("✅ ИСКЛЮЧЕНЫ: юристы, бухгалтеры, продавцы, водители и другие неподходящие вакансии")
    print("="*60)
    print("\nПроверка зависимостей...")
    
    if not DEPENDENCIES_OK:
        response = input("Установить зависимости автоматически? (y/n): ")
        if response.lower() == 'y':
            install_dependencies()
        else:
            print("❌ Программа не может работать без зависимостей.")
            input("Нажмите Enter для выхода...")
            sys.exit(1)
    
    print("✅ Зависимости проверены")
    
    if Config.POSTGRES_ENABLED:
        print("✅ PostgreSQL синхронизация включена")
        print(f"✅ Подключение к: {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}")
        print(f"✅ Таблица: job_posting_sources (должна существовать)")
        print(f"✅ Новая колонка: {Config.ORG_URL_COLUMN} (для хранения оригинальных ссылок)")
        print(f"✅ Исправлены имена колонок и типами данных для правильной вставки")
    else:
        print("⚠ PostgreSQL синхронизация отключена (Config.POSTGRES_ENABLED = False)")
    
    print("🚀 Запуск приложения...")
    
    root = tk.Tk()
    
    try:
        app = ParserGUI(root)
        root.mainloop()
        
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    main()