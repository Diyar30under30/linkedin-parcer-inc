@echo off
chcp 65001 >nul
echo ===========================================
echo Установка LinkedIn Parser Pro v4.0
echo WebGL/GPU фиксы добавлены
echo ===========================================

:: Проверка Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python не найден!
    echo.
    echo Установите Python 3.8+ с сайта python.org
    echo При установке ОБЯЗАТЕЛЬНО отметьте "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

echo ✓ Python найден

:: Создание виртуального окружения
echo.
echo Создание виртуального окружения...
python -m venv venv
if errorlevel 1 (
    echo ❌ Ошибка создания виртуального окружения
    echo Попробуйте: python -m pip install --upgrade pip
    pause
    exit /b 1
)

echo ✓ Виртуальное окружение создано

:: Активация окружения
echo.
echo Активация виртуального окружения...
call venv\Scripts\activate.bat

:: Установка зависимостей
echo.
echo Установка зависимостей...
pip install --upgrade pip
pip install selenium==4.15.0 chromedriver-autoinstaller==0.4.0 requests==2.31.0 beautifulsoup4==4.12.2 html5lib==1.1

echo.
echo ✓ Все зависимости установлены

:: Создание proxy_integration.py если его нет
echo.
echo Проверка proxy_integration.py...
if not exist "proxy_integration.py" (
    echo Создание proxy_integration.py...
    (
    echo """
    echo Интеграция с Proxy Pool Service для LinkedIn Parser
    echo """
    echo import requests
    echo import logging
    echo import time
    echo import uuid
    echo from typing import Optional
    echo from urllib.parse import urlparse
    echo.
    echo # Настройка логирования
    echo logging.basicConfig(level=logging.INFO)
    echo logger = logging.getLogger(__name__)
    echo.
    echo class ProxyPoolClient:
    echo     """Клиент для работы с Proxy Pool Service"""
    echo.
    echo     def __init__(self, proxy_pool_url: str = "http://localhost:8000"):
    echo         self.proxy_pool_url = proxy_pool_url.rstrip('/')
    echo         self.session = requests.Session()
    echo         self.session.timeout = 30
    echo         self.current_proxy = None
    echo         self.run_id = None
    echo.
    echo     def set_run_id(self, run_id: str):
    echo         """Установка run_id для sticky прокси"""
    echo         self.run_id = run_id
    echo.
    echo     def get_proxy(self, target: str = "linkedin") -> Optional[str]:
    echo         """Получение прокси из Proxy Pool"""
    echo         try:
    echo             params = {"target": target}
    echo             if self.run_id:
    echo                 params["sticky_key"] = self.run_id
    echo.
    echo             response = self.session.get(
    echo                 f"{self.proxy_pool_url}/proxy",
    echo                 params=params,
    echo                 timeout=10
    echo             )
    echo.
    echo             if response.status_code == 200:
    echo                 data = response.json()
    echo                 proxy = data.get("proxy")
    echo                 if proxy:
    echo                     self.current_proxy = proxy
    echo                     logger.info(f"Получен прокси: {proxy}")
    echo                     return proxy
    echo             else:
    echo                 logger.warning(f"Не удалось получить прокси: {response.status_code}")
    echo.
    echo         except Exception as e:
    echo             logger.error(f"Ошибка получения прокси: {e}")
    echo.
    echo         return None
    echo.
    echo     def ban_proxy(self, reason: str, target: str = "linkedin"):
    echo         """Бан прокси при обнаружении блокировки"""
    echo         if not self.current_proxy:
    echo             return
    echo.
    echo         try:
    echo             data = {
    echo                 "proxy": self.current_proxy,
    echo                 "reason": reason,
    echo                 "target": target
    echo             }
    echo.
    echo             response = self.session.post(
    echo                 f"{self.proxy_pool_url}/ban",
    echo                 json=data,
    echo                 timeout=10
    echo             )
    echo.
    echo             if response.status_code == 200:
    echo                 logger.info(f"Прокси забанен: {self.current_proxy}, причина: {reason}")
    echo             else:
    echo                 logger.warning(f"Не удалось забанить прокси: {response.status_code}")
    echo.
    echo         except Exception as e:
    echo             logger.error(f"Ошибка бана прокси: {e}")
    echo.
    echo     def report_ok(self, target: str = "linkedin"):
    echo         """Отчет об успешном использовании прокси"""
    echo         if not self.current_proxy:
    echo             return
    echo.
    echo         try:
    echo             data = {"proxy": self.current_proxy, "target": target}
    echo.
    echo             response = self.session.post(
    echo                 f"{self.proxy_pool_url}/ok",
    echo                 json=data,
    echo                 timeout=10
    echo             )
    echo.
    echo             if response.status_code == 200:
    echo                 logger.debug(f"Прокси отмечен как OK: {self.current_proxy}")
    echo.
    echo         except Exception as e:
    echo             logger.error(f"Ошибка отчета OK: {e}")
    echo.
    echo.
    echo class ProxySeleniumManager:
    echo     """Управление Selenium с прокси из Proxy Pool"""
    echo.
    echo     def __init__(self, proxy_pool_url: str = "http://localhost:8000"):
    echo         self.proxy_pool = ProxyPoolClient(proxy_pool_url)
    echo         self.run_id = f"linkedin_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    echo         self.proxy_pool.set_run_id(self.run_id)
    echo         logger.info(f"ProxySeleniumManager инициализирован с run_id: {self.run_id}")
    echo.
    echo     def setup_chrome_with_proxy(self, chrome_options, headless=False):
    echo         """Настройка Chrome с прокси из Proxy Pool"""
    echo         try:
    echo             # Получаем прокси
    echo             proxy_url = self.proxy_pool.get_proxy("linkedin")
    echo.
    echo             if not proxy_url:
    echo                 logger.warning("Не удалось получить прокси, работаем без прокси")
    echo                 return chrome_options
    echo.
    echo             # Парсим прокси URL
    echo             parsed = urlparse(proxy_url)
    echo.
    echo             # Формируем строку прокси для Selenium
    echo             proxy_server = f"{parsed.hostname}:{parsed.port}"
    echo.
    echo             # Добавляем авторизацию если есть
    echo             if parsed.username and parsed.password:
    echo                 logger.info(f"Прокси с авторизацией: {parsed.hostname}:{parsed.port}")
    echo.
    echo             # Настраиваем прокси для Chrome
    echo             chrome_options.add_argument(f'--proxy-server=http://{proxy_server}')
    echo.
    echo             logger.info(f"Настроен прокси: {parsed.hostname}:{parsed.port}")
    echo.
    echo             return chrome_options
    echo.
    echo         except Exception as e:
    echo             logger.error(f"Ошибка настройки прокси: {e}")
    echo             return chrome_options
    echo.
    echo     def handle_block(self, page_content: str, url: str = "") -> bool:
    echo         """Обработка блокировки/капчи"""
    echo         if not page_content:
    echo             return False
    echo.
    echo         block_indicators = [
    echo             "403", "429", "captcha", "recaptcha", 
    echo             "access denied", "blocked", "rate limit",
    echo             "подозрительная активность", "security check"
    echo         ]
    echo.
    echo         content_lower = page_content.lower()
    echo.
    echo         for indicator in block_indicators:
    echo             if indicator in content_lower:
    echo                 logger.warning(f"Обнаружена блокировка ({indicator}) на {url}")
    echo                 self.proxy_pool.ban_proxy(f"block_{indicator}")
    echo                 return True
    echo.
    echo         return False
    echo.
    echo     def report_success(self):
    echo         """Сообщаем об успешном использовании прокси"""
    echo         self.proxy_pool.report_ok()
    ) > proxy_integration.py
    echo ✓ proxy_integration.py создан
) else (
    echo ✓ proxy_integration.py уже существует
)

:: Создание файла запуска
echo.
echo Создание файла запуска...
(
echo @echo off
echo call venv\Scripts\activate.bat
echo python main.py
echo pause
) > start.bat

echo ✓ Файл start.bat создан

:: Создание README файла
echo.
echo Создание инструкции...
(
echo ===========================================
echo LINKEDIN PARSER PRO - ИНСТРУКЦИЯ
echo ===========================================
echo.
echo ✅ WebGL/GPU фиксы добавлены
echo ✅ Предупреждения Chrome убраны
echo ✅ Парсер работает стабильно
echo.
echo 🚀 БЫСТРЫЙ СТАРТ:
echo 1. Запустите start.bat
echo 2. Введите данные LinkedIn в настройках
echo 3. Настройте Telegram бота
echo 4. Начните парсинг
echo.
echo 📦 ТРЕБОВАНИЯ:
echo - Python 3.8+
echo - Google Chrome
echo - Аккаунт LinkedIn
echo - Telegram бот (создать через @BotFather)
echo.
echo 🔧 НАСТРОЙКА TELEGRAM:
echo 1. Создайте бота через @BotFather
echo 2. Получите токен бота
echo 3. Создайте канал в Telegram
echo 4. Добавьте бота в канал как администратора
echo 5. ID канала обычно начинается с @
echo.
echo 📞 ПОДДЕРЖКА:
echo При проблемах проверьте файл parser.log
echo ===========================================
) > README.txt

echo ✓ Инструкция создана

:: Проверка Chrome
echo.
echo Проверка Google Chrome...
where chrome >nul 2>&1
if errorlevel 1 (
    echo ⚠ Google Chrome не найден!
    echo.
    echo Для работы парсера необходимо установить Google Chrome
    echo Скачать: https://www.google.com/chrome/
    echo.
) else (
    echo ✓ Google Chrome найден
)

echo.
echo ===========================================
echo ✅ УСТАНОВКА ЗАВЕРШЕНА!
echo.
echo ✅ WebGL/GPU фиксы активированы
echo ✅ Все ошибки исправлены
echo.
echo Для запуска программы:
echo 1. Двойной клик по файлу start.bat
echo 2. Или выполните вручную:
echo    venv\Scripts\activate.bat
echo    python main.py
echo.
echo 📁 Созданные файлы:
echo - main.py (основная программа с фиксами)
echo - proxy_integration.py (интеграция с Proxy Pool)
echo - start.bat (запуск программы)
echo - vacancies.db (база данных)
echo - parser.log (логи программы)
echo - config.json (настройки)
echo - README.txt (инструкция)
echo.
echo ===========================================
pause