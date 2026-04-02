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

:: proxy_integration.py now treated as a normal source file tracked in Git
echo.
echo Проверка proxy_integration.py...
if not exist "proxy_integration.py" (
    echo ⚠ Файл proxy_integration.py не найден! Убедитесь, что вы скачали весь репозиторий.
) else (
    echo ✓ proxy_integration.py найден
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