"""
Скрипт для запуска парсинга по расписанию в Kubernetes
"""
import os
import sys
import logging
from datetime import datetime

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main import LinkedInParser, Database, Config

def run_cron_job():
    """Запуск парсинга по расписанию"""
    print("=" * 60)
    print(f"CRON JOB: Запуск парсинга LinkedIn - {datetime.now()}")
    print("=" * 60)
    
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'parser_cron_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Получаем настройки из переменных окружения
        use_proxy_pool = os.getenv("USE_PROXY_POOL", "false").lower() == "true"
        headless = os.getenv("HEADLESS", "true").lower() == "true"
        
        # Создаем парсер
        config = Config()
        parser = LinkedInParser(
            email=config.get("linkedin_email", ""),
            password=config.get("linkedin_password", ""),
            headless=headless,
            use_proxy_pool=use_proxy_pool
        )
        
        # Запускаем парсинг
        logger.info("Начало парсинга LinkedIn...")
        vacancies = parser.run_parsing()
        
        # Получаем статистику
        db = Database()
        stats = db.get_stats()
        
        # Логируем результат
        logger.info(f"Парсинг завершен. Найдено вакансий: {len(vacancies)}")
        logger.info(f"Статистика: всего {stats['total']}, новых {stats['unpublished']}")
        
        print("=" * 60)
        print(f"CRON JOB: Завершено успешно")
        print(f"Найдено вакансий: {len(vacancies)}")
        print(f"Всего в базе: {stats['total']}")
        print(f"Неопубликованных: {stats['unpublished']}")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Ошибка в cron job: {e}")
        print(f"❌ Ошибка: {e}")
        return 1

if __name__ == "__main__":
    # Проверяем, запущен ли скрипт как cron job
    if len(sys.argv) > 1 and sys.argv[1] == "--cron":
        sys.exit(run_cron_job())
    else:
        print("Использование: python run_cron.py --cron")
        sys.exit(1)