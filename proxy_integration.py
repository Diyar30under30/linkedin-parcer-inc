"""
Интеграция с Proxy Pool Service для LinkedIn Parser
"""
import requests
import logging
import time
import uuid
from typing import Optional
from urllib.parse import urlparse

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProxyPoolClient:
    """Клиент для работы с Proxy Pool Service"""
    
    def __init__(self, proxy_pool_url: str = "http://localhost:8000"):
        self.proxy_pool_url = proxy_pool_url.rstrip('/')
        self.session = requests.Session()
        self.session.timeout = 30
        self.current_proxy = None
        self.run_id = None
        
    def set_run_id(self, run_id: str):
        """Установка run_id для sticky прокси"""
        self.run_id = run_id
    
    def get_proxy(self, target: str = "linkedin") -> Optional[str]:
        """Получение прокси из Proxy Pool"""
        try:
            params = {"target": target}
            if self.run_id:
                params["sticky_key"] = self.run_id
            
            response = self.session.get(
                f"{self.proxy_pool_url}/proxy",
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                proxy = data.get("proxy")
                if proxy:
                    self.current_proxy = proxy
                    logger.info(f"Получен прокси: {proxy}")
                    return proxy
            else:
                logger.warning(f"Не удалось получить прокси: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Ошибка получения прокси: {e}")
        
        return None
    
    def ban_proxy(self, reason: str, target: str = "linkedin"):
        """Бан прокси при обнаружении блокировки"""
        if not self.current_proxy:
            return
        
        try:
            data = {
                "proxy": self.current_proxy,
                "reason": reason,
                "target": target
            }
            
            response = self.session.post(
                f"{self.proxy_pool_url}/ban",
                json=data,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Прокси забанен: {self.current_proxy}, причина: {reason}")
            else:
                logger.warning(f"Не удалось забанить прокси: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Ошибка бана прокси: {e}")
    
    def report_ok(self, target: str = "linkedin"):
        """Отчет об успешном использовании прокси"""
        if not self.current_proxy:
            return
        
        try:
            data = {"proxy": self.current_proxy, "target": target}
            
            response = self.session.post(
                f"{self.proxy_pool_url}/ok",
                json=data,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.debug(f"Прокси отмечен как OK: {self.current_proxy}")
                
        except Exception as e:
            logger.error(f"Ошибка отчета OK: {e}")


class ProxySeleniumManager:
    """Управление Selenium с прокси из Proxy Pool"""
    
    def __init__(self, proxy_pool_url: str = "http://localhost:8000"):
        self.proxy_pool = ProxyPoolClient(proxy_pool_url)
        self.run_id = f"linkedin_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        self.proxy_pool.set_run_id(self.run_id)
        logger.info(f"ProxySeleniumManager инициализирован с run_id: {self.run_id}")
    
    def setup_chrome_with_proxy(self, chrome_options, headless=False):
        """Настройка Chrome с прокси из Proxy Pool"""
        try:
            # Получаем прокси
            proxy_url = self.proxy_pool.get_proxy("linkedin")
            
            if not proxy_url:
                logger.warning("Не удалось получить прокси, работаем без прокси")
                return chrome_options
            
            # Парсим прокси URL
            parsed = urlparse(proxy_url)
            
            # Формируем строку прокси для Selenium
            proxy_server = f"{parsed.hostname}:{parsed.port}"
            
            # Добавляем авторизацию если есть
            if parsed.username and parsed.password:
                logger.info(f"Прокси с авторизацией: {parsed.hostname}:{parsed.port}")
            
            # Настраиваем прокси для Chrome
            chrome_options.add_argument(f'--proxy-server=http://{proxy_server}')
            
            logger.info(f"Настроен прокси: {parsed.hostname}:{parsed.port}")
            
            return chrome_options
            
        except Exception as e:
            logger.error(f"Ошибка настройки прокси: {e}")
            return chrome_options
    
    def handle_block(self, page_content: str, url: str = "") -> bool:
        """Обработка блокировки/капчи"""
        if not page_content:
            return False
            
        block_indicators = [
            "403", "429", "captcha", "recaptcha", 
            "access denied", "blocked", "rate limit",
            "подозрительная активность", "security check"
        ]
        
        content_lower = page_content.lower()
        
        for indicator in block_indicators:
            if indicator in content_lower:
                logger.warning(f"Обнаружена блокировка ({indicator}) на {url}")
                self.proxy_pool.ban_proxy(f"block_{indicator}")
                return True
        
        return False
    
    def report_success(self):
        """Сообщаем об успешном использовании прокси"""
        self.proxy_pool.report_ok()