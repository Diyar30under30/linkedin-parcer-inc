"""
parser.py — LinkedIn scraping layer.

Depends on: config.Config, database.Database, selenium, bs4, stdlib
"""

import re
import time
import random
import logging
import sqlite3
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

from config import Config
from database import Database


class LinkedInParser:
    """Scrapes LinkedIn job listings and persists results via Database."""

    def __init__(self, email="", password="", headless=True,
                 use_proxy_pool=False, auto_mode=False):
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
            "start_time": datetime.now(),
        }

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(Config.LOG_FILE, encoding='utf-8'),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

        if self.auto_mode:
            self.logger.info(f"Авто-режим парсинга. Session ID: {self.session_id}")

    # ── WebDriver ─────────────────────────────────────────────────────────

    def setup_driver(self):
        """Initialise Chrome WebDriver via webdriver-manager with anti-bot options."""
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            if self.auto_mode:
                chrome_options.add_argument("--disable-features=UserAgentClientHint")

            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_experimental_option("prefs", {
                "profile.default_content_setting_values.notifications": 2,
                "credentials_enable_service": False,
                "profile.password_manager_enabled": False,
            })

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            self.logger.info("WebDriver успешно настроен")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка настройки WebDriver: {e}")
            self.logger.info("Пробуем альтернативный метод...")
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
                self.logger.error(f"Альтернативный метод не сработал: {e2}")
                return False

    # ── Auth ──────────────────────────────────────────────────────────────

    def login(self):
        """Log in to LinkedIn; return True on success."""
        try:
            self.logger.info("Вход в LinkedIn...")
            self.driver.get("https://www.linkedin.com/login")
            time.sleep(3)

            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "username"))
            )

            self.driver.find_element(By.ID, "username").send_keys(self.email)
            time.sleep(1)
            self.driver.find_element(By.ID, "password").send_keys(self.password)
            time.sleep(1)
            self.driver.find_element(By.XPATH, "//button[@type='submit']").click()
            time.sleep(5)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[role='combobox']"))
                )
                self.logger.info("Успешный вход в LinkedIn")
                return True
            except Exception:
                if "captcha" in self.driver.page_source.lower():
                    self.logger.warning("Обнаружена капча")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка входа в LinkedIn: {e}")
            return False

    # ── Search ────────────────────────────────────────────────────────────

    def search_jobs(self, job_title, location):
        """Return a list of job-detail URLs matching *job_title* in *location*."""
        try:
            search_url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={job_title.replace(' ', '%20')}"
                f"&location={location.replace(' ', '%20')}"
                f"&f_TPR=r86400&sortBy=DD&f_AL=true&f_E=1%2C2%2C3%2C4"
            )
            self.logger.info(f"Поиск: {job_title} в {location}")
            self.driver.get(search_url)
            time.sleep(4)

            self._scroll_page(5)
            time.sleep(3)

            job_cards = self.driver.find_elements(
                By.CSS_SELECTOR,
                "div.job-card-container, div.job-card-list, "
                "li.jobs-search-results__list-item, div.occludable-update",
            )

            job_links = []
            seen_links: set = set()
            job_keywords = self._get_job_keywords(job_title)

            for card in job_cards[:Config.MAX_VACANCIES_PER_SEARCH * 2]:
                try:
                    if not self._filter_by_job_title(card.text.lower(), job_keywords):
                        continue
                    for link in card.find_elements(By.TAG_NAME, "a"):
                        href = link.get_attribute("href") or ""
                        if "/jobs/view/" in href and href not in seen_links:
                            if self._extract_job_id(href):
                                job_links.append(href)
                                seen_links.add(href)
                                break
                except Exception as e:
                    self.logger.debug(f"Ошибка обработки карточки: {e}")

            self.logger.info(f"Найдено {len(job_links)} вакансий для {job_title} в {location}")
            return job_links

        except Exception as e:
            self.logger.error(f"Ошибка поиска: {e}")
            return []

    # ── Page scraping ─────────────────────────────────────────────────────

    def parse_job_page(self, job_url):
        """Scrape a single job page and return a vacancy dict (or None to skip)."""
        try:
            self.driver.get(job_url)
            time.sleep(3)

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            title = self._extract_title(soup)
            company = self._extract_company(soup)
            location = self._extract_location(soup)
            description = self._extract_description(soup)
            salary = self._extract_salary(soup)

            if not title or title == "Не указано":
                self.logger.warning(f"Не удалось извлечь название для {job_url}")
                return None

            title_lower = title.lower()
            it_keywords = [
                "разработчик", "developer", "инженер", "engineer",
                "qa", "тестиров", "менеджер", "manager",
                "дизайнер", "designer", "аналитик", "analyst", "devops", "data",
            ]
            if not any(kw in title_lower for kw in it_keywords):
                self.logger.warning(f"Пропущена неподходящая вакансия: {title}")
                return None

            vacancy = {
                'title': title[:200],
                'company_name': (company or "")[:100],
                'description': (description or "Описание не найдено")[:4000],
                'salary': (salary or "не указана")[:100],
                'location': (location or "Не указана")[:100],
                'contact': job_url,
                'source': 'LinkedIn',
                'source_url': job_url,
            }
            self.logger.info(f"Обработана: {title[:50]}... (Компания: {(company or 'N/A')[:30]}, Город: {location})")
            return vacancy

        except Exception as e:
            self.logger.error(f"Ошибка парсинга {job_url}: {e}")
            return None

    # ── Field extractors ──────────────────────────────────────────────────

    def _extract_title(self, soup):
        for selector in [
            'h1', '.top-card-layout__title',
            '.jobs-unified-top-card__job-title',
            '.job-details-jobs-unified-top-card__job-title',
            'h1.job-title', '.jobs-details-top-card__job-title',
        ]:
            el = soup.select_one(selector)
            if el and el.text.strip():
                return el.text.strip()
        for h1 in soup.find_all('h1'):
            text = h1.text.strip()
            if text and len(text) > 5:
                return text
        return "Не указано"

    def _extract_company(self, soup):
        for selector in [
            '.top-card-layout__card-subtitle-item a',
            '.jobs-unified-top-card__company-name',
            '.job-details-jobs-unified-top-card__company-name',
            '.jobs-unified-top-card__primary-description-container a',
            '.jobs-details-top-card__company-url',
            '.jobs-company__box a',
        ]:
            el = soup.select_one(selector)
            if el:
                text = el.text.strip()
                if text:
                    return text
        for el in soup.find_all(['span', 'div', 'a'], class_=re.compile(r'company|employer|organization', re.I)):
            text = el.text.strip()
            if text and 2 < len(text) < 100:
                return text
        return ""

    def _extract_location(self, soup):
        try:
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
                '.jobs-details-top-card__company-info',
            ]
            company_name = self._extract_company(soup)

            for selector in location_selectors:
                for el in soup.select(selector):
                    text = el.get_text(strip=True)
                    if text and len(text) < 100:
                        if company_name and company_name in text:
                            continue
                        cleaned = self._clean_location_text(text)
                        if cleaned and cleaned != "Не указана":
                            return cleaned

            location_keywords = ['location', 'местоположение', 'город', 'адрес', 'расположение', 'регион', 'страна']
            for tag in ['span', 'div', 'li']:
                for el in soup.find_all(tag):
                    text = el.get_text(strip=True).lower()
                    if any(kw in text for kw in location_keywords):
                        cleaned = self._clean_location_text(el.get_text(strip=True))
                        if cleaned and cleaned != "Не указана":
                            return cleaned

            all_text = soup.get_text()
            for city in Config.CITIES:
                pattern = r'(?<!\w)' + re.escape(city.lower()) + r'(?!\w)'
                if re.search(pattern, all_text.lower()):
                    return city

            for tag in soup.find_all('meta'):
                if tag.get('property') in ['og:locality', 'og:region', 'og:country_name']:
                    content = tag.get('content', '')
                    cleaned = self._clean_location_text(content)
                    if cleaned and cleaned != "Не указана":
                        return cleaned

            return "Не указана"

        except Exception as e:
            self.logger.debug(f"Ошибка извлечения локации: {e}")
            return "Не указана"

    def _clean_location_text(self, text):
        try:
            if not text:
                return "Не указана"
            text = re.sub(r'\s+', ' ', text.strip())
            for prefix in ['Location:', 'Местоположение:', 'Город:', 'Адрес:', 'Локация:', 'Locality:', 'Region:']:
                if text.startswith(prefix):
                    text = text[len(prefix):].strip()
            for sep in ['·', '|', '•', '—', '-', '–']:
                if sep in text:
                    for part in text.split(sep):
                        part = part.strip()
                        if part and len(part) < 50 and self._looks_like_city(part):
                            return part
            if self._looks_like_city(text):
                return text[:50]
            return "Не указана"
        except Exception:
            return "Не указана"

    def _looks_like_city(self, text):
        if not text or len(text) > 50:
            return False
        for city in Config.CITIES:
            if city.lower() in text.lower():
                return True
        if re.match(r'^[А-ЯЁA-Z][а-яёa-z]+(?:\s+[А-ЯЁA-Z][а-яёa-z]+)*$', text):
            company_words = ['ltd', 'inc', 'company', 'корпорация', 'компания', 'ооо', 'зао', 'ао']
            if not any(w in text.lower() for w in company_words):
                return True
        return False

    def _extract_description(self, soup):
        for selector in [
            '.description__text', '.show-more-less-html__markup',
            '.jobs-description__content', '.jobs-description-content',
            '.jobs-box__html-content', '.jobs-description',
        ]:
            el = soup.select_one(selector)
            if el:
                for tag in el(['script', 'style', 'iframe', 'form']):
                    tag.decompose()
                text = el.get_text(separator='\n', strip=True)
                return re.sub(r'\n{3,}', '\n\n', text)[:5000]

        for div in soup.find_all('div', {'class': re.compile(r'desc|content|body', re.I)}):
            text = div.get_text(separator='\n', strip=True)
            if len(text) > 100:
                return re.sub(r'\n{3,}', '\n\n', text)[:5000]

        return "Описание не найдено"

    def _extract_salary(self, soup):
        try:
            salary_selectors = [
                '.salary', '.compensation', '.pay-scale',
                '.jobs-unified-top-card__job-insight',
                '.job-details-jobs-unified-top-card__job-insight',
                '.jobs-details-top-card__salary-info',
                '.job-details-jobs-unified-top-card__job-insight-text',
            ]
            for selector in salary_selectors:
                for el in soup.select(selector):
                    text = el.get_text(strip=True)
                    if text and self._is_salary_text(text):
                        return self._clean_salary_text(text)

            all_text = soup.get_text()
            salary_patterns = [
                r'(?:зарплата|зп|оклад|ставка|жалақы)[:\s]*([$\d\s.,-–—₸тгkztKZT]{10,80})',
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:₸|тг|тенге|kzt|KZT)',
                r'(\d[\d\s.,]*\d?)\s*[-–—]\s*(\d[\d\s.,]*\d?)\s*(?:₸|тг|тенге)',
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:₽|руб|рублей|RUB)',
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:[$]|usd|USD|доллар)',
                r'(?:от\s*)?(\d[\d\s.,]*\d?)\s*(?:до\s*)?(\d[\d\s.,]*\d?)?\s*(?:[€]|eur|EUR|евро)',
                r'\d[\d\s.,]{3,}?\s*(?:₸|₽|[$]|[€]|тг|тенге|руб|usd|eur|kzt)',
                r'(?:от\s*)?\d+[\d\s.,]*\d*\s*(?:тыс|к|k|т\.?)(?:\s*руб|\s*₽|\s*₸|\s*[$]|\s*[€])?',
            ]

            for pattern in salary_patterns:
                for match in re.finditer(pattern, all_text, re.IGNORECASE):
                    salary_text = match.group(0).strip()
                    if self._is_salary_text(salary_text):
                        cleaned = self._clean_salary_text(salary_text)
                        if cleaned and cleaned != "не указана":
                            return cleaned

            return "не указана"
        except Exception as e:
            self.logger.debug(f"Ошибка извлечения зарплаты: {e}")
            return "не указана"

    def _is_salary_text(self, text):
        if not text or len(text) < 4:
            return False
        text_lower = text.lower()
        has_digits = bool(re.search(r'\d', text))
        has_currency = any(c in text_lower for c in ['₽', '$', '€', '₸', '£', 'тенге', 'тг', 'руб', 'usd', 'eur', 'kzt'])
        has_keyword = any(k in text_lower for k in ['зарплата', 'зп', 'salary', 'оклад', 'ставка', 'жалақы', 'compensation'])
        has_range = bool(re.search(r'\d+\s*[-–—]\s*\d+', text))
        return has_digits and (has_currency or has_keyword or has_range)

    def _clean_salary_text(self, text):
        try:
            if not text:
                return "не указана"
            text = re.sub(r'\s+', ' ', text.strip())
            text = text.replace('–', '-').replace('—', '-')
            for old, new in {
                'тг': '₸', 'тенге': '₸', 'kzt': '₸',
                'руб': '₽', 'рублей': '₽', 'rur': '₽', 'rub': '₽',
                'доллар': '$', 'usd': '$',
                'евро': '€', 'eur': '€',
            }.items():
                text = re.sub(rf'\b{old}\b', new, text, flags=re.IGNORECASE)

            low = text.lower()
            if 'тыс' in low or 'к' in low or 'k' in low:
                for num in re.findall(r'(\d+[\d\s.,]*)', text):
                    clean = re.sub(r'[^\d]', '', num)
                    if clean:
                        text = text.replace(num, str(int(clean) * 1000))
                text = re.sub(r'тыс|к|k', '', text, flags=re.IGNORECASE)

            for word in ['зарплата', 'зп', 'оклад', 'ставка', 'жалақы', 'salary', 'compensation', 'от', 'до']:
                text = re.sub(rf'\b{word}\b\s*[:]?\s*', '', text, flags=re.IGNORECASE)

            return re.sub(r'\s+', ' ', text).strip()[:100]
        except Exception:
            return text if text else "не указана"

    # ── Filter helpers ────────────────────────────────────────────────────

    def _get_job_keywords(self, job_title):
        job_keywords_map = {
            "qa": ["qa", "тестиров", "quality assurance", "quality control", "test engineer"],
            "frontend": ["frontend", "front-end", "javascript", "react", "vue", "angular", "ui developer"],
            "backend": ["backend", "back-end", "python", "java", "node", "django", "spring", "api"],
            "fullstack": ["fullstack", "full-stack", "mern", "mean"],
            "devops": ["devops", "sre", "site reliability", "infrastructure", "cloud", "aws", "azure", "gcp"],
            "data": ["data scientist", "data analyst", "data engineer", "machine learning", "ml", "ai"],
            "manager": ["product manager", "project manager", "менеджер продукта", "менеджер проекта"],
            "designer": ["ui designer", "ux designer", "designer", "дизайнер", "ui/ux", "graphic designer"],
        }
        job_title_lower = job_title.lower()
        for key, keywords in job_keywords_map.items():
            if any(kw in job_title_lower for kw in keywords):
                return keywords
        return [w for w in job_title_lower.split() if len(w) > 2]

    def _filter_by_job_title(self, card_text, job_keywords):
        try:
            if any(kw.lower() in card_text for kw in job_keywords):
                return True
            exclude_keywords = [
                "юрист", "lawyer", "адвокат", "бухгалтер", "accountant",
                "маркетолог", "marketing", "продавец", "sales",
                "водитель", "driver", "официант", "waiter", "бармен",
                "уборщик", "cleaner", "охранник", "security", "строитель", "builder",
            ]
            if any(ex.lower() in card_text for ex in exclude_keywords):
                return False
            main_keywords = [
                "разработчик", "developer", "инженер", "engineer",
                "менеджер", "manager", "аналитик", "analyst", "дизайнер", "designer",
            ]
            return any(kw in card_text for kw in main_keywords)
        except Exception:
            return True

    @staticmethod
    def _extract_job_id(url):
        match = re.search(r'/jobs/view/(\d+)/', url)
        return match.group(1) if match else None

    # ── Scrolling ─────────────────────────────────────────────────────────

    def _scroll_page(self, times=2):
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            for _ in range(times):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                try:
                    buttons = self.driver.find_elements(
                        By.XPATH,
                        "//button[contains(@class,'infinite-scroller__show-more-button') or contains(text(),'Показать')]",
                    )
                    for btn in buttons:
                        if btn.is_displayed() and btn.is_enabled():
                            btn.click()
                            time.sleep(2)
                            break
                except Exception:
                    pass
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception as e:
            self.logger.debug(f"Ошибка при прокрутке: {e}")

    # ── Main parse loop ───────────────────────────────────────────────────

    def run_parsing(self):
        """Run a full parse session; return list of new vacancies."""
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
                for city in (Config.CITIES[:4] if self.auto_mode else Config.CITIES[:3]):
                    self.logger.info(f"🔄 Парсим: {job} в {city}")
                    time.sleep(random.uniform(Config.DELAY_MIN, Config.DELAY_MAX))

                    job_links = self.search_jobs(job, city)
                    self.session_stats["total_found"] += len(job_links)

                    if not job_links:
                        self.logger.warning(f"Не найдено вакансий для {job} в {city}")
                        continue

                    for i, job_url in enumerate(job_links):
                        self.logger.info(f"   [{i+1}/{len(job_links)}] Обработка...")
                        time.sleep(random.uniform(2, 4))

                        vacancy = self.parse_job_page(job_url)
                        if vacancy:
                            vacancy_id = self.db.save_vacancy(vacancy)
                            if vacancy_id:
                                # check whether it was a duplicate
                                conn = sqlite3.connect(Config.DB_FILE)
                                cursor = conn.cursor()
                                cursor.execute("SELECT is_duplicate FROM vacancies WHERE id=?", (vacancy_id,))
                                result = cursor.fetchone()
                                conn.close()

                                if result and result[0] == 1:
                                    self.session_stats["duplicates_found"] += 1
                                else:
                                    self.session_stats["new_vacancies"] += 1
                                    all_vacancies.append(vacancy)
                        else:
                            self.logger.warning("   ❌ Вакансия отфильтрована")

                        sleep_time = random.uniform(1, 3) if self.auto_mode else 1
                        time.sleep(sleep_time)

                    time.sleep(random.uniform(3, 5))

            if Config.POSTGRES_ENABLED:
                postgres_sent = self.db.sync_to_postgres(30)
                self.session_stats["postgres_sent"] = postgres_sent

            self.db.save_parsing_session(
                self.session_id,
                self.session_stats["total_found"],
                self.session_stats["new_vacancies"],
                self.session_stats["duplicates_found"],
                self.session_stats.get("postgres_sent", 0),
            )

            self.logger.info(
                f"✅ Парсинг завершен. "
                f"Найдено: {self.session_stats['total_found']}, "
                f"Новых: {self.session_stats['new_vacancies']}, "
                f"Дубликатов: {self.session_stats['duplicates_found']}, "
                f"PostgreSQL: {self.session_stats.get('postgres_sent', 0)}"
            )

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
                "failed",
            )
            return []
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info("WebDriver закрыт")
                except Exception:
                    pass

    def get_session_stats(self):
        """Return the session statistics dict."""
        return self.session_stats
