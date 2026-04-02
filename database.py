"""
database.py — Local SQLite persistence layer.

Depends on: config.Config, postgres.PostgresDBFixedColumns, stdlib
"""

import re
import sqlite3
import hashlib
import time
from datetime import datetime

from config import Config
from postgres import PostgresDBFixedColumns


class Database:
    """SQLite-backed store for scraped vacancies with deduplication and
    optional sync to PostgreSQL."""

    def __init__(self, db_file=None):
        self.db_file = db_file or Config.DB_FILE
        self.postgres = PostgresDBFixedColumns()
        self._init_db()

    # ── Schema ────────────────────────────────────────────────────────────

    def _init_db(self):
        """Create tables and indexes if they don't exist, then connect to PG."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        try:
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
                    fingerprint TEXT UNIQUE,
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

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_fingerprints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT UNIQUE,
                    vacancy_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (vacancy_id) REFERENCES vacancies (id)
                )
            ''')

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

        if Config.POSTGRES_ENABLED:
            if self.postgres.connect():
                print("✅ PostgreSQL подключение установлено")
            else:
                print("⚠ PostgreSQL подключение не удалось, работаем в локальном режиме")

    # ── Fingerprint / similarity ──────────────────────────────────────────

    def generate_fingerprint(self, title, company, location, description="", source_url=""):
        """Return an MD5 fingerprint stable across re-scrapes of the same job."""
        try:
            clean_title = re.sub(r'[^a-zа-яё0-9\s]', '', re.sub(r'\s+', ' ', title.strip().lower()))
            clean_company = ""
            if company:
                clean_company = re.sub(r'[^a-zа-яё0-9\s]', '', re.sub(r'\s+', ' ', company.strip().lower()))[:50]

            linkedin_id = ""
            if source_url and "linkedin.com" in source_url:
                match = re.search(r'/jobs/view/(\d+)/', source_url)
                if match:
                    linkedin_id = match.group(1)

            content = linkedin_id if linkedin_id else f"{clean_title[:100]}|{clean_company[:50]}"
            return hashlib.md5(content.encode('utf-8')).hexdigest()

        except Exception as e:
            print(f"⚠ Ошибка генерации fingerprint: {e}")
            backup = f"{title}|{company}|{location}|{source_url}"
            return hashlib.md5(backup.encode('utf-8')).hexdigest()

    def calculate_similarity(self, text1, text2):
        """Jaccard similarity between two strings (word-level)."""
        if not text1 or not text2:
            return 0.0
        text1 = re.sub(r'\s+', ' ', text1.strip().lower())
        text2 = re.sub(r'\s+', ' ', text2.strip().lower())
        if text1 == text2:
            return 1.0
        words1 = set(text1.split())
        words2 = set(text2.split())
        if not words1 or not words2:
            return 0.0
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        return intersection / union if union > 0 else 0.0

    # ── Existence checks ──────────────────────────────────────────────────

    def is_vacancy_exists(self, source_url=None, fingerprint=None, title=None, company=None, location=None):
        """Check if a vacancy already exists. Returns (exists, id, fp, match_type)."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        try:
            # Priority 1: exact URL match
            if source_url:
                cursor.execute(
                    "SELECT id, fingerprint, title, company_name, is_duplicate FROM vacancies WHERE source_url = ?",
                    (source_url,),
                )
                result = cursor.fetchone()
                if result:
                    vac_id, vac_fp, vac_title, vac_company, is_dup = result
                    if not is_dup:
                        return True, vac_id, vac_fp, "url"
                    original = self._find_original(vac_fp, vac_title, vac_company)
                    if original:
                        return True, original[0], original[1], "duplicate_url"

            # Priority 2: fingerprint
            if fingerprint:
                cursor.execute(
                    "SELECT id, source_url, title, company_name, is_duplicate FROM vacancies WHERE fingerprint = ?",
                    (fingerprint,),
                )
                result = cursor.fetchone()
                if result:
                    vac_id, _, vac_title, vac_company, is_dup = result
                    if not is_dup:
                        return True, vac_id, fingerprint, "fingerprint"
                    original = self._find_original(fingerprint, vac_title, vac_company)
                    if original:
                        return True, original[0], original[1], "duplicate_fingerprint"

            # Priority 3: fuzzy title match
            if title:
                clean_title = re.sub(r'\s+', ' ', title.strip().lower())
                cursor.execute('''
                    SELECT id, fingerprint, title, company_name FROM vacancies
                    WHERE is_duplicate = 0
                    AND LOWER(title) LIKE ?
                    ORDER BY created_at DESC
                    LIMIT 5
                ''', (f"%{clean_title[:20]}%",))

                for row in cursor.fetchall():
                    vac_id, vac_fp, vac_title_db, vac_company_db = row
                    if self.calculate_similarity(title, vac_title_db) >= Config.SIMILARITY_THRESHOLD:
                        if company:
                            if self.calculate_similarity(company, vac_company_db) >= 0.5:
                                print(f"⚠ Найдено совпадение: {title[:30]}...")
                                return True, vac_id, vac_fp, "similar_title"
                        else:
                            return True, vac_id, vac_fp, "title_only_match"

            return False, None, None, None

        except sqlite3.Error as e:
            print(f"⚠ Ошибка проверки существования вакансии: {e}")
            return False, None, None, None
        finally:
            conn.close()

    def _find_original(self, fingerprint, title, company):
        """Find the non-duplicate record for a given fingerprint or title."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT id, fingerprint FROM vacancies WHERE fingerprint = ? AND is_duplicate = 0 LIMIT 1",
                (fingerprint,),
            )
            result = cursor.fetchone()
            if result:
                return result

            clean_title = re.sub(r'\s+', ' ', title.strip().lower())
            cursor.execute(
                "SELECT id, fingerprint FROM vacancies WHERE LOWER(title) LIKE ? AND is_duplicate = 0 LIMIT 1",
                (f"%{clean_title[:30]}%",),
            )
            return cursor.fetchone()
        except Exception as e:
            print(f"⚠ Ошибка поиска оригинала: {e}")
            return None
        finally:
            conn.close()

    # ── Write ─────────────────────────────────────────────────────────────

    def save_vacancy(self, vacancy):
        """Persist a vacancy dict to SQLite and optionally forward to PostgreSQL."""
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

            fingerprint = self.generate_fingerprint(title, company, location, description, source_url)
            exists, existing_id, existing_fp, match_type = self.is_vacancy_exists(
                source_url=source_url, fingerprint=fingerprint,
                title=title, company=company, location=location,
            )

            if exists:
                cursor.execute('''
                    UPDATE vacancies
                    SET updated_at = datetime('now'),
                        last_parsed_at = datetime('now'),
                        parse_count = parse_count + 1
                    WHERE id = ? AND is_duplicate = 0
                ''', (existing_id,))
                conn.commit()
                print(f"✅ ДУБЛИКАТ ПРОПУЩЕН ({match_type}): {title[:50]}... (ID: {existing_id})")

                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO processed_fingerprints (fingerprint, vacancy_id) VALUES (?,?)",
                        (fingerprint, existing_id),
                    )
                    conn.commit()
                except Exception:
                    pass

                return existing_id

            # Check for recent duplicate (3 days)
            clean_title = re.sub(r'\s+', ' ', title.strip().lower())
            cursor.execute('''
                SELECT id, fingerprint FROM vacancies
                WHERE LOWER(title) LIKE ?
                  AND datetime(created_at) > datetime('now', '-3 days')
                  AND is_duplicate = 0
                LIMIT 1
            ''', (f"%{clean_title[:30]}%",))
            dup = cursor.fetchone()
            if dup:
                print(f"⚠ Дубликат (3 дня): {title[:50]}...")
                cursor.execute('''
                    UPDATE vacancies SET updated_at=datetime('now'), last_parsed_at=datetime('now'),
                    parse_count=parse_count+1 WHERE id=?
                ''', (dup[0],))
                conn.commit()
                return dup[0]

            # New unique vacancy
            print(f"🔍 НОВАЯ УНИКАЛЬНАЯ ВАКАНСИЯ: {title[:50]}...")
            cursor.execute('''
                INSERT INTO vacancies
                (title, description, salary, location, contact, source, source_url,
                 fingerprint, company_name, last_parsed_at, parse_count, posted_to_postgres, is_duplicate)
                VALUES (?,?,?,?,?,?,?,?,?,datetime('now'),1,0,0)
            ''', (
                title[:500],
                description[:5000] if description else "Нет описания",
                vacancy.get('salary', 'не указана')[:200],
                location[:100],
                vacancy.get('contact', '')[:500],
                vacancy.get('source', 'LinkedIn'),
                source_url[:500],
                fingerprint,
                company[:200],
            ))

            vacancy_id = cursor.lastrowid

            try:
                cursor.execute(
                    "INSERT INTO processed_fingerprints (fingerprint, vacancy_id) VALUES (?,?)",
                    (fingerprint, vacancy_id),
                )
            except Exception:
                pass

            conn.commit()
            print(f"✅ Сохранена НОВАЯ ВАКАНСИЯ в локальную БД: {title[:50]}... (ID: {vacancy_id})")

            if Config.POSTGRES_ENABLED:
                success = self.postgres.save_vacancy_to_postgres(vacancy)
                cursor.execute(
                    "UPDATE vacancies SET posted_to_postgres=? WHERE id=?",
                    (1 if success else 0, vacancy_id),
                )
                conn.commit()
                if success:
                    print("✅ Вакансия отправлена в PostgreSQL")
                else:
                    print("⚠ Не удалось отправить вакансию в PostgreSQL")

            return vacancy_id

        except sqlite3.Error as e:
            if "UNIQUE constraint failed" in str(e):
                print(f"⚠ UNIQUE violation (дубликат): {title[:50]}...")
                try:
                    cursor.execute(
                        "SELECT id FROM vacancies WHERE (fingerprint=? OR source_url=?) AND is_duplicate=0 LIMIT 1",
                        (fingerprint, source_url),
                    )
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''
                            UPDATE vacancies SET updated_at=datetime('now'), last_parsed_at=datetime('now'),
                            parse_count=parse_count+1 WHERE id=?
                        ''', (result[0],))
                        conn.commit()
                        return result[0]
                except Exception:
                    pass
            print(f"❌ Ошибка базы данных при сохранении: {e}")
            return None
        except Exception as e:
            print(f"❌ Неожиданная ошибка при сохранении: {e}")
            return None
        finally:
            conn.close()

    # ── Read ──────────────────────────────────────────────────────────────

    def get_all_vacancies(self, limit=200):
        """Return all non-duplicate vacancies, newest first."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM vacancies WHERE is_duplicate = 0 ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения вакансий: {e}")
            return []
        finally:
            conn.close()

    def get_unpublished_vacancies(self, limit=50):
        """Return vacancies not yet posted to Telegram."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM vacancies WHERE published=0 AND is_duplicate=0 ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения неопубликованных вакансий: {e}")
            return []
        finally:
            conn.close()

    def get_vacancies_for_postgres(self, limit=50):
        """Return vacancies not yet synced to PostgreSQL."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM vacancies WHERE posted_to_postgres=0 AND is_duplicate=0 ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения вакансий для PostgreSQL: {e}")
            return []
        finally:
            conn.close()

    def get_vacancy_by_id(self, vacancy_id):
        """Return a single vacancy dict or None."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM vacancies WHERE id=?", (vacancy_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения вакансии по ID: {e}")
            return None
        finally:
            conn.close()

    def search_vacancies(self, keyword="", location="", source=""):
        """Full-text search across title / description / company."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            query = "SELECT * FROM vacancies WHERE is_duplicate=0"
            params = []
            if keyword:
                query += " AND (title LIKE ? OR description LIKE ? OR company_name LIKE ?)"
                params.extend([f"%{keyword}%"] * 3)
            if location:
                query += " AND location LIKE ?"
                params.append(f"%{location}%")
            if source:
                query += " AND source=?"
                params.append(source)
            query += " ORDER BY created_at DESC"
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"⚠ Ошибка поиска вакансий: {e}")
            return []
        finally:
            conn.close()

    def get_stats(self):
        """Return a dict of aggregate statistics."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        stats = {}
        try:
            queries = {
                'total': "SELECT COUNT(*) FROM vacancies WHERE is_duplicate=0",
                'unpublished': "SELECT COUNT(*) FROM vacancies WHERE published=0 AND is_duplicate=0",
                'published': "SELECT COUNT(*) FROM vacancies WHERE published=1",
                'today': "SELECT COUNT(*) FROM vacancies WHERE date(created_at)=date('now') AND is_duplicate=0",
                'duplicates': "SELECT COUNT(*) FROM vacancies WHERE is_duplicate=1",
                'postgres_sent': "SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres=1 AND is_duplicate=0",
                'postgres_pending': "SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres=0 AND is_duplicate=0",
                'last_7_days': "SELECT COUNT(*) FROM vacancies WHERE datetime(created_at)>datetime('now','-7 days') AND is_duplicate=0",
                'unique_fingerprints': "SELECT COUNT(DISTINCT fingerprint) FROM vacancies WHERE fingerprint IS NOT NULL",
                'total_fingerprints': "SELECT COUNT(*) FROM processed_fingerprints",
            }
            for key, sql in queries.items():
                cursor.execute(sql)
                stats[key] = cursor.fetchone()[0]

            total_all = stats['total'] + stats['duplicates']
            stats['deduplication_efficiency'] = (
                (stats['duplicates'] / total_all * 100) if total_all > 0 else 0
            )
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения статистики: {e}")
            stats = {k: 0 for k in [
                'total', 'unpublished', 'published', 'today', 'duplicates',
                'postgres_sent', 'postgres_pending', 'last_7_days',
                'unique_fingerprints', 'total_fingerprints', 'deduplication_efficiency',
            ]}
        finally:
            conn.close()
        return stats

    def get_parsing_history(self, limit=10):
        """Return the last *limit* parsing sessions."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM parsing_stats ORDER BY start_time DESC LIMIT ?", (limit,)
            )
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"⚠ Ошибка получения истории парсинга: {e}")
            return []
        finally:
            conn.close()

    # ── Sync / bulk ops ───────────────────────────────────────────────────

    def sync_to_postgres(self, limit=50):
        """Push un-synced vacancies to PostgreSQL; return count sent."""
        if not Config.POSTGRES_ENABLED:
            return 0

        vacancies = self.get_vacancies_for_postgres(limit)
        if not vacancies:
            return 0

        print(f"🔄 Синхронизация {len(vacancies)} вакансий с PostgreSQL...")
        sent_count = 0

        for vacancy in vacancies:
            vacancy_data = {
                'title': vacancy.get('title', ''),
                'company_name': vacancy.get('company_name', ''),
                'description': vacancy.get('description', ''),
                'location': vacancy.get('location', ''),
                'salary': vacancy.get('salary', ''),
                'source_url': vacancy.get('source_url', ''),
                'source': vacancy.get('source', 'LinkedIn'),
            }
            success = self.postgres.save_vacancy_to_postgres(vacancy_data)

            if success:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                cursor.execute("UPDATE vacancies SET posted_to_postgres=1 WHERE id=?", (vacancy['id'],))
                conn.commit()
                conn.close()
                sent_count += 1
                print(f"✅ Синхронизировано: {vacancy.get('title', '')[:50]}...")
            else:
                print(f"❌ Не удалось синхронизировать: {vacancy.get('title', '')[:50]}...")

            time.sleep(0.5)

        print(f"✅ Синхронизация завершена. Отправлено: {sent_count}/{len(vacancies)}")
        return sent_count

    def cleanup_duplicates(self):
        """Delete all rows marked is_duplicate=1; return count deleted."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_duplicate=1")
            count = cursor.fetchone()[0]
            if count == 0:
                print("ℹ Дубликатов не найдено")
                return 0

            print(f"🔄 Найдено {count} дубликатов для удаления")
            cursor.execute("DELETE FROM vacancies WHERE is_duplicate=1")
            deleted = cursor.rowcount
            cursor.execute('''
                DELETE FROM processed_fingerprints
                WHERE fingerprint IN (SELECT fingerprint FROM vacancies WHERE is_duplicate=1)
            ''')
            conn.commit()
            print(f"✅ Удалено {deleted} дубликатов")
            return deleted
        except Exception as e:
            print(f"❌ Ошибка очистки дубликатов: {e}")
            return 0
        finally:
            conn.close()

    # ── Status mutations ──────────────────────────────────────────────────

    def mark_as_published(self, vacancy_id):
        """Mark a vacancy as published to Telegram."""
        self._update_flag(vacancy_id, "published=1 WHERE id=? AND is_duplicate=0")

    def mark_as_postgres_sent(self, vacancy_id):
        """Mark a vacancy as synced to PostgreSQL."""
        self._update_flag(vacancy_id, "posted_to_postgres=1 WHERE id=? AND is_duplicate=0")

    def mark_as_duplicate(self, vacancy_id, original_id=None):
        """Flag a vacancy as a duplicate, optionally adopting the original's fingerprint."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            if original_id:
                cursor.execute("SELECT fingerprint FROM vacancies WHERE id=?", (original_id,))
                orig = cursor.fetchone()
                if orig:
                    cursor.execute(
                        "UPDATE vacancies SET is_duplicate=1, fingerprint=?, updated_at=datetime('now') WHERE id=?",
                        (orig[0], vacancy_id),
                    )
                    conn.commit()
                    return True
            cursor.execute(
                "UPDATE vacancies SET is_duplicate=1, updated_at=datetime('now') WHERE id=?",
                (vacancy_id,),
            )
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"⚠ Ошибка пометки как дубликата: {e}")
            return False
        finally:
            conn.close()

    def delete_vacancy(self, vacancy_id):
        """Hard-delete a vacancy row; return True on success."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM vacancies WHERE id=?", (vacancy_id,))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"⚠ Ошибка удаления вакансии: {e}")
            return False
        finally:
            conn.close()

    def save_parsing_session(self, session_id, total_found, new_vacancies,
                             duplicates_found, postgres_sent=0, status="completed"):
        """Append a row to parsing_stats."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO parsing_stats
                (session_id, end_time, total_found, new_vacancies, duplicates_found, postgres_sent, status)
                VALUES (?, datetime('now'), ?, ?, ?, ?, ?)
            ''', (session_id, total_found, new_vacancies, duplicates_found, postgres_sent, status))
            conn.commit()
        except sqlite3.Error as e:
            print(f"⚠ Ошибка сохранения сессии парсинга: {e}")
        finally:
            conn.close()

    # ── Private helpers ───────────────────────────────────────────────────

    def _update_flag(self, vacancy_id, set_clause):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute(f"UPDATE vacancies SET {set_clause}", (vacancy_id,))
            conn.commit()
        except sqlite3.Error as e:
            print(f"⚠ Ошибка обновления флага: {e}")
        finally:
            conn.close()
