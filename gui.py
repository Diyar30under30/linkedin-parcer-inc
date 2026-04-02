"""
gui.py — Tkinter GUI and auto-scheduling layer.

Depends on: config.Config, database.Database,
            notifier.TelegramPublisher, parser.LinkedInParser
"""

import os
import json
import sqlite3
import threading
import webbrowser
from datetime import datetime

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext

from config import Config
from database import Database
from notifier import TelegramPublisher
from parser import LinkedInParser


# ══════════════════════════════════════════════════════════════════════════════
# AutoParser
# ══════════════════════════════════════════════════════════════════════════════

class AutoParser:
    """Runs LinkedInParser on a repeating timer and surfaces results to the GUI."""

    def __init__(self, gui_app: "ParserGUI"):
        self.gui = gui_app
        self.db = Database()
        self.is_running = False
        self.timer: threading.Timer | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.gui.safe_call(self.gui.log_message, "🔄 Авто-парсинг запущен (интервал: 10 минут)", "success")
        self.gui.safe_call(self.gui.update_status, "Авто-парсинг активен")
        self.run_auto_parse()

    def stop(self):
        if not self.is_running:
            return
        self.is_running = False
        if self.timer:
            self.timer.cancel()
        self.gui.safe_call(self.gui.log_message, "⏹ Авто-парсинг остановлен", "warning")
        self.gui.safe_call(self.gui.update_status, "Авто-парсинг остановлен")

    # ── Parse cycle ───────────────────────────────────────────────────────

    def run_auto_parse(self):
        if not self.is_running:
            return
        try:
            self.gui.safe_call(self.gui.log_message, "⏰ Запуск автоматического парсинга...", "info")
            email = self.gui.linkedin_email.get()
            password = self.gui.linkedin_password.get()

            if not email or not password:
                self.gui.safe_call(self.gui.log_message, "❌ Не указаны учетные данные LinkedIn", "error")
                self._schedule_next_run()
                return

            self.gui.safe_call(self.gui.save_config_settings)
            threading.Thread(
                target=self._run_parser_thread,
                args=(email, password),
                daemon=True,
            ).start()

        except Exception as e:
            self.gui.safe_call(self.gui.log_message, f"❌ Ошибка запуска авто-парсинга: {e}", "error")
            self._schedule_next_run()

    def _run_parser_thread(self, email, password):
        try:
            p = LinkedInParser(email=email, password=password, headless=True, auto_mode=True)
            vacancies = p.run_parsing()
            stats = p.get_session_stats()
            self.gui.safe_call(self._auto_parse_completed, vacancies, stats)
        except Exception as e:
            self.gui.safe_call(self._auto_parse_failed, str(e))

    def _auto_parse_completed(self, vacancies, stats):
        self.gui.log_message(
            f"✅ Авто-парсинг завершен. "
            f"Найдено: {stats['total_found']}, Новых: {stats['new_vacancies']}, "
            f"Дубликатов: {stats['duplicates_found']}, PostgreSQL: {stats.get('postgres_sent', 0)}",
            "success",
        )
        self.gui.show_all_vacancies()
        self.gui.update_stats()

        if stats['new_vacancies'] > 0:
            messagebox.showinfo(
                "Новые вакансии",
                f"Авто-парсинг нашел {stats['new_vacancies']} новых вакансий!\n\n"
                f"Всего: {stats['total_found']}\n"
                f"Дубликатов: {stats['duplicates_found']}\n"
                f"PostgreSQL: {stats.get('postgres_sent', 0)}",
            )
        self._schedule_next_run()

    def _auto_parse_failed(self, error):
        self.gui.log_message(f"❌ Ошибка авто-парсинга: {error}", "error")
        import time; time.sleep(300)
        if self.is_running:
            self.run_auto_parse()

    def _schedule_next_run(self):
        if not self.is_running:
            return
        self.gui.log_message(
            f"⏳ Следующий авто-парсинг через {Config.AUTO_PARSE_INTERVAL // 60} минут", "info"
        )
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(Config.AUTO_PARSE_INTERVAL, self.run_auto_parse)
        self.timer.daemon = True
        self.timer.start()


# ══════════════════════════════════════════════════════════════════════════════
# ParserGUI
# ══════════════════════════════════════════════════════════════════════════════

class ParserGUI:
    """Main Tkinter application window."""

    # ── Init ──────────────────────────────────────────────────────────────

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("LinkedIn Parser Pro v5.5.9")
        self.root.geometry("1100x750")
        self._center_window()

        self.config = self._load_config()

        self.linkedin_email = tk.StringVar(value=self.config.get("linkedin_email", ""))
        self.linkedin_password = tk.StringVar(value=self.config.get("linkedin_password", ""))
        self.telegram_token = tk.StringVar(value=self.config.get("telegram_token", ""))
        self.telegram_channel = tk.StringVar(value=self.config.get("telegram_channel", "@your_channel"))
        self.auto_parse_enabled = tk.BooleanVar(value=self.config.get("auto_parse_enabled", False))

        self.is_parsing = False
        self.db = Database()
        self.parser_thread = None
        self.auto_parser: AutoParser | None = None

        self._setup_gui()
        self.update_stats()

        self.auto_parser = AutoParser(self)
        if self.auto_parse_enabled.get():
            self.root.after(2000, self.start_auto_parsing)

        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    # ── Window helpers ────────────────────────────────────────────────────

    def _center_window(self):
        self.root.update_idletasks()
        w, h = 1100, 750
        x = (self.root.winfo_screenwidth() // 2) - (w // 2)
        y = (self.root.winfo_screenheight() // 2) - (h // 2)
        self.root.geometry(f'{w}x{h}+{x}+{y}')

    def safe_call(self, func, *args, **kwargs):
        """Thread-safe Tkinter callback."""
        if self.root:
            try:
                self.root.after(0, lambda: func(*args, **kwargs))
            except Exception:
                pass

    def _on_closing(self):
        if self.is_parsing:
            if messagebox.askokcancel("Выход", "Парсинг выполняется. Выйти?"):
                self.is_parsing = False
                if self.auto_parser:
                    self.auto_parser.stop()
                self.root.quit()
        else:
            if self.auto_parser:
                self.auto_parser.stop()
            self.root.quit()

    # ── Config I/O ────────────────────────────────────────────────────────

    def _load_config(self) -> dict:
        defaults = {
            "linkedin_email": "", "linkedin_password": "",
            "telegram_token": "", "telegram_channel": "@your_channel",
            "auto_parse_enabled": False,
        }
        try:
            if os.path.exists(Config.CONFIG_FILE):
                with open(Config.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    defaults.update(json.load(f))
        except Exception as e:
            print(f"⚠ Ошибка загрузки конфига: {e}")
        return defaults

    def _save_config(self) -> bool:
        try:
            with open(Config.CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    "linkedin_email": self.linkedin_email.get(),
                    "linkedin_password": self.linkedin_password.get(),
                    "telegram_token": self.telegram_token.get(),
                    "telegram_channel": self.telegram_channel.get(),
                    "auto_parse_enabled": self.auto_parse_enabled.get(),
                }, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"⚠ Ошибка сохранения конфига: {e}")
            return False

    # ── UI construction ───────────────────────────────────────────────────

    def _setup_gui(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)

        self._setup_settings_tab()
        self._setup_parsing_tab()
        self._setup_database_tab()
        self._setup_publish_tab()
        self._setup_stats_tab()
        self._setup_postgres_tab()
        self._setup_status_bar()

    # ── Settings tab ──────────────────────────────────────────────────────

    def _setup_settings_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="⚙ Настройки")

        li_frame = ttk.LabelFrame(tab, text="LinkedIn Аккаунт", padding=10)
        li_frame.pack(fill='x', padx=10, pady=10)
        ttk.Label(li_frame, text="Email:").grid(row=0, column=0, sticky='w', pady=5)
        ttk.Entry(li_frame, textvariable=self.linkedin_email, width=40).grid(row=0, column=1, pady=5)
        ttk.Label(li_frame, text="Пароль:").grid(row=1, column=0, sticky='w', pady=5)
        ttk.Entry(li_frame, textvariable=self.linkedin_password, show="*", width=40).grid(row=1, column=1, pady=5)

        tg_frame = ttk.LabelFrame(tab, text="Telegram Бот", padding=10)
        tg_frame.pack(fill='x', padx=10, pady=10)
        ttk.Label(tg_frame, text="Токен бота:").grid(row=0, column=0, sticky='w', pady=5)
        ttk.Entry(tg_frame, textvariable=self.telegram_token, width=50).grid(row=0, column=1, pady=5)
        ttk.Label(tg_frame, text="(получить у @BotFather)", font=('Arial', 8),
                  foreground='gray').grid(row=0, column=2, padx=5)
        ttk.Label(tg_frame, text="ID канала:").grid(row=1, column=0, sticky='w', pady=5)
        ttk.Entry(tg_frame, textvariable=self.telegram_channel, width=50).grid(row=1, column=1, pady=5)
        ttk.Label(tg_frame, text="(например: @my_channel или -1001234567890)",
                  font=('Arial', 8), foreground='gray').grid(row=1, column=2, padx=5)

        auto_frame = ttk.LabelFrame(tab, text="Автоматический парсинг", padding=10)
        auto_frame.pack(fill='x', padx=10, pady=10)
        ttk.Checkbutton(auto_frame, text="Включить авто-парсинг каждые 10 минут",
                        variable=self.auto_parse_enabled, width=40).grid(row=0, column=0, sticky='w', pady=5)
        ttk.Label(auto_frame, text="При включении парсер будет автоматически искать новые вакансии",
                  font=('Arial', 8), foreground='gray').grid(row=1, column=0, sticky='w', pady=2)

        btn_frame = ttk.Frame(tab)
        btn_frame.pack(pady=20)
        ttk.Button(btn_frame, text="💾 Сохранить настройки",
                   command=self.save_config_settings, width=25).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="🔗 Проверить Telegram",
                   command=self.test_telegram, width=25).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="🔧 Проверить БД",
                   command=self._check_database, width=25).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="🧹 Очистить дубликаты",
                   command=self._clean_duplicates_gui, width=25).pack(side='left', padx=5)

    def _clean_duplicates_gui(self):
        try:
            cleaned = Database().cleanup_duplicates()
            if cleaned > 0:
                self.log_message(f"✅ Очищено дубликатов: {cleaned}", "success")
                self.show_all_vacancies()
                self.update_stats()
                messagebox.showinfo(
                    "Очистка дубликатов",
                    f"Успешно очищено {cleaned} дубликатов!\n\n"
                    "⚠ Оригинальные вакансии сохранены.",
                )
            else:
                self.log_message("ℹ Дубликатов не найдено", "info")
                messagebox.showinfo("Очистка дубликатов", "Дубликатов не найдено.")
        except Exception as e:
            self.log_message(f"❌ Ошибка очистки дубликатов: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось очистить дубликаты:\n\n{e}")

    # ── PostgreSQL tab ────────────────────────────────────────────────────

    def _setup_postgres_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🗃 PostgreSQL")

        status_frame = ttk.LabelFrame(tab, text="Статус подключения", padding=10)
        status_frame.pack(fill='x', padx=10, pady=10)
        self.postgres_status_var = tk.StringVar(value="Не проверено")
        ttk.Label(status_frame, textvariable=self.postgres_status_var,
                  font=('Arial', 10)).pack(pady=5)
        ttk.Button(status_frame, text="🔄 Проверить подключение",
                   command=self._check_postgres_connection_gui, width=25).pack(pady=5)

        sync_frame = ttk.LabelFrame(tab, text="Управление синхронизацией", padding=10)
        sync_frame.pack(fill='x', padx=10, pady=10)
        ttk.Button(sync_frame, text="🔄 Синхронизировать с PostgreSQL",
                   command=self._sync_to_postgres_gui, width=30).pack(pady=5)
        ttk.Button(sync_frame, text="📊 Статистика PostgreSQL",
                   command=self._show_postgres_stats, width=30).pack(pady=5)

        info_frame = ttk.LabelFrame(tab, text="Конфигурация", padding=10)
        info_frame.pack(fill='x', padx=10, pady=10)
        info_text = (
            f"\nХост: {Config.POSTGRES_HOST}\n"
            f"Порт: {Config.POSTGRES_PORT}\n"
            f"База данных: {Config.POSTGRES_DB}\n"
            f"Пользователь: {Config.POSTGRES_USER}\n"
            f"Таблица: job_posting_sources\n"
            f"Новая колонка: {Config.ORG_URL_COLUMN}\n"
            f"Статус: {'✅ Включено' if Config.POSTGRES_ENABLED else '❌ Выключено'}\n"
        )
        ttk.Label(info_frame, text=info_text, justify='left',
                  font=('Consolas', 9)).pack(pady=5)
        ttk.Button(tab, text="⚠ Проверить таблицу job_posting_sources",
                   command=self._check_postgres_table, width=30).pack(pady=10)

    def _check_postgres_table(self):
        try:
            db = Database()
            if db.postgres.connect():
                if db.postgres.check_table_exists():
                    if db.postgres.check_column_exists(Config.ORG_URL_COLUMN):
                        messagebox.showinfo(
                            "Проверка таблицы",
                            f"✅ Таблица 'job_posting_sources' существует\n"
                            f"✅ Колонка '{Config.ORG_URL_COLUMN}' существует",
                        )
                    else:
                        messagebox.showwarning(
                            "Предупреждение",
                            f"✅ Таблица 'job_posting_sources' существует\n"
                            f"⚠ Колонка '{Config.ORG_URL_COLUMN}' НЕ существует!\n\n"
                            f"ALTER TABLE job_posting_sources ADD COLUMN {Config.ORG_URL_COLUMN} VARCHAR(500);",
                        )
                else:
                    messagebox.showerror("Ошибка", "❌ Таблица 'job_posting_sources' не существует!")
            else:
                messagebox.showerror("Ошибка", "Не удалось подключиться к PostgreSQL")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось проверить таблицу:\n\n{e}")

    def _check_postgres_connection_gui(self):
        try:
            db = Database()
            if db.postgres.connect():
                has_col = db.postgres.check_column_exists(Config.ORG_URL_COLUMN)
                label = "✅ Подключение установлено" + (" (org_url есть)" if has_col else " (org_url нет)")
                self.postgres_status_var.set(label)
                level = "success" if has_col else "warning"
                self.log_message(label, level)
            else:
                self.postgres_status_var.set("❌ Ошибка подключения")
                self.log_message("❌ Не удалось подключиться к PostgreSQL", "error")
        except Exception as e:
            self.postgres_status_var.set(f"❌ Ошибка: {str(e)[:50]}")
            self.log_message(f"❌ Ошибка подключения к PostgreSQL: {e}", "error")

    def _sync_to_postgres_gui(self):
        try:
            if not Config.POSTGRES_ENABLED:
                messagebox.showwarning("Внимание", "PostgreSQL интеграция выключена.")
                return

            db = Database()
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres=0 AND is_duplicate=0")
            count = cursor.fetchone()[0]
            conn.close()

            if count == 0:
                messagebox.showinfo("Информация", "Нет вакансий для синхронизации")
                return

            if not messagebox.askyesno(
                "Синхронизация с PostgreSQL",
                f"Найдено {count} вакансий для синхронизации.\n\nПродолжить?",
            ):
                return

            self.log_message(f"🔄 Начинаю синхронизацию {count} вакансий...", "info")

            pw = tk.Toplevel(self.root)
            pw.title("Синхронизация с PostgreSQL")
            pw.geometry("400x150")
            pw.transient(self.root)
            pw.grab_set()
            ttk.Label(pw, text="Синхронизация...", font=("Arial", 11, "bold")).pack(pady=10)
            bar = ttk.Progressbar(pw, mode='indeterminate', length=300)
            bar.pack(pady=10)
            bar.start(10)

            def sync_thread():
                try:
                    sent = db.sync_to_postgres(count)
                    self.safe_call(self._sync_completed, sent, count, pw)
                except Exception as e:
                    self.safe_call(self._sync_failed, str(e), pw)

            threading.Thread(target=sync_thread, daemon=True).start()

        except Exception as e:
            self.log_message(f"❌ Ошибка запуска синхронизации: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось запустить синхронизацию:\n\n{e}")

    def _sync_completed(self, sent_count, total_count, pw):
        try:
            pw.destroy()
            self.log_message(f"✅ Синхронизация завершена. Отправлено: {sent_count}/{total_count}", "success")
            self.show_all_vacancies()
            self.update_stats()
            messagebox.showinfo(
                "Синхронизация завершена",
                f"✅ Успешно отправлено: {sent_count}\n"
                f"📊 Всего обработано: {total_count}",
            )
        except Exception:
            pass

    def _sync_failed(self, error, pw):
        try:
            pw.destroy()
            self.log_message(f"❌ Ошибка синхронизации: {error}", "error")
            messagebox.showerror("Ошибка синхронизации", f"Не удалось выполнить синхронизацию:\n\n{error}")
        except Exception:
            pass

    def _show_postgres_stats(self):
        try:
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_duplicate=0")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres=1")
            sent = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE posted_to_postgres=0 AND is_duplicate=0")
            pending = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_duplicate=1")
            duplicates = cursor.fetchone()[0]
            conn.close()

            stats_text = (
                f"\n📊 СТАТИСТИКА POSTGRESQL:\n\n"
                f"Всего вакансий в локальной БД: {total}\n"
                f"✅ Отправлено в PostgreSQL: {sent}\n"
                f"⏳ Ожидают отправки: {pending}\n"
                f"🗑 Дубликатов: {duplicates}\n\n"
                f"Хост: {Config.POSTGRES_HOST}\n"
                f"Порт: {Config.POSTGRES_PORT}\n"
                f"База данных: {Config.POSTGRES_DB}\n"
                f"Статус: {'✅ Включено' if Config.POSTGRES_ENABLED else '❌ Выключено'}\n"
            )

            w = tk.Toplevel(self.root)
            w.title("Статистика PostgreSQL")
            w.geometry("500x400")
            txt = scrolledtext.ScrolledText(w, wrap=tk.WORD, font=('Arial', 10))
            txt.pack(fill='both', expand=True, padx=10, pady=10)
            txt.insert(1.0, stats_text)
            txt.config(state='disabled')
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось получить статистику:\n\n{e}")

    def _check_database(self):
        try:
            conn = sqlite3.connect(Config.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(vacancies)")
            columns = cursor.fetchall()
            conn.close()
            info = "Структура таблицы vacancies:\n\n" + "".join(f"• {c[1]} ({c[2]})\n" for c in columns)
            w = tk.Toplevel(self.root)
            w.title("Проверка базы данных")
            w.geometry("500x400")
            txt = scrolledtext.ScrolledText(w, wrap=tk.WORD, font=('Consolas', 10))
            txt.pack(fill='both', expand=True, padx=10, pady=10)
            txt.insert(1.0, info)
            txt.config(state='disabled')
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось проверить базу данных:\n\n{e}")

    # ── Parsing tab ───────────────────────────────────────────────────────

    def _setup_parsing_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🔍 Парсинг LinkedIn")

        ctrl = ttk.LabelFrame(tab, text="Управление парсингом", padding=10)
        ctrl.pack(fill='x', padx=10, pady=10)

        self.start_btn = ttk.Button(ctrl, text="▶ Начать парсинг",
                                    command=self.start_parsing, width=20)
        self.start_btn.pack(side='left', padx=5)
        self.stop_btn = ttk.Button(ctrl, text="■ Остановить",
                                   command=self.stop_parsing, width=20, state='disabled')
        self.stop_btn.pack(side='left', padx=5)
        self.auto_start_btn = ttk.Button(ctrl, text="🔄 Запустить авто-парсинг",
                                         command=self.start_auto_parsing, width=25)
        self.auto_start_btn.pack(side='left', padx=5)
        self.auto_stop_btn = ttk.Button(ctrl, text="⏹ Остановить авто-парсинг",
                                        command=self.stop_auto_parsing, width=25)
        self.auto_stop_btn.pack(side='left', padx=5)

        self.progress = ttk.Progressbar(tab, mode='indeterminate', length=400)
        self.progress.pack(pady=10)

        log_frame = ttk.LabelFrame(tab, text="Лог выполнения", padding=10)
        log_frame.pack(fill='both', expand=True, padx=10, pady=10)
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, width=80, height=20,
                                                   font=('Consolas', 9))
        self.log_text.pack(fill='both', expand=True)
        self.log_text.tag_config("error", foreground="red")
        self.log_text.tag_config("success", foreground="green")
        self.log_text.tag_config("warning", foreground="orange")
        self.log_text.tag_config("info", foreground="black")

    # ── Database tab ──────────────────────────────────────────────────────

    def _setup_database_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🗄 База данных")

        sf = ttk.LabelFrame(tab, text="Поиск вакансий", padding=10)
        sf.pack(fill='x', padx=10, pady=10)
        ttk.Label(sf, text="Ключевое слово:").pack(side='left')
        self.search_var = tk.StringVar()
        ttk.Entry(sf, textvariable=self.search_var, width=30).pack(side='left', padx=5)
        ttk.Button(sf, text="🔍 Найти", command=self.search_vacancies, width=15).pack(side='left', padx=5)
        ttk.Button(sf, text="🔄 Показать все", command=self.show_all_vacancies, width=15).pack(side='left', padx=5)
        ttk.Button(sf, text="🗑 Очистить дубликаты", command=self._clean_duplicates_gui, width=20).pack(side='left', padx=5)

        columns = ("ID", "Название", "Компания", "Город", "Зарплата", "Дата", "Опубликована", "PostgreSQL")
        self.tree = ttk.Treeview(tab, columns=columns, show='headings', height=20)
        for col, width in zip(columns, [50, 200, 120, 80, 90, 90, 90, 90]):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, minwidth=width)

        scrollbar = ttk.Scrollbar(tab, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side='left', fill='both', expand=True, padx=(10, 0), pady=10)
        scrollbar.pack(side='right', fill='y', padx=(0, 10), pady=10)

        self._setup_context_menu()
        self.show_all_vacancies()

    # ── Publish tab ───────────────────────────────────────────────────────

    def _setup_publish_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📢 Публикация в Telegram")

        sf = ttk.LabelFrame(tab, text="📊 Статистика вакансий", padding=10)
        sf.pack(fill='x', padx=10, pady=10)
        self.stats_text = tk.StringVar()
        ttk.Label(sf, textvariable=self.stats_text, font=('Arial', 10), justify='left').pack(anchor='w', padx=10, pady=5)

        pf = ttk.LabelFrame(tab, text="Управление публикацией", padding=10)
        pf.pack(fill='x', padx=10, pady=10)
        ttk.Button(pf, text="📤 Опубликовать все неопубликованные",
                   command=self.publish_all, width=35).pack(pady=5)
        ttk.Button(pf, text="⚡ Тест публикации (1 вакансия)",
                   command=self.test_publish_one, width=35).pack(pady=5)
        ttk.Button(pf, text="🔄 Обновить статистику",
                   command=self.update_stats, width=35).pack(pady=5)

    # ── Stats tab ─────────────────────────────────────────────────────────

    def _setup_stats_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📈 Статистика")

        hf = ttk.LabelFrame(tab, text="История парсинга", padding=10)
        hf.pack(fill='both', expand=True, padx=10, pady=10)

        columns = ("ID", "Время", "Найдено", "Новых", "Дубликатов", "PostgreSQL", "Статус")
        self.history_tree = ttk.Treeview(hf, columns=columns, show='headings', height=15)
        for col, width in zip(columns, [80, 150, 80, 80, 100, 90, 100]):
            self.history_tree.heading(col, text=col)
            self.history_tree.column(col, width=width, minwidth=width)

        sb = ttk.Scrollbar(hf, orient='vertical', command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=sb.set)
        self.history_tree.pack(side='left', fill='both', expand=True)
        sb.pack(side='right', fill='y')

        bf = ttk.Frame(tab)
        bf.pack(fill='x', padx=10, pady=10)
        ttk.Button(bf, text="🔄 Обновить историю",
                   command=self.update_history, width=25).pack(side='left', padx=5)
        ttk.Button(bf, text="📊 Подробная статистика",
                   command=self._show_detailed_stats, width=25).pack(side='left', padx=5)

    def _setup_status_bar(self):
        self.status_var = tk.StringVar(value="✅ Готов к работе")
        ttk.Label(self.root, textvariable=self.status_var,
                  relief='sunken', anchor='w', padding=5).pack(side='bottom', fill='x')

    def _setup_context_menu(self):
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="👁 Просмотреть вакансию", command=self._view_vacancy)
        self.context_menu.add_command(label="🌐 Открыть в браузере", command=self._open_in_browser)
        self.context_menu.add_command(label="📤 Опубликовать в Telegram", command=self._publish_selected)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="🗑 Удалить вакансию", command=self._delete_vacancy)
        self.tree.bind("<Button-3>", self._show_context_menu)
        self.tree.bind("<Double-Button-1>", lambda e: self._view_vacancy())

    # ── Logging / status ──────────────────────────────────────────────────

    def log_message(self, message, level="info"):
        try:
            ts = datetime.now().strftime("%H:%M:%S")
            prefix = {"error": "❌", "success": "✅", "warning": "⚠"}.get(level, "ℹ")
            tag = level if level in ("error", "success", "warning") else "info"
            self.log_text.insert(tk.END, f"[{ts}] {prefix} {message}\n", tag)
            self.log_text.see(tk.END)
            self.root.update()
        except Exception:
            pass

    def update_status(self, message):
        try:
            self.status_var.set(message)
            self.root.update()
        except Exception:
            pass

    # ── Save config ───────────────────────────────────────────────────────

    def save_config_settings(self):
        if self._save_config():
            self.log_message("Настройки успешно сохранены", "success")
            self.update_status("Настройки сохранены")
            if self.auto_parse_enabled.get():
                self.start_auto_parsing()
            else:
                if self.auto_parser:
                    self.auto_parser.stop()
                    self.auto_start_btn.config(state='normal')
                    self.auto_stop_btn.config(state='disabled')
        else:
            self.log_message("Ошибка при сохранении настроек", "error")

    # ── Telegram test ─────────────────────────────────────────────────────

    def test_telegram(self):
        token = self.telegram_token.get()
        if not token:
            messagebox.showwarning("Внимание", "Введите токен Telegram бота")
            return
        try:
            pub = TelegramPublisher(token=token, channel_id=self.telegram_channel.get())
            success, message = pub.test_connection()
            if success:
                self.log_message(f"Telegram: {message}", "success")
                messagebox.showinfo("Успех", f"Подключение к Telegram успешно!\n\n{message}")
            else:
                self.log_message(f"Telegram: {message}", "error")
                messagebox.showerror("Ошибка", f"Не удалось подключиться:\n\n{message}")
        except Exception as e:
            self.log_message(f"Ошибка теста Telegram: {e}", "error")
            messagebox.showerror("Ошибка", f"Произошла ошибка:\n\n{e}")

    # ── Parsing controls ──────────────────────────────────────────────────

    def start_parsing(self):
        if not self.linkedin_email.get() or not self.linkedin_password.get():
            messagebox.showwarning("Внимание", "Введите email и пароль LinkedIn")
            return
        if self.is_parsing:
            return

        self.save_config_settings()
        self.is_parsing = True
        self.start_btn.config(state='disabled')
        self.stop_btn.config(state='normal')
        self.auto_start_btn.config(state='disabled')
        self.auto_stop_btn.config(state='disabled')
        self.progress.start(10)
        self.log_text.delete(1.0, tk.END)
        self.log_message("🔄 Запуск парсинга LinkedIn...", "info")
        self.update_status("Парсинг запущен...")

        self.parser_thread = threading.Thread(
            target=self._run_parser_thread,
            args=(self.linkedin_email.get(), self.linkedin_password.get()),
            daemon=True,
        )
        self.parser_thread.start()

    def _run_parser_thread(self, email, password):
        try:
            p = LinkedInParser(email=email, password=password, headless=False)
            vacancies = p.run_parsing()
            stats = p.get_session_stats()
            self.safe_call(self._parsing_completed, len(vacancies), stats)
        except Exception as e:
            self.safe_call(self._parsing_failed, str(e))

    def _parsing_completed(self, count, stats):
        try:
            self.is_parsing = False
            self.start_btn.config(state='normal')
            self.stop_btn.config(state='disabled')
            self.auto_start_btn.config(state='normal')
            self.auto_stop_btn.config(state='normal')
            self.progress.stop()
            self.log_message(f"✅ Парсинг завершен. Найдено: {stats['total_found']}", "success")
            self.log_message(f"✅ Новых: {stats['new_vacancies']}", "success")
            self.log_message(f"⏩ Дубликатов: {stats['duplicates_found']}", "info")
            self.update_status(f"Найдено {stats['new_vacancies']} новых вакансий")
            self.show_all_vacancies()
            self.update_stats()
            self.update_history()
            if stats['new_vacancies'] > 0:
                messagebox.showinfo(
                    "Успех",
                    f"✅ Парсинг завершен!\n\n"
                    f"• Найдено всего: {stats['total_found']}\n"
                    f"• Новых вакансий: {stats['new_vacancies']}\n"
                    f"• Дубликатов: {stats['duplicates_found']}\n"
                    f"• PostgreSQL: {stats.get('postgres_sent', 0)}",
                )
        except Exception:
            pass

    def _parsing_failed(self, error):
        try:
            self.is_parsing = False
            self.start_btn.config(state='normal')
            self.stop_btn.config(state='disabled')
            self.auto_start_btn.config(state='normal')
            self.auto_stop_btn.config(state='normal')
            self.progress.stop()
            self.log_message(f"❌ Ошибка парсинга: {error}", "error")
            self.update_status("Ошибка парсинга")
            messagebox.showerror("Ошибка", f"Не удалось выполнить парсинг:\n\n{error}")
        except Exception:
            pass

    def stop_parsing(self):
        self.is_parsing = False
        self.log_message("Парсинг остановлен", "warning")
        self.update_status("Парсинг остановлен")

    def start_auto_parsing(self):
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
        if self.auto_parser:
            self.auto_parser.stop()
            self.auto_start_btn.config(state='normal')
            self.auto_stop_btn.config(state='disabled')
        self.auto_parse_enabled.set(False)
        self.save_config_settings()

    # ── Vacancy display ───────────────────────────────────────────────────

    def show_all_vacancies(self):
        try:
            self.display_vacancies(self.db.get_all_vacancies(100))
        except Exception as e:
            self.log_message(f"Ошибка загрузки вакансий: {e}", "error")

    def search_vacancies(self):
        keyword = self.search_var.get()
        try:
            vacancies = self.db.search_vacancies(keyword=keyword)
            self.display_vacancies(vacancies)
            self.log_message(f"Найдено {len(vacancies)} вакансий по запросу '{keyword}'")
        except Exception as e:
            self.log_message(f"Ошибка поиска: {e}", "error")

    def display_vacancies(self, vacancies):
        try:
            for item in self.tree.get_children():
                self.tree.delete(item)
            for vac in vacancies:
                title = vac.get('title', '')[:27] + "..." if len(vac.get('title', '')) > 30 else vac.get('title', '')
                company = vac.get('company_name', '')[:17] + "..." if len(vac.get('company_name', '')) > 20 else vac.get('company_name', '')
                salary = vac.get('salary', '')[:12] + "..." if len(vac.get('salary', '')) > 15 else vac.get('salary', '')
                date_str = str(vac.get('created_at', ''))[:10]
                self.tree.insert('', 'end', values=(
                    vac.get('id', ''), title, company,
                    vac.get('location', ''), salary, date_str,
                    "✅ Да" if vac.get('published') else "❌ Нет",
                    "✅ Да" if vac.get('posted_to_postgres') else "❌ Нет",
                ))
        except Exception:
            pass

    # ── Stats / history ───────────────────────────────────────────────────

    def update_stats(self):
        try:
            stats = self.db.get_stats()
            self.stats_text.set(
                f"Всего вакансий: {stats['total']} | "
                f"Неопубликованных: {stats['unpublished']} | "
                f"Опубликованных: {stats['published']} | "
                f"Сегодня: {stats['today']} | "
                f"PostgreSQL ждут: {stats['postgres_pending']}"
            )
        except Exception:
            pass

    def update_history(self):
        try:
            for item in self.history_tree.get_children():
                self.history_tree.delete(item)
            for session in self.db.get_parsing_history(20):
                self.history_tree.insert('', 'end', values=(
                    session.get('id', ''),
                    str(session.get('start_time', ''))[:16],
                    session.get('total_found', 0),
                    session.get('new_vacancies', 0),
                    session.get('duplicates_found', 0),
                    session.get('postgres_sent', 0),
                    session.get('status', ''),
                ))
        except Exception:
            pass

    def _show_detailed_stats(self):
        try:
            stats = self.db.get_stats()
            w = tk.Toplevel(self.root)
            w.title("Подробная статистика")
            w.geometry("450x400")
            txt = scrolledtext.ScrolledText(w, wrap=tk.WORD, font=('Arial', 10))
            txt.pack(fill='both', expand=True, padx=10, pady=10)
            txt.insert(1.0, "\n".join(f"{k}: {v}" for k, v in stats.items()))
            txt.config(state='disabled')
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось получить статистику:\n\n{e}")

    # ── Vacancy actions ───────────────────────────────────────────────────

    def _selected_vacancy_id(self):
        sel = self.tree.selection()
        if not sel:
            return None
        return self.tree.item(sel[0])['values'][0]

    def _view_vacancy(self):
        vid = self._selected_vacancy_id()
        if vid is None:
            return
        try:
            vacancy = self.db.get_vacancy_by_id(vid)
            if vacancy:
                self._show_vacancy_details(vacancy)
        except Exception as e:
            self.log_message(f"Ошибка загрузки вакансии: {e}", "error")

    def _show_vacancy_details(self, vacancy):
        w = tk.Toplevel(self.root)
        w.title(f"Вакансия: {vacancy.get('title', '')[:50]}")
        w.geometry("800x600")
        nb = ttk.Notebook(w)
        nb.pack(fill='both', expand=True, padx=10, pady=10)

        info_tab = ttk.Frame(nb)
        nb.add(info_tab, text="📋 Основная информация")
        info_txt = scrolledtext.ScrolledText(info_tab, wrap=tk.WORD, font=('Arial', 10))
        info_txt.pack(fill='both', expand=True, padx=10, pady=10)
        info_txt.insert(1.0, (
            f"{'='*70}\n🏢 ВАКАНСИЯ: {vacancy.get('title', '')}\n{'='*70}\n\n"
            f"🏭 Компания: {vacancy.get('company_name', 'Не указана')}\n"
            f"📅 Дата: {vacancy.get('created_at', '')}\n"
            f"📍 Локация: {vacancy.get('location', 'Не указана')}\n"
            f"💰 Зарплата: {vacancy.get('salary', 'не указана')}\n"
            f"📊 Опубликована: {'✅ Да' if vacancy.get('published') else '❌ Нет'}\n"
            f"📊 PostgreSQL: {'✅ Да' if vacancy.get('posted_to_postgres') else '❌ Нет'}\n"
            f"🔗 URL: {vacancy.get('source_url', 'Нет ссылки')}\n"
        ))
        info_txt.config(state='disabled')

        desc_tab = ttk.Frame(nb)
        nb.add(desc_tab, text="📝 Описание")
        desc_txt = scrolledtext.ScrolledText(desc_tab, wrap=tk.WORD, font=('Arial', 10))
        desc_txt.pack(fill='both', expand=True, padx=10, pady=10)
        desc_txt.insert(1.0, vacancy.get('description', 'Нет описания'))
        desc_txt.config(state='disabled')

        bf = ttk.Frame(w)
        bf.pack(fill='x', padx=10, pady=10)
        if vacancy.get('source_url'):
            ttk.Button(bf, text="🌐 Открыть в браузере",
                       command=lambda: webbrowser.open(vacancy['source_url'])).pack(side='left', padx=5)
        if not vacancy.get('published'):
            ttk.Button(bf, text="📤 Опубликовать в Telegram",
                       command=lambda: self._publish_single(vacancy, w)).pack(side='left', padx=5)
        ttk.Button(bf, text="✖ Закрыть", command=w.destroy).pack(side='right', padx=5)

    def _open_in_browser(self):
        vid = self._selected_vacancy_id()
        if vid is None:
            return
        vacancy = self.db.get_vacancy_by_id(vid)
        if vacancy and vacancy.get('source_url'):
            webbrowser.open(vacancy['source_url'])

    def _publish_selected(self):
        vid = self._selected_vacancy_id()
        if vid is None:
            return
        vacancy = self.db.get_vacancy_by_id(vid)
        if vacancy:
            self._publish_single(vacancy, None)

    def _publish_single(self, vacancy, parent_window):
        if not self.telegram_token.get() or not self.telegram_channel.get():
            messagebox.showwarning("Внимание", "Настройте Telegram бота")
            return
        try:
            pub = TelegramPublisher(token=self.telegram_token.get(),
                                    channel_id=self.telegram_channel.get())
            success = pub.publish_vacancy_sync(vacancy)
            if success:
                self.log_message(f"✅ Опубликовано: {vacancy.get('title', '')[:50]}", "success")
                self.show_all_vacancies()
                self.update_stats()
                if parent_window:
                    parent_window.destroy()
            else:
                self.log_message("❌ Не удалось опубликовать", "error")
        except Exception as e:
            self.log_message(f"Ошибка публикации: {e}", "error")
            messagebox.showerror("Ошибка", f"Ошибка публикации:\n\n{e}")

    def _delete_vacancy(self):
        vid = self._selected_vacancy_id()
        if vid is None:
            return
        if messagebox.askyesno("Удаление", f"Удалить вакансию ID={vid}?"):
            if self.db.delete_vacancy(vid):
                self.log_message(f"✅ Вакансия {vid} удалена", "success")
                self.show_all_vacancies()
                self.update_stats()
            else:
                self.log_message("❌ Не удалось удалить вакансию", "error")

    def _show_context_menu(self, event):
        try:
            self.tree.selection_set(self.tree.identify_row(event.y))
            self.context_menu.post(event.x_root, event.y_root)
        except Exception:
            pass

    # ── Publish all ───────────────────────────────────────────────────────

    def publish_all(self):
        if not self.telegram_token.get() or not self.telegram_channel.get():
            messagebox.showwarning("Внимание", "Настройте Telegram бота")
            return
        self.save_config_settings()

        def publish_thread():
            pub = TelegramPublisher(token=self.telegram_token.get(),
                                    channel_id=self.telegram_channel.get())
            count = pub.publish_all_unpublished()
            self.safe_call(self.log_message, f"✅ Опубликовано {count} вакансий", "success")
            self.safe_call(self.show_all_vacancies)
            self.safe_call(self.update_stats)

        threading.Thread(target=publish_thread, daemon=True).start()
        self.log_message("🔄 Публикация запущена...", "info")

    def test_publish_one(self):
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
            pub = TelegramPublisher(token=self.telegram_token.get(),
                                    channel_id=self.telegram_channel.get())
            pw = tk.Toplevel(self.root)
            pw.title("Тест публикации")
            pw.geometry("300x150")
            pw.transient(self.root)
            pw.grab_set()
            ttk.Label(pw, text="Отправка тестового сообщения...", font=("Arial", 10)).pack(pady=20)
            bar = ttk.Progressbar(pw, mode='indeterminate', length=200)
            bar.pack(pady=10)
            bar.start(10)

            def test_thread():
                success = pub.publish_vacancy_sync(vacancy)
                self.safe_call(self._test_publish_completed, success, vacancy, pw)

            threading.Thread(target=test_thread, daemon=True).start()

        except Exception as e:
            self.log_message(f"Ошибка теста публикации: {e}", "error")
            messagebox.showerror("Ошибка", f"Не удалось запустить тест:\n\n{e}")

    def _test_publish_completed(self, success, vacancy, pw):
        try:
            pw.destroy()
            if success:
                self.log_message("✅ Тестовая публикация успешна!", "success")
                self.show_all_vacancies()
                self.update_stats()
                messagebox.showinfo("Успех", f"Вакансия '{vacancy['title'][:50]}...' опубликована.")
            else:
                self.log_message("❌ Тестовая публикация не удалась", "error")
                messagebox.showwarning("Внимание", "Тест не удался.\n\nПроверьте токен и ID канала.")
        except Exception:
            pass
