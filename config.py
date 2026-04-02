import os
from dotenv import load_dotenv

load_dotenv()  # loads .env from the project root


class Config:
    # ── File paths ────────────────────────────────────────────
    DB_FILE  = "vacancies.db"
    LOG_FILE = "parser.log"
    CONFIG_FILE = "config.json"

    # ── LinkedIn search targets (non-sensitive) ────────────────
    CITIES = [
        "Астана", "Алматы", "Караганда", "Нур-Султан", "Шымкент",
        "Актобе", "Тараз", "Павлодар", "Усть-Каменогорск", "Семей",
    ]
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
        "Analyst",
    ]

    # ── Parser behaviour ──────────────────────────────────────
    DELAY_MIN                = int(os.getenv("DELAY_MIN", 3))
    DELAY_MAX                = int(os.getenv("DELAY_MAX", 5))
    MAX_VACANCIES_PER_SEARCH = int(os.getenv("MAX_VACANCIES", 20))
    HEADLESS                 = os.getenv("HEADLESS", "true").lower() == "true"

    # ── Auto-parsing ──────────────────────────────────────────
    AUTO_PARSE_INTERVAL = int(os.getenv("AUTO_PARSE_INTERVAL", 600))
    AUTO_PARSE_ENABLED  = os.getenv("AUTO_PARSE_ENABLED", "false").lower() == "true"

    # ── Deduplication ─────────────────────────────────────────
    DEDUPLICATION_ENABLED = os.getenv("DEDUPLICATION_ENABLED", "true").lower() == "true"
    SIMILARITY_THRESHOLD  = float(os.getenv("SIMILARITY_THRESHOLD", 0.7))

    # ── Telegram ──────────────────────────────────────────────
    MAX_DESCRIPTION_LENGTH = int(os.getenv("MAX_DESCRIPTION_LENGTH", 1000))
    TELEGRAM_TOKEN         = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHANNEL       = os.getenv("TELEGRAM_CHANNEL")

    # ── PostgreSQL — loaded from environment, never hardcoded ──
    POSTGRES_HOST     = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_DB       = os.getenv("POSTGRES_DB")
    POSTGRES_USER     = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_ENABLED  = os.getenv("POSTGRES_ENABLED", "false").lower() == "true"

    # ── LinkedIn credentials ───────────────────────────────────
    LINKEDIN_EMAIL    = os.getenv("LINKEDIN_EMAIL")
    LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")

    # ── Misc ──────────────────────────────────────────────────
    SYSTEM_USER_UUID = os.getenv("SYSTEM_USER_UUID", "00000000-0000-0000-0000-000000000001")
    ORG_URL_COLUMN   = "org_url"

    # ── Instance helper (keeps backward-compat with run_cron.py) ─
    def get(self, key, default=None):
        """Get a config value by attribute name (case-insensitive)."""
        attr = key.upper()
        if hasattr(self, attr):
            return getattr(self, attr)
        return default

    # ── Startup validation ────────────────────────────────────
    @classmethod
    def validate(cls):
        """Call at startup to catch missing required env vars early."""
        required = {}

        if cls.POSTGRES_ENABLED:
            required.update(
                POSTGRES_HOST=cls.POSTGRES_HOST,
                POSTGRES_DB=cls.POSTGRES_DB,
                POSTGRES_USER=cls.POSTGRES_USER,
                POSTGRES_PASSWORD=cls.POSTGRES_PASSWORD,
            )

        required.update(
            LINKEDIN_EMAIL=cls.LINKEDIN_EMAIL,
            LINKEDIN_PASSWORD=cls.LINKEDIN_PASSWORD,
        )

        missing = [k for k, v in required.items() if not v]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                f"Check your .env file."
            )
