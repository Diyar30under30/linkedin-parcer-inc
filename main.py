"""
LinkedIn Parser Pro v5.5.9 - entry point.

All application logic has been extracted to dedicated modules:
  config.py    - configuration
  postgres.py  - PostgreSQL layer
  database.py  - SQLite layer
  notifier.py  - Telegram publisher
  parser.py    - LinkedIn scraper
  gui.py       - Tkinter GUI (AutoParser + ParserGUI)
"""

import sys
import subprocess


def _install_dependencies():
    packages = [
        "selenium==4.15.0",
        "chromedriver-autoinstaller==0.4.0",
        "requests==2.31.0",
        "beautifulsoup4==4.12.2",
        "lxml",
        "psycopg2-binary",
        "webdriver-manager==4.0.1",
    ]
    print("=" * 50 + "\nUSSTANOVKA ZAVISIMOSTEJ\n" + "=" * 50)
    for pkg in packages:
        print(f"Installing {pkg}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
            print(f"  OK: {pkg}")
        except Exception as exc:
            print(f"  WARN: {pkg}: {exc}")
    print("\nAll done. Please restart the program.")
    input("Press Enter to exit...")
    sys.exit(0)


# --- dependency check ---
try:
    from selenium import webdriver          # noqa: F401
    from bs4 import BeautifulSoup          # noqa: F401
    import requests                        # noqa: F401
    import psycopg2                        # noqa: F401
    from webdriver_manager.chrome import ChromeDriverManager  # noqa: F401
except ImportError as _import_err:
    print(f"Missing dependencies: {_import_err}")
    ans = input("Auto-install? (y/n): ")
    if ans.lower() == "y":
        _install_dependencies()
    else:
        print("Cannot run without dependencies.")
        sys.exit(1)

# --- launch GUI ---
import tkinter as tk
from gui import ParserGUI

if __name__ == "__main__":
    root = tk.Tk()
    ParserGUI(root)
    root.mainloop()
