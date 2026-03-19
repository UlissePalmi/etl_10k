"""
Capture all stdout to Telegram in real-time.
Set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID environment variables.

Usage in scripts:
    from etl_10k.utils.telegram_logger import TelegramStream
    import sys

    # Redirect stdout to Telegram
    sys.stdout = TelegramStream()
"""

import requests
import os
import sys
from io import StringIO

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_message(text: str) -> bool:
    """
    Send a message to Telegram.
    Returns True if successful, False otherwise.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False  # Not configured

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

    try:
        response = requests.post(url, data=data, timeout=5)
        return response.status_code == 200
    except Exception:
        return False


class TelegramStream:
    """
    Redirect stdout to both terminal AND Telegram.
    Batches messages to avoid spam (sends every 50 lines or 10 seconds).
    """

    def __init__(self, original_stdout=None):
        self.original_stdout = original_stdout or sys.__stdout__
        self.buffer = StringIO()
        self.line_count = 0

    def write(self, text: str) -> int:
        # Write to both stdout and buffer
        self.original_stdout.write(text)
        self.buffer.write(text)
        self.line_count += text.count("\n")

        # Send to Telegram every 50 lines (batch to avoid rate limit)
        if self.line_count >= 50:
            self.flush_to_telegram()

        return len(text)

    def flush_to_telegram(self):
        """Send buffered output to Telegram."""
        content = self.buffer.getvalue().strip()
        if content and len(content) > 10:  # Only send non-empty messages
            send_message(content[-4000:])  # Telegram limit is 4096 chars, take last 4000
            self.buffer = StringIO()
            self.line_count = 0

    def flush(self):
        self.original_stdout.flush()

    def isatty(self):
        return self.original_stdout.isatty()
