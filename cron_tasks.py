#!/usr/bin/env python3
"""
Периодические задачи: очистка кодов Telegram, логов, резервное копирование SQLite.

Запуск: `python cron_tasks.py`, `python scripts/backup_db.py`, systemd timer, или HTTP `/cron/run`.
"""

import os
import shutil
import sys
import sqlite3
from datetime import datetime, timedelta

# Добавляем путь к проекту
project_path = os.path.dirname(os.path.abspath(__file__))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from gwadm.config import BACKUP_DIR, DATABASE_PATH
from gwadm.db import ensure_db, get_db_connection
from gwadm.logging_config import log_debug, log_error

BACKUP_RETENTION_COUNT = 7
BACKUP_FILENAME_PREFIX = 'database_'

def cleanup_expired_verification_codes():
    """Очищает истекшие коды верификации Telegram"""
    conn = None
    try:
        conn = get_db_connection()
        # Удаляем истекшие коды верификации (старше 10 минут)
        result = conn.execute('''
            UPDATE telegram_users
            SET verification_code = NULL,
                verification_code_expires_at = NULL
            WHERE verified = 0
              AND verification_code_expires_at IS NOT NULL
              AND datetime(verification_code_expires_at) < datetime('now', '-10 minutes')
        ''')
        conn.commit()
        deleted_count = result.rowcount
        if deleted_count > 0:
            log_debug(f"Cleaned up {deleted_count} expired verification codes")
        conn.close()
        return deleted_count
    except Exception as e:
        log_error(f"Error cleaning up expired verification codes: {e}")
        if conn:
            conn.close()
        return 0

def cleanup_old_activity_logs(days=90):
    """Очищает старые логи активности (старше указанного количества дней)"""
    conn = None
    try:
        conn = get_db_connection()
        # Удаляем логи старше указанного количества дней
        result = conn.execute('''
            DELETE FROM activity_logs
            WHERE datetime(created_at) < datetime('now', '-' || ? || ' days')
        ''', (days,))
        conn.commit()
        deleted_count = result.rowcount
        if deleted_count > 0:
            log_debug(f"Cleaned up {deleted_count} old activity logs (older than {days} days)")
        conn.close()
        return deleted_count
    except Exception as e:
        log_error(f"Error cleaning up old activity logs: {e}")
        if conn:
            conn.close()
        return 0

def _list_backup_files(backup_dir: str) -> list[tuple[float, str]]:
    backup_files = []
    for name in os.listdir(backup_dir):
        if not name.startswith(BACKUP_FILENAME_PREFIX) or not name.endswith('.db'):
            continue
        file_path = os.path.join(backup_dir, name)
        if os.path.isfile(file_path):
            backup_files.append((os.path.getmtime(file_path), file_path))
    backup_files.sort(reverse=True)
    return backup_files


def backup_database() -> bool:
    """Create a timestamped SQLite backup in BACKUP_DIR and rotate old copies."""
    try:
        db_path = DATABASE_PATH
        if not os.path.exists(db_path):
            log_debug(f"Database file not found at {db_path}, skipping backup")
            return False

        backup_dir = BACKUP_DIR
        os.makedirs(backup_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        backup_path = os.path.join(backup_dir, f'{BACKUP_FILENAME_PREFIX}{timestamp}.db')
        shutil.copy2(db_path, backup_path)

        backup_files = _list_backup_files(backup_dir)
        if len(backup_files) > BACKUP_RETENTION_COUNT:
            for _, old_backup in backup_files[BACKUP_RETENTION_COUNT:]:
                try:
                    os.remove(old_backup)
                    log_debug(f"Removed old backup: {os.path.basename(old_backup)}")
                except OSError as e:
                    log_error(f"Error removing old backup {old_backup}: {e}")

        log_debug(f"Database backup created: {backup_path}")
        return True
    except Exception as e:
        log_error(f"Error creating database backup: {e}")
        return False

def main():
    """Основная функция для выполнения всех задач"""
    ensure_db()
    log_debug(f"Cron tasks started at {datetime.now()}")
    
    # Очистка истекших кодов верификации
    cleanup_expired_verification_codes()
    
    # Очистка старых логов (опционально, раскомментируйте если нужно)
    # cleanup_old_activity_logs(days=90)
    
    # Резервное копирование базы данных (опционально, раскомментируйте если нужно)
    # backup_database()
    
    log_debug(f"Cron tasks completed at {datetime.now()}")

if __name__ == '__main__':
    main()
