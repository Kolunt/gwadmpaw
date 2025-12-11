#!/usr/bin/env python3
"""
Скрипт для периодических задач (cron jobs)
Запускается через cron на PythonAnywhere

Задачи:
- Очистка истекших кодов верификации Telegram
- Очистка старых логов (опционально)
- Резервное копирование базы данных (опционально)
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

# Добавляем путь к проекту
project_path = os.path.dirname(os.path.abspath(__file__))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

# Импортируем функции из app.py
from app import get_db_connection, log_error, log_debug

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

def backup_database():
    """Создает резервную копию базы данных"""
    try:
        db_path = os.path.join(project_path, 'database.db')
        if not os.path.exists(db_path):
            log_debug("Database file not found, skipping backup")
            return False
        
        # Создаем имя файла бэкапа с датой и временем
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(project_path, f'database.db.backup_{timestamp}')
        
        # Копируем файл базы данных
        import shutil
        shutil.copy2(db_path, backup_path)
        
        # Удаляем старые бэкапы (оставляем только последние 7)
        backup_files = []
        for file in os.listdir(project_path):
            if file.startswith('database.db.backup_') and file.endswith('.backup') == False:
                file_path = os.path.join(project_path, file)
                if os.path.isfile(file_path):
                    backup_files.append((os.path.getmtime(file_path), file_path))
        
        # Сортируем по времени модификации (новые первыми)
        backup_files.sort(reverse=True)
        
        # Удаляем старые бэкапы, оставляя только последние 7
        if len(backup_files) > 7:
            for _, old_backup in backup_files[7:]:
                try:
                    os.remove(old_backup)
                    log_debug(f"Removed old backup: {os.path.basename(old_backup)}")
                except Exception as e:
                    log_error(f"Error removing old backup {old_backup}: {e}")
        
        log_debug(f"Database backup created: {os.path.basename(backup_path)}")
        return True
    except Exception as e:
        log_error(f"Error creating database backup: {e}")
        return False

def main():
    """Основная функция для выполнения всех задач"""
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
