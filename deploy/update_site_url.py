#!/usr/bin/env python3
import sqlite3
import sys

db_path = sys.argv[1] if len(sys.argv) > 1 else "database.db"
url = sys.argv[2] if len(sys.argv) > 2 else "https://gwadm.ru"

conn = sqlite3.connect(db_path)
row = conn.execute("SELECT key FROM settings WHERE key = ?", ("site_url",)).fetchone()
if row:
    conn.execute("UPDATE settings SET value = ? WHERE key = ?", (url, "site_url"))
else:
    conn.execute(
        "INSERT INTO settings (key, value, description, category) VALUES (?, ?, ?, ?)",
        ("site_url", url, "Base site URL", "integrations"),
    )
conn.commit()
print(conn.execute("SELECT value FROM settings WHERE key = 'site_url'").fetchone()[0])
conn.close()
