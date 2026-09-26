"""Hero whispers: decorative phrases on the homepage hero background."""

import sqlite3

DEFAULT_WHISPERS = [
    ('17:46, #1 : WEQ стрелял во врага и залепил 2 раза в _Колунт_', 10),
    ('18:02, #2 : Снежинка бросила подарок и попала в цель', 20),
    ('18:15, #3 : Морозко использовал «Снежный шар»', 30),
    ('18:22, #1 : Анонимный Дед Мороз оставил посылку у двери', 40),
    ('18:31, #4 : Ёлочка получила +5 к праздничному настроению', 50),
    ('18:44, #2 : Северный ветер подул — и подарки разлетелись по адресам', 60),
    ('19:01, #3 : Жеребьёвка завершена. Кто-то уже улыбается.', 70),
    ('19:12, #1 : GWars-эльф доставил посылку без единой царапины', 80),
    ('19:28, #5 : Тайный Санта активировал режим «анонимность»', 90),
    ('19:35, #2 : В чате мерцание: «Спасибо за подарок!»', 100),
]


def upgrade(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS hero_whispers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_hero_whispers_active_sort
        ON hero_whispers (is_active, sort_order)
        '''
    )

    count = conn.execute('SELECT COUNT(*) FROM hero_whispers').fetchone()[0]
    if count == 0:
        for text, sort_order in DEFAULT_WHISPERS:
            conn.execute(
                '''
                INSERT INTO hero_whispers (text, is_active, sort_order)
                VALUES (?, 1, ?)
                ''',
                (text, sort_order),
            )

    existing = conn.execute(
        "SELECT key FROM settings WHERE key = 'hero_whispers_enabled'"
    ).fetchone()
    if not existing:
        conn.execute(
            '''
            INSERT INTO settings (key, value, category)
            VALUES ('hero_whispers_enabled', '1', 'general')
            '''
        )
