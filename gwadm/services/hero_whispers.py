"""Hero whispers: decorative phrases on the homepage."""

from gwadm.logging_config import log_error
from gwadm.services.settings import get_setting, set_setting


def is_hero_whispers_enabled() -> bool:
    return get_setting('hero_whispers_enabled', '1') == '1'


def set_hero_whispers_enabled(enabled: bool) -> bool:
    return set_setting('hero_whispers_enabled', '1' if enabled else '0', category='general')


def get_active_whispers(conn) -> list[str]:
    rows = conn.execute(
        '''
        SELECT text FROM hero_whispers
        WHERE is_active = 1
        ORDER BY sort_order, id
        '''
    ).fetchall()
    return [row['text'] for row in rows]


def list_all_whispers(conn):
    return conn.execute(
        '''
        SELECT id, text, is_active, sort_order, created_at, updated_at
        FROM hero_whispers
        ORDER BY sort_order, id
        '''
    ).fetchall()


def create_whisper(conn, text: str, sort_order: int = 100, is_active: int = 1) -> int:
    cursor = conn.execute(
        '''
        INSERT INTO hero_whispers (text, is_active, sort_order)
        VALUES (?, ?, ?)
        ''',
        (text, is_active, sort_order),
    )
    conn.commit()
    return cursor.lastrowid


def update_whisper(conn, whisper_id: int, text: str, sort_order: int, is_active: int) -> bool:
    try:
        conn.execute(
            '''
            UPDATE hero_whispers
            SET text = ?, sort_order = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (text, sort_order, is_active, whisper_id),
        )
        conn.commit()
        return True
    except Exception as e:
        log_error(f"Error updating hero whisper {whisper_id}: {e}")
        return False


def delete_whisper(conn, whisper_id: int) -> bool:
    try:
        conn.execute('DELETE FROM hero_whispers WHERE id = ?', (whisper_id,))
        conn.commit()
        return True
    except Exception as e:
        log_error(f"Error deleting hero whisper {whisper_id}: {e}")
        return False


def toggle_whisper_active(conn, whisper_id: int) -> bool:
    try:
        row = conn.execute(
            'SELECT is_active FROM hero_whispers WHERE id = ?',
            (whisper_id,),
        ).fetchone()
        if not row:
            return False
        new_value = 0 if row['is_active'] else 1
        conn.execute(
            '''
            UPDATE hero_whispers
            SET is_active = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (new_value, whisper_id),
        )
        conn.commit()
        return True
    except Exception as e:
        log_error(f"Error toggling hero whisper {whisper_id}: {e}")
        return False
