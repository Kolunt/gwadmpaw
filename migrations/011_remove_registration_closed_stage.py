"""Remove registration_closed stage; lottery start inherits its datetime."""

import sqlite3

from gwadm.logging_config import log_debug
from gwadm.services.events_stages import EVENT_STAGES, STAGE_ORDER_BY_TYPE

_LEGACY_CLOSED = 'registration_closed'
_LOTTERY = 'lottery'


def upgrade(conn: sqlite3.Connection) -> None:
    event_ids = [
        row[0]
        for row in conn.execute('SELECT DISTINCT event_id FROM event_stages').fetchall()
    ]
    for event_id in event_ids:
        rows = conn.execute(
            '''
            SELECT stage_type, start_datetime
            FROM event_stages
            WHERE event_id = ?
            ''',
            (event_id,),
        ).fetchall()
        by_type = {row[0]: row[1] for row in rows}
        closed_start = by_type.get(_LEGACY_CLOSED)
        lottery_start = by_type.get(_LOTTERY)
        if closed_start:
            if not lottery_start or lottery_start != closed_start:
                if lottery_start and lottery_start != closed_start:
                    log_debug(
                        f'migration 011: event {event_id} lottery start '
                        f'{lottery_start!r} replaced by registration_closed {closed_start!r}'
                    )
                conn.execute(
                    '''
                    UPDATE event_stages
                    SET start_datetime = ?
                    WHERE event_id = ? AND stage_type = ?
                    ''',
                    (closed_start, event_id, _LOTTERY),
                )
        conn.execute(
            'DELETE FROM event_stages WHERE event_id = ? AND stage_type = ?',
            (event_id, _LEGACY_CLOSED),
        )
        for stage_type in STAGE_ORDER_BY_TYPE:
            order = STAGE_ORDER_BY_TYPE[stage_type]
            conn.execute(
                '''
                UPDATE event_stages
                SET stage_order = ?
                WHERE event_id = ? AND stage_type = ?
                ''',
                (order, event_id, stage_type),
            )
        for stage in EVENT_STAGES:
            exists = conn.execute(
                'SELECT 1 FROM event_stages WHERE event_id = ? AND stage_type = ?',
                (event_id, stage['type']),
            ).fetchone()
            if not exists:
                conn.execute(
                    '''
                    INSERT INTO event_stages
                    (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
                    VALUES (?, ?, ?, NULL, NULL, ?, ?)
                    ''',
                    (
                        event_id,
                        stage['type'],
                        STAGE_ORDER_BY_TYPE[stage['type']],
                        1 if stage['required'] else 0,
                        0 if stage['required'] else 1,
                    ),
                )
