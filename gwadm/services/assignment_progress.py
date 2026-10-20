"""Admin kanban: gift exchange progress by assignment."""

from gwadm.db import get_db_connection

SANTA_COLUMN_LABELS = {
    'assigned': 'Получил задание',
    'read': 'Прочитал',
    'sent': 'Отправил',
    'confirmed': 'Подтверждение получения',
}

RECIPIENT_COLUMN_LABELS = {
    'assigned': 'Получил задание',
    'read': 'Прочитал',
    'sent': 'Дед отправил',
    'confirmed': 'Подтвердил получение',
}

STAGE_ORDER = ('assigned', 'read', 'sent', 'confirmed')


def assignment_is_sent(row) -> bool:
    sent_at = row.get('santa_sent_at')
    if sent_at is not None and str(sent_at).strip() != '':
        return True
    return bool(row.get('has_santa_message'))


def assignment_is_confirmed(row) -> bool:
    received = row.get('recipient_received_at')
    return received is not None and str(received).strip() != ''


def santa_stage(row) -> str:
    if assignment_is_confirmed(row):
        return 'confirmed'
    if assignment_is_sent(row):
        return 'sent'
    if row.get('santa_viewed_at'):
        return 'read'
    return 'assigned'


def recipient_stage(row) -> str:
    if assignment_is_confirmed(row):
        return 'confirmed'
    if assignment_is_sent(row):
        return 'sent'
    if row.get('recipient_viewed_at'):
        return 'read'
    return 'assigned'


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    return dict(row)


def get_event_progress_board(event_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        event = conn.execute(
            'SELECT id, name FROM events WHERE id = ?',
            (event_id,),
        ).fetchone()
        if not event:
            return None

        rows = conn.execute(
            '''
            SELECT
                ea.id AS assignment_id,
                ea.santa_user_id,
                ea.recipient_user_id,
                ea.santa_viewed_at,
                ea.recipient_viewed_at,
                ea.santa_sent_at,
                ea.recipient_received_at,
                santa.username AS santa_username,
                recipient.username AS recipient_username,
                CASE
                    WHEN (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '') THEN 1
                    WHEN EXISTS (
                        SELECT 1 FROM letter_messages lm
                        WHERE lm.assignment_id = ea.id AND lm.sender = 'santa'
                    ) THEN 1
                    ELSE 0
                END AS has_santa_message
            FROM event_assignments ea
            JOIN users santa ON santa.user_id = ea.santa_user_id
            JOIN users recipient ON recipient.user_id = ea.recipient_user_id
            WHERE ea.event_id = ?
              AND (ea.is_archived = 0 OR ea.is_archived IS NULL)
            ORDER BY ea.assigned_at ASC, ea.id ASC
            ''',
            (event_id,),
        ).fetchall()
    finally:
        conn.close()

    boards = {
        'santa': {stage: [] for stage in STAGE_ORDER},
        'recipient': {stage: [] for stage in STAGE_ORDER},
    }

    for raw in rows:
        row = _row_to_dict(raw)
        row['has_santa_message'] = bool(row.get('has_santa_message'))
        s_stage = santa_stage(row)
        r_stage = recipient_stage(row)
        boards['santa'][s_stage].append({
            'assignment_id': row['assignment_id'],
            'user_id': row['santa_user_id'],
            'username': row['santa_username'] or '—',
            'pair_username': row['recipient_username'] or '—',
            'stage': s_stage,
        })
        boards['recipient'][r_stage].append({
            'assignment_id': row['assignment_id'],
            'user_id': row['recipient_user_id'],
            'username': row['recipient_username'] or '—',
            'pair_username': row['santa_username'] or '—',
            'stage': r_stage,
        })

    return {
        'event': {'id': event['id'], 'name': event['name']},
        'boards': boards,
        'santa_columns': [(k, SANTA_COLUMN_LABELS[k]) for k in STAGE_ORDER],
        'recipient_columns': [(k, RECIPIENT_COLUMN_LABELS[k]) for k in STAGE_ORDER],
    }
