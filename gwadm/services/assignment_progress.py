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


def _contact_payload(
    user_id,
    username,
    email=None,
    phone=None,
    telegram=None,
    whatsapp=None,
    viber=None,
    max_phone=None,
) -> dict:
    return {
        'user_id': user_id,
        'username': username or '—',
        'email': (email or '').strip(),
        'phone': (phone or '').strip(),
        'telegram': (telegram or '').strip(),
        'whatsapp': (whatsapp or '').strip(),
        'viber': (viber or '').strip(),
        'max': (max_phone or '').strip(),
    }


def _contacts_from_prefixed_row(row: dict, prefix: str) -> dict:
    return _contact_payload(
        row[f'{prefix}_user_id'],
        row.get(f'{prefix}_username'),
        row.get(f'{prefix}_email'),
        row.get(f'{prefix}_phone'),
        row.get(f'{prefix}_telegram'),
        row.get(f'{prefix}_whatsapp'),
        row.get(f'{prefix}_viber'),
        row.get(f'{prefix}_max'),
    )


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
                santa.email AS santa_email,
                recipient.email AS recipient_email,
                COALESCE(santa_d.phone, santa.phone) AS santa_phone,
                COALESCE(santa_d.telegram, santa.telegram) AS santa_telegram,
                COALESCE(santa_d.whatsapp, santa.whatsapp) AS santa_whatsapp,
                COALESCE(santa_d.viber, santa.viber) AS santa_viber,
                COALESCE(santa_d.max, santa.max) AS santa_max,
                COALESCE(recipient_d.phone, recipient.phone) AS recipient_phone,
                COALESCE(recipient_d.telegram, recipient.telegram) AS recipient_telegram,
                COALESCE(recipient_d.whatsapp, recipient.whatsapp) AS recipient_whatsapp,
                COALESCE(recipient_d.viber, recipient.viber) AS recipient_viber,
                COALESCE(recipient_d.max, recipient.max) AS recipient_max,
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
            LEFT JOIN event_registration_details santa_d
                ON santa_d.event_id = ea.event_id AND santa_d.user_id = ea.santa_user_id
            LEFT JOIN event_registration_details recipient_d
                ON recipient_d.event_id = ea.event_id AND recipient_d.user_id = ea.recipient_user_id
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
        santa_contacts = _contacts_from_prefixed_row(row, 'santa')
        recipient_contacts = _contacts_from_prefixed_row(row, 'recipient')
        s_stage = santa_stage(row)
        r_stage = recipient_stage(row)
        boards['santa'][s_stage].append({
            'assignment_id': row['assignment_id'],
            'user_id': row['santa_user_id'],
            'username': row['santa_username'] or '—',
            'pair_user_id': row['recipient_user_id'],
            'pair_username': row['recipient_username'] or '—',
            'contacts': santa_contacts,
            'pair_contacts': recipient_contacts,
            'stage': s_stage,
        })
        boards['recipient'][r_stage].append({
            'assignment_id': row['assignment_id'],
            'user_id': row['recipient_user_id'],
            'username': row['recipient_username'] or '—',
            'pair_user_id': row['santa_user_id'],
            'pair_username': row['santa_username'] or '—',
            'contacts': recipient_contacts,
            'pair_contacts': santa_contacts,
            'stage': r_stage,
        })

    return {
        'event': {'id': event['id'], 'name': event['name']},
        'boards': boards,
        'santa_columns': [(k, SANTA_COLUMN_LABELS[k]) for k in STAGE_ORDER],
        'recipient_columns': [(k, RECIPIENT_COLUMN_LABELS[k]) for k in STAGE_ORDER],
    }
