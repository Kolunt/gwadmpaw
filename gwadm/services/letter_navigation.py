"""Group letter chats by event for compact navigation."""

from __future__ import annotations

from collections import defaultdict

_ROLE_SORT = {'santa': 0, 'grandchild': 1, 'admin': 2}


def build_letter_item(assignment: dict) -> dict:
    """Build a navigation item from an accessible assignment row."""
    role = assignment.get('chat_role', 'santa')
    label = ''
    if role == 'santa':
        counterpart = (
            assignment.get('recipient_first_name')
            or assignment.get('recipient_username')
            or assignment.get('recipient_last_name')
            or 'Получатель'
        )
        label = f"Получатель: {counterpart}"
    elif role == 'grandchild':
        counterpart = assignment.get('santa_username') or 'Дед Мороз'
        label = f"Дед Мороз: {counterpart}"
    elif role == 'admin':
        santa_label = assignment.get('santa_full_name') or assignment.get('santa_username') or 'Дед Мороз'
        recipient_label = (
            assignment.get('recipient_full_name')
            or assignment.get('recipient_username')
            or 'Внучок'
        )
        label = f"Санта: {santa_label} → Внучок: {recipient_label}"

    return {
        'assignment_id': assignment.get('id'),
        'event_id': assignment.get('event_id'),
        'event_name': assignment.get('event_name', 'Мероприятие'),
        'assigned_at': assignment.get('assigned_at'),
        'label': label,
        'role': role,
        'santa_label': assignment.get('santa_full_name') or assignment.get('santa_username'),
        'recipient_label': assignment.get('recipient_full_name') or assignment.get('recipient_username'),
    }


def build_letter_events(letter_items: list[dict]) -> list[dict]:
    if not letter_items:
        return []

    groups: dict[int, list[dict]] = defaultdict(list)
    for item in letter_items:
        event_id = item.get('event_id')
        if event_id is None:
            continue
        groups[int(event_id)].append(item)

    events: list[dict] = []
    for event_id, chats in groups.items():
        chats_sorted = sorted(
            chats,
            key=lambda chat: (
                _ROLE_SORT.get(chat.get('role'), 99),
                chat.get('assignment_id') or 0,
            ),
        )
        sort_key = max((chat.get('assigned_at') or '') for chat in chats_sorted)
        event_name = chats_sorted[0].get('event_name', 'Мероприятие')
        santa_assignment_id = next(
            (chat['assignment_id'] for chat in chats_sorted if chat.get('role') == 'santa'),
            None,
        )
        grandchild_assignment_id = next(
            (chat['assignment_id'] for chat in chats_sorted if chat.get('role') == 'grandchild'),
            None,
        )
        events.append(
            {
                'event_id': event_id,
                'event_name': event_name,
                'sort_key': sort_key,
                'chats': chats_sorted,
                'santa_assignment_id': santa_assignment_id,
                'grandchild_assignment_id': grandchild_assignment_id,
            }
        )

    events.sort(key=lambda event: (event.get('sort_key') or '', event['event_id']), reverse=True)
    return events


def pick_event_default_assignment_id(event_group: dict, prefer_role: str = 'santa') -> int | None:
    if prefer_role == 'santa' and event_group.get('santa_assignment_id'):
        return event_group['santa_assignment_id']
    if event_group.get('grandchild_assignment_id'):
        return event_group['grandchild_assignment_id']
    chats = event_group.get('chats') or []
    if chats:
        return chats[0].get('assignment_id')
    return None


def pick_default_assignment_id(letter_events: list[dict]) -> int | None:
    if not letter_events:
        return None
    return pick_event_default_assignment_id(letter_events[0], prefer_role='santa')


def find_letter_event(letter_events: list[dict], event_id: int | None) -> dict | None:
    if event_id is None:
        return letter_events[0] if letter_events else None
    for event in letter_events:
        if event.get('event_id') == event_id:
            return event
    return letter_events[0] if letter_events else None
