"""Tests for letter chat navigation grouping."""

from gwadm.services.letter_navigation import (
    build_letter_events,
    pick_default_assignment_id,
    pick_event_default_assignment_id,
)


def _item(event_id, assignment_id, role, assigned_at='', event_name='E'):
    return {
        'event_id': event_id,
        'assignment_id': assignment_id,
        'role': role,
        'assigned_at': assigned_at,
        'event_name': event_name,
        'label': role,
    }


def test_build_letter_events_sorts_by_assigned_at_desc():
    items = [
        _item(1, 10, 'santa', '2024-01-01', 'Old'),
        _item(1, 11, 'grandchild', '2024-01-01', 'Old'),
        _item(2, 20, 'santa', '2025-06-01', 'New'),
        _item(2, 21, 'grandchild', '2025-06-01', 'New'),
    ]
    events = build_letter_events(items)
    assert [e['event_id'] for e in events] == [2, 1]
    assert events[0]['santa_assignment_id'] == 20
    assert events[0]['grandchild_assignment_id'] == 21
    assert [c['role'] for c in events[0]['chats']] == ['santa', 'grandchild']


def test_pick_default_assignment_id_prefers_santa_in_latest_event():
    events = build_letter_events(
        [
            _item(1, 10, 'grandchild', '2024-01-01'),
            _item(2, 20, 'grandchild', '2025-01-01'),
            _item(2, 21, 'santa', '2025-01-02'),
        ]
    )
    assert pick_default_assignment_id(events) == 21


def test_pick_event_default_assignment_id_santa_first():
    event = {
        'santa_assignment_id': 5,
        'grandchild_assignment_id': 6,
        'chats': [],
    }
    assert pick_event_default_assignment_id(event) == 5
