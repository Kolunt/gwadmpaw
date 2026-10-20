"""Sidebar pulse during celebration_date stage."""

from gwadm.services.events_stages import user_in_celebration_exchange_stage


def test_user_in_celebration_exchange_stage_false_without_user():
    assert user_in_celebration_exchange_stage(None) is False


def test_user_in_celebration_exchange_stage(monkeypatch):
    monkeypatch.setattr(
        'gwadm.services.events_stages.get_db_connection',
        lambda: type(
            'Conn',
            (),
            {
                'execute': lambda self, *args, **kwargs: type(
                    'R', (), {'fetchall': lambda self: [{'event_id': 1}]}
                )(),
                'close': lambda self: None,
            },
        )(),
    )

    def fake_stage(event_id):
        if event_id == 1:
            return {'info': {'type': 'celebration_date', 'name': 'Обмен подарками'}}
        return None

    monkeypatch.setattr('gwadm.services.events_stages.get_current_event_stage', fake_stage)
    assert user_in_celebration_exchange_stage(42) is True
