"""Event page Assignments button visibility and pulse."""

from gwadm.services.events_stages import (
    event_assignments_button_state,
    is_post_lottery_stage,
    user_has_event_assignment,
)


def test_is_post_lottery_stage():
    assert is_post_lottery_stage('celebration_date') is True
    assert is_post_lottery_stage('after_party') is True
    assert is_post_lottery_stage('lottery') is False
    assert is_post_lottery_stage(None) is False


def test_event_assignments_button_hidden_before_lottery_without_assignment(monkeypatch):
    monkeypatch.setattr(
        'gwadm.services.events_stages.user_has_event_assignment',
        lambda user_id, event_id: False,
    )
    show, pulse = event_assignments_button_state(
        is_registered=True,
        stage_type='main_registration',
        user_id=1,
        event_id=10,
    )
    assert show is False
    assert pulse is False


def test_event_assignments_button_hidden_during_lottery_even_with_assignment(monkeypatch):
    monkeypatch.setattr(
        'gwadm.services.events_stages.user_has_event_assignment',
        lambda user_id, event_id: True,
    )
    show, pulse = event_assignments_button_state(
        is_registered=True,
        stage_type='lottery',
        user_id=1,
        event_id=10,
    )
    assert show is False
    assert pulse is False


def test_event_assignments_button_celebration_pulse(monkeypatch):
    monkeypatch.setattr(
        'gwadm.services.events_stages.user_has_event_assignment',
        lambda user_id, event_id: False,
    )
    show, pulse = event_assignments_button_state(
        is_registered=True,
        stage_type='celebration_date',
        user_id=1,
        event_id=10,
    )
    assert show is True
    assert pulse is True


def test_event_assignments_button_after_party_no_pulse(monkeypatch):
    monkeypatch.setattr(
        'gwadm.services.events_stages.user_has_event_assignment',
        lambda user_id, event_id: False,
    )
    show, pulse = event_assignments_button_state(
        is_registered=True,
        stage_type='after_party',
        user_id=1,
        event_id=10,
    )
    assert show is True
    assert pulse is False


def test_event_assignments_button_not_registered():
    show, pulse = event_assignments_button_state(
        is_registered=False,
        stage_type='celebration_date',
        user_id=1,
        event_id=10,
    )
    assert show is False
    assert pulse is False


def test_user_has_event_assignment_false_without_ids():
    assert user_has_event_assignment(None, 1) is False
    assert user_has_event_assignment(1, None) is False
