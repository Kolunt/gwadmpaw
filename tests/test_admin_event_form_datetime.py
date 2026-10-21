"""Admin event form: native datetime-local picker markup."""


def test_event_create_form_has_datetime_picker_markup(client):
    client.get('/login/dev')
    body = client.get('/admin/events/create').get_data(as_text=True)

    assert 'admin-event-form.js' in body
    assert 'datetime-field-wrap' in body
    assert 'datetime-picker-btn' in body
    assert 'type="datetime-local"' in body
    assert 'name="stage_main_registration_start"' in body
