"""Admin settings page markup checks."""


def test_settings_tab_panels_are_not_nested(client):
    client.get('/login/dev')
    body = client.get('/admin/settings').get_data(as_text=True)

    # Each tab panel must be a direct child of the settings form (not inside another panel).
    form_start = body.find('class="settings-form"')
    assert form_start != -1
    form_chunk = body[form_start:]

    for tab in ('general', 'integrations', 'system', 'design', 'profile-data', 'localization', 'modals'):
        marker = f'data-tab="{tab}"'
        idx = form_chunk.find(f'<div class="settings-tab-content')
        assert marker in form_chunk
        pos = form_chunk.find(marker)
        before = form_chunk[:pos]
        # No other tab-content wrapper left open before this panel's data-tab attribute.
        opens = before.count('<div class="settings-tab-content')
        closes = before.count('</div>')
        assert opens <= closes + 1, f'tab {tab} appears nested inside another tab panel'
