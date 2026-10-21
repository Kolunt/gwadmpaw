"""DaData address region toggle markup on profile edit."""



def test_profile_edit_shows_address_region_toggle_when_dadata_enabled(client, monkeypatch):
    monkeypatch.setattr(
        'gwadm.context.is_dadata_address_enabled',
        lambda: True,
    )
    client.get('/login/dev')
    response = client.get('/profile/edit')
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'address-region-toggle' in html
    assert 'Я из России' in html
    assert 'dadata-address-search-wrap' in html
    assert 'profile-dadata-wrap' in html
    assert 'bindAddressRegionToggle' in html
