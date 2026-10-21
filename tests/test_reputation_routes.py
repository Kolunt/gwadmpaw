"""Public reputation URL and legacy /rating redirect."""


def test_rating_legacy_redirects_to_reputation(client):
    response = client.get('/rating', follow_redirects=False)
    assert response.status_code == 301
    location = response.headers.get('Location', '')
    assert '/reputation' in location


def test_reputation_page_ok(client):
    response = client.get('/reputation')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'Репутация участников' in body
