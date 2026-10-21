"""DaData address suggest proxy."""

import json

from gwadm.services.dadata import map_dadata_address_data, suggest_address, DadataError


def test_map_dadata_address_data_maps_russian_address():
    mapped = map_dadata_address_data({
        'postal_code': '101000',
        'country': 'Россия',
        'city': 'Москва',
        'street_with_type': 'ул Ленина',
        'house': '10',
        'block': '2',
        'flat': '25',
    })
    assert mapped == {
        'postal_code': '101000',
        'country': 'Россия',
        'city': 'Москва',
        'street': 'ул Ленина',
        'house': '10',
        'building': '2',
        'apartment': '25',
    }


def test_map_dadata_address_data_joins_city_and_settlement():
    mapped = map_dadata_address_data({
        'postal_code': '140155',
        'country': 'Россия',
        'city': 'Раменское',
        'city_with_type': 'г Раменское',
        'settlement': 'Донино',
        'settlement_with_type': 'деревня Донино',
    })
    assert mapped['city'] == 'г Раменское, деревня Донино'
    assert mapped['postal_code'] == '140155'
    assert mapped['street'] == ''


def test_map_dadata_address_data_uses_settlement_when_no_city():
    mapped = map_dadata_address_data({
        'country': 'Россия',
        'settlement': 'Донино',
        'settlement_with_type': 'деревня Донино',
    })
    assert mapped['city'] == 'деревня Донино'


def test_map_dadata_address_data_joins_area_and_settlement_without_city():
    mapped = map_dadata_address_data({
        'postal_code': '347839',
        'country': 'Россия',
        'area': 'Каменский',
        'area_with_type': 'Каменский р-н',
        'settlement': 'Старая Станица',
        'settlement_with_type': 'хутор Старая Станица',
    })
    assert mapped['city'] == 'Каменский р-н, хутор Старая Станица'


def test_map_dadata_address_data_enriches_city_from_display_when_settlement_missing():
    mapped = map_dadata_address_data(
        {
            'postal_code': '347839',
            'country': 'Россия',
            'area': 'Каменский',
            'area_with_type': 'Каменский р-н',
            'city': 'Каменск-Шахтинский',
            'city_with_type': 'г Каменск-Шахтинский',
        },
        display_value='347839, Ростовская обл, Каменский р-н, хутор Старая Станица',
    )
    assert mapped['city'] == 'г Каменск-Шахтинский, хутор Старая Станица'


def test_suggest_address_parses_response(monkeypatch):
    monkeypatch.setattr(
        'gwadm.services.dadata.is_dadata_address_enabled',
        lambda: True,
    )
    monkeypatch.setattr(
        'gwadm.services.dadata.get_setting',
        lambda key, default=None: {
            'dadata_api_key': 'key',
            'dadata_secret_key': 'secret',
        }.get(key, default),
    )

    class FakeResponse:
        status_code = 200
        text = '{}'

        @staticmethod
        def json():
            return {
                'suggestions': [
                    {'value': 'г Москва', 'data': {'city': 'Москва', 'country': 'Россия'}},
                ],
            }

    monkeypatch.setattr('gwadm.services.dadata.requests.post', lambda *a, **k: FakeResponse())
    items = suggest_address('моск')
    assert len(items) == 1
    assert items[0]['value'] == 'г Москва'


def test_suggest_address_short_query_raises():
    try:
        suggest_address('a')
        assert False, 'expected DadataError'
    except DadataError as exc:
        assert '2 символ' in str(exc).lower()


def _csrf_headers(client):
    client.get('/login/dev')
    client.get('/')
    with client.session_transaction() as sess:
        token = sess.get('_csrf_token')
    assert token
    return {
        'Content-Type': 'application/json',
        'X-CSRF-Token': token,
    }


def test_suggest_route_disabled_by_default(client):
    headers = _csrf_headers(client)
    response = client.post(
        '/dadata/suggest/address',
        data=json.dumps({'query': 'москва'}),
        headers=headers,
    )
    assert response.status_code == 403
    body = response.get_json()
    assert body['success'] is False


def test_suggest_route_success(client, monkeypatch):
    headers = _csrf_headers(client)
    monkeypatch.setattr(
        'gwadm.blueprints.integrations.is_dadata_address_enabled',
        lambda: True,
    )
    monkeypatch.setattr(
        'gwadm.blueprints.integrations.suggest_address',
        lambda query, count=7: [
            {
                'value': 'г Москва, ул Ленина, д 10',
                'unrestricted_value': '101000, г Москва, ул Ленина, д 10',
                'data': {
                    'postal_code': '101000',
                    'country': 'Россия',
                    'city': 'Москва',
                    'street_with_type': 'ул Ленина',
                    'house': '10',
                },
            },
        ],
    )
    response = client.post(
        '/dadata/suggest/address',
        data=json.dumps({'query': 'москва ленина'}),
        headers=headers,
    )
    assert response.status_code == 200
    body = response.get_json()
    assert body['success'] is True
    assert len(body['suggestions']) == 1
    assert body['suggestions'][0]['fields']['city'] == 'Москва'
    assert body['suggestions'][0]['fields']['postal_code'] == '101000'
