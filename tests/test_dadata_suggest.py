"""DaData address suggest proxy."""

import json

from gwadm.services.dadata import (
    extract_dadata_location,
    map_dadata_address_data,
    sanitize_dadata_locations,
    suggest_address,
    DadataError,
)


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
        'region': 'Московская',
        'region_with_type': 'Московская обл',
        'city': 'Раменское',
        'city_with_type': 'г Раменское',
        'settlement': 'Донино',
        'settlement_with_type': 'деревня Донино',
    })
    assert mapped['city'] == 'Московская обл, г Раменское, деревня Донино'


def test_map_dadata_address_data_joins_region_city_and_village():
    mapped = map_dadata_address_data({
        'country': 'Россия',
        'region': 'Московская',
        'region_with_type': 'Московская обл',
        'city': 'Раменское',
        'city_with_type': 'г Раменское',
        'settlement': 'Гжель',
        'settlement_with_type': 'село Гжель',
    })
    assert mapped['city'] == 'Московская обл, г Раменское, село Гжель'


def test_map_dadata_address_data_skips_redundant_region_for_moscow():
    mapped = map_dadata_address_data({
        'country': 'Россия',
        'region': 'Москва',
        'region_with_type': 'г Москва',
        'city': 'Москва',
        'city_with_type': 'г Москва',
        'street_with_type': 'ул Ленина',
    })
    assert mapped['city'] == 'г Москва'
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
        'region': 'Ростовская',
        'region_with_type': 'Ростовская обл',
        'area': 'Каменский',
        'area_with_type': 'Каменский р-н',
        'settlement': 'Старая Станица',
        'settlement_with_type': 'хутор Старая Станица',
    })
    assert mapped['city'] == 'Ростовская обл, Каменский р-н, хутор Старая Станица'


def test_map_dadata_address_data_includes_region_for_stanitsa_without_city():
    mapped = map_dadata_address_data({
        'country': 'Россия',
        'region': 'Краснодарский',
        'region_with_type': 'Краснодарский край',
        'area': 'Анапский',
        'area_with_type': 'Анапский р-н',
        'settlement': 'Гостагаевская',
        'settlement_with_type': 'ст-ца Гостагаевская',
    })
    assert mapped['city'] == 'Краснодарский край, Анапский р-н, ст-ца Гостагаевская'


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

    captured = {}

    def fake_post(*_args, **kwargs):
        captured['json'] = kwargs.get('json')
        return FakeResponse()

    monkeypatch.setattr('gwadm.services.dadata.requests.post', fake_post)
    items = suggest_address('моск')
    assert len(items) == 1
    assert items[0]['value'] == 'г Москва'
    assert captured['json'] == {'query': 'моск', 'count': 7}


def test_suggest_address_passes_locations_and_street_bounds(monkeypatch):
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
            return {'suggestions': []}

    captured = {}

    def fake_post(*_args, **kwargs):
        captured['json'] = kwargs.get('json')
        return FakeResponse()

    monkeypatch.setattr('gwadm.services.dadata.requests.post', fake_post)
    suggest_address(
        'ленина',
        locations=[{'city_fias_id': 'abc', 'settlement': 'Донино'}],
        street_only=True,
    )
    assert captured['json']['from_bound'] == {'value': 'street'}
    assert captured['json']['to_bound'] == {'value': 'house'}
    assert captured['json']['locations'] == [{'city_fias_id': 'abc', 'settlement': 'Донино'}]


def test_extract_dadata_location_collects_fias_fields():
    loc = extract_dadata_location({
        'postal_code': '347839',
        'region_fias_id': 'reg',
        'city': 'Каменск-Шахтинский',
        'city_fias_id': 'city-id',
        'settlement': 'Старая Станица',
        'settlement_fias_id': 'settle-id',
    })
    assert loc['postal_code'] == '347839'
    assert loc['city_fias_id'] == 'city-id'
    assert loc['settlement_fias_id'] == 'settle-id'


def test_sanitize_dadata_locations_filters_unknown_keys():
    cleaned = sanitize_dadata_locations([
        {'city': 'Москва', 'hack': 'x', 'city_fias_id': '1'},
        'bad',
    ])
    assert cleaned == [{'city': 'Москва', 'city_fias_id': '1'}]


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
        lambda query, count=7, locations=None, street_only=False: [
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
    assert 'location' in body['suggestions'][0]


def test_suggest_route_accepts_street_only(client, monkeypatch):
    headers = _csrf_headers(client)
    captured = {}

    def fake_suggest(query, count=7, locations=None, street_only=False):
        captured['street_only'] = street_only
        captured['locations'] = locations
        return []

    monkeypatch.setattr(
        'gwadm.blueprints.integrations.is_dadata_address_enabled',
        lambda: True,
    )
    monkeypatch.setattr('gwadm.blueprints.integrations.suggest_address', fake_suggest)
    response = client.post(
        '/dadata/suggest/address',
        data=json.dumps({
            'query': 'ленина',
            'street_only': True,
            'locations': [{'city_fias_id': '1', 'ignored': 'x'}],
        }),
        headers=headers,
    )
    assert response.status_code == 200
    assert captured['street_only'] is True
    assert captured['locations'] == [{'city_fias_id': '1'}]
