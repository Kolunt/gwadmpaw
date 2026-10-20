"""Dadata API key verification."""

from gwadm.services.telegram import verify_dadata_api


def test_verify_dadata_api_missing_keys():
    ok, msg = verify_dadata_api('', '')
    assert ok is False
    assert 'обязательны' in msg.lower()


def test_verify_dadata_api_success(monkeypatch):
    class FakeResponse:
        status_code = 200
        text = '{"balance":100.5}'

        @staticmethod
        def json():
            return {'balance': 100.5}

    def fake_get(url, **kwargs):
        assert 'profile/balance' in url
        return FakeResponse()

    monkeypatch.setattr('gwadm.services.telegram.requests.get', fake_get)
    ok, msg = verify_dadata_api('api', 'secret')
    assert ok is True
    assert 'проверены' in msg.lower()
    assert '100.5' in msg
