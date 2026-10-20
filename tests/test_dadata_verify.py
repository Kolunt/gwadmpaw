"""Dadata API key verification."""

from gwadm.services.telegram import verify_dadata_api


def test_verify_dadata_api_missing_keys():
    ok, msg = verify_dadata_api('', '')
    assert ok is False
    assert 'обязательны' in msg.lower()


def test_verify_dadata_api_success(monkeypatch):
    class FakeResponse:
        status_code = 200
        text = '{"version":"1"}'

    monkeypatch.setattr(
        'gwadm.services.telegram.requests.get',
        lambda *args, **kwargs: FakeResponse(),
    )
    ok, msg = verify_dadata_api('api', 'secret')
    assert ok is True
    assert 'проверены' in msg.lower()
