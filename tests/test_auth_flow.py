"""Smoke tests for auth-related routes."""

def test_login_landing_page(client):
    response = client.get("/login")
    assert response.status_code == 200


def test_login_go_redirects_to_gwars(client):
    response = client.get("/login/go")
    assert response.status_code in (302, 303)
    assert "gwars.io" in response.headers.get("Location", "")


def test_login_go_mobile_interstitial(client):
    response = client.get(
        "/login/go",
        headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"},
    )
    assert response.status_code == 200


def test_cron_requires_token(client):
    response = client.get("/cron/run")
    assert response.status_code == 401
