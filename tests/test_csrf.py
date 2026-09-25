"""CSRF protection tests."""

def test_post_without_csrf_is_rejected(client):
    client.get("/login")
    with client.session_transaction() as sess:
        sess["user_id"] = 283494

    response = client.post(
        "/api/profile/update",
        json={"bio": "test"},
        headers={
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    assert response.status_code == 400
    data = response.get_json()
    assert data["success"] is False


def test_post_with_csrf_token_is_accepted(client):
    client.get("/login")
    with client.session_transaction() as sess:
        sess["user_id"] = 283494
        token = sess.get("_csrf_token")

    response = client.post(
        "/api/profile/update",
        json={"bio": "csrf-test"},
        headers={
            "Content-Type": "application/json",
            "X-CSRF-Token": token,
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    assert response.status_code == 200
