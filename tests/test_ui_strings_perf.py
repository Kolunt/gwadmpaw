"""Regression: get_string uses in-memory cache without opening DB per call."""

import gwadm.db as gwadm_db
import gwadm.services.ui_strings as ui_strings
from gwadm.services.ui_strings import ensure_ui_strings_seeded


def test_get_string_hot_path_no_db_after_cache_warmed(app):
    gwadm_db.ensure_db()
    ui_strings.invalidate_ui_strings_cache()

    conn = gwadm_db.get_db_connection()
    try:
        ensure_ui_strings_seeded(conn)
    finally:
        conn.close()

    from gwadm.i18n import STATIC_FALLBACK_EN, STATIC_FALLBACK_RU

    db_opens: list[int] = []

    def counting_factory():
        db_opens.append(1)
        return gwadm_db.get_db_connection()

    ui_strings.get_string(
        'Home', 'ru', counting_factory, STATIC_FALLBACK_RU, STATIC_FALLBACK_EN
    )
    assert len(db_opens) == 1, 'first lookup should open DB to build cache'

    db_opens.clear()
    for _ in range(20):
        ui_strings.get_string(
            'Home', 'ru', counting_factory, STATIC_FALLBACK_RU, STATIC_FALLBACK_EN
        )
        ui_strings.get_string(
            'Events', 'ru', counting_factory, STATIC_FALLBACK_RU, STATIC_FALLBACK_EN
        )

    assert db_opens == [], 'hot path must not open DB when cache exists'

    ui_strings.invalidate_ui_strings_cache()
