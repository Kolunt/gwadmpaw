"""Admin localization: _parse_updates_from_form with repeated fields."""

from flask import Flask

from gwadm.blueprints.admin.localization import _parse_updates_from_form


def test_parse_updates_from_repeated_form_fields():
    mini = Flask(__name__)
    with mini.test_request_context(
        '/admin/localization',
        method='POST',
        data={
            'msg_key': ['KeyOne', 'KeyTwo'],
            'ru': ['r1', 'r2'],
            'en': ['e1', 'e2'],
        },
    ):
        out = _parse_updates_from_form()
    assert out == [
        ('KeyOne', 'ru', 'r1'),
        ('KeyOne', 'en', 'e1'),
        ('KeyTwo', 'ru', 'r2'),
        ('KeyTwo', 'en', 'e2'),
    ]
