"""Jinja contact_usable and address line helpers."""

from gwadm.jinja_filters import contact_usable, format_recipient_address_line, format_recipient_fio_line


def test_contact_usable():
    assert contact_usable('+79161234567') is True
    assert contact_usable('не использую') is False
    assert contact_usable('—') is False
    assert contact_usable('') is False
    assert contact_usable(None) is False


def test_format_recipient_address_line():
    line = format_recipient_address_line({
        'recipient_postal_code': '121467',
        'recipient_country': 'Россия',
        'recipient_city': 'Москва',
        'recipient_street': 'Молодогвардейская',
        'recipient_house': '1',
        'recipient_building': '2',
        'recipient_apartment': '12',
    })
    assert '121467' in line
    assert 'Москва' in line
    assert 'кв. 12' in line


def test_format_recipient_fio_line():
    line = format_recipient_fio_line({
        'recipient_last_name': 'Иванова',
        'recipient_first_name': 'Евгения',
        'recipient_middle_name': 'Александровна',
    })
    assert line == 'Иванова Евгения Александровна'
    assert format_recipient_fio_line({'recipient_first_name': 'Иван'}) == 'Иван'
    assert format_recipient_fio_line({}) == ''
