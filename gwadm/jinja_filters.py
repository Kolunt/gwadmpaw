"""Jinja template filters."""

from gwadm.services.multiline_text import format_multiline_html

_CONTACT_EMPTY = frozenset({'не использую', 'нет', '-', '—', 'none', 'no', 'n/a'})


def format_gender(value):
    """Convert gender codes (0/1) into human-readable labels."""
    if value is None:
        return "не указан"
    value_str = str(value).strip()
    if value_str == "0":
        return "Мужчина"
    if value_str == "1":
        return "Женщина"
    return value_str or "не указан"


def format_rating(value):
    try:
        rating_float = float(value)
        rounded = round(rating_float, 1)
        if abs(rating_float - rounded) < 0.0001 and rounded % 1 == 0:
            return str(int(rounded))
        formatted = f"{rating_float:.1f}"
        if formatted.endswith(".0"):
            return formatted[:-2]
        return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted
    except (ValueError, TypeError):
        return str(value)


def contact_usable(value) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    return text.lower() not in _CONTACT_EMPTY


def format_recipient_address_line(recipient) -> str:
    if not recipient:
        return ''
    chunks = []
    postal = str(recipient.get('recipient_postal_code') or '').strip()
    country = str(recipient.get('recipient_country') or '').strip()
    city = str(recipient.get('recipient_city') or '').strip()
    street = str(recipient.get('recipient_street') or '').strip()
    house = str(recipient.get('recipient_house') or '').strip()
    building = str(recipient.get('recipient_building') or '').strip()
    apartment = str(recipient.get('recipient_apartment') or '').strip()
    if postal:
        chunks.append(postal)
    locality = ', '.join(part for part in (country, city) if part)
    if locality:
        chunks.append(locality)
    street_parts = []
    if street:
        street_parts.append(street)
    if house:
        street_parts.append(f'д. {house}')
    if building:
        street_parts.append(f'корп. {building}')
    if apartment:
        street_parts.append(f'кв. {apartment}')
    if street_parts:
        chunks.append(', '.join(street_parts))
    return ', '.join(chunks)


def register_template_filters(app):
    app.template_filter("format_gender")(format_gender)
    app.template_filter("format_rating")(format_rating)
    app.template_filter("format_multiline")(format_multiline_html)
    app.template_filter("contact_usable")(contact_usable)
    app.template_filter("format_recipient_address_line")(format_recipient_address_line)
