"""Jinja template filters."""


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


def register_template_filters(app):
    app.template_filter("format_gender")(format_gender)
    app.template_filter("format_rating")(format_rating)
