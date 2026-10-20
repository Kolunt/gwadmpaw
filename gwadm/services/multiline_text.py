"""Normalize and render user-authored multiline text."""

import re

from markupsafe import Markup, escape


def normalize_multiline_text(value, max_length=None):
    if value is None:
        return ''
    text = str(value)
    text = text.replace('\r\n', '\n')
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = text.replace('\u2028', '\n').replace('\u2029', '\n')
    text = text.strip()
    text = re.sub(r'\n{3,}', '\n\n', text)
    if max_length and len(text) > max_length:
        text = text[:max_length]
    return text


def format_multiline_html(value):
    """Escape HTML and render newlines as <br> tags."""
    text = normalize_multiline_text(value)
    if not text:
        return Markup('')
    return Markup(escape(text).replace('\n', Markup('<br>\n')))
