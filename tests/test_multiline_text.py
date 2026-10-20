"""Tests for multiline user text normalization and HTML rendering."""

from markupsafe import Markup

from gwadm.services.multiline_text import format_multiline_html, normalize_multiline_text


def test_normalize_newlines_and_br_tags():
    assert normalize_multiline_text('a\r\nb') == 'a\nb'
    assert normalize_multiline_text('line1<br>line2') == 'line1\nline2'
    assert normalize_multiline_text('x<br/>y<br />z') == 'x\ny\nz'
    assert normalize_multiline_text('  hi\n\n\nbye  ') == 'hi\n\nbye'


def test_normalize_max_length():
    assert normalize_multiline_text('abcdef', max_length=3) == 'abc'


def test_format_multiline_html_escapes_and_breaks():
    result = format_multiline_html('safe<script>\nnext')
    assert isinstance(result, Markup)
    assert '<script>' not in str(result)
    assert '&lt;script&gt;' in str(result)
    assert '<br>' in str(result)


def test_format_multiline_html_legacy_br_input():
    result = format_multiline_html('Теплые слова<br>Несколько штук')
    html = str(result)
    assert '&lt;br&gt;' not in html
    assert 'Теплые слова<br>' in html or 'Теплые слова<br>\n' in html
    assert 'Несколько штук' in html


def test_format_multiline_empty():
    assert format_multiline_html(None) == Markup('')
    assert format_multiline_html('') == Markup('')


def test_format_multiline_jinja_filter(app):
    from flask import render_template_string

    with app.app_context():
        rendered = render_template_string(
            '{{ text | format_multiline }}',
            text='line1<br>line2',
        )
    assert 'line1' in rendered
    assert 'line2' in rendered
    assert '<br>' in rendered
    assert '&lt;br&gt;' not in rendered
