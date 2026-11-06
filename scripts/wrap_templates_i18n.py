"""
Wrap known UI string literals in templates with Jinja {{ _('...') }} / {{ _("...") }}.

Fast path: only considers registry keys that appear as substrings in each region.
Skips <script>...</script> and <style>...</style>.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _jinja_call(msg_key: str) -> str:
    if "'" in msg_key:
        escaped = msg_key.replace("\\", "\\\\").replace('"', '\\"')
        return '{{ _("' + escaped + '") }}'
    return "{{ _('" + msg_key.replace("'", "\\'") + "') }}"


def _load_keys() -> list[str]:
    from gwadm.ui_string_registry import UI_STRING_SEEDS

    return sorted({row[0] for row in UI_STRING_SEEDS}, key=lambda s: (-len(s), s))


def _split_skip_regions(html: str) -> list[tuple[str, bool]]:
    lower = html.lower()
    chunks: list[tuple[str, bool]] = []
    pos = 0
    while pos < len(html):
        idx_script = lower.find("<script", pos)
        idx_style = lower.find("<style", pos)
        next_tags = [i for i in (idx_script, idx_style) if i != -1]
        if not next_tags:
            chunks.append((html[pos:], False))
            break
        nxt = min(next_tags)
        if nxt > pos:
            chunks.append((html[pos:nxt], False))
        if lower[nxt : nxt + 7] == "<script":
            close = lower.find("</script>", nxt)
            if close == -1:
                chunks.append((html[nxt:], True))
                break
            end = close + len("</script>")
        else:
            close = lower.find("</style>", nxt)
            if close == -1:
                chunks.append((html[nxt:], True))
                break
            end = close + len("</style>")
        chunks.append((html[nxt:end], True))
        pos = end
    return chunks


def _transform_chunk(chunk: str, all_keys: list[str], skip: bool) -> str:
    if skip:
        return chunk
    relevant = [k for k in all_keys if k in chunk]
    if not relevant:
        return chunk
    out = chunk

    # Simple page titles: {% block title %}...{% endblock %} (no Jinja inside)
    def _wrap_block_title(m: re.Match[str]) -> str:
        inner = m.group(1).strip()
        if "{{" in inner or "{%" in inner:
            return m.group(0)
        if inner not in relevant:
            return m.group(0)
        return "{% block title %}" + _jinja_call(inner) + "{% endblock %}"

    out = re.sub(
        r"\{%\s*block\s+title\s*%\}\s*(.*?)\s*\{%\s*endblock\s*%\}",
        _wrap_block_title,
        out,
        flags=re.DOTALL | re.IGNORECASE,
    )

    for key in relevant:
        jc = _jinja_call(key)
        if jc in out:
            continue
        # Attributes (double and single quotes)
        for attr in ("title", "aria-label", "placeholder", "alt"):
            out = out.replace(f'{attr}="{key}"', f'{attr}="{jc}"')
            out = out.replace(f"{attr}='{key}'", f'{attr}="{jc}"')

        # Text nodes with optional whitespace
        text_pat = re.compile(r">(\s*)" + re.escape(key) + r"(\s*)<")

        def text_repl(m: re.Match[str], *, jc=jc, k=key) -> str:
            if "{{ _('" in m.string[max(0, m.start() - 15) : m.start()] or '{{ _("' in m.string[
                max(0, m.start() - 15) : m.start()
            ]:
                return m.group(0)
            return f">{m.group(1)}{jc}{m.group(2)}<"

        out = text_pat.sub(text_repl, out)

    return out


def transform_html(html: str, keys: list[str]) -> str:
    return "".join(_transform_chunk(ch, keys, sk) for ch, sk in _split_skip_regions(html))


def main() -> None:
    keys = _load_keys()
    tmpl = ROOT / "templates"
    n_changed = 0
    for path in sorted(tmpl.rglob("*.html")):
        if "debug" in path.name.lower():
            continue
        raw = path.read_text(encoding="utf-8")
        new = transform_html(raw, keys)
        if new != raw:
            path.write_text(new, encoding="utf-8")
            n_changed += 1
            print("updated", path.relative_to(ROOT))
    print(f"files changed: {n_changed}")


if __name__ == "__main__":
    main()
