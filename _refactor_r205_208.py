#!/usr/bin/env python3
"""Refactor gwadm app.py into blueprints R-205..R-209."""
from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
APP_PATH = ROOT / "app.py"
TEMPLATES = ROOT / "templates"


def read_app_lines() -> list[str]:
    return APP_PATH.read_text(encoding="utf-8").splitlines()


def write_app_lines(lines: list[str]) -> None:
    APP_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def find_def_line(lines: list[str], name: str) -> int:
    prefix = f"def {name}("
    for i, line in enumerate(lines):
        if line.startswith(prefix):
            return i
    raise KeyError(f"function not found: {name}")


def find_block_end(lines: list[str], start: int) -> int:
    for j in range(start + 1, len(lines)):
        line = lines[j]
        if not line:
            continue
        if line[0].isspace():
            continue
        if line.startswith("def ") or line.startswith("@app.") or line.startswith("# =========="):
            return j
    return len(lines)


def extract_def(lines: list[str], name: str) -> tuple[list[str], tuple[int, int]]:
    start = find_def_line(lines, name)
    end = find_block_end(lines, start)
    return lines[start:end], (start, end)


def extract_route(lines: list[str], name: str) -> tuple[list[str], tuple[int, int]]:
    start_def = find_def_line(lines, name)
    start = start_def
    while start > 0 and lines[start - 1].lstrip().startswith("@"):
        start -= 1
    end = find_block_end(lines, start_def)
    return lines[start:end], (start, end)


def remove_ranges(lines: list[str], ranges: list[tuple[int, int]]) -> list[str]:
    remove = set()
    for a, b in ranges:
        remove.update(range(a, b))
    return [line for i, line in enumerate(lines) if i not in remove]


def join_block(block: list[str]) -> str:
    return "\n".join(block)


def transform_routes(text: str, bp: str = "bp") -> str:
    text = text.replace("@app.route", f"@{bp}.route")
    return text


def strip_admin_prefix(text: str) -> str:
    def repl(m: re.Match) -> str:
        path = m.group(1)
        if path.startswith("/admin"):
            path = path[len("/admin") :] or "/"
        return f"@bp.route('{path}'"

    return re.sub(r"@bp\.route\('([^']*)'", repl, text)


def apply_url_for_replacements(text: str, extra: dict[str, str] | None = None) -> str:
    mapping = dict(URL_FOR_REPLACEMENTS)
    if extra:
        mapping.update(extra)
    for old, new in sorted(mapping.items(), key=lambda x: -len(x[0])):
        text = text.replace(f"url_for('{old}'", f"url_for('{new}'")
        text = text.replace(f'url_for("{old}"', f'url_for("{new}"')
    return text


URL_FOR_REPLACEMENTS = {
    "events": "events.events",
    "event_view": "events.event_view",
    "event_register": "events.event_register",
    "event_unregister": "events.event_unregister",
    "assignments": "assignments.assignments",
    "letter": "assignments.letter",
    "assignment_mark_sent": "assignments.assignment_mark_sent",
    "assignment_mark_received": "assignments.assignment_mark_received",
    "stop_impersonation": "admin.stop_impersonation",
    "impersonation_stop": "admin.stop_impersonation",
    "telegram_verify_generate": "integrations.telegram_verify_generate",
    "telegram_verify_status": "integrations.telegram_verify_status",
    "telegram_verify_unlink": "integrations.telegram_verify_unlink",
    "cron_run": "integrations.cron_run",
    "verify_dadata": "integrations.verify_dadata",
    "verify_smtp": "integrations.verify_smtp",
    "verify_telegram": "integrations.verify_telegram",
}

ADMIN_FUNCS = [
    "admin_panel",
    "admin_rating_settings_fix_points",
    "admin_rating_settings",
    "admin_broadcasts",
    "admin_broadcasts_send",
    "admin_broadcasts_templates",
    "admin_broadcasts_template_get",
    "admin_telegram_menu",
    "admin_telegram_menu_get",
    "admin_test",
    "admin_users",
    "admin_user_impersonate",
    "admin_user_create",
    "admin_user_edit",
    "admin_user_delete",
    "admin_user_roles",
    "admin_roles",
    "admin_role_create",
    "admin_role_edit",
    "admin_role_delete",
    "admin_titles",
    "admin_title_create",
    "admin_title_edit",
    "admin_title_delete",
    "admin_user_titles",
    "admin_settings",
    "admin_faq",
    "admin_faq_create",
    "admin_faq_edit",
    "admin_faq_delete",
    "admin_faq_category_create",
    "admin_faq_category_edit",
    "admin_faq_category_delete",
    "admin_rules_init_defaults",
    "admin_rules",
    "admin_rules_edit",
    "admin_logs",
    "admin_awards",
    "admin_award_create",
    "admin_award_edit",
    "admin_award_delete",
    "admin_events",
    "admin_event_create",
    "admin_event_view",
    "admin_event_participants",
    "admin_event_distribution_positive_view",
    "admin_event_distribution_positive_create_assignments",
    "admin_event_distribution_positive_unassign",
    "admin_event_distribution_positive_generate",
    "admin_event_participant_add",
    "admin_event_participant_upgrade",
    "admin_event_participant_downgrade",
    "admin_event_participant_remove",
    "admin_event_participant_confirm",
    "admin_event_participant_reject",
    "admin_event_distribution_positive_save",
    "admin_event_edit",
    "admin_event_delete",
    "admin_letters_archived",
    "admin_letters",
    "admin_rating_detail",
    "admin_rating_event_annul",
    "admin_rating_event_restore",
]

ADMIN_MODULE = {
    "admin_panel": "dashboard",
    "admin_rating_settings_fix_points": "rating",
    "admin_rating_settings": "rating",
    "admin_broadcasts": "broadcasts",
    "admin_broadcasts_send": "broadcasts",
    "admin_broadcasts_templates": "broadcasts",
    "admin_broadcasts_template_get": "broadcasts",
    "admin_telegram_menu": "telegram_menu",
    "admin_telegram_menu_get": "telegram_menu",
    "admin_test": "test",
    "admin_users": "users",
    "admin_user_impersonate": "impersonation_only",
    "admin_user_create": "users",
    "admin_user_edit": "users",
    "admin_user_delete": "users",
    "admin_user_roles": "users",
    "admin_roles": "roles",
    "admin_role_create": "roles",
    "admin_role_edit": "roles",
    "admin_role_delete": "roles",
    "admin_titles": "titles",
    "admin_title_create": "titles",
    "admin_title_edit": "titles",
    "admin_title_delete": "titles",
    "admin_user_titles": "titles",
    "admin_settings": "settings",
    "admin_faq": "faq",
    "admin_faq_create": "faq",
    "admin_faq_edit": "faq",
    "admin_faq_delete": "faq",
    "admin_faq_category_create": "faq",
    "admin_faq_category_edit": "faq",
    "admin_faq_category_delete": "faq",
    "admin_rules_init_defaults": "rules",
    "admin_rules": "rules",
    "admin_rules_edit": "rules",
    "admin_logs": "logs",
    "admin_awards": "awards",
    "admin_award_create": "awards",
    "admin_award_edit": "awards",
    "admin_award_delete": "awards",
    "admin_events": "events",
    "admin_event_create": "events",
    "admin_event_view": "events",
    "admin_event_participants": "events",
    "admin_event_distribution_positive_view": "events",
    "admin_event_distribution_positive_create_assignments": "events",
    "admin_event_distribution_positive_unassign": "events",
    "admin_event_distribution_positive_generate": "events",
    "admin_event_participant_add": "events",
    "admin_event_participant_upgrade": "events",
    "admin_event_participant_downgrade": "events",
    "admin_event_participant_remove": "events",
    "admin_event_participant_confirm": "events",
    "admin_event_participant_reject": "events",
    "admin_event_distribution_positive_save": "events",
    "admin_event_edit": "events",
    "admin_event_delete": "events",
    "admin_letters_archived": "assignments_admin",
    "admin_letters": "assignments_admin",
    "admin_rating_detail": "rating",
    "admin_rating_event_annul": "rating",
    "admin_rating_event_restore": "rating",
}

for fn in ADMIN_FUNCS:
    URL_FOR_REPLACEMENTS[fn] = f"admin.{fn}"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str) -> None:
    ensure_parent(path)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def service_header(doc: str) -> str:
    return f'"""{doc}"""\n\n'


def extract_many(lines: list[str], names: list[str]) -> tuple[str, list[tuple[int, int]]]:
    chunks: list[str] = []
    ranges: list[tuple[int, int]] = []
    for name in names:
        block, rng = extract_def(lines, name)
        chunks.append(join_block(block))
        ranges.append(rng)
    return "\n\n".join(chunks) + "\n", ranges


def patch_templates() -> None:
    if not TEMPLATES.exists():
        return
    for path in TEMPLATES.rglob("*.html"):
        text = path.read_text(encoding="utf-8")
        new = apply_url_for_replacements(text)
        if new != text:
            path.write_text(new, encoding="utf-8")


def patch_extensions_events() -> None:
    path = ROOT / "gwadm" / "extensions.py"
    text = path.read_text(encoding="utf-8")
    imports = [
        ("events_bp", "gwadm.blueprints.events", "events"),
        ("assignments_bp", "gwadm.blueprints.assignments", "assignments"),
        ("admin_bp", "gwadm.blueprints.admin", "admin"),
        ("impersonation_bp", "gwadm.blueprints.admin.impersonation", "impersonation"),
        ("integrations_bp", "gwadm.blueprints.integrations", "integrations"),
    ]
    for var, mod, _ in imports:
        if var not in text:
            pass
    block = '''    from gwadm.blueprints.events import bp as events_bp
    from gwadm.blueprints.assignments import bp as assignments_bp
    from gwadm.blueprints.admin import bp as admin_bp
    from gwadm.blueprints.admin.impersonation import impersonation_bp
    from gwadm.blueprints.integrations import bp as integrations_bp

    app.register_blueprint(events_bp)
    app.register_blueprint(assignments_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(impersonation_bp)
    app.register_blueprint(integrations_bp)
'''
    if "events_bp" not in text:
        text = text.replace(
            "    app.register_blueprint(profile_bp)\n",
            "    app.register_blueprint(profile_bp)\n" + block,
        )
        path.write_text(text, encoding="utf-8")



SERVICE_DB_IMPORTS = """from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
"""

BLUEPRINT_COMMON = """from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug
"""


def phase_205(lines: list[str]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    event_routes = ["events", "event_view", "event_register", "event_unregister"]
    blocks = []
    for name in event_routes:
        block, rng = extract_route(lines, name)
        blocks.append(transform_routes(apply_url_for_replacements(join_block(block))))
        ranges.append(rng)
    for dup in ("has_required_contacts", "get_missing_required_fields"):
        try:
            _, rng = extract_def(lines, dup)
            ranges.append(rng)
        except KeyError:
            pass
    events_py = f'"""Public events routes."""\n\nimport sqlite3\nfrom datetime import datetime\n\n{BLUEPRINT_COMMON}\nfrom gwadm.services.events_stages import EVENT_STAGES, get_event_now\nfrom gwadm.services.events import (\n    get_event_registrations_paginated,\n    get_missing_required_fields,\n    is_event_finished,\n    is_registration_open,\n)\nfrom gwadm.services.activity import log_activity\n\nbp = Blueprint("events", __name__)\n\n' + "\n\n".join(blocks) + "\n"
    write_text(ROOT / "gwadm" / "blueprints" / "events.py", events_py)

    prof_path = ROOT / "gwadm" / "blueprints" / "profile.py"
    prof = prof_path.read_text(encoding="utf-8")
    api_blocks = []
    for name in ("api_profile_data", "api_profile_update"):
        block, rng = extract_route(lines, name)
        api_blocks.append(transform_routes(apply_url_for_replacements(join_block(block))))
        ranges.append(rng)
    if "api_profile_data" not in prof:
        insert = "\nfrom gwadm.services.events import get_missing_required_fields\n\n" + "\n\n".join(api_blocks) + "\n"
        prof = prof.rstrip() + "\n" + insert
        prof_path.write_text(prof, encoding="utf-8")
    patch_extensions_events()
    return ranges


def phase_206(lines: list[str]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    assign_funcs = [
        "create_random_assignments",
        "save_event_assignments",
        "get_user_assignments",
        "get_admin_letter_assignments",
        "mark_assignment_sent",
        "mark_assignment_received",
        "_format_full_address",
    ]
    body, franges = extract_many(lines, assign_funcs)
    ranges.extend(franges)
    svc = service_header("Letter assignments and event pairing.") + SERVICE_DB_IMPORTS
    svc += "import os\nfrom werkzeug.utils import secure_filename\n\n"
    svc += "from gwadm.config import ASSIGNMENT_RECEIPT_FOLDER, LETTER_UPLOAD_FOLDER, ALLOWED_LETTER_IMAGE_EXTENSIONS\n"
    svc += "from gwadm.services.activity import log_activity\n\n"
    svc += body
    write_text(ROOT / "gwadm" / "services" / "assignments.py", svc)

    cfg = ROOT / "gwadm" / "config.py"
    cfg_text = cfg.read_text(encoding="utf-8")
    extra = """

LETTER_UPLOAD_RELATIVE = 'uploads/letter_attachments'
ASSIGNMENT_RECEIPT_RELATIVE = 'uploads/assignment_receipts'
ALLOWED_LETTER_IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp'}
LETTER_UPLOAD_FOLDER = str(ROOT_DIR / 'static' / 'uploads' / 'letter_attachments')
ASSIGNMENT_RECEIPT_FOLDER = str(ROOT_DIR / 'static' / 'uploads' / 'assignment_receipts')
"""
    if "LETTER_UPLOAD_RELATIVE" not in cfg_text:
        cfg.write_text(cfg_text.rstrip() + extra + "\n", encoding="utf-8")

    user_routes = ["letter", "assignments", "assignment_mark_sent", "assignment_mark_received"]
    blocks = []
    for name in user_routes:
        block, rng = extract_route(lines, name)
        blocks.append(transform_routes(apply_url_for_replacements(join_block(block))))
        ranges.append(rng)
    assign_bp = f'"""User letter and assignment routes."""\n\nimport os\n{BLUEPRINT_COMMON}\nfrom werkzeug.utils import secure_filename\n\nfrom gwadm.config import ASSIGNMENT_RECEIPT_FOLDER, LETTER_UPLOAD_FOLDER\n'
    assign_bp += "from gwadm.services.assignments import mark_assignment_received, mark_assignment_sent\n\nbp = Blueprint('assignments', __name__)\n\n"
    assign_bp += "\n\n".join(blocks) + "\n"
    write_text(ROOT / "gwadm" / "blueprints" / "assignments.py", assign_bp)
    return ranges


def phase_207(lines: list[str]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    perm_funcs = [
        "get_all_permissions",
        "get_role_permissions",
        "assign_permission_to_role",
        "remove_permission_from_role",
        "has_permission",
    ]
    body, pr = extract_many(lines, perm_funcs)
    ranges.extend(pr)
    write_text(
        ROOT / "gwadm" / "services" / "permissions.py",
        service_header("Role permissions.") + SERVICE_DB_IMPORTS + body,
    )

    title_funcs = ["get_all_titles", "get_title_by_name", "assign_title", "remove_title"]
    body, tr = extract_many(lines, title_funcs)
    ranges.extend(tr)
    titles_path = ROOT / "gwadm" / "services" / "titles.py"
    titles_text = titles_path.read_text(encoding="utf-8")
    if "def get_all_titles" not in titles_text:
        titles_text = titles_text.rstrip() + "\n\n" + body
        titles_path.write_text(titles_text, encoding="utf-8")

    award_funcs = ["assign_award", "remove_award"]
    body, ar = extract_many(lines, award_funcs)
    ranges.extend(ar)
    awards_path = ROOT / "gwadm" / "services" / "awards.py"
    awards_text = awards_path.read_text(encoding="utf-8")
    if "def assign_award" not in awards_text:
        awards_text = awards_text.rstrip() + "\n\n" + body
        awards_path.write_text(awards_text, encoding="utf-8")

    content_funcs = [
        "get_faq_categories",
        "set_setting",
        "init_default_rules",
        "init_default_modal_texts",
        "init_default_faq_items",
    ]
    body, cr = extract_many(lines, content_funcs)
    ranges.extend(cr)
    write_text(
        ROOT / "gwadm" / "services" / "content_init.py",
        service_header("Default content and settings helpers.") + SERVICE_DB_IMPORTS + "from gwadm.services.settings import get_setting\n\n" + body,
    )

    rating_funcs = [
        "_normalize_contact_value",
        "_normalize_multiline_text",
        "_sync_contact_snowflakes",
        "_get_snowflake_source_label",
        "recalculate_all_snowflake_events",
    ]
    body, rr = extract_many(lines, rating_funcs)
    ranges.extend(rr)
    write_text(
        ROOT / "gwadm" / "services" / "rating.py",
        service_header("Rating snowflake helpers.") + SERVICE_DB_IMPORTS + body,
    )

    admin_event_funcs = [
        "get_participants_for_review",
        "approve_participant",
        "get_approved_participants",
        "get_events_requiring_review",
    ]
    body, er = extract_many(lines, admin_event_funcs)
    ranges.extend(er)
    write_text(
        ROOT / "gwadm" / "services" / "event_admin.py",
        service_header("Admin event participant review.") + SERVICE_DB_IMPORTS + body,
    )

    admin_dir = ROOT / "gwadm" / "blueprints" / "admin"
    modules: dict[str, list[str]] = {}
    for fn in ADMIN_FUNCS:
        mod = ADMIN_MODULE[fn]
        modules.setdefault(mod, []).append(fn)

    for mod, funcs in modules.items():
        if mod in ("impersonation", "impersonation_only"):
            continue
        blocks = []
        for name in funcs:
            block, rng = extract_route(lines, name)
            t = transform_routes(strip_admin_prefix(apply_url_for_replacements(join_block(block))))
            blocks.append(t)
            ranges.append(rng)
        mod_file = admin_dir / f"{mod}.py"
        content = f'"""Admin: {mod}."""\n\n{BLUEPRINT_COMMON}\nfrom gwadm.blueprints.admin import bp\n\n' + "\n\n".join(blocks) + "\n"
        write_text(mod_file, content)

    imp_blocks = []
    for name in ("admin_user_impersonate", "stop_impersonation"):
        block, rng = extract_route(lines, name)
        if name == "admin_user_impersonate":
            t = transform_routes(strip_admin_prefix(apply_url_for_replacements(join_block(block))))
            imp_blocks.append(t)
        else:
            t = transform_routes(apply_url_for_replacements(join_block(block)))
            t = t.replace("@bp.route", "@impersonation_bp.route")
            imp_blocks.append(t)
        ranges.append(rng)
    write_text(
        admin_dir / "impersonation.py",
        '"""Admin impersonation."""\n\nfrom flask import Blueprint\n' + BLUEPRINT_COMMON +
        "from gwadm.blueprints.admin import bp\n\nimpersonation_bp = Blueprint('impersonation', __name__)\n\n" +
        "\n\n".join(imp_blocks) + "\n",
    )

    init_py = '''"""Admin blueprint package."""

from flask import Blueprint

bp = Blueprint("admin", __name__, url_prefix="/admin")

from gwadm.blueprints.admin import (  # noqa: E402,F401
    assignments_admin,
    awards,
    broadcasts,
    dashboard,
    events,
    faq,
    logs,
    rating,
    roles,
    rules,
    settings,
    telegram_menu,
    test,
    titles,
    users,
)
'''
    write_text(admin_dir / "__init__.py", init_py)
    return ranges


def phase_208(lines: list[str]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    tg_funcs = [
        "handle_telegram_message",
        "handle_telegram_callback",
        "handle_start_command",
        "handle_start_with_code",
        "handle_menu_command",
        "handle_verify_command",
        "handle_verification_code",
        "get_base_url",
        "handle_events_command",
        "handle_assignments_command",
        "handle_faq_command",
        "handle_rules_command",
        "verify_dadata_api",
        "verify_smtp_connection",
        "verify_telegram_bot",
        "send_telegram_message",
        "send_telegram_message_with_keyboard",
        "generate_telegram_verification_code",
        "verify_telegram_code",
        "get_telegram_bot_menu",
        "set_telegram_bot_commands",
        "send_email_via_smtp",
    ]
    body, tr = extract_many(lines, tg_funcs)
    ranges.extend(tr)
    write_text(
        ROOT / "gwadm" / "services" / "telegram.py",
        service_header("Telegram bot and verification helpers.") + SERVICE_DB_IMPORTS + body,
    )

    int_routes = [
        "telegram_verify_generate",
        "telegram_verify_status",
        "telegram_verify_unlink",
        "telegram_webhook",
        "verify_dadata",
        "verify_smtp",
        "verify_telegram",
        "cron_run",
    ]
    blocks = []
    for name in int_routes:
        block, rng = extract_route(lines, name)
        blocks.append(transform_routes(apply_url_for_replacements(join_block(block))))
        ranges.append(rng)
    integrations = f'"""External integrations."""\n\nimport secrets\nfrom datetime import datetime\n\nfrom flask import Blueprint, jsonify, request, session\n\nfrom gwadm.config import CRON_SECRET_TOKEN\nfrom gwadm.db import get_db_connection\nfrom gwadm.decorators import require_login, require_role\nfrom gwadm.logging_config import log_error\nfrom gwadm.services.settings import get_setting\n\nbp = Blueprint("integrations", __name__)\n\n' + "\n\n".join(blocks) + "\n"
    write_text(ROOT / "gwadm" / "blueprints" / "integrations.py", integrations)

    try:
        _, rng = extract_route(lines, "debug")
        ranges.append(rng)
    except KeyError:
        pass
    return ranges


def phase_209(lines: list[str]) -> list[tuple[int, int]]:
    write_text(
        ROOT / "gwadm" / "jinja_filters.py",
        Path("_refactor_assets_jinja.py").read_text(encoding="utf-8") if (ROOT / "_refactor_assets_jinja.py").exists() else '"""filters"""\n',
    )
    return []


PHASES = {
    "205": phase_205,
    "206": phase_206,
    "207": phase_207,
    "208": phase_208,
    "209": phase_209,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", default="all")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    if not args.no_backup and not (ROOT / "app.py.bak_refactor").exists():
        shutil.copy2(APP_PATH, ROOT / "app.py.bak_refactor")
    lines = read_app_lines()
    phases = list(PHASES.keys()) if args.phase == "all" else [p.strip() for p in args.phase.split(",")]
    for ph in phases:
        if ph not in PHASES:
            print(f"Unknown phase {ph}", file=sys.stderr)
            return 1
        print(f"Running phase R-{ph}...")
        PHASES[ph](lines)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
