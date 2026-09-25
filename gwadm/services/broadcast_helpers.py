"""Shared helpers for broadcast delivery."""


def replace_broadcast_placeholders(text, recipient):
    """Replace placeholders in broadcast text with recipient data."""
    name = ''
    if recipient.get('first_name') or recipient.get('last_name'):
        name_parts = []
        if recipient.get('first_name'):
            name_parts.append(recipient['first_name'])
        if recipient.get('last_name'):
            name_parts.append(recipient['last_name'])
        name = ' '.join(name_parts).strip()
    if not name:
        name = recipient.get('username', '')

    replacements = {
        '[name]': name,
        '[username]': recipient.get('username', ''),
        '[email]': recipient.get('email', ''),
        '[telegram]': recipient.get('telegram', ''),
        '[phone]': recipient.get('phone', ''),
        '[id]': str(recipient.get('user_id', '')),
        '[level]': str(recipient.get('level', '')) if recipient.get('level') else '',
        '[syndicate]': str(recipient.get('synd', '')) if recipient.get('synd') else '',
        '[first_name]': recipient.get('first_name', ''),
        '[last_name]': recipient.get('last_name', ''),
        '[city]': recipient.get('city', ''),
        '[country]': recipient.get('country', ''),
    }

    result = text or ''
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value or '')
    return result
