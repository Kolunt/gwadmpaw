"""Shared validation and storage for image uploads."""

import os
import secrets
import time

from werkzeug.utils import secure_filename

from gwadm.config import MAX_UPLOAD_BYTES


def _detect_image_type(data: bytes) -> str | None:
    if data.startswith(b'\x89PNG\r\n\x1a\n'):
        return '.png'
    if data.startswith(b'\xff\xd8\xff'):
        return '.jpg'
    if data.startswith((b'GIF87a', b'GIF89a')):
        return '.gif'
    if len(data) >= 12 and data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return '.webp'
    return None


def validate_image_upload(
    file,
    allowed_extensions: set[str],
    max_bytes: int = MAX_UPLOAD_BYTES,
    allow_svg: bool = False,
) -> tuple[bytes | None, str | None]:
    """Validate an uploaded image file. Returns (file_bytes, error_message)."""
    if not file or not getattr(file, 'filename', None):
        return None, 'Файл не выбран.'

    filename = secure_filename(file.filename)
    if not filename:
        return None, 'Недопустимое имя файла.'

    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in allowed_extensions:
        return None, 'Недопустимый тип файла.'

    file.seek(0, os.SEEK_END)
    size = file.tell()
    file.seek(0)
    if size == 0:
        return None, 'Пустой файл.'
    if size > max_bytes:
        max_mb = max_bytes // (1024 * 1024)
        return None, f'Файл слишком большой (максимум {max_mb} МБ).'

    data = file.read(max_bytes + 1)
    file.seek(0)
    if len(data) > max_bytes:
        max_mb = max_bytes // (1024 * 1024)
        return None, f'Файл слишком большой (максимум {max_mb} МБ).'

    if ext == '.svg':
        if not allow_svg:
            return None, 'SVG-файлы не разрешены.'
        sample = data[:2048].lower()
        if b'<svg' not in sample:
            return None, 'Файл не является корректным SVG.'
        return data, None

    detected = _detect_image_type(data)
    if not detected:
        return None, 'Файл не является корректным изображением.'

    if detected == '.jpg' and ext in ('.jpg', '.jpeg'):
        return data, None
    if detected != ext:
        return None, 'Расширение файла не соответствует содержимому.'

    return data, None


def save_validated_image(
    data: bytes,
    upload_dir: str,
    prefix: str,
    ext: str,
) -> str:
    """Save validated bytes under upload_dir; returns the generated filename."""
    os.makedirs(upload_dir, exist_ok=True)
    unique_name = f"{prefix}_{int(time.time())}_{secrets.token_hex(4)}{ext}"
    filepath = os.path.join(upload_dir, unique_name)
    with open(filepath, 'wb') as handle:
        handle.write(data)
    return unique_name
