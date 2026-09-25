"""Unit tests for image upload validation."""

import io

from werkzeug.datastructures import FileStorage

from gwadm.config import ALLOWED_LETTER_IMAGE_EXTENSIONS, MAX_UPLOAD_BYTES
from gwadm.services.uploads import validate_image_upload

PNG_BYTES = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xdb"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _file(data: bytes, filename: str) -> FileStorage:
    return FileStorage(stream=io.BytesIO(data), filename=filename)


def test_validate_png_upload():
    data, error = validate_image_upload(_file(PNG_BYTES, "photo.png"), ALLOWED_LETTER_IMAGE_EXTENSIONS)
    assert error is None
    assert data == PNG_BYTES


def test_reject_invalid_extension():
    data, error = validate_image_upload(_file(PNG_BYTES, "photo.exe"), ALLOWED_LETTER_IMAGE_EXTENSIONS)
    assert data is None
    assert error is not None


def test_reject_oversized_file():
    huge = PNG_BYTES + (b"0" * (MAX_UPLOAD_BYTES + 1))
    data, error = validate_image_upload(_file(huge, "big.png"), ALLOWED_LETTER_IMAGE_EXTENSIONS)
    assert data is None
    assert "большой" in error.lower()


def test_reject_fake_image_content():
    data, error = validate_image_upload(
        _file(b"not an image", "photo.png"),
        ALLOWED_LETTER_IMAGE_EXTENSIONS,
    )
    assert data is None
    assert error is not None
