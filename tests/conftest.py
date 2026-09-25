"""Pytest fixtures for gwadm."""

import os

import pytest


@pytest.fixture(scope="session")
def app(tmp_path_factory):
    db_path = str(tmp_path_factory.mktemp("data") / "test.db")
    os.environ["DATABASE_PATH"] = db_path
    os.environ["FLASK_ENV"] = "development"
    os.environ.pop("SECRET_KEY", None)
    os.environ.setdefault("ENABLE_DEV_LOGIN", "1")

    import gwadm.config as config
    import gwadm.db as gwadm_db

    config.FLASK_ENV = "development"
    config.SECRET_KEY = "dev-only-insecure-key"
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None

    from gwadm import create_app

    application = create_app()
    application.config["TESTING"] = True
    return application


@pytest.fixture
def client(app):
    return app.test_client()
