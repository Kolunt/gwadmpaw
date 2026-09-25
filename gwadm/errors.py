"""Application error handlers."""

import traceback

from flask import render_template
from werkzeug.exceptions import HTTPException

from gwadm.logging_config import log_error


def register_error_handlers(app):
    @app.errorhandler(404)
    def handle_not_found(error):
        return render_template('errors/404.html', error=error), 404

    @app.errorhandler(500)
    def handle_server_error(error):
        log_error(f"Internal server error: {error}")
        return render_template('errors/500.html', error=error), 500

    @app.errorhandler(Exception)
    def handle_unexpected_error(error):
        if isinstance(error, HTTPException):
            code = error.code or 500
            if code == 404:
                return handle_not_found(error)
            if code == 500:
                return handle_server_error(error)
            return render_template('errors/generic.html', error=error, status_code=code), code
        log_error(f"Unhandled exception: {error}\n{traceback.format_exc()}")
        return render_template('errors/500.html', error=error), 500
