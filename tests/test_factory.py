"""Tests for :mod:`references.factory`."""

import unittest
from references.factory import create_web_app, create_worker_app
from flask import Flask
from celery import Celery


class TestWebAppFactory(unittest.TestCase):
    """We require a Flask application instance."""

    def test_returns_flask_app(self):
        """:func:`.create_web_app` returns a :class:`flask.Flask` instance."""
        app = create_web_app()
        self.assertIsInstance(app, Flask)


class TestProcessAppFactory(unittest.TestCase):
    """We require a Celery worker application instance."""

    def test_returns_celery_app(self):
        """:func:`.create_web_app` returns a :class:`.Celery` instance."""
        app = create_worker_app()
        self.assertIsInstance(app, Celery)
