"""Tests for :mod:`references.factory`."""

import unittest
from references.factory import create_web_app, create_process_app
from flask import Flask
from celery import Celery


class TestWebAppFactory(unittest.TestCase):
    """Tests for :func:`.create_web_app`."""

    def test_returns_flask_app(self):
        """A :class:`flask.Flask` application is returned."""
        app = create_web_app()
        self.assertIsInstance(app, Flask)


class TestProcessAppFactory(unittest.TestCase):
    """Tests for :func:`.create_process_app`."""

    def test_returns_celery_app(self):
        """A :class:`celery.Celery` application is returned."""
        app = create_process_app()
        self.assertIsInstance(app, Celery)
