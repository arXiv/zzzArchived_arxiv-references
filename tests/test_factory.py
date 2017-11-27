"""Tests for :mod:`references.factory`."""

import unittest
import os
from unittest import mock
from references.factory import create_web_app, create_worker_app
from flask import Flask
from celery import Celery


class TestWebAppFactory(unittest.TestCase):
    """We require a Flask application instance."""

    def setUp(self):
        os.environ['INSTANCE_CREDENTIALS'] = ''

    @mock.patch('references.services.credentials')
    def test_returns_flask_app(self, mock_credentials):
        """:func:`.create_web_app` returns a :class:`flask.Flask` instance."""
        type(mock_credentials).init_app = mock.MagicMock()
        app = create_web_app()
        self.assertIsInstance(app, Flask)


class TestProcessAppFactory(unittest.TestCase):
    """We require a Celery worker application instance."""

    def setUp(self):
        os.environ['INSTANCE_CREDENTIALS'] = ''

    @mock.patch('references.services.credentials')
    def test_returns_celery_app(self, mock_credentials):
        """:func:`.create_web_app` returns a :class:`.Flask` instance."""
        type(mock_credentials).init_app = mock.MagicMock()
        app = create_worker_app()
        self.assertIsInstance(app, Flask)
