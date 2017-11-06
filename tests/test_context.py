"""Tests for :mod:`references.services.utils` module."""

from unittest import TestCase, mock
from references import context
from flask import Flask
from celery import Celery
from celery.app.utils import Settings
import os


class FlaskApplicationContext(TestCase):
    """A :class:`flask.Flask` application context is available."""

    def setUp(self):
        """Initialize the :class:`flask.Flask` app."""
        self.app = Flask('test')

    def test_get_application_global(self):
        """:func:`.context.get_application_global` returns :class:`flask.g`."""
        with self.app.app_context():
            glob = context.get_application_global()
            self.assertTrue(hasattr(glob, 'get') and
                            hasattr(glob.get, '__call__'),
                            "g has get() method")
            self.assertTrue(hasattr(glob, '__setitem__') and
                            hasattr(glob.__setitem__, '__call__'),
                            "g has __setitem__() method")

    def test_get_application_config(self):
        """:func:`.context.get_application_config` returns a ``dict``-like."""
        with self.app.app_context():
            config = context.get_application_config()
            self.assertTrue(hasattr(config, 'get') and
                            hasattr(config.get, '__call__'),
                            "config has get() method")
            self.assertTrue(hasattr(config, '__setitem__') and
                            hasattr(config.__setitem__, '__call__'),
                            "config has __setitem__() method")


class NoApplicationContext(TestCase):
    """No application context is available."""

    def test_get_application_global(self):
        """:func:`.context.get_application_global` returns ``None``."""
        self.assertIsNone(context.get_application_global())

    def test_get_application_config(self):
        """:func:`.context.get_application_config` returns ``os.environ."""
        config = context.get_application_config()
        self.assertTrue(hasattr(config, 'get') and
                        hasattr(config.get, '__call__'),
                        "config has get() method")
        self.assertTrue(hasattr(config, '__setitem__') and
                        hasattr(config.__setitem__, '__call__'),
                        "config has __setitem__() method")
        self.assertEqual(config, os.environ, "config is os.environ")
