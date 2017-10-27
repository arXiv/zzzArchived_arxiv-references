"""Tests for :mod:`references.services.utils` module."""

from unittest import TestCase, mock
from references.services import util
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
        """:func:`.util.get_application_global` returns a :class:`flask.g`."""
        with self.app.app_context():
            glob = util.get_application_global()
            self.assertTrue(hasattr(glob, 'get') and
                            hasattr(glob.get, '__call__'),
                            "g has get() method")
            self.assertTrue(hasattr(glob, '__setitem__') and
                            hasattr(glob.__setitem__, '__call__'),
                            "g has __setitem__() method")

    def test_get_application_config(self):
        """:func:`.util.get_application_config` returns a ``dict``-like."""
        with self.app.app_context():
            config = util.get_application_config()
            self.assertTrue(hasattr(config, 'get') and
                            hasattr(config.get, '__call__'),
                            "config has get() method")
            self.assertTrue(hasattr(config, '__setitem__') and
                            hasattr(config.__setitem__, '__call__'),
                            "config has __setitem__() method")


class CeleryApplicationContext(TestCase):
    """A :class:`celery.Celery` application context is available."""

    def setUp(self):
        """Initialize the app."""
        self.app = Celery('test')

    def test_get_application_global(self):
        """:func:`.util.get_application_global` returns ``None``."""
        self.assertIsNone(util.get_application_global())

    def test_get_application_config(self):
        """:func:`.util.get_application_config` returns :class:`.Settings`."""
        config = util.get_application_config()
        self.assertTrue(hasattr(config, 'get') and
                        hasattr(config.get, '__call__'),
                        "config has get() method")
        self.assertTrue(hasattr(config, '__setitem__') and
                        hasattr(config.__setitem__, '__call__'),
                        "config has __setitem__() method")
        self.assertIsInstance(config, Settings, "config is a Settings object")

    def tearDown(self):
        """Delete the app."""
        del self.app
        from celery import _state as celery_state
        del celery_state._tls.current_app


class NoApplicationContext(TestCase):
    """No application context is available."""

    def test_get_application_global(self):
        """:func:`.util.get_application_global` returns ``None``."""
        self.assertIsNone(util.get_application_global())

    def test_get_application_config(self):
        """:func:`.util.get_application_config` returns ``os.environ."""
        config = util.get_application_config()
        self.assertTrue(hasattr(config, 'get') and
                        hasattr(config.get, '__call__'),
                        "config has get() method")
        self.assertTrue(hasattr(config, '__setitem__') and
                        hasattr(config.__setitem__, '__call__'),
                        "config has __setitem__() method")
        self.assertEqual(config, os.environ, "config is os.environ")
