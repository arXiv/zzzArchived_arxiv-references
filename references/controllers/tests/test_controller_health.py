"""Tests for the :mod:`references.controllers.health` module."""

import unittest
from unittest import mock
from references.controllers.health import health_check, _getServices


class TestHealthCheck(unittest.TestCase):
    @mock.patch('references.controllers.health.cermine')
    @mock.patch('references.controllers.health.data_store')
    @mock.patch('references.controllers.health.grobid')
    @mock.patch('references.controllers.health.refextract')
    def test_health_check_ok(self, *mocks):
        """A dict of health states is returned."""
        status, code, _ = health_check()
        self.assertIsInstance(status, dict)
        self.assertEqual(len(status), len(_getServices()))
        for stat in status.values():
            self.assertTrue(stat)

    @mock.patch('references.controllers.health.cermine')
    @mock.patch('references.controllers.health.data_store')
    @mock.patch('references.controllers.health.grobid')
    @mock.patch('references.controllers.health.refextract')
    def test_health_check_failure(self, *mocks):
        """A dict of health states is returned."""
        for obj in mocks:
            type(obj).session = mock.PropertyMock(side_effect=RuntimeError)

        status, code, _ = health_check()
        self.assertIsInstance(status, dict)
        self.assertEqual(len(status), len(_getServices()))
        for stat in status.values():
            self.assertFalse(stat)
