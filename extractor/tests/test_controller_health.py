"""Tests for the :mod:`references.web.health` module."""

import unittest
from unittest import mock
from references.web.health import health_check


class TestHealthCheck(unittest.TestCase):
    @mock.patch('references.web.health.cermine')
    @mock.patch('references.web.health.referencesStore')
    @mock.patch('references.web.health.extractionEvents')
    @mock.patch('references.web.health.grobid')
    @mock.patch('references.web.health.metrics')
    @mock.patch('references.web.health.refExtract')
    def test_health_check_ok(self, *mocks):
        """A dict of health states is returned."""
        status = health_check()
        self.assertIsInstance(status, dict)
        self.assertEqual(len(status), 6)
        for stat in status.values():
            self.assertTrue(stat)

    @mock.patch('references.web.health.cermine')
    @mock.patch('references.web.health.referencesStore')
    @mock.patch('references.web.health.extractionEvents')
    @mock.patch('references.web.health.grobid')
    @mock.patch('references.web.health.metrics')
    @mock.patch('references.web.health.refExtract')
    def test_health_check_failure(self, *mocks):
        """A dict of health states is returned."""
        for obj in mocks:
            type(obj).session = mock.PropertyMock(side_effect=RuntimeError)

        status = health_check()
        self.assertIsInstance(status, dict)
        self.assertEqual(len(status), 6)
        for stat in status.values():
            self.assertFalse(stat)
