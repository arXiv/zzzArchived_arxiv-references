"""Tests for the :mod:`reflink.web.health` module."""

import unittest
from unittest import mock
from reflink.web.health import health_check


class TestHealthCheck(unittest.TestCase):
    @mock.patch('reflink.web.health.cermine')
    @mock.patch('reflink.web.health.referencesStore')
    @mock.patch('reflink.web.health.extractionEvents')
    @mock.patch('reflink.web.health.grobid')
    @mock.patch('reflink.web.health.metrics')
    @mock.patch('reflink.web.health.refExtract')
    def test_health_check_ok(self, *mocks):
        """A dict of health states is returned."""
        status = health_check()
        self.assertIsInstance(status, dict)
        self.assertEqual(len(status), 6)
        for stat in status.values():
            self.assertTrue(stat)

    @mock.patch('reflink.web.health.cermine')
    @mock.patch('reflink.web.health.referencesStore')
    @mock.patch('reflink.web.health.extractionEvents')
    @mock.patch('reflink.web.health.grobid')
    @mock.patch('reflink.web.health.metrics')
    @mock.patch('reflink.web.health.refExtract')
    def test_health_check_failure(self, *mocks):
        """A dict of health states is returned."""
        for obj in mocks:
            type(obj).session = mock.PropertyMock(side_effect=RuntimeError)

        status = health_check()
        self.assertIsInstance(status, dict)
        self.assertEqual(len(status), 6)
        for stat in status.values():
            self.assertFalse(stat)
