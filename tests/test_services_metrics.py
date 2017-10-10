"""Tests for :mod:`references.services.metrics` module."""

import unittest
from unittest import mock
from references.services.metrics import MetricsSession


class TestMetricsReport(unittest.TestCase):
    """Tests for :meth:`.MetricsSession.report` method."""

    @mock.patch('boto3.client')
    def test_report_calls_cloudwatch(self, mock_client):
        """Valid data are reported to CloudWatch."""
        session = MetricsSession()
        session.report('CoolMetric', 42)
        self.assertEqual(session.cloudwatch.put_metric_data.call_count, 1)
        args, kwargs = session.cloudwatch.put_metric_data.call_args
        self.assertIn('MetricData', kwargs)
        self.assertEqual(kwargs['MetricData'][0]['MetricName'], 'CoolMetric')
        self.assertEqual(kwargs['MetricData'][0]['Value'], 42)

    @mock.patch('boto3.client')
    def test_units_are_included(self, mock_client):
        """Unit specification is included in CloudWatch payload."""
        session = MetricsSession()
        session.report('CoolMetric', 42, 'Microseconds')
        self.assertEqual(session.cloudwatch.put_metric_data.call_count, 1)
        args, kwargs = session.cloudwatch.put_metric_data.call_args
        self.assertIn('Unit', kwargs['MetricData'][0])
        self.assertEqual(kwargs['MetricData'][0]['Unit'], 'Microseconds')

    @mock.patch('boto3.client')
    def test_dimensions_are_included(self, mock_client):
        """Dimension specification is included in CloudWatch payload."""
        session = MetricsSession()
        session.report('CoolMetric', 42, dimensions={'Test': 'This'})
        self.assertEqual(session.cloudwatch.put_metric_data.call_count, 1)
        args, kwargs = session.cloudwatch.put_metric_data.call_args
        self.assertIn('Dimensions', kwargs['MetricData'][0])
        self.assertEqual(kwargs['MetricData'][0]['Dimensions'][0]['Name'],
                         'Test')
        self.assertEqual(kwargs['MetricData'][0]['Dimensions'][0]['Value'],
                         'This')


class TestMetricsReporterDecorator(unittest.TestCase):
    """Tests for the :func:`.metrics.MetricsSession.reporter` decorator."""

    @mock.patch('boto3.client')
    def test_wraps_function(self, mock_client):
        """:func:`MetricsSession.reporter` returns a callable."""
        session = MetricsSession()

        @session.reporter
        def test_func():
            return 1

        self.assertTrue(hasattr(test_func, '__call__'))
        self.assertEqual(test_func(), 1)

    @mock.patch('boto3.client')
    def test_returns_value_when_no_metrics_passed(self, mock_client):
        """If no metrics returned, the return value is returned unchanged."""
        session = MetricsSession()

        @session.reporter
        def test_func():
            return 1

        self.assertEqual(test_func(), 1)
        self.assertEqual(session.cloudwatch.put_metric_data.call_count, 0)

    @mock.patch('boto3.client')
    def test_reports_metrics_and_returns_the_rest(self, mock_client):
        """Provided metrics are reported and the remainder is returned."""
        session = MetricsSession()

        @session.reporter
        def test_func():
            return 1, [{'metric': 'Success', 'value': 1e9}]

        self.assertEqual(test_func(), 1)
        self.assertEqual(session.cloudwatch.put_metric_data.call_count, 1)
        args, kwargs = session.cloudwatch.put_metric_data.call_args
        self.assertIn('MetricData', kwargs)
        self.assertEqual(kwargs['MetricData'][0]['MetricName'], 'Success')
        self.assertEqual(kwargs['MetricData'][0]['Value'], 1e9)
