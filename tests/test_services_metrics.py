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


class TestMetricsCredentials(unittest.TestCase):
    """Metrics reporting depends on credential availabilty."""

    def tearDown(self):
        """Unset environment variables."""
        import os
        import flask
        import celery
        if 'AWS_ACCESS_KEY_ID' in os.environ:
            del os.environ['AWS_ACCESS_KEY_ID']
        if 'AWS_SECRET_ACCESS_KEY' in os.environ:
            del os.environ['AWS_SECRET_ACCESS_KEY']
        if 'AWS_SESSION_TOKEN' in os.environ:
            del os.environ['AWS_SESSION_TOKEN']
        if flask.current_app:
            flask.current_app = None
        # Revert to the default app.
        celery._state._set_current_app(celery._state.default_app)

    def test_no_app_available(self):
        """Should rely on os.environ for credentials when no app available."""
        import os
        from references.services import metrics
        os.environ['AWS_ACCESS_KEY_ID'] = 'foo'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'baz'
        os.environ['AWS_SESSION_TOKEN'] = 'bat'
        session = metrics.current_session()
        self.assertEqual(session.aws_access_key, 'foo')
        self.assertEqual(session.aws_secret_key, 'baz')
        self.assertEqual(session.aws_session_token, 'bat')

    def test_flask_app_with_no_credentials(self):
        """Should rely on app config for when credentials not available."""
        from flask import Flask
        from references.services import metrics
        app = Flask('test')
        app.config['AWS_ACCESS_KEY_ID'] = 'qwerty'
        app.config['AWS_SECRET_ACCESS_KEY'] = 'ytrewq'
        app.config['AWS_SESSION_TOKEN'] = 'asdfghjkl'
        with app.app_context():
            session = metrics.current_session()
            self.assertEqual(session.aws_access_key, 'qwerty')
            self.assertEqual(session.aws_secret_key, 'ytrewq')
            self.assertEqual(session.aws_session_token, 'asdfghjkl')

    @mock.patch('requests.get')
    def test_flask_app_with_credentials(self, mock_get):
        """Use credentials service when available."""
        mock_response = mock.MagicMock()
        mock_response.ok = True
        type(mock_response).json = mock.MagicMock(return_value={
          "Code": "Success",
          "LastUpdated": "2012-04-26T16:39:16Z",
          "Type": "AWS-HMAC",
          "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
          "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "Token": "token",
          "Expiration": "2017-05-17T15:09:54Z"
        })
        mock_get.return_value = mock_response

        from flask import Flask, g
        from references.services import credentials
        from references.services import metrics
        app = Flask('test')
        app.config['AWS_ACCESS_KEY_ID'] = 'qwerty'
        app.config['AWS_SECRET_ACCESS_KEY'] = 'ytrewq'
        app.config['AWS_SESSION_TOKEN'] = 'asdfghjkl'
        with app.app_context():
            credentials.init_app(app)
            creds = credentials.current_session()
            self.assertTrue('credentials' in g)

            session = metrics.current_session()
            self.assertEqual(session.aws_access_key, 'ASIAIOSFODNN7EXAMPLE')
            self.assertEqual(session.aws_secret_key,
                             'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
            self.assertEqual(session.aws_session_token, 'token')
            self.assertEqual(session.aws_access_key, creds.access_key)
            self.assertEqual(session.aws_secret_key, creds.secret_key)
            self.assertEqual(session.aws_session_token, creds.session_token)

    def test_celery_app_with_no_credentials(self):
        """Should rely on app config for when credentials not available."""
        from celery import Celery, current_app
        from references.services import metrics
        app = Celery('test')
        app.conf['AWS_ACCESS_KEY_ID'] = 'qwerty'
        app.conf['AWS_SECRET_ACCESS_KEY'] = 'ytrewq'
        app.conf['AWS_SESSION_TOKEN'] = 'asdfghjkl'
        session = metrics.current_session()
        self.assertEqual(session.aws_access_key, 'qwerty')
        self.assertEqual(session.aws_secret_key, 'ytrewq')
        self.assertEqual(session.aws_session_token, 'asdfghjkl')

    @mock.patch('requests.get')
    def test_celery_app_with_credentials(self, mock_get):
        """Use credentials service when available."""
        mock_response = mock.MagicMock()
        mock_response.ok = True
        type(mock_response).json = mock.MagicMock(return_value={
          "Code": "Success",
          "LastUpdated": "2012-04-26T16:39:16Z",
          "Type": "AWS-HMAC",
          "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
          "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "Token": "token",
          "Expiration": "2017-05-17T15:09:54Z"
        })
        mock_get.return_value = mock_response

        from celery import Celery
        from references.services import credentials
        from references.services import metrics
        app = Celery('test')
        app.conf['AWS_ACCESS_KEY_ID'] = 'qwerty'
        app.conf['AWS_SECRET_ACCESS_KEY'] = 'ytrewq'
        app.conf['AWS_SESSION_TOKEN'] = 'asdfghjkl'

        credentials.init_app(app)
        creds = credentials.current_session()

        session = metrics.current_session()
        self.assertEqual(session.aws_access_key, 'ASIAIOSFODNN7EXAMPLE')
        self.assertEqual(session.aws_secret_key,
                         'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
        self.assertEqual(session.aws_session_token, 'token')
        self.assertEqual(session.aws_access_key, creds.access_key)
        self.assertEqual(session.aws_secret_key, creds.secret_key)
        self.assertEqual(session.aws_session_token, creds.session_token)
