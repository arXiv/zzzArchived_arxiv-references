"""Metric reporting for extractions."""

import boto3
import os
from flask import _app_ctx_stack as stack
from references.types import Callable
from functools import wraps


class MetricsSession(object):
    """Reports processing metrics to CloudWatch."""

    namespace = 'arXiv/References'

    def __init__(self, endpoint_url: str=None, aws_access_key: str=None,
                 aws_secret_key: str=None, region_name: str=None) -> None:
        """Initialize with AWS configuration."""
        self.cloudwatch = boto3.client('cloudwatch', region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)

    def report(self, metric: str, value: object, units: str=None,
               dimensions: dict=None) -> None:
        """
        Put data for a metric to CloudWatch.

        Parameters
        ----------
        metric : str
        value : str, int, float
        units : str or None
        dimensions : dict or None
        """
        metric_data = {'MetricName': metric, 'Value': value}
        if units is not None:
            metric_data.update({'Unit': units})
        if dimensions is not None:
            metric_data.update({
                'Dimensions': [{'Name': key, 'Value': value}
                               for key, value in dimensions.items()]
            })
        self.cloudwatch.put_metric_data(Namespace=self.namespace,
                                        MetricData=[metric_data])

    def reporter(self, func) -> Callable:
        """Generate a decorator to handle metrics reporting."""
        @wraps(func)
        def metrics_wrapper(*args, **kwargs):
            """Report metrics data returned by ``func``."""
            result = func(*args, **kwargs)
            if type(result) is tuple and len(result) > 1 \
                    and hasattr(result[-1], '__iter__') \
                    and len(result[-1]) > 0 and 'metric' in result[-1][0]:
                metrics = result[-1]
                remainder = result[:-1]
                for item in metrics:
                    self.report(item['metric'], item['value'],
                                item.get('units'), item.get('dimensions'))
                if len(remainder) == 1:
                    return remainder[0]
                return remainder
            return result
        return metrics_wrapper


class Metrics(object):
    """Provides metric reporting service."""

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('CLOUDWATCH_ENDPOINT', None)
        app.config.setdefault('AWS_ACCESS_KEY_ID', 'asdf1234')
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', 'fdsa5678')
        app.config.setdefault('AWS_REGION', 'us-east-1')

    def get_session(self) -> None:
        try:
            endpoint_url = self.app.config['CLOUDWATCH_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            region_name = self.app.config['AWS_REGION']
        except (RuntimeError, AttributeError) as e:    # No app context.
            endpoint_url = os.environ.get('CLOUDWATCH_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf')
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa')
            region_name = os.environ.get('AWS_REGION', 'us-east-1')
        return MetricsSession(endpoint_url, aws_access_key, aws_secret_key,
                              region_name)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'metrics'):
                ctx.metrics = self.get_session()
            return ctx.metrics
        return self.get_session()     # No application context.


metrics = Metrics()
