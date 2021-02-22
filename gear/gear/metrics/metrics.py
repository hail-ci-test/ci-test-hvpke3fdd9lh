from google.cloud import monitoring_v3
from collections import defaultdict
from functools import wraps
import asyncio
import datetime
import os
from typing import Dict, Callable
from google.api_core.exceptions import GoogleAPIError
from hailtop.config import get_deploy_config


CLUSTER_NAME = 'vdc'
RESOURCE_TYPE = 'k8s_container'
PROJECT_ID = os.environ.get('HAIL_GCP_PROJECT')
GCP_ZONE = os.environ.get('HAIL_GCP_ZONE')
CONTAINER_NAME = os.environ.get('KUBERNETES_CONTAINER_NAME')
POD_NAME = os.environ.get('KUBERNETES_POD_NAME')

METRIC_BASE = 'custom.googleapis.com'
MIN_GRANULARITY_SECS = 15
metrics_uploader = None

deploy_config = get_deploy_config()


def init():
    global metrics_uploader
    if metrics_uploader is None:
        metrics_uploader = Metrics()


async def update_loop():
    await metrics_uploader.run()


def count(metric_name):
    def wrap(fun):
        @wraps(fun)
        async def wrapped(*args, **kwargs):
            metrics_uploader.increment(metric_name)
            return await fun(*args, **kwargs)
        return wrapped
    return wrap


def gauge(metric_name):
    def wrap(fun):
        metrics_uploader.gauge_callbacks[metric_name] = fun
        return fun
    return wrap


class Metrics:
    def __init__(self):
        self.counters = defaultdict(monitoring_v3.Point)
        self.gauges = defaultdict(monitoring_v3.Point)
        self.gauge_callbacks: Dict[str, Callable] = dict()

        self.client = monitoring_v3.MetricServiceClient()

    async def run(self):
        while True:
            await asyncio.sleep(MIN_GRANULARITY_SECS)
            self.query_gauges()
            if len(self.counters) > 0 or len(self.gauges) > 0:
                self.push_metrics()

    def increment(self, metric_name):
        self.counters[metric_name].value.double_value += 1
        self.counters[metric_name].interval.start_time = datetime.datetime.now()  # FIXME

    def query_gauges(self):
        for metric_name, gauge_f in self.gauge_callbacks.items():
            self.gauges[metric_name].value.double_value = gauge_f()

    def push_metrics(self):
        series_request = monitoring_v3.CreateTimeSeriesRequest()
        series_request.name = self.client.common_project_path(PROJECT_ID)
        counter_series = [Metrics.make_time_series(metric_name, point, "CUMULATIVE")
                          for metric_name, point in self.counters.items()]
        gauge_series = [Metrics.make_time_series(metric_name, point, "GAUGE")
                        for metric_name, point in self.gauges.items()]
        series_request.time_series = counter_series + gauge_series
        try:
            self.client.create_time_series(series_request)
        except GoogleAPIError as e:
            raise e
        self.counters = defaultdict(monitoring_v3.Point)
        self.gauges = defaultdict(monitoring_v3.Point)

    @staticmethod
    def make_time_series(metric_name, point, kind):
        gcp_series = monitoring_v3.TimeSeries()
        gcp_series.metric.type = f'{METRIC_BASE}/{metric_name}'
        gcp_series.metric_kind = kind
        Metrics.add_resource(gcp_series)
        point.interval.end_time = datetime.datetime.now()
        gcp_series.points.append(point)
        return gcp_series

    @staticmethod
    def add_resource(gcp_series):
        gcp_series.resource.type = RESOURCE_TYPE
        gcp_series.resource.labels['cluster_name'] = CLUSTER_NAME
        gcp_series.resource.labels['namespace_name'] = deploy_config.default_namespace()
        gcp_series.resource.labels['location'] = GCP_ZONE
        gcp_series.resource.labels['container_name'] = CONTAINER_NAME
        gcp_series.resource.labels['pod_name'] = POD_NAME
