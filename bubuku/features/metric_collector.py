import requests
import asyncio
import logging
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('MetricCollector')


class MetricCollector:
    _OFFLINE_PARTITIONS_MBEAN = {
        'name': 'OfflinePartitions',
        'mbean': 'kafka.controller:type=KafkaController,name=OfflinePartitionsCount',
        'field': 'Value'}
    _UNDER_REPLICATED_PARTITIONS_MBEAN = {
        'name': 'UnderReplicatedPartitions',
        'mbean': 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions',
        'field': 'Value'}
    _PREFERRED_REPLICA_IMBALANCE_MBEAN = {
        'name': 'PreferredReplicaImbalance',
        'mbean': 'kafka.controller:name=PreferredReplicaImbalanceCount,type=KafkaController',
        'field': 'Value'}
    _BYTES_IN_MBEAN = {
        'name': 'BytesIn',
        'mbean': 'kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics',
        'field': 'OneMinuteRate'
    }
    _JOLOKIA_PORT = 8778

    def __init__(self, zk: BukuExhibitor):
        self.zk = zk

    async def _get_metrics_from_broker(self, broker_id: int):
        broker_address = self.zk.get_broker_address(broker_id)
        data = {'broker_address': broker_address, 'broker_id': broker_id, 'metrics': {}}
        for metric in self.get_metric_mbeans():
            metric_fetched = False
            try:
                response = requests.get("http://{}:{}/jolokia/read/{}".format(
                    broker_address, self._JOLOKIA_PORT, metric['mbean']))
                if response.status_code == 200:
                    response_body = response.json()
                    if response_body.get('status') == 200:
                        if response_body.get('value', {}).get(metric['field']) is not None:
                            data['metrics'][metric['name']] = response_body['value'][metric['field']]
                            metric_fetched = True
                if not metric_fetched:
                    _LOG.error("Fetching metric {} for broker: {} failed. Response from broker: {}:{}".format(
                        metric['name'], broker_id, response.status_code, response.text))
            except Exception as e:
                _LOG.error("Fetching metric {} for broker {} failed".format(metric['name'], broker_id), exc_info=e)
        return data

    async def _get_metrics_from_brokers(self, broker_ids):
        metrics = []
        for broker_id in broker_ids:
            metrics.append(asyncio.ensure_future(self._get_metrics_from_broker(broker_id)))
        metrics = await asyncio.gather(*metrics)
        return metrics

    def get_metrics_from_brokers(self, broker_ids=None):
        """
        Get metrics for brokers in the cluster
        :param broker_ids: List of broker_ids to fetch metrics for
        :return: List of dictionaries containing metrics for each broker
        {
            "metrics": {...},
            "broker_id": int,
            "broker_address": str
        }
        """
        broker_ids = self.zk.get_broker_ids() if not broker_ids else broker_ids
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._get_metrics_from_brokers(broker_ids))
        except Exception as e:
            _LOG.error('Could not fetch metrics from brokers', exc_info=e)
        finally:
            loop.close()

    @classmethod
    def get_metric_mbeans(cls):
        return [
            cls._OFFLINE_PARTITIONS_MBEAN,
            cls._UNDER_REPLICATED_PARTITIONS_MBEAN,
            cls._PREFERRED_REPLICA_IMBALANCE_MBEAN,
            cls._BYTES_IN_MBEAN,
        ]
