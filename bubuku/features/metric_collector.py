import requests
import asyncio
import logging
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('MetricCollector')


class MetricCollector:
    _OFFLINE_PARTITIONS_MBEAN = {
        'name': 'OfflinePartitions',
        'mbean': 'kafka.controller:type=KafkaController,name=OfflinePartitionsCount'}
    _UNDER_REPLICATED_PARTITIONS_MBEAN = {
        'name': 'UnderReplicatedPartitions',
        'mbean': 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions'}
    _PREFERRED_REPLICA_IMBALANCE_MBEAN = {
        'name': 'PreferredReplicaImbalance',
        'mbean': 'kafka.controller:name=PreferredReplicaImbalanceCount,type=KafkaController'}
    _JOLOKIA_PORT = 8778

    def __init__(self, zk: BukuExhibitor):
        self.zk = zk

    async def _get_metrics_from_broker(self, broker_id: int):
        broker_address = self.zk.get_broker_address(broker_id)
        data = {'broker_address': broker_address, 'broker_id': broker_id}
        for metric in self.get_metric_mbeans():
            request = requests.get("http://{}:{}/jolokia/read/{}".format(
                broker_address, self._JOLOKIA_PORT, metric['mbean']))
            if request.status_code == 200:
                response = request.json()
                if response.get('status') == 200:
                    data[metric['name']] = response.get('value', {}).get('Value')
        return data

    async def _get_metrics_from_brokers(self, broker_ids):
        metrics = []
        for broker_id in broker_ids:
            metrics.append(asyncio.ensure_future(self._get_metrics_from_broker(broker_id)))
        metrics = await asyncio.gather(*metrics)
        return metrics

    def get_metrics_from_brokers(self, broker_ids=None):
        broker_ids = self.zk.get_broker_ids() if not broker_ids else broker_ids
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            metrics = loop.run_until_complete(self._get_metrics_from_brokers(broker_ids))
            return metrics
        except Exception as e:
            _LOG.error('Could not fetch metrics from brokers', exc_info=e)
        finally:
            loop.close()

    @classmethod
    def get_metric_mbeans(cls):
        return [
            cls._OFFLINE_PARTITIONS_MBEAN,
            cls._UNDER_REPLICATED_PARTITIONS_MBEAN,
            cls._PREFERRED_REPLICA_IMBALANCE_MBEAN
        ]
