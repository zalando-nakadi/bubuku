import requests
import asyncio
import logging
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('MetricCollector')


class MetricCollector:
    _OFFLINE_PARTITIONS_MBEAN = {
        'name': 'OfflinePartitions',
        'mbean': 'kafka.controller:type=KafkaController,name=OfflinePartitionsCount',
        'verify_is_present': True
    }
    _UNDER_REPLICATED_PARTITIONS_MBEAN = {
        'name': 'UnderReplicatedPartitions',
        'mbean': 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions',
        'verify_is_present': True
    }
    _PREFERRED_REPLICA_IMBALANCE_MBEAN = {
        'name': 'PreferredReplicaImbalance',
        'mbean': 'kafka.controller:name=PreferredReplicaImbalanceCount,type=KafkaController',
        'verify_is_present': True
    }
    _JOLOKIA_PORT = 8778

    def __init__(self, zk: BukuExhibitor):
        self.zk = zk
        self._all_required_metrics_fetched = False

    async def _get_metrics_from_broker(self, broker_id: int):
        broker_address = self.zk.get_broker_address(broker_id)
        data = {'broker_address': broker_address, 'broker_id': broker_id, 'metrics': {}}
        for metric in self.get_metric_mbeans():
            metric_fetched = False
            request = requests.get("http://{}:{}/jolokia/read/{}".format(
                broker_address, self._JOLOKIA_PORT, metric['mbean']))
            if request.status_code == 200:
                response = request.json()
                if response.get('status') == 200:
                    if response.get('value', {}).get('Value') is not None:
                        metric_fetched = True
                        data['metrics'][metric['name']] = response['value']['Value']
            if metric['verify_is_present'] and not metric_fetched:
                self._all_required_metrics_fetched = False
        return data

    async def _get_metrics_from_brokers(self, broker_ids):
        metrics = []
        for broker_id in broker_ids:
            metrics.append(asyncio.ensure_future(self._get_metrics_from_broker(broker_id)))
        metrics = await asyncio.gather(*metrics)
        return metrics

    def get_metrics_from_brokers(self, broker_ids=None):
        self._all_required_metrics_fetched = True
        broker_ids = self.zk.get_broker_ids() if not broker_ids else broker_ids
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            metrics = {
                'metrics': loop.run_until_complete(self._get_metrics_from_brokers(broker_ids)),
                'all_metrics_fetched': self._all_required_metrics_fetched
            }
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
