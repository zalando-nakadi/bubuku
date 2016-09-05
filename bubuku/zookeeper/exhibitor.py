import logging
import random

import requests
from requests import RequestException
import boto3

from bubuku.zookeeper import AddressListProvider

_LOG = logging.getLogger('bubuku.zookeeper.exhibitor')


class AWSExhibitorAddressProvider(AddressListProvider):
    def __init__(self, zk_stack_name: str, region: str):
        self.zk_stack_name = zk_stack_name
        self.region = region
        self.exhibitors = []

    def get_latest_address(self) -> (list, int):
        json_ = self._query_exhibitors(self.exhibitors)
        if not json_:
            self.exhibitors = self.get_addresses_by_lb_name()
            json_ = self._query_exhibitors(self.exhibitors)
        if isinstance(json_, dict) and 'servers' in json_ and 'port' in json_:
            self.exhibitors = json_['servers']
            return sorted(json_['servers']), int(json_['port'])
        return None

    def _query_exhibitors(self, exhibitors):
        if not exhibitors:
            return None
        random.shuffle(exhibitors)
        for host in exhibitors:
            url = 'http://{}:{}{}'.format(host, 8181, '/exhibitor/v1/cluster/list')
            try:
                response = requests.get(url, timeout=3.1)
                return response.json()
            except RequestException as e:
                _LOG.warn('Failed to query zookeeper list information from {}'.format(url), exc_info=e)
        return None

    def get_addresses_by_lb_name(self) -> list:
        lb_name = self.zk_stack_name

        private_ips = []

        if self.region is not None:
            elb = boto3.client('elb', region_name=self.region)
            ec2 = boto3.client('ec2', region_name=self.region)

            response = elb.describe_instance_health(LoadBalancerName=lb_name)

            for instance in response['InstanceStates']:
                if instance['State'] == 'InService':
                    private_ips.append(ec2.describe_instances(
                        InstanceIds=[instance['InstanceId']])['Reservations'][0]['Instances'][0]['PrivateIpAddress'])

        else:
            private_ips = [lb_name]
        _LOG.info("Ip addresses for {} are: {}".format(lb_name, private_ips))
        return private_ips


class LocalAddressProvider(AddressListProvider):
    def get_latest_address(self) -> (list, int):
        return ('zookeeper',), 2181
