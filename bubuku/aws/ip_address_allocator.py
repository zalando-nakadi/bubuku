import logging

import netaddr

from bubuku.aws import AWSResources

_LOG = logging.getLogger('bubuku.cluster.aws.subnet')


class IpAddressAllocator(object):
    def __init__(self, aws: AWSResources, vpc_id: str, az: str):
        self.aws = aws
        self._vpc_id = vpc_id
        self._az = az

    def _get_subnets(self, prefix_filter: str) -> list:
        """
        Returns lists of subnets, which names start
        with the specified prefix (it should be either 'dmz-' or
        'internal-'), sorted by the Availability Zone and filtered by vpc id
        """
        _LOG.info('Getting subnets for vpc_id: %s and availability_zone: %s', self._vpc_id, self._az)

        resp = self.aws.ec2_client.describe_subnets()
        subnets = []

        for subnet in resp['Subnets']:
            if subnet['VpcId'] != self._vpc_id:
                continue
            if subnet['AvailabilityZone'] != self._az:
                continue
            for tag in subnet['Tags']:
                if tag['Key'] == 'Name' and tag['Value'].startswith(prefix_filter):
                    subnets.append(subnet)
                    break
        _LOG.info('Got subnets %s ', subnets)
        return subnets

    def allocate_ip_addresses(self, address_count: int) -> list:
        """
        Allocate unused private IP addresses by checking the current
        reservations
        Return list of tuples (subnet, ip)
        """
        _LOG.info('Allocating IP addresses ...')

        def try_next_address(ips, subnet):
            try:
                return str(next(ips))
            except StopIteration:
                raise Exception('Out of available IP addresses in subnet {}'.format(subnet['CidrBlock']))

        #
        # Here we have to account for the behavior of launch_*_nodes
        # which iterate through subnets to put the instances into
        # different Availability Zones.
        #
        subnets = self._get_subnets('internal-')
        network_ips = [netaddr.IPNetwork(s['CidrBlock']).iter_hosts() for s in subnets]

        for idx, ips in enumerate(network_ips):
            #
            # Some of the first addresses in each subnet are
            # taken by AWS system instances that we can't see,
            # so we try to skip them.
            #
            for _ in range(10):
                try_next_address(ips, subnets[idx])

        i = 0
        result_subnets_ips = []
        while i < address_count:
            idx = i % len(subnets)
            subnet = subnets[idx]
            ip = try_next_address(network_ips[idx], subnet)
            resp = self.aws.ec2_client.describe_instances(Filters=[{
                'Name': 'private-ip-address',
                'Values': [ip]
            }])
            if not resp['Reservations']:
                i += 1
                _LOG.info('Got ip address %s ', ip)
                result_subnets_ips.append((subnet, ip))

        _LOG.info('IP Addresses are allocated')

        return result_subnets_ips
