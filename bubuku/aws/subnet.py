import logging

import netaddr

from bubuku.aws.cluster_config import ClusterConfig
from bubuku.aws import AWSResources

_LOG = logging.getLogger('bubuku.cluster.aws.subnet')


def get_subnets(ec2, prefix_filter: str, cluster_config: ClusterConfig) -> list:
    '''
    Returns lists of subnets, which names start
    with the specified prefix (it should be either 'dmz-' or
    'internal-'), sorted by the Availability Zone and filtered by vpc id
    '''
    vpc_id = cluster_config.get_vpc_id()
    availability_zone = cluster_config.get_availability_zone()
    _LOG.info('Getting subnets for vpc_id: %s and availability_zone: %s', vpc_id, availability_zone)

    resp = ec2.describe_subnets()
    subnets = []

    for subnet in resp['Subnets']:
        if subnet['VpcId'] != vpc_id:
            continue
        for tag in subnet['Tags']:
            if tag['Key'] == 'Name' and tag['Value'].startswith(prefix_filter):
                if availability_zone:
                    if subnet['AvailabilityZone'] == availability_zone:
                        subnets.append(subnet)
                else:
                    subnets.append(subnet)
    _LOG.info('Got subnets %s ', subnets)
    return subnets


class IpAddressPoolDepletedException(Exception):
    def __init__(self, cidr_block: str):
        msg = "Pool of unused IP addresses depleted in subnet: {}".format(cidr_block)
        super(IpAddressPoolDepletedException, self).__init__(msg)


def allocate_ip_addresses(aws_: AWSResources, cluster_config, address_count: int) -> list:
    '''
    Allocate unused private IP addresses by checking the current
    reservations
    Return list of tuples (subnet, ip)
    '''
    _LOG.info('Allocating IP addresses ...')

    def try_next_address(ips, subnet):
        try:
            return str(next(ips))
        except StopIteration:
            raise IpAddressPoolDepletedException(subnet['CidrBlock'])

    #
    # Here we have to account for the behavior of launch_*_nodes
    # which iterate through subnets to put the instances into
    # different Availability Zones.
    #
    subnets = get_subnets(aws_.ec2_client, 'internal-', cluster_config)
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
    result_ips = []
    while i < address_count:
        idx = i % len(subnets)
        subnet = subnets[idx]
        ip = try_next_address(network_ips[idx], subnet)
        resp = aws_.ec2_client.describe_instances(Filters=[{
            'Name': 'private-ip-address',
            'Values': [ip]
        }])
        if not resp['Reservations']:
            i += 1
            _LOG.info('Got ip address %s ', ip)
            result_ips.append((subnet, ip))

    _LOG.info('IP Addresses are allocated')

    return result_ips
