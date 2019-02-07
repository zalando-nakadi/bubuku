import logging
from typing import List

from instance_control.aws import AWSResources
from instance_control.command import Command
from instance_control.command.upgrade import get_image_version

_LOG = logging.getLogger('bubuku.cluster.command.get')


def _print_table(items: List[dict]):
    headers = []
    for i in items:
        headers.extend(i.keys())
    headers = sorted(set(headers))
    lengths = {h: len(h) for h in headers}
    for d in items:
        for k, v in d.items():
            v = str(v)
            if lengths[k] < len(v):
                lengths[k] = len(v)
    format_ = ''
    for idx in range(0, len(headers)):
        format_ += '{' + str(idx) + ':' + str(lengths[headers[idx]]) + '} '
    print(format_.format(*headers))
    for item in items:
        print(format_.format(*[item.get(h) for h in headers]))


class GetCommand(Command):
    def __init__(self, cluster_config_path: str):
        super().__init__(cluster_config_path)

    def execute(self):
        aws_ = AWSResources(region=self.cluster_config['region'])
        instances = list(aws_.ec2_resource.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
            {'Name': 'tag:Name', 'Values': [self.cluster_config['cluster_name']]}]))

        if len(instances) == 0:
            _LOG.info('Cluster has no running instances')
            return

        data = []
        for instance in instances:
            data.append({
                '1 Ip': instance.private_ip_address,
                '2 Id': instance.instance_id,
                '3 Volume': [x['Ebs']['VolumeId'] for x in instance.block_device_mappings if
                             x['DeviceName'] == '/dev/xvdk'][0],
                '4 Availability zone': instance.placement['AvailabilityZone'],
                '5 Image Version': get_image_version(instance)
            })

        _print_table(data)
