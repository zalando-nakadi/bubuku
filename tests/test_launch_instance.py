import logging

from bubuku.aws import AWSResources
from bubuku.aws.cluster_config import ClusterConfig, ConfigLoader
from bubuku.aws.ec2_node_launcher import Ec2NodeLauncher
from bubuku.aws.node import KAFKA_LOGS_EBS


class EmptyConfigLoader(ConfigLoader):
    def load_config(self):
        return {'environment': {'CLUSTER_NAME': 'bubuku-staging-rolling-restart'},
                'volumes': {'ebs': {'/dev/xvdk': KAFKA_LOGS_EBS}}}


if __name__ == '__main__':
    logging.basicConfig(level=getattr(logging, 'INFO', None))
    cluster_config = ClusterConfig(EmptyConfigLoader())
    cluster_config.set_instance_type('m4.xlarge')
    cluster_config.set_availability_zone('eu-central-1a')
    cluster_config.set_vpc_id('vpc-e5d3b28c')
    aws = AWSResources(region=cluster_config.get_aws_region())
    ec2_node_launcher = Ec2NodeLauncher(aws, cluster_config)
    ec2_node_launcher.launch()
