import copy
import logging

import yaml

from bubuku.aws import AWSResources
from bubuku.aws.cluster_config import ClusterConfig
from bubuku.aws.ip_address_allocator import IpAddressAllocator
from bubuku.aws.node import KAFKA_LOGS_EBS

_LOG = logging.getLogger('bubuku.aws.ec2_node')


class Ec2NodeLauncher(object):
    def __init__(self, aws: AWSResources, cluster_config: ClusterConfig):
        self.aws = aws
        self.cluster_config = cluster_config

    def _launch_instance(self, ip: str, subnet_: dict, ami: object, security_group_id: str, iam_profile):
        _LOG.info('Launching node {} in {}', ip, subnet_['AvailabilityZone'])

        #
        # Override any ephemeral volumes with NoDevice mapping,
        # otherwise auto-recovery alarm cannot be actually enabled.
        #
        _LOG.info('Overriding ephemeral volumes to be able to set up AWS auto recovery alarm ')
        block_devices = []
        for bd in ami.block_device_mappings:
            if 'Ebs' in bd:
                #
                # This has to be our root EBS.
                #
                # If the Encrypted flag is present, we have to delete
                # it even if it matches the actual snapshot setting,
                # otherwise amazon will complain rather loudly.
                #
                # Take a deep copy before deleting the key:
                #
                bd = copy.deepcopy(bd)

                root_ebs = bd['Ebs']
                if 'Encrypted' in root_ebs:
                    del (root_ebs['Encrypted'])

                block_devices.append(bd)
            else:
                # ignore any ephemeral volumes (aka. instance storage)
                block_devices.append({
                    'DeviceName': bd['DeviceName'],
                    'NoDevice': ''
                })

        user_data = self.cluster_config.get_user_data()
        user_data['volumes']['ebs']['/dev/xvdk'] = KAFKA_LOGS_EBS
        taupage_user_data = '#taupage-ami-config\n{}'.format(yaml.safe_dump(user_data))

        resp = self.aws.ec2_client.run_instances(
            ImageId=ami.id,
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[security_group_id],
            UserData=taupage_user_data,
            InstanceType=self.cluster_config.get_instance_type(),
            SubnetId=subnet_['SubnetId'],
            PrivateIpAddress=ip,
            BlockDeviceMappings=block_devices,
            IamInstanceProfile={'Arn': iam_profile['Arn']},
            DisableApiTermination=False,
            EbsOptimized=True)

        instance_id = resp['Instances'][0]['InstanceId']
        _LOG.info('Instance {} launched, waiting for it to initialize', instance_id)
        self.aws.ec2_client.create_tags(
            Resources=[instance_id],
            Tags=[
                {'Key': 'Name', 'Value': self.cluster_config.get_cluster_name()},
                {'Key': 'StackName', 'Value': self.cluster_config.get_cluster_name()}
            ]
        )

        return instance_id

    def _create_auto_recovery_alarm(self, cluster_name: str, instance_id):
        _LOG.info('Creating AWS auto recovery alarm for {}', instance_id)
        alarm_actions = ['arn:aws:automate:{}:ec2:recover'.format(self.aws.region)]
        alarm_name = '{}-{}-auto-recover'.format(cluster_name, instance_id)

        self.aws.cloudwatch_client.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmActions=alarm_actions,
            MetricName='StatusCheckFailed_System',
            Namespace='AWS/EC2',
            Statistic='Minimum',
            Dimensions=[{
                'Name': 'InstanceId',
                'Value': instance_id
            }],
            Period=60,  # 1 minute
            EvaluationPeriods=2,
            Threshold=0,
            ComparisonOperator='GreaterThanThreshold')
        _LOG.info('Created alarm {}', alarm_name)

    def launch(self):
        _LOG.info('Preparing AWS configuration for ec2 instance creation')
        ip_address_allocator = IpAddressAllocator(self.aws, self.cluster_config)
        ip, subnet = ip_address_allocator.allocate_ip_addresses(1)[0]
        self._launch_instance(
            ip,
            subnet,
            self._find_taupage_amis(),
            self._get_security_group_id(),
            self._get_instance_profile())

    def _get_instance_profile(self):
        profile_name = 'profile-{}'.format(self.cluster_config.get_cluster_name())
        profile = self.aws.iam_client.get_instance_profile(InstanceProfileName=profile_name)
        _LOG.info("IAM profile {} exists, using it", profile_name)
        return profile['InstanceProfile']

    def _get_security_group_id(self) -> dict:
        _LOG.info('Configuring security group ...')
        security_groups = self.aws.ec2_client.describe_security_groups(
            Filters=[{'Name': 'group-name', 'Values': [self.cluster_config.get_cluster_name()]}])
        if security_groups['SecurityGroups']:
            sg = security_groups['SecurityGroups'][0]
            _LOG.info('Security group for {} exists, will use it {}',
                      self.cluster_config.get_cluster_name(), sg['GroupId'])
            return sg['GroupId']
        raise Exception('Security group does not exist for {}'.format(self.cluster_config.get_cluster_name()))

    def _get_ip_permission(self, port: int):
        return {
            'IpProtocol': 'tcp',
            'FromPort': port,
            'ToPort': port,
            'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
        }

    def _find_taupage_amis(self) -> dict:
        region = self.cluster_config.get_aws_region()
        _LOG.info('Finding latest Taupage AMI in {}..', region)

        filters = [{'Name': 'name', 'Values': ['*Taupage-AMI-*']},
                   {'Name': 'is-public', 'Values': ['false']},
                   {'Name': 'state', 'Values': ['available']},
                   {'Name': 'root-device-type', 'Values': ['ebs']}]
        images = list(self.aws.ec2_resource.images.filter(Filters=filters))
        if not images:
            raise Exception('No Taupage AMI found')
        most_recent_image = sorted(images, key=lambda i: i.name)[-1]

        _LOG.info('The latest AMI is {}', most_recent_image)

        return most_recent_image
