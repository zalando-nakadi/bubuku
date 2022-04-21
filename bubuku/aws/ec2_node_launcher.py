import copy
import logging
import time
import yaml

from bubuku.aws import AWSResources
from bubuku.aws.cluster_config import ClusterConfig
from bubuku.aws.ip_address_allocator import IpAddressAllocator
from bubuku.aws.node import KAFKA_LOGS_EBS

_LOG = logging.getLogger('bubuku.aws.ec2_node')


class Ec2NodeLauncher(object):
    def __init__(self, aws: AWSResources, cluster_config: ClusterConfig, az: str):
        self._aws = aws
        self._cluster_config = cluster_config
        self._az = az

    def _launch_instance(self, ip: str, subnet: dict, ami: object, security_group_id: str, iam_profile):
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

        user_data = self._cluster_config.get_user_data()
        user_data['volumes']['ebs']['/dev/xvdk'] = KAFKA_LOGS_EBS
        taupage_user_data = '#taupage-ami-config\n{}'.format(yaml.safe_dump(user_data))

        _LOG.info('Launching node %s in %s', ip, subnet['AvailabilityZone'])
        resp = self._aws.ec2_client.run_instances(
            ImageId=ami.id,
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[security_group_id],
            UserData=taupage_user_data,
            InstanceType=self._cluster_config.get_instance_type(),
            SubnetId=subnet['SubnetId'],
            PrivateIpAddress=ip,
            BlockDeviceMappings=block_devices,
            IamInstanceProfile={'Arn': iam_profile['Arn']},
            DisableApiTermination=False,
            EbsOptimized=True)

        instance_id = resp['Instances'][0]['InstanceId']
        _LOG.info('Instance %s launched', instance_id)

        attempts = 2
        while True:
            try:
                self._aws.ec2_client.create_tags(
                    Resources=[instance_id],
                    Tags=(self._cluster_config.get_tags() + [
                        {'Key': 'Name', 'Value': self._cluster_config.get_cluster_name()},
                        {'Key': 'StackName', 'Value': self._cluster_config.get_cluster_name()}
                    ])
                )
                break

            except Exception as e:
                attempts -= 1
                if attempts == 0:
                    raise e
                _LOG.error('Failed to create instance tags, will retry...', exc_info=e)
                time.sleep(5)

        return instance_id

    def create_auto_recovery_alarm(self, instance_id):
        _LOG.info('Creating AWS auto recovery alarm for %s', instance_id)
        alarm_actions = ['arn:aws:automate:{}:ec2:recover'.format(self._cluster_config.get_aws_region())]
        alarm_name = '{}-{}-auto-recover'.format(self._cluster_config.get_cluster_name(), instance_id)

        self._aws.cloudwatch_client.put_metric_alarm(
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
        _LOG.info('Created alarm %s', alarm_name)

    def launch(self):
        _LOG.info('Preparing AWS configuration for ec2 instance creation')
        ip_address_allocator = IpAddressAllocator(self._aws, self._cluster_config.get_vpc_id(), self._az)
        subnet, ip = ip_address_allocator.allocate_ip_addresses(1)[0]
        return self._launch_instance(ip,
                                     subnet,
                                     self._find_ami(),
                                     self._get_security_group_id(),
                                     self._get_instance_profile())

    def _get_instance_profile(self):
        profile_name = 'profile-{}'.format(self._cluster_config.get_cluster_name())
        profile = self._aws.iam_client.get_instance_profile(InstanceProfileName=profile_name)
        _LOG.info("IAM profile %s exists, using it", profile_name)
        return profile['InstanceProfile']

    def _get_security_group_id(self) -> str:
        _LOG.info('Configuring security group ...')
        security_groups = self._aws.ec2_client.describe_security_groups(
            Filters=[{'Name': 'group-name', 'Values': [self._cluster_config.get_cluster_name()]}])
        if security_groups['SecurityGroups']:
            sg = security_groups['SecurityGroups'][0]
            _LOG.info('Security group for %s exists, will use it %s',
                      self._cluster_config.get_cluster_name(), sg['GroupId'])
            return sg['GroupId']
        raise Exception('Security group does not exist for {}'.format(self._cluster_config.get_cluster_name()))

    def _get_ip_permission(self, port: int):
        return {
            'IpProtocol': 'tcp',
            'FromPort': port,
            'ToPort': port,
            'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
        }

    def _find_ami(self) -> dict:
        _LOG.info('Finding latest Taupage AMI.')
        filters = [{'Name': 'image-id', 'Values': [self._cluster_config.get_ami_id()]}]
        images = list(self._aws.ec2_resource.images.filter(Filters=filters))
        if not images:
            raise Exception('No Taupage AMI found')
        most_recent_image = sorted(images, key=lambda i: i.name)[-1]  # It s expected that image is only one

        _LOG.info('The AMI to use is %s', most_recent_image)

        return most_recent_image
