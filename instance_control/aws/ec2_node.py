#!/usr/bin/env python3

import copy
import logging
import time

import boto3
import yaml
from botocore.config import Config

from instance_control import config
from instance_control.aws import security_group, iam, subnet, metric
from instance_control.aws import taupage

_LOG = logging.getLogger('bubuku.cluster.aws.ec2_node')


class EC2(object):
    def __init__(self, region, retries=100):
        self.session = boto3.Session()
        self.retries = retries
        self.region = region
        boto3.set_stream_logger('boto3', logging.INFO)
        self._client = None
        self._resource = None

    @property
    def client(self):
        if not self._client:
            self._client = self.session.client(
                'ec2',
                region_name=self.region,
                config=Config(retries={'max_attempts': self.retries}))
        return self._client

    @property
    def resource(self):
        if not self._resource:
            self._resource = self.session.resource(
                'ec2',
                region_name=self.region,
                config=Config(retries={'max_attempts': self.retries}))
        return self._resource

    def _create_tagged_volume(self, cluster_config: dict, zone: str, name: str):
        _LOG.info('Creating EBS volume %s in %s', name, zone)
        vol = self.client.create_volume(
            AvailabilityZone=zone,
            VolumeType=cluster_config['volume_type'],
            Size=cluster_config['volume_size'],
            Encrypted=False,
        )
        _LOG.info('%s is successfully created', vol['VolumeId'])

        _LOG.info('Tagging %s with Taupage:erase-on-boot, to format only once', vol['VolumeId'])
        tags = [
            {
                'Key': 'Name',
                'Value': name
            }, {
                'Key': 'Taupage:erase-on-boot',
                'Value': 'True'
            }
        ]
        self.client.create_tags(Resources=[vol['VolumeId']], Tags=tags)

    def _launch_instance(self, ip: str, subnet_: dict, ami: object, security_group_id: str, cluster_config: dict):
        _LOG.info('Launching node %s in %s', ip, subnet_['AvailabilityZone'])

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

        if cluster_config['create_ebs']:
            self._create_tagged_volume(cluster_config, subnet_['AvailabilityZone'], config.KAFKA_LOGS_EBS)

        user_data = cluster_config['user_data']
        user_data['volumes']['ebs']['/dev/xvdk'] = config.KAFKA_LOGS_EBS
        taupage_user_data = '#taupage-ami-config\n{}'.format(yaml.safe_dump(user_data))

        resp = self.client.run_instances(
            ImageId=ami.id,
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[security_group_id],
            UserData=taupage_user_data,
            InstanceType=cluster_config['instance_type'],
            SubnetId=subnet_['SubnetId'],
            PrivateIpAddress=ip,
            BlockDeviceMappings=block_devices,
            IamInstanceProfile={'Arn': cluster_config['instance_profile']['Arn']},
            DisableApiTermination=False,
            EbsOptimized=True)

        instance_id = resp['Instances'][0]['InstanceId']
        _LOG.info('Instance %s launched, waiting for it to initialize', instance_id)
        self.client.create_tags(
            Resources=[instance_id],
            Tags=[{'Key': 'Name', 'Value': cluster_config['cluster_name']}])

        return instance_id

    def _launch_nodes(self, cluster_config: dict, node_ips: list):
        starting_instances = []
        for subnet_, ip in node_ips:
            starting_instances.append(
                self._launch_instance(
                    ip,
                    subnet_,
                    cluster_config['taupage_amis'],
                    security_group_id=cluster_config['security_group']['GroupId'],
                    cluster_config=cluster_config))
        # wait for all instances to start
        while starting_instances:
            _LOG.info("Waiting for instances to start: {}".format(starting_instances))
            time.sleep(5)
            resp = self.client.describe_instances(InstanceIds=starting_instances)
            started = [i['InstanceId'] for i in resp['Reservations']['Instances'] if i['State']['Name'] != 'pending']
            if started:
                _LOG.info('Instances {} started', started)
            for instance_id in started:
                starting_instances.remove(instance_id)
                metric.create_auto_recovery_alarm(cluster_config, instance_id)

    def create(self, cluster_config: dict, instance_count: int):
        artifact_name = 'bubuku-appliance'
        cluster_config['docker_image'] = 'registry.opensource.zalan.do/aruha/{}:{}'.format(
            artifact_name, cluster_config['image_version'])
        _LOG.info('Preparing AWS configuration for ec2 instance creation')
        try:
            cluster_config['taupage_amis'] = taupage.find_amis(cluster_config['region'])
            cluster_config['subnets'] = subnet.get_subnets('internal-', cluster_config)

            node_ips = subnet.allocate_ip_addresses(cluster_config, instance_count)

            cluster_config['security_group'] = security_group.create_or_ger_security_group(cluster_config)
            cluster_config['user_data'] = taupage.generate_user_data(cluster_config)
            cluster_config['instance_profile'] = iam.create_or_get_instance_profile(cluster_config)

            self._launch_nodes(cluster_config, node_ips)

        except Exception as e:
            _LOG.error('''
                    You were trying to deploy Bubuku, but the process has failed :-(
                    
                    One of the reasons might be that some of Private IP addresses we were
                    going to use to launch the EC2 instances were taken by some other
                    instances in the middle of the process.  If that is the case, simply
                    retrying the operation might resolve the problem (you still might need
                    to clean up after this attempt before retrying).
                    
                    Please review the error message to see if that is the case, then
                    either correct the error or retry.
                    
                ''')
            raise e
