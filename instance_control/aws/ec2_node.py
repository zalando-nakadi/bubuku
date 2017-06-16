#!/usr/bin/env python3

import copy
import logging
import time

import boto3
import yaml

from instance_control import config
from instance_control.aws import security_group, iam, subnet, metric
from instance_control.aws import taupage

_LOG = logging.getLogger('bubuku.cluster.aws.ec2_node')


def _create_tagged_volume(ec2_client: object, cluster_config: dict, zone: str, name: str):
    _LOG.info('Creating EBS volume %s in %s', name, zone)
    ebs_data = {
        "AvailabilityZone": zone,
        "VolumeType": cluster_config['volume_type'],
        "Size": cluster_config['volume_size'],
        "Encrypted": False, }
    vol = ec2_client.create_volume(**ebs_data)
    _LOG.info('%s is successfully created', vol['VolumeId'])

    _LOG.info('Tagging %s with Taupage:erase-on-boot, to format only once', vol['VolumeId'])
    tags = [{'Key': 'Name',
             'Value': name},
            {'Key': 'Taupage:erase-on-boot',
             'Value': 'True'}]
    ec2_client.create_tags(Resources=[vol['VolumeId']], Tags=tags)


def _launch_instance(region: str, ip: dict, ami: object, subnet: dict, security_group_id: str, cluster_config: dict):
    _LOG.info('Launching node %s in %s', ip['_defaultIp'], subnet['AvailabilityZone'])
    ec2_client = boto3.client('ec2', region_name=region)

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
            block_devices.append({'DeviceName': bd['DeviceName'],
                                  'NoDevice': ''})

    if cluster_config['create_ebs']:
        _create_tagged_volume(ec2_client, cluster_config, subnet['AvailabilityZone'], config.KAFKA_LOGS_EBS)

    user_data = cluster_config['user_data']
    user_data['volumes']['ebs']['/dev/xvdk'] = config.KAFKA_LOGS_EBS
    taupage_user_data = '#taupage-ami-config\n{}'.format(yaml.safe_dump(user_data))

    resp = ec2_client.run_instances(
        ImageId=ami.id,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[security_group_id],
        UserData=taupage_user_data,
        InstanceType=cluster_config['instance_type'],
        SubnetId=subnet['SubnetId'],
        PrivateIpAddress=ip['PrivateIp'],
        BlockDeviceMappings=block_devices,
        IamInstanceProfile={'Arn': cluster_config['instance_profile']['Arn']},
        DisableApiTermination=False,
        EbsOptimized=True)

    instance = resp['Instances'][0]
    instance_id = instance['InstanceId']
    _LOG.info('Instance %s launched, waiting for it to initialize', instance_id)

    ec2_client.create_tags(Resources=[instance_id],
                           Tags=[{'Key': 'Name', 'Value': cluster_config['cluster_name']}])

    # wait for instance to initialize before we can assign a
    # public IP address to it or tag the attached volume
    while True:
        resp = ec2_client.describe_instances(InstanceIds=[instance_id])
        instance = resp['Reservations'][0]['Instances'][0]
        if instance['State']['Name'] != 'pending':
            _LOG.info('Instance %s is up', instance_id)
            break
        _LOG.info('Waiting 5 secs more ...')
        time.sleep(5)

    metric.create_auto_recovery_alarm(cluster_config, instance_id)


def _launch_nodes(cluster_config: dict):
    i = 0
    for ip in cluster_config['node_ips']:
        subnets = cluster_config['subnets']
        _launch_instance(cluster_config['region'], ip,
                         ami=cluster_config['taupage_amis'],
                         subnet=subnets[i % len(subnets)],
                         security_group_id=cluster_config['security_group']['GroupId'],
                         cluster_config=cluster_config)
        if i + 1 == cluster_config['cluster_size']:
            _LOG.info("Creation is finished")
            break
        i += 1
        _LOG.info("Sleeping for one minute before launching next node..")
        time.sleep(60)


def create(cluster_config: dict):
    artifact_name = 'bubuku-appliance'
    cluster_config['docker_image'] = 'registry.opensource.zalan.do/aruha/{}:{}'.format(artifact_name,
                                                                                       cluster_config['image_version'])
    _LOG.info('Preparing AWS configuration for ec2 instance creation')
    try:
        cluster_config['taupage_amis'] = taupage.find_amis(cluster_config['region'])
        cluster_config['subnets'] = subnet.get_subnets('internal-', cluster_config)
        cluster_config['node_ips'] = subnet.allocate_ip_addresses(cluster_config)
        cluster_config['security_group'] = security_group.create_or_ger_security_group(cluster_config)
        cluster_config['user_data'] = taupage.generate_user_data(cluster_config)
        cluster_config['instance_profile'] = iam.create_or_get_instance_profile(cluster_config)

        _launch_nodes(cluster_config)

    except:
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
        raise
