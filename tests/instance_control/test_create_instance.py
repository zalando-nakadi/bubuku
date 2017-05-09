#!/usr/bin/env python3

import collections
import copy
import re
import sys
import time

import boto3
import netaddr
import requests
import yaml
from botocore.exceptions import ClientError


def setup_security_groups(cluster_name: str, node_ips: dict, vpc_id: str, result: dict) -> dict:
    for region, ips in node_ips.items():
        print('Configuring Security Group in {}..'.format(region))
        ec2 = boto3.client('ec2', region)
        existing_sg = ec2.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [cluster_name]}])
        if existing_sg['SecurityGroups']:
            result[region] = existing_sg['SecurityGroups'][0]
            return

        print("Security Group does not exists, creating Security Group rule")
        sg_name = cluster_name
        sg = ec2.create_security_group(GroupName=sg_name,
                                       VpcId=vpc_id,
                                       Description='Bubuku Security Group')
        result[region] = sg
        ec2.create_tags(Resources=[sg['GroupId']], Tags=[{'Key': 'Name', 'Value': sg_name}])
        ec2.authorize_security_group_ingress(GroupId=sg['GroupId'], IpPermissions=[get_ip_permission(22),
                                                                                   get_ip_permission(8004),
                                                                                   get_ip_permission(8080),
                                                                                   get_ip_permission(8778),
                                                                                   get_ip_permission(9100),
                                                                                   get_ip_permission(9092)])


def get_ip_permission(port: int):
    return {
        'IpProtocol': 'tcp',
        'FromPort': port,
        'ToPort': port,
        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
    }


def find_taupage_amis(regions: list) -> dict:
    '''
    Find latest Taupage AMI for each region
    '''
    result = {}
    for region in regions:
        print('Finding latest Taupage AMI in {}..'.format(region))
        ec2 = boto3.resource('ec2', region)
        filters = [{'Name': 'name', 'Values': ['*Taupage-AMI-*']},
                   {'Name': 'is-public', 'Values': ['false']},
                   {'Name': 'state', 'Values': ['available']},
                   {'Name': 'root-device-type', 'Values': ['ebs']}]
        images = list(ec2.images.filter(Filters=filters))
        if not images:
            raise Exception('No Taupage AMI found')
        most_recent_image = sorted(images, key=lambda i: i.name)[-1]
        result[region] = most_recent_image
    return result


class IpAddressPoolDepletedException(Exception):
    def __init__(self, cidr_block: str):
        msg = "Pool of unused IP addresses depleted in subnet: {}".format(cidr_block)
        super(IpAddressPoolDepletedException, self).__init__(msg)


def generate_private_ip_addresses(ec2: object, subnets: list, cluster_size: int):
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
    while i < cluster_size:
        idx = i % len(subnets)

        ip = try_next_address(network_ips[idx], subnets[idx])

        resp = ec2.describe_instances(Filters=[{
            'Name': 'private-ip-address',
            'Values': [ip]
        }])
        if not resp['Reservations']:
            i += 1
            yield ip


def allocate_ip_addresses(region_subnets: dict, cluster_size: int, node_ips: dict):
    '''
    Allocate unused private IP addresses by checking the current
    reservations, and optionally allocate Elastic IPs.
    '''
    for region, subnets in region_subnets.items():
        print('Allocating IP addresses in {}..'.format(region))
        ec2 = boto3.client('ec2', region_name=region)

        for ip in generate_private_ip_addresses(ec2, subnets, cluster_size):
            address = {'PrivateIp': ip, '_defaultIp': ip}
            node_ips[region].append(address)


def get_subnets(prefix_filter: str, availability_zone: str, vpc_id: str, regions: list) -> dict:
    '''
    Returns a dict of per-region lists of subnets, which names start
    with the specified prefix (it should be either 'dmz-' or
    'internal-'), sorted by the Availability Zone and filtered by vpc id
    '''
    subnets = collections.defaultdict(list)
    for region in regions:
        ec2 = boto3.client('ec2', region)
        resp = ec2.describe_subnets()

        for subnet in sorted(resp['Subnets'], key=lambda subnet: subnet['AvailabilityZone']):
            if subnet['VpcId'] != vpc_id:
                continue
            for tag in subnet['Tags']:
                if tag['Key'] == 'Name' and tag['Value'].startswith(prefix_filter):
                    if availability_zone:
                        if subnet['AvailabilityZone'] == availability_zone:
                            subnets[region].append(subnet)
                    else:
                        subnets[region].append(subnet)

    return subnets


def generate_taupage_user_data(options: dict) -> str:
    '''
    Generate Taupage user data to start a Bubuku node
    http://docs.stups.io/en/latest/components/taupage.html
    '''
    data = {'runtime': 'Docker',
            'source': options['docker_image'],
            'application_id': options['cluster_name'],
            'application_version': options['image_version'],
            'networking': 'host',
            'ports': {'9092': '9092',
                      '8004': '8004',
                      '8080': '8080',
                      '8778': '8778'},
            'enhanced_cloudwatch_metrics': True,
            'application_logrotate_size': '100M',
            'application_logrotate_rotate': 4,
            'environment': {
                'CLUSTER_NAME': options['cluster_name'],
                'ZOOKEEPER_PREFIX': options['environment']['zookeeper_prefix'],
                'BUKU_FEATURES': options['environment']['buku_features'],
                'JMX_PORT': options['environment']['jmx_port'],
                'KAFKA_HEAP_OPTS': options['environment']['kafka_heap_opts'],
                'STARTUP_TIMEOUT_TYPE': options['environment']['startup_timeout_type'],
                'STARTUP_TIMEOUT_INITIAL': options['environment']['startup_timeout_initial'],
                'STARTUP_TIMEOUT_STEP': options['environment']['startup_timeout_step']
            },
            'volumes': {
                'ebs': {
                    '/dev/xvdk': None
                }
            },
            'mounts': {
                '/data': {
                    'partition': '/dev/xvdk',
                    'filesystem': 'ext4',
                    'options': 'noatime,nodiratime,nobarrier'
                }
            },
            'scalyr_account_key': options['scalyr_key']
            }

    if options['environment']:
        data['environment'].update(options['environment'])

    return data


def create_instance_profile(cluster_name: str):
    profile_name = 'profile-{}'.format(cluster_name)
    role_name = 'role-{}'.format(cluster_name)
    policy_datavolume = 'policy-{}-datavolume'.format(cluster_name)
    policy_metadata = 'policy-{}-metadata'.format(cluster_name)
    policy_zmon = 'policy-{}-zmon'.format(cluster_name)

    iam = boto3.client('iam')

    try:
        profile = iam.get_instance_profile(InstanceProfileName=profile_name)
        return profile['InstanceProfile']
    except ClientError:
        print("Instance profile does not exists, creating ...")
        pass

    profile = iam.create_instance_profile(InstanceProfileName=profile_name)

    role = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument="""{
        "Version": "2012-10-17",
        "Statement": [{
             "Action": "sts:AssumeRole",
             "Effect": "Allow",
             "Principal": {
                 "Service": "ec2.amazonaws.com"
             }
        }]
    }""")

    policy_datavolume_document = """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "ec2:DescribeTags",
                    "ec2:DeleteTags",
                    "ec2:DescribeVolumes",
                    "ec2:AttachVolume"
                ],
                "Resource": "*"
            }
        ]
    }"""
    policy_metadata_document = """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "ec2:Describe*",
                "Resource": "*",
                "Effect": "Allow"
            },
            {
                "Action": "elasticloadbalancing:Describe*",
                "Resource": "*",
                "Effect": "Allow"
            }
        ]
    }"""
    policy_zmon_document = """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "cloudwatch:PutMetricData",
                "Resource": "*",
                "Effect": "Allow"
            }
        ]
    }"""
    iam.put_role_policy(RoleName=role_name,
                        PolicyName=policy_datavolume,
                        PolicyDocument=policy_datavolume_document)
    iam.put_role_policy(RoleName=role_name,
                        PolicyName=policy_metadata,
                        PolicyDocument=policy_metadata_document)
    iam.put_role_policy(RoleName=role_name,
                        PolicyName=policy_zmon,
                        PolicyDocument=policy_zmon_document)

    iam.add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)

    return profile['InstanceProfile']


def create_tagged_volume(ec2: object, options: dict, zone: str, name: str):
    ebs_data = {
        "AvailabilityZone": zone,
        "VolumeType": options['volume_type'],
        "Size": options['volume_size'],
        "Encrypted": False, }
    vol = ec2.create_volume(**ebs_data)

    tags = [{'Key': 'Name',
             'Value': name},
            {'Key': 'Taupage:erase-on-boot',
             'Value': 'True'}]
    ec2.create_tags(Resources=[vol['VolumeId']], Tags=tags)


def launch_instance(region: str, ip: dict, ami: object, subnet: dict, security_group_id: str, options: dict):
    print('Launching node {} in {}..'.format(ip['_defaultIp'], region))
    ec2 = boto3.client('ec2', region_name=region)

    #
    # Override any ephemeral volumes with NoDevice mapping,
    # otherwise auto-recovery alarm cannot be actually enabled.
    #
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

    volume_name = 'kafka-logs-ebs'
    if options['create_ebs']:
        create_tagged_volume(ec2, options, subnet['AvailabilityZone'], volume_name)

    user_data = options['user_data']
    user_data['volumes']['ebs']['/dev/xvdk'] = volume_name
    taupage_user_data = '#taupage-ami-config\n{}'.format(yaml.safe_dump(user_data))

    resp = ec2.run_instances(
        ImageId=ami.id,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[security_group_id],
        UserData=taupage_user_data,
        InstanceType=options['instance_type'],
        SubnetId=subnet['SubnetId'],
        PrivateIpAddress=ip['PrivateIp'],
        BlockDeviceMappings=block_devices,
        IamInstanceProfile={'Arn': options['instance_profile']['Arn']},
        DisableApiTermination=False)

    instance = resp['Instances'][0]
    instance_id = instance['InstanceId']

    ec2.create_tags(Resources=[instance_id],
                    Tags=[{'Key': 'Name', 'Value': options['cluster_name']}])

    # wait for instance to initialize before we can assign a
    # public IP address to it or tag the attached volume
    while True:
        resp = ec2.describe_instances(InstanceIds=[instance_id])
        instance = resp['Reservations'][0]['Instances'][0]
        if instance['State']['Name'] != 'pending':
            break
        time.sleep(5)

    # add an auto-recovery alarm for this instance
    cw = boto3.client('cloudwatch', region_name=region)
    alarm_actions = ['arn:aws:automate:{}:ec2:recover'.format(region)]
    if options['alarm_topics']:
        alarm_actions.append(options['alarm_topics'][region])

    cw.put_metric_alarm(AlarmName='{}-{}-auto-recover'.format(options['cluster_name'], instance_id),
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


def launch_normal_nodes(options: dict):
    for region, ips in options['node_ips'].items():
        subnets = options['subnets'][region]
        for i, ip in enumerate(ips):
            launch_instance(region, ip,
                            ami=options['taupage_amis'][region],
                            subnet=subnets[i % len(subnets)],
                            security_group_id=options['security_groups'][region]['GroupId'],
                            options=options)
            if i + 1 >= options['cluster_size']:
                print("Done. Node(s) is created.")
                break
            print("Sleeping for one minute before launching next node..")
            time.sleep(60)


def create_instance_with(cluster_config: dict):
    regions=cluster_config['regions'],
    vpc_id=cluster_config['vpc_id'],
    availability_zone=cluster_config['availability_zone'],
    create_ebs=cluster_config['create_ebs'],
    cluster_name=cluster_config['cluster_name'],
    cluster_size=cluster_config['cluster_size'],
    instance_type=cluster_config['instance_type'],
    volume_type=cluster_config['volume_type'],
    volume_size=cluster_config['volume_size'],
    scalyr_key=cluster_config['scalyr_key'],
    image_version=cluster_config['image_version'],
    environment=cluster_config['environment']

    if not cluster_name:
        raise Exception('You must specify the cluster name')

    cluster_name_re = '^[a-z][a-z0-9-]*[a-z0-9]$'
    if not re.match(cluster_name_re, cluster_name):
        raise Exception(
            'Cluster name must only contain lowercase latin letters, digits and dashes '
            '(it also must start with a letter and cannot end with a dash), in other words '
            'it must be matched by the following regular expression: {}'.format(cluster_name_re))

    if not regions:
        raise Exception('Please specify at least one region')

    artifact_name = 'bubuku-appliance'
    docker_image = 'registry.opensource.zalan.do/aruha/{}:{}'.format(artifact_name, image_version)
    url = 'https://registry.opensource.zalan.do/teams/aruha/artifacts/{}/tags'.format(artifact_name)
    if not next(tag for tag in requests.get(url).json() if tag['name'] == image_version):
        raise Exception('Docker image was not found')

    print("Environment variables: ")
    for k, v in environment.items():
        print("{}={}".format(k, v))

    # List of IP addresses by region
    node_ips = collections.defaultdict(list)

    # Mapping of region name to the Security Group
    security_groups = {}
    alarm_topics = {}

    try:
        taupage_amis = find_taupage_amis(regions)
        subnets = get_subnets('internal-', availability_zone, vpc_id, regions)
        allocate_ip_addresses(subnets, cluster_size, node_ips)
        setup_security_groups(cluster_name, node_ips, vpc_id, security_groups)
        user_data = generate_taupage_user_data(locals())
        instance_profile = create_instance_profile(cluster_name)

        #
        # FIXME: using an instance profile right after creating one
        # can result in 'not found' error, because of eventual
        # consistency.  For now fix with a sleep, should rather
        # examine exception and retry after some delay.
        #
        time.sleep(30)

        launch_normal_nodes(locals())

    except:
        sys.stderr.write('''
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
