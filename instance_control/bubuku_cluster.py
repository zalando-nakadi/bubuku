import json
import time
from subprocess import call

import boto3
import click
from instance_control import create_instance


@click.group()
def cli():
    pass


def stop_kafka(ip: str, user: str, odd: str):
    piu = ["piu", ip, "-O", odd, "\"detaching ebs, terminate and launch instance\""]
    call(piu)
    ssh = ["ssh", "-tA", user + '@' + odd, "ssh", "-o", "StrictHostKeyChecking=no", user + '@' + ip,
           "'docker stop taupageapp'"]
    call(ssh)


def terminate_instance(instance):
    instance.terminate()

    print("Instance is {}. Waiting ...".format(instance.state['Name']))
    while True:
        instance.load()
        if instance.state['Name'] == 'terminated':
            print("Instance is terminated")
            return
        time.sleep(10)


def delete_alarm(cluster_config: dict, instance):
    cw_client = boto3.client('cloudwatch')
    cw_client.delete_alarms(
        AlarmNames=['{}-{}-auto-recover'.format(cluster_config['cluster_name'], instance.instance_id)])


def wait_volumes_attached(ec2_client, ec2_resource):
    response = ec2_client.describe_volumes(Filters=[{'Name': 'tag:Name', 'Values': ['kafka-logs-ebs']}])
    volumes = [ec2_resource.Volume(v['VolumeId']) for v in response['Volumes']]
    print('Wait for {} volume(s) to be attached'.format(len(volumes)))
    while True:
        volumes = [v for v in volumes if clear_tag(v)]
        if len(volumes) == 0:
            print('Volume is attached, tag:Name is cleaned')
            return
        else:
            time.sleep(10)


def clear_tag(v):
    v.load()
    if v.state == 'in-use':
        v.create_tags(Tags=[{'Key': 'Name', 'Value': ''}])
        return False
    return True


@cli.command('upgrade', help='Stop Kafka gracefully, detach esb volume, terminate the instance and launch a new one')
@click.option('--cluster-name', required=True)
@click.option('--image-version')
@click.option('--cluster-config', default='clusters_config.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def upgrade(cluster_name: str, image_version: str, cluster_config: str,
            ip: str, user: str, odd: str):
    cluster_config = prepare(cluster_name, cluster_config)
    if image_version:
        cluster_config['image_version'] = image_version
    ec2_client = boto3.client('ec2', region_name=cluster_config['regions'][0])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])

    stop_kafka(ip, user, odd)
    instances = ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    instance = next(i for i in instances if i.private_ip_address == ip)
    # detach ebs
    volumes = ec2_client.describe_instance_attribute(InstanceId=instance.instance_id, Attribute='blockDeviceMapping')
    data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
    data_volume_id = data_volume['Ebs']['VolumeId']
    volume = ec2_resource.Volume(data_volume_id)
    volume.create_tags(Tags=[{'Key': 'Name', 'Value': 'kafka-logs-ebs'}])
    ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)

    delete_alarm(cluster_config, instance)
    terminate_instance(instance)
    cluster_config['availability_zone'] = volume.availability_zone
    cluster_config['create_ebs'] = False

    create_instance.create_instance_with(cluster_config)
    wait_volumes_attached(ec2_client, ec2_resource)


@cli.command('attach', help='Launch instance and attach it to the existing EBS volume')
@click.option('--cluster-name', required=True)
@click.option('--volume-id', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def attach(cluster_name: str, volume_id: str, cluster_config: str):
    cluster_config = prepare(cluster_name, cluster_config)
    ec2_client = boto3.client('ec2', region_name=cluster_config['regions'][0])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])
    volume = ec2_resource.Volume(volume_id)
    volume.create_tags(Tags=[{'Key': 'Name', 'Value': 'kafka-logs-ebs'}])

    cluster_config['create_ebs'] = False
    cluster_config['availability_zone'] = volume.availability_zone
    print('Launching instance in availability zone: ', cluster_config['availability_zone'])

    create_instance.create_instance_with(cluster_config)
    wait_volumes_attached(ec2_client, ec2_resource)


@cli.command('create', help='Launch instance with EBS attached')
@click.option('--cluster-name', required=True)
@click.option('--cluster-size', type=int)
@click.option('--availability-zone', default=None)
@click.option('--image-version')
@click.option('--cluster-config', default='clusters_config.json')
def create(cluster_name: str, cluster_size: int, availability_zone: str, image_version: str, cluster_config: str):
    cluster_config = prepare(cluster_name, cluster_config)
    if cluster_size:
        cluster_config['cluster_size'] = cluster_size
    if image_version:
        cluster_config['image_version'] = image_version
    cluster_config['create_ebs'] = True
    cluster_config['availability_zone'] = availability_zone

    create_instance.create_instance_with(cluster_config)
    ec2_client = boto3.client('ec2', region_name=cluster_config['regions'][0])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])
    wait_volumes_attached(ec2_client, ec2_resource)


@cli.command('terminate', help='Terminate instance')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def terminate(cluster_name: str, cluster_config: str, ip: str, user: str, odd: str):
    cluster_config = prepare(cluster_name, cluster_config)
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])

    stop_kafka(ip, user, odd)
    instances = ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    instance = next(i for i in instances if i.private_ip_address == ip)
    delete_alarm(cluster_config, instance)
    terminate_instance(instance)


@cli.command('get', help='Cluster nodes overview: ip, instance id and volume id')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def get(cluster_name: str, cluster_config: str):
    cluster_config = prepare(cluster_name, cluster_config)
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])
    instances = list(ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                                                            {'Name': 'tag:Name',
                                                             'Values': [cluster_config['cluster_name']]}]))
    max_ip = 0
    max_id = 0
    max_ebs = 0
    for instance in instances:
        max_ip = max_ip if max_ip > len(instance.private_ip_address) else len(instance.private_ip_address)
        max_id = max_id if max_id > len(instance.instance_id) else len(instance.instance_id)
        max_ebs = max_ebs if max_ebs > len(instance.block_device_mappings[0]['Ebs']['VolumeId']) else len(
            instance.block_device_mappings[0]['Ebs']['VolumeId'])

    if len(instances) == 0:
        print('Nothing to print')
        return

    print('ip' + ' ' * max_ip + ' id' + ' ' * max_id + ' volume' + ' ' * max_ebs)
    for instance in instances:
        len_ip = max_ip - len(instance.private_ip_address)
        len_id = max_id - len(instance.instance_id)
        len_ebs = max_ebs - len(instance.block_device_mappings[0]['Ebs']['VolumeId'])
        print(instance.private_ip_address + ' ' * len_ip + '   ' +
              instance.instance_id + ' ' * len_id + '   ' +
              instance.block_device_mappings[0]['Ebs']['VolumeId'] + ' ' * len_ebs)


def prepare(cluster_name: str, cluster_config_path: str):
    with open(cluster_config_path) as data_file:
        cluster_configs = json.load(data_file)
    cluster_config = next((cluster for cluster in cluster_configs if cluster['cluster_name'] == cluster_name), None)
    if cluster_config:
        call(["zaws", "login", cluster_config['account']])
        return cluster_config
    else:
        raise Exception("No cluster config found for {} in {}".format(cluster_name, cluster_config_path))


if __name__ == '__main__':
    cli()
