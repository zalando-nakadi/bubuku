import boto3
from instance_control import create_instance
import time


def start(cluster_config: dict, volume_id: str):
    ec2_client = boto3.client('ec2', region_name=cluster_config['regions'][0])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])
    volume = ec2_resource.Volume(volume_id)
    volume.create_tags(Tags=[{'Key': 'Name', 'Value': 'kafka-logs-ebs'}])

    cluster_config['availability_zone'] = volume.availability_zone
    print('Launching instance in availability zone: ', cluster_config['availability_zone'])

    create_instance.run(
        regions=cluster_config['regions'],
        vpc_id=cluster_config['vpc_id'],
        odd_sg_id=cluster_config['odd_sg_id'],
        availability_zone=cluster_config['availability_zone'],
        create_ebs=False,
        cluster_name=cluster_config['cluster_name'],
        cluster_size=cluster_config['cluster_size'],
        instance_type=cluster_config['instance_type'],
        volume_type=cluster_config['volume_type'],
        volume_size=cluster_config['volume_size'],
        scalyr_key=cluster_config['scalyr_key'],
        image_version=cluster_config['image_version'],
        environment=cluster_config['environment'])

    response = ec2_client.describe_volumes(Filters=[{'Name': 'tag:Name', 'Values': ['kafka-logs-ebs']}])
    volumes = [ec2_resource.Volume(v['VolumeId']) for v in response['Volumes']]

    while True:
        volumes = [v for v in volumes if clear_tag(v)]
        if len(volumes) == 0:
            print('Done clearing tags')
            return
        else:
            print('Wait for volumes to be attached', len(volumes))
            time.sleep(10)


def clear_tag(v):
    v.load()
    if v.state == 'in-use':
        v.create_tags(Tags=[{'Key': 'Name', 'Value': ''}])
        return False
    return True
