import time
from subprocess import call

import boto3
from instance_control import create_instance


def stop_kafka(ip: str, user: str, odd: str):
    piu = ["piu", ip, "-O", odd, "\"detaching ebs, terminate and launch instance\""]
    call(piu)
    ssh = ["ssh", "-tA", user+'@'+odd, "ssh", "-o", "StrictHostKeyChecking=no", user+'@'+ip, "'docker stop taupageapp'"]
    call(ssh)


def get_instance(ec2_resource, ip: str):
    # find instance id by ip
    instances = ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    return next(i for i in instances if i.private_ip_address == ip)


def detach_volume(ec2_client, ec2_resource, instance):
    # find attached ebs volume
    volumes = ec2_client.describe_instance_attribute(InstanceId=instance.instance_id, Attribute='blockDeviceMapping')
    data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
    data_volume_id = data_volume['Ebs']['VolumeId']

    volume = ec2_resource.Volume(data_volume_id)
    volume.create_tags(Tags=[{'Key': 'Name', 'Value': 'kafka-logs-ebs'}])

    # find detach ebs volume
    ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)
    return volume


def terminate_instance(instance):
    instance.terminate()

    while True:
        instance.load()
        if instance.state['Name'] == 'terminated':
            return
        print("Instance is {}. Wait 10 secs more ...".format(instance.state['Name']))
        time.sleep(10)


def delete_alarm(cluster_config: dict, instance):
    cw_client = boto3.client('cloudwatch')
    cw_client.delete_alarms(AlarmNames=['{}-{}-auto-recover'.format(cluster_config['cluster_name'], instance.instance_id)])


def lunch_instance(cluster_config: dict):
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


def start(cluster_config: dict, ip: str, user:str, odd: str):
    """
    Connects to instance, gracefully terminates Kafka, detaches EBS volume and launch instance
    """
    ec2_client = boto3.client('ec2', region_name=cluster_config['regions'][0])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])

    stop_kafka(ip, user, odd)
    instance = get_instance(ec2_resource, ip)
    volume = detach_volume(ec2_client, ec2_resource, instance)
    delete_alarm(cluster_config, instance)
    terminate_instance(instance)
    cluster_config['availability_zone'] = volume.availability_zone
    lunch_instance(cluster_config)

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


if __name__ == '__main__':
    start()