import time
from subprocess import call

import boto3


def stop_kafka(ip: str, user: str, odd: str):
    piu = ["piu", ip, "-O", odd, "\"detaching ebs, terminate and launch instance\""]
    call(piu)
    ssh = ["ssh", "-tA", user+'@'+odd, "ssh", "-o", "StrictHostKeyChecking=no", user+'@'+ip, "'docker stop taupageapp'"]
    call(ssh)


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


def start(cluster_config: dict, ip: str, user:str, odd: str):
    """
    Stops Kafka gracefully, detaches EBS volume and terminates the instance
    """
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['regions'][0])

    stop_kafka(ip, user, odd)
    instances = ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    instance = next(i for i in instances if i.private_ip_address == ip)
    delete_alarm(cluster_config, instance)
    terminate_instance(instance)
