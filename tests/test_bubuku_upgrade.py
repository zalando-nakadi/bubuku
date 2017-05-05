import boto3
from subprocess import call
import test_create_bubuku_node
import time

ip = "10.246.2.11"
user = "adyachkov@"
odd = "odd-transfereu-central-1.aruha-test.zalan.do"
stack_name = "bubuku-staging-esb-mount-13"

# TODO
# Start only instance
# Terminate only instance
# Clean up after replacing instance
# Clean up termination of the instance
# Check Kafka works after detach


def login():
    zaws = ["zaws", "login", "aruha-test"]
    call(zaws)
    # try to stop Kafka gracefully
    piu = ["piu", ip, "-O", "odd-transfereu-central-1.aruha-test.zalan.do", "\"detaching ebs, terminate and launch instance\""]
    call(piu)

    ssh = ["ssh", "-tA", user+odd, "ssh", "-o", "StrictHostKeyChecking=no", user + ip, "'docker stop taupageapp'"]
    call(ssh)


def get_instance(ec2_resource):
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


def terminate_instance(ec2_client, ec2_resource, instance):
    ec2_resource.instances.filter(InstanceIds=[instance.instance_id]).terminate()

    instance_status = ec2_client.describe_instance_status(InstanceIds=[instance.instance_id])
    if len(instance_status['InstanceStatuses']) == 1:
        while instance_status['InstanceStatuses'][0]['InstanceState']['Name'] != 'terminated':
            print("Instance is terminating. Wait 10 secs more ...")
            time.sleep(10)


def lunch_instance():
    test_create_bubuku_node.run(
        regions=['eu-central-1'],
        vpc_id='vpc-0d7c4564',
        odd_sg_id='sg-8c47ebe4',
        availability_zone='eu-central-1a',
        create_ebs=False,
        cluster_name=stack_name,
        cluster_size=1,
        instance_type='m4.large',
        volume_type='st1',
        volume_size=500,
        scalyr_key='0xRvOECatMkiGFqpSV2YFQMuDO6Hi6aRDPUQ7gUglXFY-',
        image_version= '1.2.2.2-kafka-10-jdk',
        environment=['ZOOKEEPER_STACK_NAME=exhibitor-staging',
                     'HEALTH_PORT=8888',
                     'BUBUKU_MODE=amazon',
                     'zookeeper_prefix=/stagingebsmount',
                     'buku_features=restart_on_exhibitor,graceful_terminate,use_ip_address',
                     'jmx_port=8004',
                     'kafka_heap_opts=-Xmx6G -Xms6G',
                     'startup_timeout_type=linear',
                     'startup_timeout_initial=300',
                     'startup_timeout_step=60']);


def start():
    ec2_client = boto3.client('ec2', region_name='eu-central-1')
    ec2_resource = boto3.resource('ec2', region_name='eu-central-1')

    login()
    instance = get_instance(ec2_resource)
    volume = detach_volume(ec2_client, ec2_resource, instance)
    terminate_instance(ec2_client, ec2_resource, instance)
    lunch_instance()
    volume.create_tags(Tags=[{'Key': 'Name', 'Value': ''}])
    # ebs volume will be attached by Taupage


if __name__ == '__main__':
    start()