import json
import logging
import os
import subprocess
import tempfile

from time import sleep

from instance_control.aws import AWSResources
from instance_control.config import read_cluster_config

COPY_VOLUME_DEVICE = '/dev/xvdh'

_LOG = logging.getLogger('resize')


class ReplaceVolumeCommand(object):
    def __init__(self, data, aws: AWSResources):
        self._data = data
        self._aws = aws

    def run(self):
        run_again = True
        while run_again:
            run_again = False
            for state in self._data['instances']:
                if self._process_instance(state):
                    run_again = True
            if run_again:
                sleep(1)

    def _create_volume(self, ip: str):
        instance = self._get_instance(ip)
        _LOG.info('Creating EBS volume of size %d in az %s', self._data['volume_size'],
                  instance.placement['AvailabilityZone'])
        vol = self._aws.ec2_client.create_volume(
            AvailabilityZone=instance.placement['AvailabilityZone'],
            VolumeType=self._data['volume_type'],
            Size=self._data['volume_size'],
            Encrypted=False,
        )
        _LOG.info('%s is successfully created', vol['VolumeId'])
        return vol['VolumeId']

    def _attach_volume(self, ip, volume: str):
        vol = self._aws.ec2_resource.Volume(volume)

        if vol.state == 'in-use':
            # Volume was attached already
            raise Exception('Didnt expect volume to be already in use')

        self._aws.ec2_client.attach_volume(
            Device=COPY_VOLUME_DEVICE,
            InstanceId=self._get_instance(ip).instance_id,
            VolumeId=volume
        )

    def _mount_volume(self, ip: str):
        MOUNT_SCRIPT = """#!/bin/bash
if [[ "x$1" == "x" ]]; then exit 1; fi
function detect_volume() {
  SFILE=`file -s $1`
  if [[ $SFILE == *"symbolic link"* ]]; then 
    detect_volume "/dev/`echo "${SFILE##* }"`" 
  else
    echo ${1}; 
  fi
}
VOL="$(detect_volume $1)"
SFILE=`file -s $VOL`
if [[ $SFILE == *filesystem* ]]; then 
  echo "Filesystem exists"
else 
  mkfs -q -t ext4 $VOL; 
fi
mkdir -p /mounts/data-clone
chown application:application /mounts/data-clone
chmod 777 /mounts/data-clone
mount | grep "$VOL " || mount $VOL /mounts/data-clone
"""
        f = tempfile.NamedTemporaryFile('w+t', delete=False)
        try:
            f.write(MOUNT_SCRIPT)
            f.close()
            self._get_access(ip, 'Mount new volume to machine')
            self._copy_file(f.name, 'mount.sh', ip)
            self._execute_on_machine(ip, 'chmod +x mount.sh')
            self._execute_on_machine(ip, 'sudo ./mount.sh {}'.format(COPY_VOLUME_DEVICE))
        finally:
            os.unlink(f.name)

    def _restart_copy(self, ip):
        COPY_SCRIPT = """#!/bin/bash
arg=$1 
if [ -f rsync.pid ]; then
  read PID <rsync.pid

  if [ "x$arg" == "xstop" ]; then 
    kill -9 $PID
    rm rsync.pid
    echo "terminated"
    exit
  fi

  if [ -n "$PID" -a -e /proc/$PID ]; then
    echo "running"
  else
    echo "done"
  fi
else
  if [ "x$arg" == "xstop" ]; then 
    echo "terminated"
    exit
  fi
  cp -rpv /mounts/data/kafka-logs /mounts/data-clone/ >> rsync.log &
  echo $! >rsync.pid
  echo "started"
fi
"""
        f = tempfile.NamedTemporaryFile('w+t', delete=False)
        try:
            f.write(COPY_SCRIPT)
            f.close()
            self._get_access(ip, 'Copy script to migrate data')
            self._copy_file(f.name, 'copy_data.sh', ip)
            self._execute_on_machine(ip, 'chmod +x copy_data.sh')
            self._execute_on_machine(ip, 'sudo ./copy_data.sh stop')
            state = self._execute_on_machine(ip, 'sudo ./copy_data.sh').decode('utf-8').strip()
            _LOG.info("Started copy process %s", state)
        finally:
            os.unlink(f.name)

    def _wait_for_copy(self, ip):
        self._get_access(ip, 'Check migration process of kafka data on instance %s' % ip)
        return self._execute_on_machine(ip, 'sudo ./copy_data.sh').decode('utf-8').strip()

    def _get_access(self, ip, reason):
        if reason:
            subprocess.check_call(
                ["piu", "request-access", '{}@{}'.format(self._data['user'], ip), "--region", self._data['region'],
                 "-O", self._data['odd'], reason])

    def _run_command(self, command):
        _LOG.info("Running %s", ' '.join(command))
        return subprocess.check_output(command)

    def _copy_file(self, name, target_name, ip):
        self._run_command(['scp', name, '{}@{}:{}'.format(self._data['user'], self._data['odd'], target_name)])
        self._run_command(
            ['ssh', '-tA', '{}@{}'.format(self._data['user'], self._data['odd']),
             'scp -o StrictHostKeyChecking=no {} {}@{}:'.format(target_name, self._data['user'], ip)])
        self._run_command(
            ['ssh', '-tA', '{}@{}'.format(self._data['user'], self._data['odd']), 'rm {} '.format(target_name)])

    def _execute_on_machine(self, ip, command):
        return self._run_command([
            'ssh', '-tA', '{}@{}'.format(self._data['user'], self._data['odd']),
            'ssh -o StrictHostKeyChecking=no {} {}'.format(ip, command)])

    def _get_instance(self, ip: str):
        instances = list(self._aws.ec2_resource.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
            {'Name': 'network-interface.addresses.private-ip-address', 'Values': [ip]}]))

        if not instances:
            raise Exception('Instance with ip %s is not found' % ip)
        instance = instances[0]
        instance.load()
        return instance

    def _process_instance(self, state):
        # Method returns true in case if it should be called again to check something
        # 1. Create volume
        # 2. Attach volume
        # 3. Mount volume
        # 5. Start copying data
        # 6. Stop kafka
        # 7. Sync data once more
        # 8. Terminate instance
        # 9. Start instance with new volume and new configuration
        _LOG.info("Processing instance %s in state %s", state['ip'], state['state'])
        if state['state'] == 'init':
            state['volume'] = self._create_volume(state['ip'])
            state['state'] = 'attach'
            return True
        if state['state'] == 'attach':
            self._attach_volume(state['ip'], state['volume'])
            state['state'] = 'mount'
            return True
        if state['state'] == 'mount':
            self._mount_volume(state['ip'])
            state['state'] = 'copy1'
            return True
        if state['state'] == 'copy1':
            self._restart_copy(state['ip'])
            state['state'] = 'waitcopy1'
            return True
        if state['state'] == 'waitcopy1':
            result = self._wait_for_copy(state['ip'])
            if result == 'done':
                state['state'] = 'stopkafka'
                return True
            else:
                _LOG.info("Copy is in state '%s', waiting", result)
                return False
        return False


def replace_volumes_command(cluster_config: str, ip: str, user: str, odd: str, size: int, progress: str):
    if cluster_config:
        cluster_config = read_cluster_config(cluster_config)
    try:
        with open(progress, 'r') as f:
            progress_data = json.loads(f.read())
        aws_ = AWSResources(region=progress_data['region'])
    except IOError:
        if not odd or not user or not size:
            raise Exception('Odd, user and size should be set!')
        if cluster_config and ip:
            raise Exception('Only cluster config or ip should be set. Together they make no sense')
        if not cluster_config and not ip:
            raise Exception('Either cluster config or ip should be set')
        progress_data = {
            'odd': odd,
            'user': user,
            'volume_size': size,
            'volume_type': cluster_config['volume_type'] if cluster_config else 'gp2',
            'region': cluster_config['region'] if cluster_config else 'eu-central-1'
        }
        aws_ = AWSResources(region=progress_data['region'])
        if ip:
            progress_data['instances'] = [
                {
                    'ip': ip,
                    'state': 'init'
                }
            ]
        else:
            instances = list(aws_.ec2_resource.instances.filter(Filters=[
                {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
                {'Name': 'tag:Name', 'Values': [cluster_config['cluster_name']]}]))

            if len(instances) == 0:
                _LOG.info('Cluster has no running instances')
                return
            progress_data['instances'] = []
            for item in instances:
                progress_data['instances'].append({
                    'ip': item.private_ip_address,
                    'state': 'init'
                })
    try:
        return ReplaceVolumeCommand(progress_data, aws_).run()
    finally:
        new_progress = '{}.tmp'.format(progress)
        with open(new_progress, 'w') as f:
            f.write(json.dumps(progress_data, indent=2))
        os.replace(new_progress, progress)
