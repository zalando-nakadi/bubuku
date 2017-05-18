import logging

import click

from instance_control.command.attach import AttachCommand
from instance_control.command.create import CreateCommand
from instance_control.command.get import GetCommand
from instance_control.command.terminate import TerminateCommand
from instance_control.command.upgrade import UpgradeCommand

_LOG = logging.getLogger('bubuku.cluster.cli')


@click.group()
def cli():
    pass


@cli.command('upgrade', help='Stop Kafka gracefully, detach esb volume, terminate the instance and launch a new one')
@click.option('--cluster-name', required=True)
@click.option('--image-version', help='Docker image version to use. By default the version from config is used. '
                                      'If provided then verrides image version from config')
@click.option('--cluster-config', default='clusters_config.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def upgrade(cluster_name: str, image_version: str, cluster_config: str, ip: str, user: str, odd: str):
    _LOG.info('Calling command to upgrade cluster %s', cluster_name)
    UpgradeCommand(cluster_name, cluster_config, image_version, ip, user, odd).run()


@cli.command('attach', help='Launch instance and attach it to the existing EBS volume')
@click.option('--cluster-name', required=True)
@click.option('--volume-id', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def attach(cluster_name: str, volume_id: str, cluster_config: str):
    _LOG.info('Calling command to launch instance and attach volume %s to it in cluster %s', volume_id, cluster_name)
    AttachCommand(cluster_name, cluster_config, volume_id).run()


@cli.command('create', help='Launch instance with EBS attached')
@click.option('--cluster-name', required=True)
@click.option('--cluster-size', type=int)
@click.option('--availability-zone', default=None)
@click.option('--image-version', help='Docker image version to use. By default the version from config is used. '
                                      'If provided then verrides image version from config')
@click.option('--cluster-config', default='clusters_config.json')
def create(cluster_name: str, cluster_size: int, availability_zone: str, image_version: str, cluster_config: str):
    _LOG.info('Calling command to create node in cluster %s', cluster_name)
    CreateCommand(cluster_name, cluster_config, cluster_size, availability_zone, image_version).run()


@cli.command('terminate', help='Terminate instance')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def terminate(cluster_name: str, cluster_config: str, ip: str, user: str, odd: str):
    _LOG.info('Calling command to terminate node %s in cluster %s', ip, cluster_name)
    TerminateCommand(cluster_name, cluster_config, ip, user, odd).run()


@cli.command('get', help='Cluster nodes overview: ip, instance id and volume id')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def get(cluster_name: str, cluster_config: str):
    _LOG.info('Getting cluster state %s', cluster_name)
    GetCommand(cluster_name, cluster_config).run()


if __name__ == '__main__':
    logging.basicConfig(level=getattr(logging, 'INFO', None))
    cli()
