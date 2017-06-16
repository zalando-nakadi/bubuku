#!/usr/bin/env python3

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


@cli.command('upgrade', help='Stop Kafka gracefully, detach EBS volume, terminate the instance and launch a new one')
@click.option('--image-version', help='Docker image version to use. By default the version from config is used. '
                                      'If provided then overrides image version from config')
@click.option('--cluster-config', default='bubuku-1.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def upgrade(image_version: str, cluster_config: str, ip: str, user: str, odd: str):
    UpgradeCommand(cluster_config, image_version, ip, user, odd).run()


@cli.command('attach', help='Launch instance and attach it to the existing EBS volume')
@click.option('--volume-id', required=True)
@click.option('--cluster-config', default='bubuku-1.json')
def attach(volume_id: str, cluster_config: str):
    AttachCommand(cluster_config, volume_id).run()


@cli.command('create', help='Launch instance with EBS attached')
@click.option('--cluster-size', type=int)
@click.option('--availability-zone', default=None)
@click.option('--image-version', help='Docker image version to use. By default the version from config is used. '
                                      'If provided then verrides image version from config')
@click.option('--cluster-config', default='bubuku-1.json')
def create(cluster_size: int, availability_zone: str, image_version: str, cluster_config: str):
    CreateCommand(cluster_config, cluster_size, availability_zone, image_version).run()


@cli.command('terminate', help='Terminate instance')
@click.option('--cluster-config', default='bubuku-1.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def terminate(cluster_config: str, ip: str, user: str, odd: str):
    TerminateCommand(cluster_config, ip, user, odd).run()


@cli.command('get', help='Cluster nodes overview: ip, instance id and volume id')
@click.option('--cluster-config', default='bubuku-1.json')
def get(cluster_config: str):
    GetCommand(cluster_config).run()


if __name__ == '__main__':
    logging.basicConfig(level=getattr(logging, 'INFO', None))
    cli()
