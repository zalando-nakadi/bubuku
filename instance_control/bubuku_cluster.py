import logging

import click

from instance_control.command import upgrade, get, terminate, create, attach


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
    upgrade.run(cluster_name, image_version, cluster_config, ip, user, odd)


@cli.command('attach', help='Launch instance and attach it to the existing EBS volume')
@click.option('--cluster-name', required=True)
@click.option('--volume-id', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def attach(cluster_name: str, volume_id: str, cluster_config: str):
    attach.run(cluster_name, volume_id, cluster_config)


@cli.command('create', help='Launch instance with EBS attached')
@click.option('--cluster-name', required=True)
@click.option('--cluster-size', type=int)
@click.option('--availability-zone', default=None)
@click.option('--image-version', help='Docker image version to use. By default the version from config is used. '
                                      'If provided then verrides image version from config')
@click.option('--cluster-config', default='clusters_config.json')
def create(cluster_name: str, cluster_size: int, availability_zone: str, image_version: str, cluster_config: str):
    create.run(cluster_name, cluster_size, availability_zone, image_version, cluster_config)


@cli.command('terminate', help='Terminate instance')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def terminate(cluster_name: str, cluster_config: str, ip: str, user: str, odd: str):
    terminate.run(cluster_name, cluster_config, ip, user, odd)


@cli.command('get', help='Cluster nodes overview: ip, instance id and volume id')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def get(cluster_name: str, cluster_config: str):
    get.run(cluster_name, cluster_config)


if __name__ == '__main__':
    logging.basicConfig(level=getattr(logging, 'INFO', None))
    cli()
