import json
from subprocess import call

import click
from instance_control import test_upgrade, test_attach, test_terminate, test_create, test_get


@click.group()
def cli():
    zaws = ["zaws", "login", "aruha-test"]
    call(zaws)
    pass


@cli.command('upgrade', help='Stop Kafka gracefully, detach esb volume, terminate the instance and launch a new one')
@click.option('--cluster-name', required=True)
@click.option('--image-version')
@click.option('--cluster-config', default='clusters_config.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def upgrade(cluster_name: str, image_version: str, cluster_config: str,
            ip: str, user: str, odd: str):
    cluster_config = get_cluster_configuration(cluster_name, cluster_config)
    if image_version:
        cluster_config['image_version'] = image_version
    test_upgrade.start(cluster_config, ip=ip, user=user, odd=odd)


@cli.command('attach', help='Launch instance and attach it to the existing EBS volume')
@click.option('--cluster-name', required=True)
@click.option('--volume-id', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def attach(cluster_name: str, volume_id: str, cluster_config: str):
    cluster_config = get_cluster_configuration(cluster_name, cluster_config)
    test_attach.start(cluster_config, volume_id)


@cli.command('create', help='Launch instance with EBS attached')
@click.option('--cluster-name', required=True)
@click.option('--cluster-size', type=int)
@click.option('--availability-zone', default=None)
@click.option('--image-version')
@click.option('--cluster-config', default='clusters_config.json')
def launch(cluster_name: str, cluster_size: int, availability_zone: str, image_version: str, cluster_config: str):
    cluster_config = get_cluster_configuration(cluster_name, cluster_config)
    if cluster_size:
        cluster_config['cluster_size'] = cluster_size
    cluster_config['availability_zone'] = availability_zone
    if image_version:
        cluster_config['image_version'] = image_version
    test_create.start(cluster_config)


@cli.command('terminate', help='Terminate instance')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
@click.option('--ip', required=True)
@click.option('--user', required=True)
@click.option('--odd', required=True)
def terminate(cluster_name: str, cluster_config: str, ip: str, user: str, odd: str):
    cluster_config = get_cluster_configuration(cluster_name, cluster_config)
    test_terminate.start(cluster_config, ip=ip, user=user, odd=odd)


@cli.command('get', help='Get cluster overview')
@click.option('--cluster-name', required=True)
@click.option('--cluster-config', default='clusters_config.json')
def terminate(cluster_name: str, cluster_config: str):
    cluster_config = get_cluster_configuration(cluster_name, cluster_config)
    test_get.start(cluster_config)


def get_cluster_configuration(cluster_name: str, cluster_config_path: str):
    with open(cluster_config_path) as data_file:
        cluster_configs = json.load(data_file)
    cluster_config = next(cluster for cluster in cluster_configs if cluster['cluster_name'] == cluster_name)
    if cluster_config:
        return cluster_config
    else:
        raise Exception("No cluster config found for {} in {}".format(cluster_name, cluster_config_path))


if __name__ == '__main__':
    cli()
