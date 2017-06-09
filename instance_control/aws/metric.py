import logging

import boto3

_LOG = logging.getLogger('bubuku.cluster.aws.metric')


def create_auto_recovery_alarm(cluster_config: dict, instance_id):
    _LOG.info('Creating AWS auto recovery alarm for %s', instance_id)
    cw = boto3.client('cloudwatch', region_name=cluster_config['region'])
    alarm_actions = ['arn:aws:automate:{}:ec2:recover'.format(cluster_config['region'])]

    alarm_name = '{}-{}-auto-recover'.format(cluster_config['cluster_name'], instance_id)
    cw.put_metric_alarm(AlarmName=alarm_name,
                        AlarmActions=alarm_actions,
                        MetricName='StatusCheckFailed_System',
                        Namespace='AWS/EC2',
                        Statistic='Minimum',
                        Dimensions=[{
                            'Name': 'InstanceId',
                            'Value': instance_id
                        }],
                        Period=60,  # 1 minute
                        EvaluationPeriods=2,
                        Threshold=0,
                        ComparisonOperator='GreaterThanThreshold')
    _LOG.info('Created alarm %s', alarm_name)
