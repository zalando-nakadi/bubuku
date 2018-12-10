import logging

from bubuku.aws import AWSResources

_LOG = logging.getLogger('bubuku.aws.metric')


def create_auto_recovery_alarm(aws_: AWSResources, cluster_name: str, instance_id):
    _LOG.info('Creating AWS auto recovery alarm for %s', instance_id)

    alarm_actions = ['arn:aws:automate:{}:ec2:recover'.format(aws_.region)]

    alarm_name = '{}-{}-auto-recover'.format(cluster_name, instance_id)

    aws_.cloudwatch_client.put_metric_alarm(
        AlarmName=alarm_name,
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
