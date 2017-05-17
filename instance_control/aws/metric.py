import boto3


def create_auto_recovery_alarm(cluster_config: dict, instance_id):
    cw = boto3.client('cloudwatch', region_name=cluster_config['region'])
    alarm_actions = ['arn:aws:automate:{}:ec2:recover'.format(cluster_config['region'])]

    cw.put_metric_alarm(AlarmName='{}-{}-auto-recover'.format(cluster_config['cluster_name'], instance_id),
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
