import json
import logging
import time

from botocore.exceptions import ClientError

from bubuku.aws import AWSResources
from bubuku.aws.cluster_config import ClusterConfig

_LOG = logging.getLogger('bubuku.aws.iam')


def create_or_get_instance_profile(aws_: AWSResources, cluster_config: ClusterConfig):
    profile_name = 'profile-{}'.format(cluster_config.get_cluster_name())
    role_name = 'role-{}'.format(cluster_config.get_cluster_name())
    policy_datavolume = 'policy-{}-datavolume'.format(cluster_config.get_cluster_name())
    policy_metadata = 'policy-{}-metadata'.format(cluster_config.get_cluster_name())
    policy_zmon = 'policy-{}-zmon'.format(cluster_config.get_cluster_name())

    try:
        profile = aws_.iam_client.get_instance_profile(InstanceProfileName=profile_name)
        _LOG.info("IAM profile %s exists, using it", profile_name)
        return profile['InstanceProfile']
    except ClientError:
        _LOG.info("IAM profile %s does not exists, creating ...", profile_name)
        pass

    profile = aws_.iam_client.create_instance_profile(InstanceProfileName=profile_name)

    _LOG.info("Creating iam role %s", role_name)
    aws_.iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument="""{
        "Version": "2012-10-17",
        "Statement": [{
             "Action": "sts:AssumeRole",
             "Effect": "Allow",
             "Principal": {
                 "Service": "ec2.amazonaws.com"
             }
        }]
    }""")

    policy_datavolume_document = """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "ec2:DescribeTags",
                    "ec2:DeleteTags",
                    "ec2:DescribeVolumes",
                    "ec2:AttachVolume"
                ],
                "Resource": "*"
            }
        ]
    }"""
    policy_metadata_document = """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "ec2:Describe*",
                "Resource": "*",
                "Effect": "Allow"
            },
            {
                "Action": "elasticloadbalancing:Describe*",
                "Resource": "*",
                "Effect": "Allow"
            }
        ]
    }"""
    policy_zmon_document = """{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "cloudwatch:PutMetricData",
                "Resource": "*",
                "Effect": "Allow"
            }
        ]
    }"""

    _LOG.info("Creating IAM policy %s", policy_datavolume)
    aws_.iam_client.put_role_policy(RoleName=role_name,
                                    PolicyName=policy_datavolume,
                                    PolicyDocument=policy_datavolume_document)
    _LOG.info("Creating IAM policy %s", policy_metadata)
    aws_.iam_client.put_role_policy(RoleName=role_name,
                                    PolicyName=policy_metadata,
                                    PolicyDocument=policy_metadata_document)
    _LOG.info("Creating IAM policy %s", policy_zmon)
    aws_.iam_client.put_role_policy(RoleName=role_name,
                                    PolicyName=policy_zmon,
                                    PolicyDocument=policy_zmon_document)

    if cluster_config.get_kms_key_id():
        policy_kms_document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "kms:Decrypt",
                    "Effect": "Allow",
                    "Resource": [cluster_config.get_kms_key_id()]
                }
            ]
        })
        _LOG.info("Creating IAM policy $s", policy_kms_document)
        aws_.iam_client.put_role_policy(RoleName=role_name,
                                        PolicyName='policy-{}-kms'.format(cluster_config.get_cluster_name()),
                                        PolicyDocument=policy_kms_document)

    aws_.iam_client.add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)

    _LOG.info("IAM profile %s is created", profile_name)

    #
    # FIXME: using an instance profile right after creating one
    # can result in 'not found' error, because of eventual
    # consistency.  For now fix with a sleep, should rather
    # examine exception and retry after some delay.
    #
    _LOG.info('Waiting 30 secs after IAM profile creation to be sure it is available')
    time.sleep(30)

    return profile['InstanceProfile']
