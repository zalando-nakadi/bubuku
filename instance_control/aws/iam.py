import boto3
import logging
from botocore.exceptions import ClientError

_LOG = logging.getLogger('bubuku.cluster.aws.taupage')


def create_instance_profile(cluster_config: dict):
    profile_name = 'profile-{}'.format(cluster_config['cluster_name'])
    role_name = 'role-{}'.format(cluster_config['cluster_name'])
    policy_datavolume = 'policy-{}-datavolume'.format(cluster_config['cluster_name'])
    policy_metadata = 'policy-{}-metadata'.format(cluster_config['cluster_name'])
    policy_zmon = 'policy-{}-zmon'.format(cluster_config['cluster_name'])

    iam = boto3.client('iam')

    try:
        profile = iam.get_instance_profile(InstanceProfileName=profile_name)
        return profile['InstanceProfile']
    except ClientError:
        _LOG.info("Instance profile does not exists, creating ...")
        pass

    profile = iam.create_instance_profile(InstanceProfileName=profile_name)

    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument="""{
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
    iam.put_role_policy(RoleName=role_name,
                        PolicyName=policy_datavolume,
                        PolicyDocument=policy_datavolume_document)
    iam.put_role_policy(RoleName=role_name,
                        PolicyName=policy_metadata,
                        PolicyDocument=policy_metadata_document)
    iam.put_role_policy(RoleName=role_name,
                        PolicyName=policy_zmon,
                        PolicyDocument=policy_zmon_document)

    iam.add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)

    return profile['InstanceProfile']
