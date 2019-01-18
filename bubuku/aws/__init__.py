import logging

import boto3
from botocore.config import Config


class AWSResources(object):
    def __init__(self, region, retries=100):
        boto3.set_stream_logger('boto3', logging.INFO)
        self.session = boto3.Session()
        self.region = region
        self.retries = retries
        self._ec2_client = None
        self._ec2_resource = None
        self._cloudwatch_client = None
        self._iam_client = None

    @property
    def ec2_client(self):
        if not self._ec2_client:
            self._ec2_client = self.session.client(
                'ec2',
                region_name=self.region,
                config=Config(retries={'max_attempts': self.retries}))
        return self._ec2_client

    @property
    def ec2_resource(self):
        if not self._ec2_resource:
            self._ec2_resource = self.session.resource(
                'ec2',
                region_name=self.region,
                config=Config(retries={'max_attempts': self.retries}))
        return self._ec2_resource

    @property
    def cloudwatch_client(self):
        if not self._cloudwatch_client:
            self._cloudwatch_client = self.session.client(
                'cloudwatch',
                region_name=self.region,
                config=Config(retries={'max_attempts': self.retries}))
        return self._cloudwatch_client

    @property
    def iam_client(self):
        if not self._iam_client:
            self._iam_client = self.session.client('iam', config=Config(retries={'max_attempts': self.retries}))
        return self._iam_client
