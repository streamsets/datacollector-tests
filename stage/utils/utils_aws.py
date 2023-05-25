# Copyright 2020 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A module providing utils for working with AWS
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from streamsets.testframework.utils import get_random_string


def allow_public_access(client, s3_bucket, allow_list, allow_write):
    """Changes permissions on s3_bucket to publicly allow listing and/or writing.  Make sure to pair it with
    restore_public_access to restore the permissions to their original settings.
    """
    try:
        public_access_block = client.get_public_access_block(Bucket=s3_bucket)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchPublicAccessBlockConfiguration':
            public_access_block = None
        else:
            raise e

    client.delete_public_access_block(Bucket=s3_bucket)

    try:
        bucket_policy = client.get_bucket_policy(Bucket=s3_bucket)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
            bucket_policy = None
        else:
            raise e

    list_bucket = f'''{{
                "Sid":"AddPerm",
                "Effect":"Allow",
                "Principal": "*",
                "Action":["s3:ListBucket"],
                "Resource":["arn:aws:s3:::{s3_bucket}"]
            }},''' if allow_list else ''

    write_bucket = f'''{{
                "Sid":"AllObjectActions",
                "Effect":"Allow",
                "Principal": "*",
                "Action":["s3:*Object"],
                "Resource":["arn:aws:s3:::{s3_bucket}/*"]
            }},''' if allow_write else ''

    policy = f'''{{
        "Version":"2012-10-17",
        "Statement":[
            {list_bucket}
            {write_bucket}
            {{
                "Sid":"AddPerm",
                "Effect":"Allow",
                "Principal": "*",
                "Action":["s3:GetObject"],
                "Resource":["arn:aws:s3:::{s3_bucket}/*"]
            }}
        ]
    }}'''
    client.put_bucket_policy(Bucket=s3_bucket, Policy=policy)

    return public_access_block, bucket_policy


def restore_public_access(client, s3_bucket, public_access_block, bucket_policy):
    """Restores the original permissions on s3_bucket, after calling allow_public_access."""
    if bucket_policy:
        client.put_bucket_policy(Bucket=s3_bucket, Policy=bucket_policy['Policy'])
    else:
        client.delete_bucket_policy(Bucket=s3_bucket)

    if public_access_block:
        client.put_public_access_block(
            Bucket=s3_bucket,
            PublicAccessBlockConfiguration=public_access_block['PublicAccessBlockConfiguration']
        )


def configure_stage_for_anonymous(s3_stage):
    """Configures s3_stage for anonymous credentials"""
    # Blanking out the credentials isn't necessary with anonymous, but we're doing it here just in case to ensure
    # that we'll actually be doing things anonymously and not accidentally using the credentials
    s3_stage.set_attributes(authentication_method='WITH_ANONYMOUS_CREDENTIALS', access_key_id='', secret_access_key='')


def create_anonymous_client():
    """Creates an anonymous s3 client.  This is useful if you need to read an object created by an anonymous user, which
    the normal client won't have access to.
    """
    return boto3.client('s3', config=Config(signature_version=UNSIGNED))


def create_bucket(aws):
    """Creates a bucket with the same root name than  aws.s3_bucket_name"""
    s3_bucket = f'{aws.s3_bucket_name}-{get_random_string().lower()}'
    aws.s3.create_bucket(Bucket=s3_bucket, ObjectOwnership='ObjectWriter', CreateBucketConfiguration={'LocationConstraint': aws.region})
    aws.s3.put_public_access_block(Bucket=s3_bucket,
        PublicAccessBlockConfiguration={'BlockPublicAcls': False, 'IgnorePublicAcls': False,
        'BlockPublicPolicy': False,'RestrictPublicBuckets': False})

    aws.s3.put_bucket_acl(ACL='public-read-write', Bucket=s3_bucket)

    aws.s3.put_bucket_tagging(
        Bucket=s3_bucket,
        Tagging={
            'TagSet': [
                {'Key': 'stf-env', 'Value': 'nightly-tests'},
                {'Key': 'managed-by', 'Value': 'ep'},
                {'Key': 'dept', 'Value': 'eng'},
            ]
        }
    )
    allow_public_access(aws.s3, s3_bucket, True, True)
    return s3_bucket
