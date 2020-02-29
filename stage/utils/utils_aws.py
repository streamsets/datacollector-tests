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

from botocore.exceptions import ClientError

def allow_public_access(client, s3_bucket, allow_list, allow_write):
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
    if bucket_policy:
        client.put_bucket_policy(Bucket=s3_bucket, Policy=bucket_policy['Policy'])
    else:
        client.delete_bucket_policy(Bucket=s3_bucket)

    if public_access_block:
        client.put_public_access_block(
            Bucket=s3_bucket,
            PublicAccessBlockConfiguration=public_access_block['PublicAccessBlockConfiguration']
        )