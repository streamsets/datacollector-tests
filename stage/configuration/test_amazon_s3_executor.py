# Copyright 2021 StreamSets Inc.
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
import pytest
import json
import string

from streamsets.testframework.decorators import stub
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string


S3_SANDBOX_PREFIX = 'sandbox'

@stub
def test_access_key_id(sdc_builder, sdc_executor):
    pass


@stub
def test_bucket(sdc_builder, sdc_executor):
    pass


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CREATE_NEW_OBJECT'}])
def test_content(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'delete_original_object': False, 'task': 'COPY_OBJECT'},
                                              {'delete_original_object': True, 'task': 'COPY_OBJECT'}])
def test_delete_original_object(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'region': 'OTHER'}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'COPY_OBJECT'}])
def test_new_object_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_object(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_host(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_port(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_user(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'region': 'AP_NORTHEAST_1'},
                                              {'region': 'AP_NORTHEAST_2'},
                                              {'region': 'AP_NORTHEAST_3'},
                                              {'region': 'AP_SOUTHEAST_1'},
                                              {'region': 'AP_SOUTHEAST_2'},
                                              {'region': 'AP_SOUTH_1'},
                                              {'region': 'CA_CENTRAL_1'},
                                              {'region': 'CN_NORTHWEST_1'},
                                              {'region': 'CN_NORTH_1'},
                                              {'region': 'EU_CENTRAL_1'},
                                              {'region': 'EU_WEST_1'},
                                              {'region': 'EU_WEST_2'},
                                              {'region': 'EU_WEST_3'},
                                              {'region': 'OTHER'},
                                              {'region': 'SA_EAST_1'},
                                              {'region': 'US_EAST_1'},
                                              {'region': 'US_EAST_2'},
                                              {'region': 'US_GOV_WEST_1'},
                                              {'region': 'US_WEST_1'},
                                              {'region': 'US_WEST_2'}])
def test_region(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_retry_count(sdc_builder, sdc_executor):
    pass


@stub
def test_secret_access_key(sdc_builder, sdc_executor):
    pass


@stub
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CHANGE_EXISTING_OBJECT'}])
def test_tags(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CHANGE_EXISTING_OBJECT'},
                                              {'task': 'COPY_OBJECT'},
                                              {'task': 'CREATE_NEW_OBJECT'}])
def test_task(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@aws('s3', 'kms')
@pytest.mark.parametrize('server_side_encryption_option', ['KMS', 'NONE', 'S3'])
@sdc_min_version('5.11.0')
def test_server_side_encryption_option(sdc_builder, sdc_executor, aws, server_side_encryption_option, acl="DEFAULT"):
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    # Bucket name is inside the record itself
    raw_str = f'{{ "bucket" : "{s3_bucket}", "company" : "StreamSets Inc."}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CREATE_NEW_OBJECT',
                               object=s3_key,
                               content='${record:value("/company")}',
                               set_session_tags=False,
                               server_side_encryption_option=server_side_encryption_option)

    # Configuration Server Side Encryption with KMS
    if server_side_encryption_option == 'KMS':
        # Use SSE with KMS
        s3_executor.set_attributes(aws_kms_key_arn=aws.kms_key_arn)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap.destination
    record_deduplicator >> to_error

    s3_executor_pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_executor_pipeline)

    client = aws.s3

    try:
        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_executor_pipeline).wait_for_finished()

        # Check if the pipeline works correctly.
        assert len(wiretap.output_records) == 1, 'The number of output records processed is incorrect'

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # Read data from S3 to assert it is what got ingested into the pipeline.
        s3_contents = [
            client.get_object(Bucket=s3_bucket, Key=s3_content['Key'])['Body'].read().decode().strip()
            for s3_content in list_s3_objs['Contents']]

        assert s3_contents[0] == 'StreamSets Inc.'

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_obj_key = client.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])

        if server_side_encryption_option == "KMS":
            assert s3_obj_key['ServerSideEncryption'] == 'aws:kms'
            assert s3_obj_key['SSEKMSKeyId'] == aws.kms_key_arn

        else:
            ### Amazon S3 now applies server-side encryption with Amazon S3 managed keys (SSE-S3) as the base level of
            # encryption for every bucket in Amazon S3. Starting January 5, 2023, all new object uploads to Amazon S3
            # are automatically encrypted at no additional cost and with no impact on performance. The automatic
            # encryption status for S3 bucket default encryption configuration and for new object uploads is available
            # in AWS CloudTrail logs, S3 Inventory, S3 Storage Lens, the Amazon S3 console, and as an additional
            # Amazon S3 API response header in the AWS Command Line Interface and AWS SDKs.
            assert s3_obj_key['ServerSideEncryption'] == 'AES256'

    finally:
        aws.delete_s3_data(s3_bucket, s3_key)
