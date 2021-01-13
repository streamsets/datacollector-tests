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

import json
import logging
import string

from streamsets.sdk.models import Configuration
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

from .utils.utils_aws import allow_public_access, restore_public_access, configure_stage_for_anonymous, \
    create_anonymous_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'

MIN_SDC_VERSION_WITH_EXECUTOR_EVENTS = Version('3.4.0')


@aws('s3')
@sdc_min_version('2.6.0.1-0002')
def test_s3_executor_create_object(sdc_builder, sdc_executor, aws):
    """Test for S3 executor stage. We do so by running a dev raw data source generator to S3 executor
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been created by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to limit number of objects to one.

    For recent SDC versions we also check that the corresponding 'file-created' event is generated.

    S3 Destination pipeline:
    dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap.destination
                           record_deduplicator >> to_error
    """
    _run_test_s3_executor_create_object(sdc_builder, sdc_executor, aws, False)


@aws('s3')
@sdc_min_version('3.16.0')
def test_s3_executor_create_object_anonymous(sdc_builder, sdc_executor, aws):
    """Test for S3 executor stage. We do so by running a dev raw data source generator to S3 executor
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been created by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to limit number of objects to one. Uses anonymous credentials.

    For recent SDC versions we also check that the corresponding 'file-created' event is generated.

    S3 Destination pipeline:
    dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap.destination
                           record_deduplicator >> to_error
    """
    _run_test_s3_executor_create_object(sdc_builder, sdc_executor, aws, True)


def _run_test_s3_executor_create_object(sdc_builder, sdc_executor, aws, anonymous):
    # Setup test static.
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'
    raw_str = f'{{"bucket": "{s3_bucket}", "company": "StreamSets Inc."}}'

    # Build the pipeline.
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
                               content='${record:value("/company")}')
    if anonymous:
        configure_stage_for_anonymous(s3_executor)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap.destination
    record_deduplicator >> to_error

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    client = aws.s3
    public_access_block = None
    bucket_policy = None
    try:
        if anonymous:
            public_access_block, bucket_policy = allow_public_access(client, s3_bucket, True, True)

        sdc_executor.start_pipeline(s3_exec_pipeline).wait_for_finished()

        # Assert record count to S3 the size of the objects put.
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # Read data from S3 to assert it is what got ingested into the pipeline.
        client_to_read = create_anonymous_client() if anonymous else client
        s3_contents = [
            client_to_read.get_object(Bucket=s3_bucket, Key=s3_content['Key'])['Body'].read().decode().strip()
            for s3_content in list_s3_objs['Contents']]

        assert s3_contents[0] == 'StreamSets Inc.'

        # Check if the 'file-created' event was generated (only for recent sdc versions).
        if Version(sdc_builder.version) >= MIN_SDC_VERSION_WITH_EXECUTOR_EVENTS:
            assert len(wiretap.output_records) == 1
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'file-created'

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, s3_exec_pipeline)
        restore_public_access(client, s3_bucket, public_access_block, bucket_policy)
        aws.delete_s3_data(s3_bucket, s3_key)


@aws('s3')
@sdc_min_version('3.4.0')
def test_s3_executor_copy_object(sdc_builder, sdc_executor, aws):
    """Test the copy action of S3 executor stage and its corresponding event. We configure the S3 executor stage
    to copy S3 objects according to the 'key_src' and 'key_dst' values provided in the record. We use the S3
    client to create an S3 object with key 'key_src' and check that the stage copies that object into the
    'key_dst' key and generate the corresponding event.

    Pipeline:
        dev_raw_data_source >> s3_executor >= wiretap.destination

    """
    object_content = get_random_string(string.ascii_letters, 10)
    key_suffix = get_random_string(string.ascii_letters, 10)
    s3_key_src = f'{S3_SANDBOX_PREFIX}/src_{key_suffix}'
    s3_key_dst = f'{S3_SANDBOX_PREFIX}/dst_{key_suffix}'
    s3_bucket = aws.s3_bucket_name
    input_data = {'bucket': s3_bucket, 'key_src': s3_key_src, 'key_dst': s3_key_dst}

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='COPY_OBJECT',
                               object='${record:value("/key_src")}',
                               new_object_path='${record:value("/key_dst")}')

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> s3_executor >= wiretap.destination

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # Create source object in S3 bucket.
        client.put_object(Body=object_content, Bucket=s3_bucket, Key=s3_key_src)

        # Start pipeline and wait for finished
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Check if there exists an object with the destination key.
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_dst)
        assert 'Contents' in list_s3_objs  # If no object was found, there is no 'Contents' key
        assert len(list_s3_objs['Contents']) == 1

        # Check if the content matches that of the input data.
        s3_content = client.get_object(Bucket=s3_bucket, Key=s3_key_dst)['Body'].read().decode().strip()
        assert s3_content == object_content

        # Check if the 'file-moved' event was generated.
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values['sdc.event.type'] == 'file-moved'

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, pipeline)
        client.delete_object(Bucket=s3_bucket, Key=s3_key_src)
        client.delete_object(Bucket=s3_bucket, Key=s3_key_dst)


@aws('s3')
@sdc_min_version('2.6.0.1-0002')
def test_s3_executor_tag_object(sdc_builder, sdc_executor, aws):
    """Test for S3 executor stage. We do so by running a dev raw data source generator to S3 destination
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been created by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to limit number of objects to one.

    For recent SDC versions we also check that the corresponding 'file-changed' event is generated.

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap.destination
                                                   >> to_error
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'
    raw_str = f'{{"bucket": "{s3_bucket}", "key": "{s3_key}"}}'

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CHANGE_EXISTING_OBJECT',
                               object='${record:value("/key")}',
                               tags=Configuration(property_key='key', company='${record:value("/company")}'))

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap.destination
    record_deduplicator >> to_error

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    client = aws.s3
    try:
        # Pre-create the object so that it exists.
        client.put_object(Body='Secret Data', Bucket=s3_bucket, Key=s3_key)

        sdc_executor.start_pipeline(s3_exec_pipeline).wait_for_finished()

        tags = client.get_object_tagging(Bucket=s3_bucket, Key=s3_key)['TagSet']
        assert len(tags) == 1

        # Check if the 'file-created' event was generated (only for recent sdc versions).
        if Version(sdc_builder.version) >= MIN_SDC_VERSION_WITH_EXECUTOR_EVENTS:
            assert len(wiretap.output_records) == 1
            assert wiretap.output_records[0].header.values['sdc.event.type'] == 'file-changed'

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, s3_exec_pipeline)
        aws.delete_s3_data(s3_bucket, s3_key)


@aws('s3')
@sdc_min_version('2.6.0.1-0002')
def test_s3_executor_non_existing_bucket(sdc_builder, sdc_executor, aws):
    """Variant of S3 executor testing focusing on what happens when calculated bucket does not exists."""
    # setup test static
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    # Bucket name is inside the record itself
    raw_str = '{"bucket" : "guess-what-this-simply-does-not-exists-I-know-caused-I-said-so"}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    record_deduplicator = builder.add_stage('Record Deduplicator')

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CREATE_NEW_OBJECT',
                               object=s3_key,
                               content='${record:value("/company")}')

    wiretap1 = builder.add_wiretap()
    wiretap2 = builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap2.destination
    record_deduplicator >> wiretap1.destination

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    sdc_executor.start_pipeline(s3_exec_pipeline).wait_for_finished()

    # All records should go to error stream.
    assert len(wiretap1.output_records) == len(wiretap2.output_records)


@aws('s3')
@sdc_min_version('2.6.0.1-0002')
def test_s3_executor_non_existing_object(sdc_builder, sdc_executor, aws):
    """Variant of S3 executor testing focusing on what happens when we try to apply tags on non existing object."""
    # setup test static
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'
    raw_str = f'{{"bucket": "{s3_bucket}", "key": "{s3_key}"}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    record_deduplicator = builder.add_stage('Record Deduplicator')

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CHANGE_EXISTING_OBJECT',
                               object='${record:value("/key")}',
                               tags=Configuration(property_key='key', company='${record:value("/company")}'))

    wiretap1 = builder.add_wiretap()
    wiretap2 = builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> s3_executor >= wiretap2.destination
    record_deduplicator >> wiretap1.destination

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    sdc_executor.start_pipeline(s3_exec_pipeline).wait_for_finished()

    # All records should go to error stream.
    assert len(wiretap1.output_records) == len(wiretap2.output_records)


def _ensure_pipeline_is_stopped(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)
