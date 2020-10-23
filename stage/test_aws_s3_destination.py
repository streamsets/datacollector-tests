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

from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

from .utils.utils_aws import configure_stage_for_anonymous, \
    create_anonymous_client, create_bucket

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'


@aws('s3')
def test_s3_whole_file_transfer_existing_file(sdc_builder, sdc_executor, aws):
    """Test simple scenario of moving files from source to target using WHOLE_FILE_FORMAT."""
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'
    s3_dest_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'
    data = 'Completely random string that is transferred as whole file format.'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    origin = builder.add_stage('Amazon S3', type='origin')
    origin.set_attributes(bucket=aws.s3_bucket_name, data_format='WHOLE_FILE',
                          prefix_pattern=f'{s3_key}/*',
                          max_batch_size_in_records=100)

    target = builder.add_stage('Amazon S3', type='destination')
    target.set_attributes(bucket=aws.s3_bucket_name, data_format='WHOLE_FILE', partition_prefix=s3_dest_key,
                          file_name_expression='output.txt', object_name_prefix='', file_exists='TO_ERROR')

    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> target
    origin >= finisher

    pipeline = builder.build().configure_for_environment(aws)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # We create both input as well as output file
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}input.txt', Body=data.encode('ascii'))
        logger.info(f"Pre creating output file {s3_dest_key}output.txt")
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_dest_key}output.txt', Body=data.encode('ascii'))

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.AmazonS3_02.inputRecords.counter').count == 1
        assert history.latest.metrics.counter('stage.AmazonS3_02.outputRecords.counter').count == 0
        assert history.latest.metrics.counter('stage.AmazonS3_02.errorRecords.counter').count == 1

    finally:
        logger.info('Deleting input S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)

        logger.info('Deleting output S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_dest_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_dest_key)


@aws('s3')
@sdc_min_version('3.11.0')
def test_s3_error_destination(sdc_builder, sdc_executor, aws):
    """Test sending records to S3 error destination."""
    _run_test_s3_error_destination(sdc_builder, sdc_executor, aws, False)


@aws('s3')
@sdc_min_version('3.16.0')
def test_s3_error_destination_anonymous(sdc_builder, sdc_executor, aws):
    """Test sending records to S3 error destination with anonymous credentials."""
    _run_test_s3_error_destination(sdc_builder, sdc_executor, aws, True)


def _run_test_s3_error_destination(sdc_builder, sdc_executor, aws, anonymous):
    if anonymous:
        s3_bucket = create_bucket(aws)
        logger.info(f'Bucket {s3_bucket} created')
    else:
        s3_bucket = aws.s3_bucket_name

    s3_key = f'{S3_SANDBOX_PREFIX}/errDest-{get_random_string()}/'
    random_string = get_random_string(string.ascii_letters, 10)
    random_raw_json_str = '{{"text":"{0}"}}'.format(random_string)
    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    s3_err = builder.add_error_stage('Write to Amazon S3')
    s3_err.set_attributes(
        bucket=s3_bucket,
        common_prefix=s3_key
    )
    if anonymous:
        configure_stage_for_anonymous(s3_err)

    origin = builder.add_stage('Dev Raw Data Source', type='origin')
    origin.set_attributes(
        data_format='JSON',
        raw_data=random_raw_json_str,
        stop_after_first_batch=True
    )

    target = builder.add_stage('To Error', type='destination')

    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> target
    origin >= finisher

    pipeline = builder.build().configure_for_environment(aws)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # We should have exactly one file in the bucket
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert 'Contents' in list_s3_objs  # If no object was found, there is no 'Contents' key
        assert len(list_s3_objs['Contents']) == 1

        # Now we build and run another pipeline with an S3 Origin to read the data back
        builder = sdc_builder.get_pipeline_builder()
        s3_origin = builder.add_stage('Amazon S3', type='origin')
        s3_origin.set_attributes(
            bucket=s3_bucket,
            data_format='SDC_JSON',
            prefix_pattern=f'{s3_key}*',
            max_batch_size_in_records=100
        )
        if anonymous:
            configure_stage_for_anonymous(s3_origin)
        trash = builder.add_stage('Trash')
        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])
        s3_origin >> trash
        s3_origin >= finisher
        pipeline = builder.build().configure_for_environment(aws)
        pipeline.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        assert len(snapshot[s3_origin].output) == 1
        assert snapshot[s3_origin].output[0].get_field_data('/text') == random_string

    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)
        if anonymous:
            logger.info(f'Deleting bucket {s3_bucket}')
            aws.s3.delete_bucket(Bucket=s3_bucket)



@aws('s3')
def test_s3_whole_file_transfer(sdc_builder, sdc_executor, aws):
    """Test simple scenario of moving files from source to target using WHOLE_FILE_FORMAT."""
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'
    s3_dest_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'
    data = 'Completely random string that is transfered as whole file format.'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    origin = builder.add_stage('Amazon S3', type='origin')
    origin.set_attributes(bucket=aws.s3_bucket_name, data_format='WHOLE_FILE',
                          prefix_pattern=f'{s3_key}/*',
                          max_batch_size_in_records=100)

    target = builder.add_stage('Amazon S3', type='destination')
    target.set_attributes(bucket=aws.s3_bucket_name, data_format='WHOLE_FILE', partition_prefix=s3_dest_key,
                          file_name_expression='output.txt')

    # TLKT-248: Add ability to directly read events from snapshots
    identity = builder.add_stage('Dev Identity')
    trash = builder.add_stage('Trash')

    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> target
    origin >= finisher
    target >= identity
    identity >> trash

    pipeline = builder.build().configure_for_environment(aws)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/input.txt', Body=data.encode('ascii'))
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, timeout_sec=70).snapshot

        # Validate event generation
        assert len(snapshot[identity].output) == 1
        assert snapshot[identity].output[0].get_field_data('/targetFileInfo/bucket') == aws.s3_bucket_name
        assert snapshot[identity].output[0].get_field_data(
            '/targetFileInfo/objectKey') == f'{s3_dest_key}sdc-output.txt'

        # We should have exactly one file on the destination side
        list_s3_objs = client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_dest_key)
        assert len(list_s3_objs['Contents']) == 1

        # With our secret message
        s3_obj_key = client.get_object(Bucket=aws.s3_bucket_name, Key=list_s3_objs['Contents'][0]['Key'])
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert s3_contents == data
    finally:
        logger.info('Deleting input S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)

        logger.info('Deleting output S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_dest_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_dest_key)


@aws('s3')
@sdc_min_version('2.6.0.1-0002')
def test_s3_destination_non_existing_bucket(sdc_builder, sdc_executor, aws):
    """Variant of S3 destination testing focusing on what happens when calculated bucket does not exists.
    """
    # setup test static
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    # Bucket name is inside the record itself
    raw_str = '{"bucket" : "guess-what-this-simply-does-not-exists-I-know-caused-I-said-so"}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str)

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket='${record:value("/bucket")}', data_format='JSON', partition_prefix=s3_key)

    dev_raw_data_source >> s3_destination

    s3_dest_pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_dest_pipeline)

    # Read snapshot of the pipeline
    snapshot = sdc_executor.capture_snapshot(s3_dest_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(s3_dest_pipeline)

    # All records should go to error stream.
    input_records = snapshot[dev_raw_data_source.instance_name].output
    stage = snapshot[s3_destination.instance_name]
    assert len(stage.error_records) == len(input_records)


@aws('s3')
def test_s3_destination(sdc_builder, sdc_executor, aws):
    """Test for S3 target stage. We do so by running a dev raw data source generator to S3 destination
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been ingested by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to determine exactly what has been ingested. The pipeline looks like:

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_destination
                                                   >> to_error
    """
    _run_test_s3_destination(sdc_builder, sdc_executor, aws, False, False)


@aws('s3')
@sdc_min_version('3.16.0')
def test_s3_destination_anonymous(sdc_builder, sdc_executor, aws):
    """Test for S3 target stage. We do so by running a dev raw data source generator to S3 destination
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been ingested by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to determine exactly what has been ingested. Uses anonymous credentials.
    The pipeline looks like:

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_destination
                                                   >> to_error """

    _run_test_s3_destination(sdc_builder, sdc_executor, aws, False, True)


@aws('s3', 'kms')
@sdc_min_version('3.9.0')
def test_s3_destination_sse_kms(sdc_builder, sdc_executor, aws):
    """Test for S3 target stage using SSE-KMS. We do so by running a dev raw data source generator to S3 destination
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been ingested by the pipeline; we also verify that the data was in fact encrypted in the server using the KMS. We
    use a record deduplicator processor in between dev raw data source origin and S3 destination in order to determine
    exactly what has been ingested. When anonymous a bucket is created to avoid concurrent access to the same
    bucket and locking problems. The pipeline looks like:

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_destination
                                                   >> to_error   """

    _run_test_s3_destination(sdc_builder, sdc_executor, aws, True, False)


def _run_test_s3_destination(sdc_builder, sdc_executor, aws, sse_kms, anonymous):
    if anonymous:
        s3_bucket = create_bucket(aws)
        logger.info(f'Bucket {s3_bucket} created')
    else:
        s3_bucket = aws.s3_bucket_name

    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    # Bucket name is inside the record itself
    raw_str = f'{{ "bucket" : "{s3_bucket}", "company" : "StreamSets Inc."}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    bucket_val = (s3_bucket if sdc_builder.version < '2.6.0.1-0002' else '${record:value("/bucket")}')
    s3_destination.set_attributes(bucket=bucket_val, data_format='JSON', partition_prefix=s3_key)
    if sse_kms:
        # Use SSE with KMS
        s3_destination.set_attributes(use_server_side_encryption=True,
                                      server_side_encryption_option='KMS',
                                      aws_kms_key_arn=aws.kms_key_arn)
    if anonymous:
        configure_stage_for_anonymous(s3_destination)

    # TLKT-248: Add ability to directly read events from snapshots
    identity = builder.add_stage('Dev Identity')
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> s3_destination
    record_deduplicator >> to_error

    s3_destination >= identity
    identity >> trash

    s3_dest_pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_dest_pipeline)

    client = aws.s3

    try:
        # start pipeline and capture pipeline messages to assert
        snapshot = sdc_executor.capture_snapshot(s3_dest_pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(s3_dest_pipeline)

        # Validate event generation
        assert len(snapshot[identity].output) == 1
        assert snapshot[identity].output[0].get_field_data('/bucket') == s3_bucket
        assert snapshot[identity].output[0].get_field_data('/recordCount') == 1

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # read data from S3 to assert it is what got ingested into the pipeline
        client_to_read = create_anonymous_client() if anonymous else client
        s3_obj_key = client_to_read.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])

        # We're comparing the logic structure (JSON) rather than byte-to-byte to allow for different ordering, ...
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert json.loads(s3_contents) == json.loads(raw_str)

        if sse_kms:
            # assert that the data was stored with SSE using the KMS
            assert s3_obj_key['ServerSideEncryption'] == 'aws:kms'
            assert s3_obj_key['SSEKMSKeyId'] == aws.kms_key_arn
    finally:
        try:
            delete_keys = {'Objects': [{'Key': k['Key']}
                                       for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
            client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)
        finally:
            if anonymous:
                logger.info(f'Deleting bucket {s3_bucket}')
                aws.s3.delete_bucket(Bucket=s3_bucket)
