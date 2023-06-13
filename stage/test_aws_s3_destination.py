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
import pytest
import string

from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

from .utils.utils_aws import configure_stage_for_anonymous, \
    create_anonymous_client, create_bucket

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'
CHECKSUM_ALGORITHM_DEFAULT = 'MD5'


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
    try:
        if anonymous:
            s3_bucket = create_bucket(aws)
            logger.info(f'Bucket {s3_bucket} created')
        else:
            s3_bucket = aws.s3_bucket_name

        s3_key = f'{S3_SANDBOX_PREFIX}/errDest-{get_random_string()}/'
        random_string = get_random_string(string.ascii_letters, 10)
        random_raw_json_str = f'{{"text":"{random_string}"}}'

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

        origin >> target

        pipeline = builder.build().configure_for_environment(aws)
        pipeline.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline)

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

        wiretap = builder.add_wiretap()
        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        s3_origin >> wiretap.destination
        s3_origin >= finisher

        read_pipeline = builder.build().configure_for_environment(aws)
        read_pipeline.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(read_pipeline)

        client = aws.s3
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # We should have exactly one file in the bucket
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert 'Contents' in list_s3_objs  # If no object was found, there is no 'Contents' key
        assert len(list_s3_objs['Contents']) == 1

        sdc_executor.start_pipeline(read_pipeline).wait_for_finished()
        assert len(wiretap.output_records) == 1
        assert [record.field['text'] for record in wiretap.output_records][0] == random_string
    finally:
        try:
            aws.delete_s3_data(s3_bucket, s3_key)
        finally:
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

    wiretap = builder.add_wiretap()

    origin >> target
    target >= wiretap.destination

    pipeline = builder.build().configure_for_environment(aws)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/input.txt', Body=data.encode('ascii'))
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 1, timeout_sec=120)

        # Validate event generation
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].get_field_data('/targetFileInfo/bucket') == aws.s3_bucket_name
        assert wiretap.output_records[0].get_field_data(
            '/targetFileInfo/objectKey') == f'{s3_dest_key}sdc-output.txt'

        # We should have exactly one file on the destination side
        list_s3_objs = client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_dest_key)
        assert len(list_s3_objs['Contents']) == 1

        # With our secret message
        s3_obj_key = client.get_object(Bucket=aws.s3_bucket_name, Key=list_s3_objs['Contents'][0]['Key'])
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert s3_contents == data
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting input S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)

        logger.info('Deleting output S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_dest_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_dest_key)

@aws('s3')
def test_s3_whole_file_writes_with_checksum(sdc_builder, sdc_executor, aws):
    """Test simple scenario of moving files from source to target using WHOLE_FILE_FORMAT with checksum"""
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'
    s3_dest_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    origin = builder.add_stage('Amazon S3', type='origin')
    origin.set_attributes(bucket=aws.s3_bucket_name, data_format='WHOLE_FILE',
                          prefix_pattern=f'{s3_key}/*',
                          verify_checksum=True,
                          max_batch_size_in_records=100)

    target = builder.add_stage('Amazon S3', type='destination')
    target.set_attributes(bucket=aws.s3_bucket_name, data_format='WHOLE_FILE',
                          include_checksum_in_events=True,
                          file_exists='OVERWRITE',
                          checksum_algorithm=CHECKSUM_ALGORITHM_DEFAULT,
                          partition_prefix=s3_dest_key,
                          file_name_expression='output.txt')

    wiretap = builder.add_wiretap()

    origin >> target
    target >= wiretap.destination

    pipeline = builder.build().configure_for_environment(aws)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/input.txt')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 1, timeout_sec=120)

        # Validate event generation
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].get_field_data('/targetFileInfo/bucket') == aws.s3_bucket_name, \
            'The targetFileInfo/bucket should have been a similar bucket to the bucket where the whole file is written.'
        assert wiretap.output_records[0].get_field_data('/sourceFileInfo/bucket') == aws.s3_bucket_name, \
            'The sourceFileInfo/bucket should have been the same as the origin system.'
        assert wiretap.output_records[0].get_field_data(
            '/targetFileInfo/objectKey') == f'{s3_dest_key}sdc-output.txt', \
            'The targetFileInfo/objectKey should have been a similar objectKey to the objectKey name that was written.'

        # We should have exactly one file on the destination side
        list_s3_objs = client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_dest_key)
        assert len(list_s3_objs['Contents']) == 1, 'The should have one file on the destination file'

        s3_obj_key = client.get_object(Bucket=aws.s3_bucket_name, Key=list_s3_objs['Contents'][0]['Key'])
        checksum = s3_obj_key['ResponseMetadata']['HTTPHeaders']['etag'].replace('"', '')

        assert wiretap.output_records[0].get_field_data('/checksum') == checksum, \
            'The checksum should have been the same as the ETag in the HTTP headers'
        assert wiretap.output_records[0].get_field_data('/checksumAlgorithm') == CHECKSUM_ALGORITHM_DEFAULT, \
            'The checksumAlgorithm should have been the same as the destination system.'

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
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
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket='${record:value("/bucket")}', data_format='JSON', partition_prefix=s3_key)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [s3_destination, wiretap.destination]

    s3_dest_pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_dest_pipeline)

    sdc_executor.start_pipeline(s3_dest_pipeline).wait_for_finished()

    # All records should go to error stream.
    assert len(wiretap.error_records) == 1
    assert 'S3_21' == wiretap.error_records[0].header['errorCode']
    assert 'The specified bucket does not exist' in wiretap.error_records[0].header['errorMessage']


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


def _run_test_s3_destination(sdc_builder, sdc_executor, aws, sse_kms, anonymous, acl="DEFAULT"):
    try:
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
                                                                                      raw_data=raw_str,
                                                                                      stop_after_first_batch=True)

        s3_destination = builder.add_stage('Amazon S3', type='destination')
        bucket_val = (s3_bucket if sdc_builder.version < '2.6.0.1-0002' else '${record:value("/bucket")}')
        s3_destination.set_attributes(bucket=bucket_val, data_format='JSON', partition_prefix=s3_key, acl=acl)
        if sse_kms:
            # Use SSE with KMS
            s3_destination.set_attributes(use_server_side_encryption=True,
                                          server_side_encryption_option='KMS',
                                          aws_kms_key_arn=aws.kms_key_arn)
        if anonymous:
            configure_stage_for_anonymous(s3_destination)

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> s3_destination
        s3_destination >= wiretap.destination

        s3_dest_pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
        sdc_executor.add_pipeline(s3_dest_pipeline)

        client = aws.s3

        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_dest_pipeline).wait_for_finished()

        # Validate event generation
        assert len(wiretap.output_records) == 1
        assert [record.field['bucket'] for record in wiretap.output_records][0] == s3_bucket
        assert [record.field['recordCount'] for record in wiretap.output_records][0] == 1

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

        if acl == "BUCKET_OWNER_FULL_CONTROL":
            s3_acl = aws.s3.get_object_acl(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])
            assert s3_acl['Owner']['ID'] == s3_acl['Grants'][0]['Grantee']['ID'], "Failed to set ACL"
            assert s3_acl['Grants'][0]['Permission'] == 'FULL_CONTROL', "Failed to set ACL"

    finally:
        try:
            aws.delete_s3_data(s3_bucket, s3_key)
        finally:
            if anonymous:
                logger.info(f'Deleting bucket {s3_bucket}')
                aws.s3.delete_bucket(Bucket=s3_bucket)


@aws('s3')
@sdc_min_version('4.2.0')
def test_s3_multithreading_multiple_batches(sdc_builder, sdc_executor, aws):
    """Test for S3 target stage. Data loss scenario happened when multiple threads try writing within the same
    millisecond. We just add the runnerId in the file name, if the option is specified (true by default):

    S3 Destination pipeline:
        dev_data_generator >> s3_destination
        dev_data_generator >> wiretap.destination
    """
    number_of_records = 100
    batch_size = 1
    delay_between_batches = 1
    number_of_threads = 10
    try:
        s3_bucket = aws.s3_bucket_name

        s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        dev_data_generator = builder.add_stage('Dev Data Generator')
        dev_data_generator.fields_to_generate = [
            {'field': 'id', 'type': 'POKEMON'},
        ]
        dev_data_generator.set_attributes(delay_between_batches=delay_between_batches,
                                          batch_size=batch_size,
                                          records_to_be_generated=number_of_records,
                                          number_of_threads=number_of_threads)

        s3_destination = builder.add_stage('Amazon S3', type='destination')
        s3_destination.set_attributes(bucket=s3_bucket, data_format='JSON', partition_prefix=s3_key)

        wiretap = builder.add_wiretap()

        dev_data_generator >> [s3_destination, wiretap.destination]

        s3_dest_pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
        sdc_executor.add_pipeline(s3_dest_pipeline)

        client = aws.s3

        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_dest_pipeline).wait_for_finished()

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == number_of_records

        records = []
        for record in wiretap.output_records:
            # We need to get every field inside each record
            records = records + [record.field]

        file_names = []
        for i in range(0, number_of_records - 1):
            # We just check each file in s3 contains ths same as wiretap
            s3_obj_key = client.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][i]['Key'])
            s3_contents = s3_obj_key['Body'].read().decode().strip()
            file_names = file_names + [list_s3_objs['Contents'][i]['Key']]
            assert json.loads(s3_contents) in records

        for i in range(0, number_of_threads - 1):
            # We also check we have at least one file name (computed before) of each thread (from 000 to 009)
            thread_number = '-' + str(i).zfill(3)
            assert any(thread_number in file_name for file_name in file_names)
    finally:
        aws.delete_s3_data(s3_bucket, s3_key)


@aws('s3')
@sdc_min_version('4.4.0')
def test_s3_with_tags(sdc_builder, sdc_executor, aws):
    """Test for tags on S3 destination.
    We create a file and verify that the tags are correctly propagated to the object created in S3.

        S3 Destination pipeline:
            dev_raw_data_source >> s3_destination
    """
    s3_bucket = aws.s3_bucket_name

    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    s3_tag = {"key": "this-is-a-test-tag-key", "value": "this-is-a-test-tag-value"}

    # Bucket name is inside the record itself
    raw_str = f'{{ "bucket" : "{s3_bucket}", "company" : "StreamSets Inc."}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    bucket_val = (s3_bucket if sdc_builder.version < '2.6.0.1-0002' else '${record:value("/bucket")}')
    s3_destination.set_attributes(bucket=bucket_val, data_format='JSON', partition_prefix=s3_key, add_tags=True,
                                  tags=[s3_tag])

    dev_raw_data_source >> s3_destination

    s3_dest_pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_dest_pipeline)

    client = aws.s3
    try:
        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_dest_pipeline).wait_for_finished()

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_obj_key = client.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])

        # We're comparing the logic structure (JSON) rather than byte-to-byte to allow for different ordering, ...
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert json.loads(s3_contents) == json.loads(raw_str)

        object_tagging = client.get_object_tagging(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])
        assert s3_tag in object_tagging['tagset'], "tag not found in tagset"
    finally:
        aws.delete_s3_data(s3_bucket, s3_key)


@aws('s3')
@sdc_min_version('4.4.0')
def test_s3_whole_file_transfer_with_tags(sdc_builder, sdc_executor, aws):
    """Test for tags on S3 destination using WHOLE_FILE_FORMAT..
    We create a file and verify that the tags are correctly propagated to the object created in S3.

        S3 Destination pipeline:
            dev_raw_data_source >> s3_destination
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'
    s3_dest_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/'
    s3_tag = {"key": "this-is-a-test-tag-key", "value": "this-is-a-test-tag-value"}
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
                          file_name_expression='output.txt', add_tags=True,
                          tags=[s3_tag])

    origin >> target

    pipeline = builder.build().configure_for_environment(aws)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/input.txt', Body=data.encode('ascii'))
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 1, timeout_sec=120)

        # We should have exactly one file on the destination side
        list_s3_objs = client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_dest_key)
        assert len(list_s3_objs['Contents']) == 1

        # With our secret message
        s3_obj_key = client.get_object(Bucket=aws.s3_bucket_name, Key=list_s3_objs['Contents'][0]['Key'])
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert s3_contents == data

        object_tagging = client.get_object_tagging(Bucket=aws.s3_bucket_name, Key=list_s3_objs['Contents'][0]['Key'])
        assert s3_tag in object_tagging['TagSet'], "Tag not found in TagSet"
    finally:
        logger.info('Deleting input S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)

        logger.info('Deleting output S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_dest_key)
        aws.delete_s3_data(aws.s3_bucket_name, s3_dest_key)


@aws('s3')
@sdc_min_version('3.19.0')
def test_s3_region_other(sdc_builder, sdc_executor, aws):
    """
    Test that using a specific region and specifying the endpoint works as expected
    We create a file and verify that the tags are correctly propagated to the object created in S3.

        S3 Destination pipeline:
            dev_raw_data_source >> s3_destination
    """
    s3_bucket = aws.s3_bucket_name

    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    # Bucket name is inside the record itself
    raw_str = f'{{ "bucket" : "{s3_bucket}", "company" : "StreamSets Inc."}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    bucket_val = (s3_bucket if sdc_builder.version < '2.6.0.1-0002' else '${record:value("/bucket")}')
    s3_destination.set_attributes(bucket=bucket_val, data_format='JSON', partition_prefix=s3_key)

    dev_raw_data_source >> s3_destination

    s3_dest_pipeline = builder.build().configure_for_environment(aws)

    s3_destination.set_attributes(use_specific_region=True, region='OTHER', endpoint=f's3.{aws.region}.amazonaws.com')

    sdc_executor.add_pipeline(s3_dest_pipeline)

    client = aws.s3
    try:
        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_dest_pipeline).wait_for_finished()

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_obj_key = client.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])

        # We're comparing the logic structure (JSON) rather than byte-to-byte to allow for different ordering, ...
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert json.loads(s3_contents) == json.loads(raw_str)
    finally:
        aws.delete_s3_data(s3_bucket, s3_key)


@aws('s3')
@sdc_min_version('5.0.0')
def test_s3_vpc_endpoint_and_region(sdc_builder, sdc_executor, aws):
    """
    Test that using a specific VPC endpoint and region and specifying works as expected
    We create a file and verify that the tags are correctly propagated to the object created in S3.

        S3 Destination pipeline:
            dev_raw_data_source >> s3_destination
    """
    if not aws.vpc_endpoint or not aws.signing_region:
        pytest.skip("VPC endpoint and signing region needed in this test.")

    s3_bucket = aws.s3_bucket_name

    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'

    # Bucket name is inside the record itself
    raw_str = f'{{ "bucket" : "{s3_bucket}", "company" : "StreamSets Inc."}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    bucket_val = (s3_bucket if sdc_builder.version < '2.6.0.1-0002' else '${record:value("/bucket")}')
    s3_destination.set_attributes(bucket=bucket_val, data_format='JSON', partition_prefix=s3_key)

    dev_raw_data_source >> s3_destination

    s3_dest_pipeline = builder.build().configure_for_environment(aws)

    s3_destination.set_attributes(use_specific_region=True, region='OTHER', use_custom_endpoint=True)

    # Specific configs for this test
    s3_destination.set_attributes(endpoint=aws.vpc_endpoint, signing_region=aws.signing_region)

    sdc_executor.add_pipeline(s3_dest_pipeline)

    client = aws.s3
    try:
        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_dest_pipeline).wait_for_finished()

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_obj_key = client.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])

        # We're comparing the logic structure (JSON) rather than byte-to-byte to allow for different ordering, ...
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert json.loads(s3_contents) == json.loads(raw_str)
    finally:
        aws.delete_s3_data(s3_bucket, s3_key)

@aws('s3')
@sdc_min_version('3.16.0')
def test_s3_destination_write_text_data_with_prefix_suffix_and_partition(sdc_builder, sdc_executor, aws):
    """
    Test using a prefix, suffix, and partition for S3, and check if the result contains the prefix, suffix,
    and partition.

    S3_destination_pipeline:
        dev_raw_data_source >> s3.destination >> wiretap
    """
    s3_bucket = aws.s3_bucket_name

    s3_prefix = 'test_write_text_data'
    s3_suffix = 'txt'
    s3_partition = '${record:id()}'

    # Bucket name is inside the record itself
    raw_str = f'{{ "bucket" : "{s3_bucket}", "company" : "StreamSets Inc."}}' \

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str,
                                                                                  stop_after_first_batch=True)

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket=s3_bucket,
                                  data_format='JSON',
                                  common_prefix=s3_prefix,
                                  partition_prefix=s3_partition,
                                  object_name_suffix=s3_suffix)

    wiretap = builder.add_wiretap()

    dev_raw_data_source >> s3_destination
    s3_destination >= wiretap.destination

    pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Validate event generation
        result = wiretap.output_records
        assert len(result) == 1
        assert [record.field['bucket'] for record in result][0] == s3_bucket
        assert [record.field['recordCount'] for record in result][0] == 1, \
            'The record count should have been one record'

        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        assert len(list_s3_objs['Contents']) == 1, 'The content should have been one element on the destination file'
        assert list_s3_objs['Contents'][0]['Key'].startswith(s3_prefix + '/' + 'rawData::0' + '/sdc-'), \
            'The path should have started the prefix and the partition'
        assert list_s3_objs['Contents'][0]['Key'].endswith(s3_suffix), 'The path should have finished with the suffix. '

        s3_obj_content = client.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])
        s3_body_file_content = s3_obj_content['Body'].read().decode().splitlines()
        assert len(s3_body_file_content) == 1, 'Only one line in the destination file'

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting input S3 data from bucket %s with location %s ...', aws.s3_bucket_name, s3_prefix)
        aws.delete_s3_data(aws.s3_bucket_name, s3_prefix)

@aws('s3')
@sdc_min_version('5.7.0')
@pytest.mark.parametrize('acl', ['BUCKET_OWNER_FULL_CONTROL'])
def test_s3_destination_acl(sdc_builder, sdc_executor, aws, acl):
    """Test for S3 target stage. We do so by running a dev raw data source generator to S3 destination
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been ingested by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to determine exactly what has been ingested. Uses anonymous credentials.
    The pipeline looks like:

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_destination
                                                   >> to_error """

    _run_test_s3_destination(sdc_builder, sdc_executor, aws, False, True, acl=acl)
