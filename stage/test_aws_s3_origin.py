# Copyright 2018 StreamSets Inc.
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
import time

import pytest
from streamsets.testframework.markers import aws
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'
DEFAULT_READ_ORDER = 'LEXICOGRAPHICAL'
DEFAULT_DATA_FORMAT = 'JSON'
SINGLETHREADED = 1
MULTITHREADED = 5
DEFAULT_NUMBER_OF_RECORDS = 5


# Keep this tests as first in the file since it will cause problems due a non-read s3 objects
@aws('s3')
def test_s3_origin_multithread_start_stop(sdc_builder, sdc_executor, aws):
    """Basic setup for amazon S3Origin tests. It receives different variables indicating the read order, data format...
    In order to parametrize all this configuration properties and make tests simpler.
    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/get_random_string(string.ascii_letters, 10)/sdc'

    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)
    json_str = '{"f1":"' + field_val_1 + '", "f2":"' + field_val_2 + '"}'

    s3_obj_count = 50

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format=DEFAULT_DATA_FORMAT,
                             prefix_pattern=f'{s3_key}*', number_of_threads=5, read_order=DEFAULT_READ_ORDER)

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        for i in range(s3_obj_count):
            client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}{i}', Body=json_str)

        input_records = 0
        output_records = 0

        # In case of multithreaded pipeline we want to verify the amount of records.
        sdc_executor.start_pipeline(s3_origin_pipeline)
        time.sleep(2)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        input_records += history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records += history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

        sdc_executor.start_pipeline(s3_origin_pipeline)
        time.sleep(5)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        input_records += history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records += history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

        sdc_executor.start_pipeline(s3_origin_pipeline)
        time.sleep(20)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        input_records += history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records += history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

        assert output_records == s3_obj_count
        assert input_records == s3_obj_count

    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)


@aws('s3')
@pytest.mark.parametrize('data_format', ['JSON', 'WHOLE_FILE', 'TEXT'])
def test_s3_origin_data_formats(sdc_builder, sdc_executor, aws, data_format):
    """Tests that S3 origin can handle different data formats in a single threaded scenario"""
    base_s3_origin(sdc_builder, sdc_executor, aws, DEFAULT_READ_ORDER, data_format, SINGLETHREADED,
                   DEFAULT_NUMBER_OF_RECORDS)


@aws('s3')
@pytest.mark.parametrize('read_order', ['TIMESTAMP', 'LEXICOGRAPHICAL'])
def test_s3_origin_read_order(sdc_builder, sdc_executor, aws, read_order):
    """Tests that S3Origin can read information in different reading orders"""
    base_s3_origin(sdc_builder, sdc_executor, aws, read_order, DEFAULT_DATA_FORMAT, SINGLETHREADED,
                   DEFAULT_NUMBER_OF_RECORDS)


@aws('s3')
@pytest.mark.parametrize('read_order', ['TIMESTAMP', 'LEXICOGRAPHICAL'])
def test_s3_origin_read_order(sdc_builder, sdc_executor, aws, read_order):
    """Tests that S3Origin can read information in different reading orders"""
    base_s3_origin(sdc_builder, sdc_executor, aws, read_order, DEFAULT_DATA_FORMAT, SINGLETHREADED, 50)


@aws('s3')
def test_s3_origin_multithreaded(sdc_builder, sdc_executor, aws):
    """Tests a specific multithreaded scenario with more threads."""
    base_s3_origin(sdc_builder, sdc_executor, aws, DEFAULT_READ_ORDER, DEFAULT_DATA_FORMAT, 10, 50)


def base_s3_origin(sdc_builder, sdc_executor, aws, read_order, data_format, number_of_threads, number_of_records):
    """Basic setup for amazon S3Origin tests. It receives different variables indicating the read order, data format...
    In order to parametrize all this configuration properties and make tests simpler.
    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/get_random_string(string.ascii_letters, 10)/sdc'

    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)
    json_str = '{"f1":"' + field_val_1 + '", "f2":"' + field_val_2 + '"}'

    if data_format == 'JSON':
        json_str = json.dumps(json_str)

    s3_obj_count = number_of_records

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format=data_format, prefix_pattern=f'{s3_key}*',
                             number_of_threads=number_of_threads, read_order=read_order)

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    pipeline_name = (
        f'Amazon S3 - number of threads: {number_of_threads} - read order: {read_order} - data_format: {data_format} - '
        f'number of records: {number_of_records}')

    s3_origin_pipeline = builder.build(title=pipeline_name).configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        for i in range(s3_obj_count):
            client.put_object(Bucket=s3_bucket, Key=f'{s3_key}{i}', Body=json_str)

        if number_of_threads == SINGLETHREADED:
            # Snapshot the pipeline and compare the records.
            snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).snapshot
            sdc_executor.stop_pipeline(s3_origin_pipeline)

            output_records = [record.field for record in snapshot[s3_origin.instance_name].output]

            verify_data_formats(output_records, json_str, data_format)

        else:
            # In case of multithreaded pipeline we want to verify the amount of records.
            sdc_executor.start_pipeline(s3_origin_pipeline)

            time.sleep(20)

            sdc_executor.stop_pipeline(s3_origin_pipeline)

            history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
            assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == s3_obj_count
            assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == s3_obj_count

    finally:
        if number_of_records > 0:
            # Clean up S3.
            delete_keys = {'Objects': [{'Key': k['Key']}
                                       for k in
                                       client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
            client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


def verify_data_formats(output_records, raw_str, data_format):
    # Verify the data from the different records depending on the data format used.
    if data_format == 'JSON':
        assert str(output_records[0]) in raw_str.replace("\\", "")
    elif data_format == 'WHOLE_FILE':
        assert output_records[0]['fileInfo']['size'] == len(raw_str)
    elif data_format == 'TEXT':
        assert raw_str == str(output_records[0]['text'])


@aws('s3')
def test_s3_origin_empty_bucket(sdc_builder, sdc_executor, aws):
    """Tests that a empty bucket can be targeted with any major error popping up.
    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/get_random_string(string.ascii_letters, 10)/sdc'
    s3_obj_count = 0

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format=DEFAULT_DATA_FORMAT, prefix_pattern=f'{s3_key}*')

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    s3_origin_pipeline = builder.build(title='Amazon S3 origin empty bucket pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    sdc_executor.start_pipeline(s3_origin_pipeline)

    time.sleep(10)

    sdc_executor.stop_pipeline(s3_origin_pipeline)

    history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == s3_obj_count
    assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == s3_obj_count


@aws('s3')
def test_invalid_configs_same_bucket_same_prefix(sdc_builder, sdc_executor, aws):
    """Test invalid configs and test that they are being caught properly.
    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/get_random_string(string.ascii_letters, 10)/sdc'
    prefix = 'myprefix'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name, common_prefix=prefix, error_handling_option='ARCHIVE',
                             archiving_option='MOVE_TO_BUCKET', error_prefix=prefix, error_bucket=aws.s3_bucket_name,
                             post_processing_option='ARCHIVE', post_process_prefix=prefix,
                             post_process_bucket=aws.s3_bucket_name, data_format=DEFAULT_DATA_FORMAT,
                             prefix_pattern=f'{s3_key}*')

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    s3_origin_pipeline = builder.build(title='Amazon S3 configs origin pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    try:
        sdc_executor.validate_pipeline(s3_origin_pipeline)
        assert False, 'Should not reach here.'
    except Exception as error:
        assert error.issues['issueCount'] == 2

        assert 'S3_SPOOLDIR_14' in error.issues['stageIssues']['AmazonS3_01'][0]['message']
        assert 'POST_PROCESSING' in error.issues['stageIssues']['AmazonS3_01'][0]['configGroup']

        assert 'S3_SPOOLDIR_14' in error.issues['stageIssues']['AmazonS3_01'][1]['message']
        assert 'ERROR_HANDLING' in error.issues['stageIssues']['AmazonS3_01'][1]['configGroup']


@aws('s3')
def test_invalid_configs_diff_bucket_same_prefix(sdc_builder, sdc_executor, aws):
    """Test invalid configs and test that they are being caught properly.
    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/get_random_string(string.ascii_letters, 10)/sdc'
    prefix = 'myprefix'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name, common_prefix=prefix, error_handling_option='ARCHIVE',
                             archiving_option='MOVE_TO_BUCKET', error_prefix=prefix, error_bucket=aws.s3_bucket_name,
                             post_processing_option='ARCHIVE', post_process_prefix=prefix,
                             post_process_bucket='post-process-bucket', data_format=DEFAULT_DATA_FORMAT,
                             prefix_pattern=f'{s3_key}*')

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    s3_origin_pipeline = builder.build(title='Amazon S3 configs  pipeline').configure_for_environment(aws)

    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    try:
        sdc_executor.validate_pipeline(s3_origin_pipeline)
        assert False, 'Should not reach here.'
    except Exception as error:
        assert error.issues['issueCount'] == 2

        assert 'S3_SPOOLDIR_12' in error.issues['stageIssues']['AmazonS3_01'][0]['message']
        assert 'POST_PROCESSING' in error.issues['stageIssues']['AmazonS3_01'][0]['configGroup']

        assert 'S3_SPOOLDIR_14' in error.issues['stageIssues']['AmazonS3_01'][1]['message']
        assert 'ERROR_HANDLING' in error.issues['stageIssues']['AmazonS3_01'][1]['configGroup']
