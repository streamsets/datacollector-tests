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
import io
import json
import logging
import os
import string
import tempfile
import time
from zipfile import ZipFile

import pytest
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string
from xlwt import Workbook

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'
DEFAULT_READ_ORDER = 'LEXICOGRAPHICAL'
DEFAULT_DATA_FORMAT = 'JSON'
SINGLETHREADED = 1
MULTITHREADED = 5
DEFAULT_NUMBER_OF_RECORDS = 5


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')

    return hook


@aws('s3')
@sdc_min_version('3.7.0')
def test_s3_origin_multithread_start_stop(sdc_builder, sdc_executor, aws):
    """Test that using multithreaded pipeline we can start our pipeline multiple times adding more objects in between
    without reading any duplicated record neither missing them.

    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
        s3_origin >= pipeline_finished_executor
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    data = dict(f1=get_random_string(), f2=get_random_string())

    s3_obj_count = 10
    total_error_count = s3_obj_count
    input_records = 0
    output_records = 0

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format=DEFAULT_DATA_FORMAT,
                             prefix_pattern=f'{s3_key}/*', number_of_threads=MULTITHREADED,
                             read_order=DEFAULT_READ_ORDER)

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:

        for iteration in range(1, 4):
            # Insert objects into S3.
            for i in range(s3_obj_count):
                client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{iteration}-{i}', Body=json.dumps(data))

            # In case of multithreaded pipeline we want to verify the amount of records.
            snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).snapshot

            sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status('FINISHED')

            records = [record.field for record in snapshot[s3_origin.instance_name].output]

            for x in range(0, len(records)):
                assert records[x]['f1'] == data['f1']
                assert records[x]['f2'] == data['f2']

            history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
            input_records += history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
            output_records += history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

            assert output_records == total_error_count + iteration  # Adding iteration because of the events
            assert input_records == total_error_count

            total_error_count += s3_obj_count

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        input_records += history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records += history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

        assert output_records == total_error_count + iteration + 1  # Adding +1 because of the last event
        assert input_records == total_error_count

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
def test_s3_origin_read_order_multiple_records(sdc_builder, sdc_executor, aws, read_order):
    """Tests that S3Origin can read information in different reading orders"""
    base_s3_origin(sdc_builder, sdc_executor, aws, read_order, DEFAULT_DATA_FORMAT, SINGLETHREADED, 50)


@aws('s3')
@sdc_min_version('3.7.0')
def test_s3_origin_multithreaded(sdc_builder, sdc_executor, aws):
    """Tests a specific multithreaded scenario with more threads."""
    base_s3_origin(sdc_builder, sdc_executor, aws, DEFAULT_READ_ORDER, DEFAULT_DATA_FORMAT, 10, 50)


def base_s3_origin(sdc_builder, sdc_executor, aws, read_order, data_format, number_of_threads, number_of_records):
    """Basic setup for amazon S3Origin tests. It receives different variables indicating the read order, data format...
    In order to parametrize all this configuration properties and make tests simpler.
    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
        s3_origin >= pipeline_finished_executor
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    json_data = dict(f1=get_random_string(), f2=get_random_string())

    s3_obj_count = number_of_records

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format=data_format, prefix_pattern=f'{s3_key}/*',
                             number_of_threads=number_of_threads, read_order=read_order)

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

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
            client.put_object(Bucket=s3_bucket, Key=f'{s3_key}/{i}', Body=json.dumps(json_data))

        if number_of_threads == SINGLETHREADED:
            # Snapshot the pipeline and compare the records.
            snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).snapshot
            sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED')

            output_records = [record.field for record in snapshot[s3_origin.instance_name].output]

            verify_data_formats(output_records, json.dumps(json_data), data_format)

        else:
            # In case of multithreaded pipeline we want to verify the amount of records.
            sdc_executor.start_pipeline(s3_origin_pipeline)
            sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED')

            history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
            assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == s3_obj_count
            # We need to add 1 since the event is considered a record
            assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == s3_obj_count + 1

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
        assert str(output_records[0]['f1']) in raw_str
        assert str(output_records[0]['f2']) in raw_str
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
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
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
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
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
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
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


@aws('s3')
def test_s3_event_finisher(sdc_builder, sdc_executor, aws):
    """
    Test that event pipeline finisher is being sent when no more data is available in S3

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(10)]

    s3_obj_count = 1

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format='JSON',
                             json_content='ARRAY_OBJECTS', prefix_pattern=f'{s3_key}*')

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    pipeline_name = 'Amazon S3 - No more data event'

    s3_origin_pipeline = builder.build(title=pipeline_name).configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}{s3_obj_count}', Body=json.dumps(data))

        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).snapshot

        output_records_values = [record.field for record in snapshot[s3_origin.instance_name].output]

        assert len(output_records_values) == s3_obj_count * 10
        assert output_records_values == data

        sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED', timeout_sec=60)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 10
        # We need to add 1 since the event is considered a record
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 10 + 1

    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


# SDC-11176 S3 Origin is only sending one no-more-data event, it should send one if there is some refill of data
@aws('s3')
def test_s3_event_finisher_multiple_events(sdc_builder, sdc_executor, aws):
    """
    Test that no-more-data event is being sent when no more data is available in S3, then refill, then sent it again.

    We insert objects in the bucket, then read them, then insert again. after the second no-more-data event the jython
    evaluator produces a new event that stop the pipeline.

    S3 Origin pipeline:
        s3_origin >> trash
        s3_origin >= jython_evaluator
        jython_evaluator >> trash2
        jython_evaluator >= pipeline_finished_executor
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(10)]
    data2 = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(10)]

    s3_obj_count = 1

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format='JSON',
                             json_content='ARRAY_OBJECTS', prefix_pattern=f'{s3_key}*')

    trash = builder.add_stage('Trash')
    trash2 = builder.add_stage('Trash')

    jython_evaluator = builder.add_stage('Jython Evaluator')
    jython_evaluator.set_attributes(init_script="state['record-count'] = 0", script="""
for record in records:
    output.write(record)
    state['record-count'] = state['record-count'] + 1

if (state['record-count'] >= 2):
    event = sdcFunctions.createEvent('stop-pipeline', 1)
    sdcFunctions.toEvent(event)
    """)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'stop-pipeline'}"])

    s3_origin >> trash
    s3_origin >= jython_evaluator
    jython_evaluator >> trash2
    jython_evaluator >= pipeline_finished_executor

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}{s3_obj_count}-1', Body=json.dumps(data))

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_pipeline_output_records_count(10, timeout_sec=300)

        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}{s3_obj_count}-2', Body=json.dumps(data2))

        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline).snapshot

        output_records_values = [record.field for record in snapshot[s3_origin.instance_name].output]

        assert len(output_records_values) == s3_obj_count * 10
        assert output_records_values == data2

        sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED', timeout_sec=300)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 2 * 10
        # We have 2 events and we are generating a third one to effectively stop the pipeline
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 2 * (10 + 1) + 1

    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)

        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('s3')
def test_s3_multiple_records_in_object(sdc_builder, sdc_executor, aws):
    """
    Tests that verifies that all the records are properly read when the batch size is the same as the number of records
    per object.

    Using the snapshot we will verify the content of the records and using the pipeline history the amount of records
    processed.

    S3 Origin pipeline:
        s3_origin >> trash
    """
    S3_OBJ_COUNT = 5

    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}/sdc'

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(50)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='JSON',
                             json_content='ARRAY_OBJECTS',
                             prefix_pattern=f'{s3_key}*',
                             max_batch_size_in_records=50)

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    pipeline_name = f'Amazon S3 - ReadAllRecordsFromS3toFS'

    s3_origin_pipeline = builder.build(title=pipeline_name).configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        for i in range(S3_OBJ_COUNT):
            client.put_object(Bucket=s3_bucket, Key=f'{s3_key}{i}', Body=json.dumps(data))

        # Snapshot the pipeline and compare the records.
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).snapshot

        output_records = [record.field for record in snapshot[s3_origin.instance_name].output]

        for i in range(0, len(output_records)):
            assert output_records[i]['f1'] == data[i]['f1']
            assert output_records[i]['f2'] == data[i]['f2']

        sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED')

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        output_records_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert output_records_count == S3_OBJ_COUNT * 50 + 1  # Adding +1 since event also count as record

    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


# SDC-11163: Amazon S3 origin never removes POLL_OFFSET key on upgrade
@aws('s3')
def test_offset_upgrade(sdc_builder, sdc_executor, aws):
    """Ensure that when upgrading from older offset format (that can be generated by either SCH or by upgrading
       pre-multithreaded pipeline) we properly upgrade the offset and the pipeline will not re-read everything
       from the source bucket.
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}/sdc'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='TEXT',
                             prefix_pattern=f'{s3_key}*')

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    # We hard code offset to be pre-migration to multi-threaded origin and thus forcing the origin to upgrade it
    offset = {
        'offsets': {
            '$com.streamsets.datacollector.pollsource.offset$': None,
        },
        'version': 2
    }
    sdc_executor.api_client.update_pipeline_committed_offsets(pipeline.id, body=offset)

    client = aws.s3
    try:
        # Insert object into S3
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}input1', Body='Input Line')

        # Finally run the pipeline (until it's end)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # This execution should read in total 1 record and write out 2 (+ no-more-data event)
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 2

        # Now let's validate that the offset doesn't have the poll key any more
        offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
        assert offset is not None
        assert '$com.streamsets.datacollector.pollsource.offset$' not in offset['offsets']

        # Insert another object into S3 and run the pipeline second time
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}input2', Body='Input Line')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # But it should read only the second file (rather then reading from the begging)
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 2
    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


# SDC-11410: S3 Origin reads excel files
@aws('s3')
@sdc_min_version('3.9.0')
def test_s3_excel_offset(sdc_builder, sdc_executor, aws):
    """
    Test that an excel file on a s3 bucket is properly read

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    s3_obj_count = 1

    # Create the Excel file
    file_excel = io.BytesIO()  # create a file-like object
    workbook = Workbook()
    sheet = workbook.add_sheet('0')

    colcount = 5
    rowcount = 1000

    for col in range(colcount):
        for row in range(rowcount):
            sheet.write(row, col, 'TAB({row}, {col})'.format(row=row, col=col))

    workbook.save(file_excel)
    file_excel.seek(0)  # Move the pointer to first position of the file object so when
    # it reads the content it starts from the beginning.

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='EXCEL',
                             prefix_pattern=f'{s3_key}*',
                             excel_header_option="NO_HEADER")

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.upload_fileobj(Bucket=s3_bucket, Key=f'{s3_key}{s3_obj_count}', Fileobj=file_excel)

        # Snapshot the pipeline and compare the records.
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).snapshot

        output_records = [record.field for record in snapshot[s3_origin.instance_name].output]

        len_records = len(output_records)

        # Compare the results get from the output_records
        for row_res in range(len_records):
            for col_res in range(colcount):
                assert output_records[row_res][str(col_res)] == "TAB({row}, {col})".format(row=row_res, col=col_res)

        sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED', timeout_sec=60)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == rowcount
        # We need to add 1 since the event is considered a record
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == rowcount + 1

    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('s3')
def test_s3_compressed_file_offset(sdc_builder, sdc_executor, aws):
    """
    Tests that compressed file offsets are properly handled

    S3 Origin pipeline:
        s3_origin >> trash
        s3_origin >= pipeline_finished_executor
    """
    s3_bucket = aws.s3_bucket_name
    directory_to_write = tempfile.gettempdir()

    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    zip_file_name = get_random_string()
    json1_file_name = get_random_string()
    json2_file_name = get_random_string()

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(50)]

    json_data = "".join(json.dumps(x) for x in data)

    with open(f'{directory_to_write}/{json1_file_name}.json', 'w') as outfile:
        outfile.write(json_data)

    with open(f'{directory_to_write}/{json2_file_name}.json', 'w') as outfile:
        outfile.write(json_data)

    with ZipFile(f'{zip_file_name}.zip', 'w') as zipfile:
        zipfile.write(f'{directory_to_write}/{json1_file_name}.json', f'{json1_file_name}.json')
        zipfile.write(f'{directory_to_write}/{json2_file_name}.json', f'{json2_file_name}.json')

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='JSON',
                             compression_format='ARCHIVE',
                             file_name_pattern_within_compressed_directory='*.json',
                             json_content='MULTIPLE_OBJECTS',
                             prefix_pattern=f'{s3_key}/*')

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.upload_file(Bucket=s3_bucket, Key=f'{s3_key}/{zip_file_name}.zip',
                           Filename=f'{zip_file_name}.zip')

        # Snapshot the pipeline and compare the records.
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).snapshot

        output_records = [record.field for record in snapshot[s3_origin.instance_name].output]

        for i in range(len(output_records)):
            assert output_records[i]['f1'] == data[i]['f1']
            assert output_records[i]['f2'] == data[i]['f2']

        sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED')

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 50 * 2
        # We need to add 1 since the event is considered a record
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 50 * 2 + 1

    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('s3')
@pytest.mark.parametrize('records_per_file', [10, 1000, 10000])
def test_s3_origin_timestamp_last_file_offset(sdc_builder, sdc_executor, aws, records_per_file):
    """Test that the last file offset (-1) is properly committed. When using TIMESTAMP ordering, last file
    is duplicated when restarting the pipeline.

    The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
        s3_origin >= pipeline_finished_executor
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Create test data files
    data_file_first_filename = 'data-file-first.txt'
    test_data_first = [f'First Message {i}' for i in range(records_per_file)]

    data_file_second_filename = 'data-file-second.txt'
    test_data_second = [f'Second  Message {i}' for i in range(records_per_file)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format='TEXT',
                             prefix_pattern=f'{s3_key}/*',
                             max_batch_size_in_records=1000,
                             read_order='TIMESTAMP')
    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{data_file_first_filename}',
                          Body='\n'.join(test_data_first).encode('ascii'))
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{data_file_second_filename}',
                          Body='\n'.join(test_data_second).encode('ascii'))

        # Read files once
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()

        # Verify read data
        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == records_per_file*2

        # Start pipeline again, wait some time and assert that no duplicated data has been read
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished(timeout_sec=30)

        # Assert no more data has been read on the second run
        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 0

    finally:
        # If no files have been processed we need to stop the pipeline, otherwise it will be finished
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(s3_origin_pipeline, force=True)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)


@aws('s3')
@pytest.mark.parametrize('read_order', ['LEXICOGRAPHICAL', 'TIMESTAMP'])
def test_s3_restart_with_file_offset(sdc_builder, sdc_executor, aws, read_order):

    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
    records_per_file = 100_000

    # Create test data files
    data_file_filename = 'big-data-file.txt'
    test_data = [f'Message {i}' for i in range(records_per_file)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format='TEXT',
                             prefix_pattern=f'{s3_key}/*',
                             read_order=read_order,
                             max_batch_size_in_records=100)
    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{data_file_filename}',
                          Body='\n'.join(test_data).encode('ascii'))

        # Read 2 batches & stop the pipeline halfway through the file
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_pipeline_batch_count(2)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        # Check amount of records read so far
        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        # Restart the pipeline and wait until it reads all data
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished(timeout_sec=150)

        # Check amount of records read in the second run
        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        input_records_second = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        # Assert records_per_file have been read
        assert records_per_file == input_records + input_records_second

    finally:
        # If no files have been processed we need to stop the pipeline, otherwise it will be finished
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(s3_origin_pipeline, force=True)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)

# SDC-11925: Allow specifying subset of sheets to import when reading Excel files
@aws('s3')
@sdc_min_version('3.10.0')
@pytest.mark.parametrize('read_all_sheets', [True, False])
def test_s3_excel_sheet_selection(sdc_builder, sdc_executor, aws, read_all_sheets):
    """Ensure that configuring subset of sheets to import properly works."""
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Create the Excel file on the fly
    file_excel = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.add_sheet('A')
    sheet.write(0, 0, 'A')
    sheet.write(1, 0, 'a')
    sheet = workbook.add_sheet('B')
    sheet.write(0, 0, 'B')
    sheet.write(1, 0, 'b')
    workbook.save(file_excel)
    file_excel.seek(0)

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.bucket = s3_bucket
    s3_origin.data_format = 'EXCEL'
    s3_origin.prefix_pattern = f'{s3_key}*'
    s3_origin.excel_header_option = 'WITH_HEADER'
    s3_origin.read_all_sheets = read_all_sheets
    s3_origin.import_sheets = ['A']

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.upload_fileobj(Bucket=s3_bucket, Key=f'{s3_key}', Fileobj=file_excel)

        # Snapshot the pipeline and compare the records.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        if read_all_sheets:
            assert len(snapshot[s3_origin].output) == 2
        else:
            assert len(snapshot[s3_origin].output) == 1

        assert snapshot[s3_origin].output[0].get_field_data('/A') == 'a'
        if read_all_sheets:
            assert snapshot[s3_origin].output[1].get_field_data('/B') == 'b'

    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


# SDC-11926: Add ability to skip cells that have no associated header when reading from Excel
@aws('s3')
@sdc_min_version('3.10.0')
@pytest.mark.parametrize('skip', [True, False])
def test_s3_excel_skip_cells_missing_header(sdc_builder, sdc_executor, aws, skip):
    """Ensure that configuration option skip cells with missing header works properly."""
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Create the Excel file on the fly
    file_excel = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.add_sheet('A')
    sheet.write(0, 0, 'A')
    sheet.write(1, 0, 'a')
    sheet.write(1, 2, 'extra value with no header')
    workbook.save(file_excel)
    file_excel.seek(0)

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.bucket = s3_bucket
    s3_origin.data_format = 'EXCEL'
    s3_origin.prefix_pattern = f'{s3_key}*'
    s3_origin.excel_header_option = 'WITH_HEADER'
    s3_origin.skip_cells_with_no_header = skip

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.upload_fileobj(Bucket=s3_bucket, Key=f'{s3_key}', Fileobj=file_excel)

        # Snapshot the pipeline and compare the records.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[s3_origin].output) == 1
        assert snapshot[s3_origin].output[0].get_field_data('/A') == 'a'

        if skip:
            # Unclear how validate if given path doesn't exists
            with pytest.raises(Exception) as ex:
                snapshot[s3_origin].output[0].get_field_data('/2')
        else:
            assert snapshot[s3_origin].output[0].get_field_data('/2') == 'extra value with no header'

    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)