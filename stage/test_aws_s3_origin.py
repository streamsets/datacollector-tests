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
import string
import tempfile
import time
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import pytest
from streamsets.testframework.markers import aws, sdc_min_version, large
from streamsets.testframework.utils import get_random_string, Version
from xlwt import Workbook

from .utils.utils_aws import allow_public_access, restore_public_access, configure_stage_for_anonymous

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'
DEFAULT_READ_ORDER = 'LEXICOGRAPHICAL'
DEFAULT_DATA_FORMAT = 'JSON'
SINGLETHREADED = 1
MULTITHREADED = 5
DEFAULT_NUMBER_OF_RECORDS = 5
DATACOLLECTOR_TEST_FILES_BUCKET = 'datacollector-test-files'

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
        data_collector.sdc_properties['production.maxBatchSize'] = '100000'
    return hook


@aws('s3')
@sdc_min_version('3.7.0')
def test_s3_origin_empty_origin_stop_when_nomoredata_received(sdc_builder, sdc_executor, aws):
    """Test that an empty origin linked to a Pipeline Finisher Executor which ends the pipeline when
    a no-more-data is received actually emits this event.

    The pipeline looks like:
        s3_origin >> trash
        s3_origin >= pipeline_finisher_executor
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    # Just discard errors, we are not interested on them here
    builder.add_error_stage('Discard')

    # Declare and configure parts of the pipeline
    s3_origin = builder.add_stage('Amazon S3', type = 'origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format=DEFAULT_DATA_FORMAT,
                             prefix_pattern=f'{s3_key}/*', number_of_threads=MULTITHREADED,
                             read_order=DEFAULT_READ_ORDER)

    trash = builder.add_stage('Trash')

    # Pipeline Finisher Executor, note the precondition
    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    # Implement pipeline topology
    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline no data no more data event').\
        configure_for_environment(aws)
    # This is not supposed to fail under any context
    s3_origin_pipeline.configuration['shouldRetry'] = False

    # Add built pipeline to the DC executor
    sdc_executor.add_pipeline(s3_origin_pipeline)

    try:
        # Run the pipeline
        # Do not continue until the pipeline finishes
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True, timeout_sec=70).snapshot
        sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED', timeout_sec=60)
        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        # If we are here this means that a no-more-data was sent

        # Check that the number of records is correct
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert input_records == 0, 'Observed %d input records (expected 0)' % input_records
        assert output_records == 1, 'Observed %d output records (expected 1)' % output_records

        # We have exactly one output record, check that it is a no-more-data event
        num_event_records = len(snapshot[s3_origin.instance_name].event_records)
        assert num_event_records == 1, 'Received %d event records (expected 1)' % num_event_records
        event_record = snapshot[s3_origin.instance_name].event_records[0]
        event_type = event_record.header.values['sdc.event.type']
        assert event_type == 'no-more-data', 'Received %s as event type (expected no-more-data)' % event_type

    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
            raise AssertionError('Pipeline was still running after 60s')


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
            snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True,  timeout_sec=70).snapshot

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


@aws('s3')
@sdc_min_version('3.7.0')
def test_s3_origin_multithreaded_text_data_format(sdc_builder, sdc_executor, aws):
    """Tests a specific multithreaded scenario with more threads and text data format"""
    base_s3_origin(sdc_builder, sdc_executor, aws, DEFAULT_READ_ORDER, 'TEXT', 10, 50)


@aws('s3')
@sdc_min_version('3.16.0')
def test_s3_origin_anonymous(sdc_builder, sdc_executor, aws):
    """Tests accessing a public object where we can list bucket contents."""
    base_s3_origin(sdc_builder, sdc_executor, aws, DEFAULT_READ_ORDER, DEFAULT_DATA_FORMAT, SINGLETHREADED,
                   DEFAULT_NUMBER_OF_RECORDS, anonymous=True)


@aws('s3')
@sdc_min_version('3.16.0')
def test_s3_origin_anonymous_no_list(sdc_builder, sdc_executor, aws):
    """Tests accessing a public object where we cannot list bucket contents."""
    base_s3_origin(sdc_builder, sdc_executor, aws, DEFAULT_READ_ORDER, DEFAULT_DATA_FORMAT, SINGLETHREADED,
                   1, anonymous=True, allow_list=False)

@aws('s3')
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('use_path_style_address_model', [True, False])
def test_s3_origin_use_path_style_address_model(sdc_builder, sdc_executor, aws, use_path_style_address_model):
    base_s3_origin(sdc_builder, sdc_executor, aws, DEFAULT_READ_ORDER, DEFAULT_DATA_FORMAT, SINGLETHREADED,
                   1, use_path_style_address_model=use_path_style_address_model)


def base_s3_origin(sdc_builder, sdc_executor, aws, read_order, data_format, number_of_threads, number_of_records,
                   anonymous=False, allow_list=True, use_path_style_address_model=None):
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

    if Version(sdc_builder.version) >= Version('3.7.0'):
        s3_origin.set_attributes(bucket=s3_bucket,
                                 data_format=data_format,
                                 prefix_pattern=f'{s3_key}/*' if allow_list else f'{s3_key}/0',
                                 number_of_threads=number_of_threads,
                                 read_order=read_order)
    elif number_of_threads == 1:
        s3_origin.set_attributes(bucket=s3_bucket,
                                 data_format=data_format,
                                 prefix_pattern=f'{s3_key}/*' if allow_list else f'{s3_key}/0',
                                 read_order=read_order)
    else:
        pytest.skip("Multithreaded features are supported in S3 origin only for SDC Versions >= 3.7.0")

    # Since Use Path Style Addess Model doesn't exist in all versions, we set it conditionally only if it should
    # have some real value.
    if use_path_style_address_model is not None:
        s3_origin.use_path_style_address_model = use_path_style_address_model

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

    if anonymous:
        configure_stage_for_anonymous(s3_origin)

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    public_access_block = None
    bucket_policy = None
    try:
        acl = 'public-read' if anonymous else 'private'

        if anonymous:
            public_access_block, bucket_policy = allow_public_access(client, s3_bucket, allow_list, False)

        # Insert objects into S3.
        for i in range(s3_obj_count):
            client.put_object(Bucket=s3_bucket, Key=f'{s3_key}/{i}', Body=json.dumps(json_data), ACL=acl)

        if number_of_threads == SINGLETHREADED:
            # Snapshot the pipeline and compare the records.
            snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline,
                                                     timeout_sec=70,
                                                     start_pipeline=True).snapshot
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
        restore_public_access(client, s3_bucket, public_access_block, bucket_policy)
        if number_of_records > 0:
            # Clean up S3.
            if number_of_records > 1:
                delete_keys = {'Objects': [{'Key': k['Key']}
                                           for k in
                                           client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
            else:
                delete_keys = {'Objects': [{'Key' : f'{s3_key}/0'}]}
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
                             post_process_bucket='post-process-bucket-qwertyuiopasdfhklhkjlzxcv', data_format=DEFAULT_DATA_FORMAT,
                             prefix_pattern=f'{s3_key}*')

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    s3_origin_pipeline = builder.build().configure_for_environment(aws)

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

        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True, timeout_sec=70).snapshot

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
    records_per_file = 10

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(records_per_file)]
    data2 = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(records_per_file)]

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
    if (record.attributes['sdc.event.type'] == 'no-more-data'):
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
        # Insert objects into S3, process them and add an additional file
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}{s3_obj_count}-1', Body=json.dumps(data))
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_pipeline_output_records_count(10, timeout_sec=300)
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}{s3_obj_count}-2', Body=json.dumps(data2))

        # Take snapshot and check output records
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline,  timeout_sec=70).snapshot
        output_records_values = [record.field for record in snapshot[s3_origin.instance_name].output]
        assert len(output_records_values) == s3_obj_count * records_per_file
        assert output_records_values == data2

        sdc_executor.get_pipeline_status(s3_origin_pipeline).wait_for_status(status='FINISHED', timeout_sec=300)

        # Check that the origin has produced the expected records
        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        output_count = history.latest.metrics.counter(f'stage.{s3_origin.instance_name}.outputRecords.counter').count
        assert 2 * records_per_file == output_count

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
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True,  timeout_sec=70).snapshot

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
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True,  timeout_sec=70).snapshot

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
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True,  timeout_sec=70).snapshot

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
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == records_per_file * 2

        # Start pipeline again, wait some time and assert that no duplicated data has been read
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished(timeout_sec=70)

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
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True,  timeout_sec=70).snapshot
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
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True,  timeout_sec=70).snapshot
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


# SDC-11924: Better handling of various error header states in Excel parser
@aws('s3')
@sdc_min_version('3.10.0')
def test_s3_excel_parsing_incomplete_header(sdc_builder, sdc_executor, aws):
    """Ensure that incomplete header won't cause pipeline failure."""
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Create the Excel file on the fly
    file_excel = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.add_sheet('A')
    sheet.write(0, 0, 'A')
    # Second column is completely missing for header row
    sheet.write(0, 2, 'C')
    sheet.write(1, 0, 'a')
    sheet.write(1, 1, 'b')
    sheet.write(1, 2, 'c')
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

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.upload_fileobj(Bucket=s3_bucket, Key=f'{s3_key}', Fileobj=file_excel)

        # Snapshot the pipeline and compare the records.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True,  timeout_sec=70).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[s3_origin].output) == 1
        assert snapshot[s3_origin].output[0].get_field_data('/A') == 'a'
        assert snapshot[s3_origin].output[0].get_field_data('/C') == 'c'

    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


# SDC-14931: Excel parsing can throw DATA_PARSER_02 - Parser error: 'java.util.NoSuchElementException'
@aws('s3')
@sdc_min_version('3.10.0')
def test_s3_excel_last_sheet_empty(sdc_builder, sdc_executor, aws):
    """Ensure that when the last sheet have only header line but no data, no errors will be generated."""
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Create the Excel file on the fly
    file_excel = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.add_sheet('A') # First sheet contains header row and one data row
    sheet.write(0, 0, 'A')
    sheet.write(1, 0, 'a')
    sheet = workbook.add_sheet('B') # Second sheet only contains header row
    sheet.write(0, 0, 'B')
    workbook.save(file_excel)
    file_excel.seek(0)

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    origin = builder.add_stage('Amazon S3', type='origin')
    origin.bucket = s3_bucket
    origin.data_format = 'EXCEL'
    origin.prefix_pattern = f'{s3_key}*'
    origin.excel_header_option = 'WITH_HEADER'

    wiretap = builder.add_wiretap()
    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    origin >> wiretap.destination
    origin >= finisher

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.upload_fileobj(Bucket=s3_bucket, Key=f'{s3_key}', Fileobj=file_excel)

        # Read the file off the bucket
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['A'] == 'a'

        history = sdc_executor.get_pipeline_history(pipeline)
        # TLKT-564: sdc_models.Metrics support every metric type except of meter
        assert history.latest.metrics._data['meters']['stage.AmazonS3_01.errorRecords.meter']['count'] == 0
        assert history.latest.metrics._data['meters']['stage.AmazonS3_01.stageErrors.meter']['count'] == 0
    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('s3')
@sdc_min_version('3.11.0')
def test_s3_origin_events(sdc_builder, sdc_executor, aws):
    """Test simple scenario of generating events:
        - a new-file event when starting to read a new file, containing the filepath
        - a finished-file event once the pipeline is finished, containing the filepath, record count and error count
        - a no-more-data event when there are no more files to process

    The pipeline has 2 finishers, one for finished-file and one for no-more-data events. When we first run the pipeline
    it will stop once the finished-file event is generated, having read the totality of the file. When we run the
    pipeline a second time, it will try to grab a non-existing next file and generate the no-more-data event.

    The pipeline looks like:
    s3 origin >> trash
    s3 origin >= [pipeline finisher executor, pipeline finisher executor]
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}'
    records_per_file = 3
    batch_size = 10
    data_file = 'data-file.txt'
    test_data = [f'Message {i}' for i in range(records_per_file)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    origin = builder.add_stage('Amazon S3', type='origin')
    origin.set_attributes(bucket=aws.s3_bucket_name, data_format='TEXT',
                          prefix_pattern=f'{s3_key}/*',
                          max_batch_size_in_records=batch_size)

    target = builder.add_stage('Trash')

    file_finished_finisher = builder.add_stage('Pipeline Finisher Executor')
    file_finished_finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

    no_more_data_finisher = builder.add_stage('Pipeline Finisher Executor')
    no_more_data_finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    origin >> target
    origin >= [file_finished_finisher, no_more_data_finisher]

    pipeline = builder.build().configure_for_environment(aws)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{data_file}',
                          Body='\n'.join(test_data).encode('ascii'))

        # Run until finished-file
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=batch_size,  timeout_sec=70).snapshot
        output_records = snapshot[origin.instance_name].output
        event_records = snapshot[origin.instance_name].event_records

        # Assert that 3 records have been read and 2 events have been generated
        assert 3 == len(output_records)
        assert 2 == len(event_records)

        # Assert that first event is of type new-file and contains the correct filepath
        assert 'new-file' == event_records[0].header['values']['sdc.event.type']
        assert f'{s3_key}/{data_file}' == event_records[0].field['filepath']

        # Assert that second event is of type finished-file and contains the correct filepath, recordCount & errorCount
        assert 'finished-file' == event_records[1].header['values']['sdc.event.type']
        assert f'{s3_key}/{data_file}' == event_records[1].field['filepath']
        assert 3 == event_records[1].field['record-count']
        assert 0 == event_records[1].field['error-count']

        # Restart pipeline to generate no more data event
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=batch_size,  timeout_sec=70).snapshot
        event_records = snapshot[origin.instance_name].event_records
        assert 'no-more-data' == event_records[0].header['values']['sdc.event.type']

    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)

@aws('s3')
@large
def test_s3_read_large_file(sdc_builder, sdc_executor, aws):
    """This is a test for reading a large file (>2GB) from S3 in CSV format and test the fix for SDC-12774
        1. Create and run a pipeline from S3 origin to Trash
        2. Read the specified CSV file
        3. Check the results (number of record in the file vs number of records read).
    """
    FILE_NAME = 'large-file-test-sdc-12774.csv'
    NUMBER_OF_RECORDS_IN_THE_FILE = 300_000_000

    builder = sdc_builder.get_pipeline_builder()

    # Since we will be a reading a large file, set the batch size to 100,000
    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.set_attributes(bucket=DATACOLLECTOR_TEST_FILES_BUCKET, data_format='DELIMITED',
                             delimiter_format_type='CSV',
                             prefix_pattern=FILE_NAME,
                             max_batch_size_in_records=100_000)

    trash = builder.add_stage('Trash')

    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= [pipeline_finisher]

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    try:
        # The file that is being read is large. Set the timeout to 1 hour
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=3600)

        history = sdc_executor.get_pipeline_history(pipeline)
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        assert input_records == NUMBER_OF_RECORDS_IN_THE_FILE

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@aws('s3')
@pytest.mark.parametrize('file_exists', [True, False])
def test_s3_single_file_in_directory_no_wildcards(sdc_builder, sdc_executor, aws, file_exists):
    """Tests that a single file is read from a directory.
    The pipeline stops with a finisher.
    When parameter file_exists is false tests that no-more-data event is triggered.
    Common prefix + prefix pattern has NO wildcards.
    Test for bug fixed in SDC-15066 - The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
        s3_origin >= finisher
    """
    s3_bucket = aws.s3_bucket_name
    # / at the end of the common prefix should be explicit
    s3_common_prefix = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc/'
    s3_file_name = f'{get_random_string()}.txt'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format='WHOLE_FILE', prefix_pattern=s3_file_name,
                            common_prefix = s3_common_prefix)

    wiretap = builder.add_wiretap()
    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    s3_origin >> wiretap.destination
    s3_origin >= finisher

    s3_origin_pipeline = builder.build(title=f'S3 single file - File Exist {file_exists}').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    test_data = [f'Message {i}' for i in range(10)]
    try:
        # Insert objects into S3.
        if file_exists:
            file_key = f'{s3_common_prefix}{s3_file_name}'
        else:
            file_key = f'{s3_common_prefix}{s3_file_name}xxx'

        client.put_object(Bucket=aws.s3_bucket_name, Key=file_key,
                          Body='\n'.join(test_data).encode('ascii'))

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()

        if file_exists:
            assert [record.field['fileInfo']['filename'] for record in wiretap.output_records] == [
                f'{s3_common_prefix}{s3_file_name}']
        else:
            assert [record.field['fileInfo']['filename'] for record in wiretap.output_records] == []

        assert 0 == len(sdc_executor.get_stage_errors(s3_origin_pipeline, s3_origin))

    finally:
        # If no files have been processed we need to stop the pipeline, otherwise it will be finished
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(s3_origin_pipeline, force=True)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name,
                                                          Prefix=f'{s3_common_prefix}{s3_file_name}')[
                                       'Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)


@aws('s3')
@pytest.mark.parametrize('read_order', ['LEXICOGRAPHICAL', 'TIMESTAMP'])
def test_s3_restart_with_file_offset_and_xml_data_format(sdc_builder, sdc_executor, aws, read_order):
    """ This test is for xml data format, which was not working properly when reset with file offset.
        It checks that no stage error happens after the pipeline is restarted with an offset halfway in the file.
        Snapshot pipeline:
            s3_origin >> trash
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
    records_in_file = 10_000

    # Create test data files
    data_file_filename = 'xml-file.xml'
    xml_root = ET.Element('root')
    records = ET.SubElement(xml_root, 'records')
    for i in range(records_in_file):
        record = ET.SubElement(records, 'record')
        record_id = ET.SubElement(record, 'id')
        record_id.text = str(i)
        name = ET.SubElement(record, 'name')
        name.text = f'record-{i}'
    test_data = ET.tostring(xml_root)

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format='XML',
                             delimiter_element='/root/records/record',
                             prefix_pattern=f'{s3_key}/*.xml',
                             read_order=read_order,
                             max_batch_size_in_records=10)
    trash = builder.add_stage('Trash')

    s3_origin >> trash

    s3_origin_pipeline = builder.build(title='Amazon S3 origin restart pipeline with XML data format')\
        .configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{data_file_filename}',
                          Body=test_data)

        # Read 2 batches & stop the pipeline halfway through the file
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_pipeline_batch_count(2)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        # Restart the pipeline and wait until it reads all data
        sdc_executor.start_pipeline(s3_origin_pipeline)\
            .wait_for_pipeline_batch_count((records_in_file - input_records)/10)

        # Assert no stage errors have happened
        assert 0 == len(sdc_executor.get_stage_errors(s3_origin_pipeline, s3_origin))

    finally:
        # If no files have been processed we need to stop the pipeline, otherwise it will be finished
        try:
            if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(s3_origin_pipeline, force=True)
        finally:
            # Clean up S3.
            delete_keys = {'Objects': [{'Key': k['Key']}
                                       for k in
                                       client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
            client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)


@aws('s3')
@pytest.mark.parametrize('read_order', ['LEXICOGRAPHICAL', 'TIMESTAMP'])
def test_s3_restart_pipeline_with_changed_common_prefix(sdc_builder, sdc_executor, aws, read_order):
    """ This test is for xml data format, which was not working properly when reset with file offset.
        It checks that no stage error happens after the pipeline is restarted with an offset halfway in the file.
        Snapshot pipeline:
            s3_origin >> trash
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
    n_files = 10
    records_in_file = 1000

    # Create test data files
    data_file_filename = 'file-{}.txt'
    records = [f'Record {i}' for i in range(records_in_file)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format='TEXT',
                             delimiter_element='/root/records/record',
                             prefix_pattern=f'{s3_key}/*.txt',
                             read_order=read_order,
                             max_batch_size_in_records=10)

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin restart pipeline with XML data format') \
        .configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        for i in range(n_files):
            client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{data_file_filename.format(i)}',
                              Body='\n'.join(records).encode('ascii'))

        # Start and stop the pipeline to have an offset different than -1 in an existing file
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        # Update prefix_pattern so that no file is found
        s3_origin_pipeline.stages.get(label=s3_origin.label).set_attributes(prefix_pattern=f'{s3_key}/*.xml')
        sdc_executor.update_pipeline(s3_origin_pipeline)

        # Restart the pipeline and wait until it reads all data
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()

        # Check no input records were found
        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        num_input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        assert 0 == num_input_records

    finally:
        # If no files have been processed we need to stop the pipeline, otherwise it will be finished
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(s3_origin_pipeline, force=True)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)[
                                       'Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)


@aws('s3')
def test_s3_whole_file_empty_directory(sdc_builder, sdc_executor, aws):
    """Tests that a file is read and an empty directory does not stop the pipeline with an error.
    SDC-14835 - The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'
    # File Name starts with B. Directory AAAAAA comes first.
    s3_directory_name = f'{s3_key}/AAAAAA/'
    s3_file_name = f'{s3_key}/B{get_random_string()}.txt'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format='WHOLE_FILE', prefix_pattern=f'**/*',
                             read_order='LEXICOGRAPHICAL', common_prefix = f'/{s3_key}/')

    wiretap = builder.add_wiretap()
    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    s3_origin >> wiretap.destination
    s3_origin >= finisher

    s3_origin_pipeline = builder.build(title='Amazon S3 directory empty whole file').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    test_data = [f'Message {i}' for i in range(10)]
    try:
        # Insert objects into S3.
        client.put_object(Bucket=aws.s3_bucket_name, Key=s3_directory_name)
        client.put_object(Bucket=aws.s3_bucket_name, Key=s3_file_name,
                          Body='\n'.join(test_data).encode('ascii'))

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()

        assert [record.field['fileInfo']['filename'] for record in wiretap.output_records] == [s3_file_name]

    finally:
        # If no files have been processed we need to stop the pipeline, otherwise it will be finished
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(s3_origin_pipeline, force=True)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)[
                                       'Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)

@aws('s3')
def test_s3_stop_resume_file_not_found(sdc_builder, sdc_executor, aws):
    """Tests to process a file, stop the pipeline, delete the file and
    when resuming the pipeline it does not throw an exception:

    S3 Origin pipeline:
        s3_origin >> trash

    Test for Bug SDC-15174
    """

    s3_bucket = aws.s3_bucket_name
    s3_directory = get_random_string()
    s3_key = f'{S3_SANDBOX_PREFIX}/{s3_directory}'
    s3_file_name = f'{s3_key}/ABC.txt'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket, data_format='WHOLE_FILE', prefix_pattern=f'**/*',
                             read_order='LEXICOGRAPHICAL', common_prefix = f'/{s3_key}/')

    wiretap = builder.add_wiretap()
    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    s3_origin >> wiretap.destination
    s3_origin >= finisher

    s3_origin_pipeline = builder.build(title='AWS S3 dir stop resume file not found').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    test_data = [f'Message {i}' for i in range(10)]
    try:
        # Insert objects into S3.
        client.put_object(Bucket=aws.s3_bucket_name, Key=s3_file_name,
                          Body='\n'.join(test_data).encode('ascii'))

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()

        assert [record.field['fileInfo']['filename'] for record in wiretap.output_records] == [s3_file_name]

        # Delete the file
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_file_name)[
                                       'Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)

        # Start the pipeline, assert it is running and stop the pipeline.
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()
        assert sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') != 'RUN_ERROR'

    finally:
        if client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['KeyCount'] > 0:
            delete_keys = {'Objects': [{'Key': k['Key']}
                                       for k in
                                       client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)[
                                           'Contents']]}
            client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)

        # If no files have been processed we need to stop the pipeline, otherwise it will be finished
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(s3_origin_pipeline, force=True)


# SDC-14922: S3 Origin does not properly continue processing files after error in parsing
@aws('s3')
def test_s3_continue_processing_after_file_error(sdc_builder, sdc_executor, aws):
    """Ensure that the origin continues processing data after one of the input files error-ed out."""
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Amazon S3', type='origin')
    origin.set_attributes(bucket=s3_bucket, data_format='JSON', prefix_pattern=f'**/*',
                          read_order='LEXICOGRAPHICAL', common_prefix = f'/{s3_key}/')

    wiretap = builder.add_wiretap()

    finisher = builder.add_stage('Pipeline Finisher Executor')
    finisher.stage_record_preconditions = ['${record:eventType() == "no-more-data"}']

    origin >> wiretap.destination
    origin >= finisher

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f"{s3_key}/a.json", Body='{"id": 1}'.encode('ascii'))
        client.put_object(Bucket=s3_bucket, Key=f"{s3_key}/b.json", Body='Not JSON'.encode('ascii'))
        client.put_object(Bucket=s3_bucket, Key=f"{s3_key}/c.json", Body='{"id": 2}'.encode('ascii'))

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 2
        assert records[0].field['id'] == 1
        assert records[1].field['id'] == 2
    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)[
                                       'Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)

