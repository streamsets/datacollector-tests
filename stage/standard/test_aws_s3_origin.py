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

import pytest
from streamsets.testframework.markers import aws
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'

# Reference https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
S3_BUCKET_NAMES = [
    # For 3 characters we use 2 letters + 1 digit to avoid colliding with system buckets
    ('minsize', get_random_string(string.digits, 2) + get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', get_random_string(string.ascii_lowercase, 63)),
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('hypen', get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('period', get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('digits', get_random_string(string.digits)),
    ('hexadecimal', get_random_string(string.hexdigits).lower())
]

# Reference https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
S3_PATHS = [
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('letters', get_random_string(string.ascii_letters)),
    ('digits', get_random_string(string.digits)),
    ('hexadecimal', get_random_string(string.hexdigits).lower()),
    ('forward_slash', get_random_string() + '/' + get_random_string()),
    ('start_forward_slash', '/' + get_random_string()),
    # ('end_forward_slash', get_random_string() + '/'), Not testing this case since it introduces a double forward slash
    ('exclamation_point', get_random_string() + '!' + get_random_string()),
    ('start_exclamation_point', '!' + get_random_string()),
    ('end_exclamation_point', get_random_string() + '!'),
    ('hypen', get_random_string() + '-' + get_random_string()),
    ('start_hypen', '-' + get_random_string()),
    ('end_hypen', get_random_string() + '-'),
    ('underscore', get_random_string() + '_' + get_random_string()),
    ('start_underscore', get_random_string() + '_'),
    ('end_underscore', '_' + get_random_string()),
    ('period', get_random_string() + '.' + get_random_string()),
    ('start_period', '.' + get_random_string()),
    ('end_period', get_random_string() + '.'),
    ('asterisk', get_random_string() + '*' + get_random_string()),
    ('start_asterisk', '*' + get_random_string()),
    ('end_asterisk', get_random_string() + '*'),
    ('dot', get_random_string() + '.' + get_random_string()),
    ('start_dot', '.' + get_random_string()),
    ('end_dot', get_random_string() + '.'),
    ('single_quote', get_random_string() + '\'' + get_random_string()),
    ('start_single_quote', '\'' + get_random_string()),
    ('end_single_quote', get_random_string() + '\''),
    ('open_parenthesis', get_random_string() + '(' + get_random_string()),
    ('start_open_parenthesis', '(' + get_random_string()),
    ('end_open_parenthesis', get_random_string() + '('),
    ('close_parenthesis', get_random_string() + ')' + get_random_string()),
    ('start_close_parenthesis', ')' + get_random_string()),
    ('end_close_parenthesis', get_random_string() + ')'),
]


@aws('s3')
@pytest.mark.parametrize('test_name, s3_bucket', S3_BUCKET_NAMES, ids=[i[0] for i in S3_BUCKET_NAMES])
def test_object_names_bucket(sdc_builder, sdc_executor, aws, test_name, s3_bucket):
    """
    Verify that we can respect all the documented buckets names possible
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(10)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='JSON',
                             json_content='ARRAY_OBJECTS',
                             prefix_pattern=f'{s3_key}*')

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
        client.create_bucket(Bucket=s3_bucket, CreateBucketConfiguration={'LocationConstraint': aws.region})
        client.put_bucket_tagging(
            Bucket=s3_bucket,
            Tagging={
                'TagSet': [
                    {'Key': 'stf-env', 'Value': 'nightly-tests'},
                    {'Key': 'managed-by', 'Value': 'ep'},
                    {'Key': 'dept', 'Value': 'eng'},
                ]
            }
        )

        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}', Body=json.dumps(data))

        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).wait_for_finished().snapshot

        output_records_values = [record.field for record in snapshot[s3_origin.instance_name].output]

        assert len(output_records_values) == 10
        assert output_records_values == data
    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        try:
            delete_keys = {'Objects': [{'Key': k['Key']}
                                       for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
            client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)
        finally:
            client.delete_bucket(Bucket=s3_bucket)


@aws('s3')
@pytest.mark.parametrize('test_name, path_name', S3_PATHS, ids=[i[0] for i in S3_PATHS])
def test_object_names_path(sdc_builder, sdc_executor, aws, test_name, path_name):
    """
    Verify that we can respect all the documented buckets names possible
    """
    s3_key = path_name
    s3_bucket = aws.s3_bucket_name

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(10)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='JSON',
                             json_content='ARRAY_OBJECTS',
                             prefix_pattern=f'{s3_key}*')

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
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}', Body=json.dumps(data))

        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline, start_pipeline=True).wait_for_finished().snapshot

        output_records_values = [record.field for record in snapshot[s3_origin.instance_name].output]

        assert len(output_records_values) == 10
        assert output_records_values == data
    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('s3')
def test_dataflow_events_new_file(sdc_builder, sdc_executor, aws):
    """
    Test that we receive an new-file event whenever we start reading a file.
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    data = [dict(f1=get_random_string(), f2=get_random_string()) for _ in range(10)]

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='JSON',
                             json_content='ARRAY_OBJECTS',
                             prefix_pattern=f'{s3_key}*')

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
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}', Body=json.dumps(data))

        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline,
                                                 start_pipeline=True).wait_for_finished().snapshot

        output_records_values = [record.field for record in snapshot[s3_origin.instance_name].output]

        assert len(output_records_values) == 10
        assert output_records_values == data

        assert len(snapshot[s3_origin.instance_name].event_records) == 1

        event_record = snapshot[s3_origin.instance_name].event_records[0]
        event_type = event_record.header.values['sdc.event.type']
        assert event_type == 'new-file', 'Received %s as event type (expected no-more-data)' % event_type
    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('s3')
def test_dataflow_events_no_more_data(sdc_builder, sdc_executor, aws):
    """
    Test that an empty origin linked to a Pipeline Finisher Executor which ends the pipeline when
    a no-more-data is received actually emits this event.
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    # Just discard errors, we are not interested on them here
    builder.add_error_stage('Discard')

    # Declare and configure parts of the pipeline
    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format='JSON', prefix_pattern=f'{s3_key}/*')

    trash = builder.add_stage('Trash')

    # Pipeline Finisher Executor, note the precondition
    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    # Implement pipeline topology
    s3_origin >> trash
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline no data no more data event'). \
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
def test_resume_offset(sdc_builder, sdc_executor, aws):
    """
    Test that we can start our pipeline multiple times without reading any duplicated record neither missing them.
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name, data_format='JSON', prefix_pattern=f'{s3_key}/*')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    wiretap = builder.add_wiretap()

    s3_origin >> wiretap.destination
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        total_data = []
        for i in range(100):
            actual_data = dict(f1=get_random_string())
            total_data.append(actual_data)
            client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{i}', Body=json.dumps(actual_data))
        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_pipeline_output_records_count(20)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        first_iteration_records = [dict(f1=record.field['f1']) for record in wiretap.output_records]

        assert len(first_iteration_records) != 0
        assert all(element in total_data for element in first_iteration_records)

        wiretap.reset()

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()

        second_iteration_records = [dict(f1=record.field['f1']) for record in wiretap.output_records]

        assert len(second_iteration_records) != 0
        assert len(second_iteration_records) + len(first_iteration_records) == len(total_data)
        assert all(element in (second_iteration_records + first_iteration_records) for element in total_data)
        assert all(element in total_data for element in (second_iteration_records + first_iteration_records))

    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)


@aws('s3')
def test_multiple_batch(sdc_builder, sdc_executor, aws):
    """
    Test that using multithreaded pipeline we can start our pipeline multiple times adding more objects in between
    without reading any duplicated record neither missing them.
    """
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=aws.s3_bucket_name,
                             data_format='JSON',
                             prefix_pattern=f'{s3_key}/*',
                             max_batch_size_in_records=50)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    wiretap = builder.add_wiretap()

    s3_origin >> wiretap.destination
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build(title='Amazon S3 origin multithreaded pipeline').configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        total_data = []
        for i in range(100):
            actual_data = dict(f1=get_random_string())
            total_data.append(actual_data)
            client.put_object(Bucket=aws.s3_bucket_name, Key=f'{s3_key}/{i}', Body=json.dumps(actual_data))

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()

        records = [dict(f1=record.field['f1']) for record in wiretap.output_records]

        assert len(records) == len(total_data)
        assert all(element in records for element in total_data)
        assert all(element in total_data for element in records)

    finally:
        # Clean up S3.
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)
