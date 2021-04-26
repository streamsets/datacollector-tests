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
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'

# Reference https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
S3_BUCKET_NAMES = [
    ('minsize', lambda: get_random_string(string.ascii_lowercase, 3)),
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 63)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('hypen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('period', lambda: get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits)),
    ('hexadecimal', lambda: get_random_string(string.hexdigits).lower())
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
def test_data_types(sdc_builder, sdc_executor, aws):
    pytest.skip("AWS S3 doesn't talk to a structured system, so we don't need to test each data type.")


@aws('s3')
@pytest.mark.parametrize('test_name, bucket_generator', S3_BUCKET_NAMES, ids=[i[0] for i in S3_BUCKET_NAMES])
def test_object_names_bucket(sdc_builder, sdc_executor, aws, test_name, bucket_generator):
    """
    Verify that we can respect all the documented buckets names possible
    """
    client = aws.s3
    retry = 0
    s3_bucket = None
    # Since S3 buckets are globally unique, doing our usual randomization doesn't work well - we always have a chance
    # to create bucket that already exists. That is why we have a retry logic - we try to generate several bucket names
    # and see which one we manage to "claim".
    while s3_bucket is None and retry < 10:
        retry = retry + 1
        s3_bucket = bucket_generator()
        logger.info(f"Retry {retry} with bucket name '{s3_bucket}'")

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
        except Exception as e:
            s3_bucket = None
            logger.error(f"Can't use bucket name '{s3_bucket}': {e}")

    # We might not be able to find suitable bucket in max retries in which case we will simply die
    assert s3_bucket is not None

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

    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> wiretap.destination
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    try:
        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}', Body=json.dumps(data))

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()
        output_records_values = [record.field for record in wiretap.output_records]

        assert len(output_records_values) == 10
        assert output_records_values == data
    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        try:
            aws.delete_s3_data(s3_bucket, s3_key)
        except Exception as e:
            logger.error(f"Can't remove files from bucket {s3_bucket}: {e}")
        finally:
            try:
                client.delete_bucket(Bucket=s3_bucket)
            except Exception as e:
                logger.error(f"Can't delete buckeet: {e}")


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

    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    s3_origin >> wiretap.destination
    s3_origin >= pipeline_finished_executor

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}', Body=json.dumps(data))

        sdc_executor.start_pipeline(s3_origin_pipeline).wait_for_finished()
        output_records_values = [record.field for record in wiretap.output_records]

        assert len(output_records_values) == 10
        assert output_records_values == data
    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        aws.delete_s3_data(s3_bucket, s3_key)


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

    events_wiretap = builder.add_wiretap()
    records_wiretap = builder.add_wiretap()

    s3_origin >> records_wiretap.destination
    s3_origin >= events_wiretap.destination

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}', Body=json.dumps(data))

        sdc_executor.start_pipeline(s3_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(s3_origin_pipeline, 'output_record_count', 1)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        output_records_values = [record.field for record in records_wiretap.output_records]
        assert len(output_records_values) == 10
        assert output_records_values == data

        # We have exactly one output record, check that it is a new-file event
        event_record = events_wiretap.output_records[0]
        event_type = event_record.header.values['sdc.event.type']
        assert event_type == 'new-file', 'Received %s as event type (expected new-file)' % event_type
    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        aws.delete_s3_data(s3_bucket, s3_key)


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
    wiretap = builder.add_wiretap()

    s3_origin >> trash
    s3_origin >= wiretap.destination

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    # This is not supposed to fail under any context
    s3_origin_pipeline.configuration['shouldRetry'] = False

    # Add built pipeline to the DC executor
    sdc_executor.add_pipeline(s3_origin_pipeline)

    try:
        sdc_executor.start_pipeline(s3_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(s3_origin_pipeline, 'output_record_count', 1)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        history = sdc_executor.get_pipeline_history(s3_origin_pipeline)
        # If we are here this means that a no-more-data was sent

        # Check that the number of records is correct
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert input_records == 0, 'Observed %d input records (expected 0)' % input_records
        # We expect 2 since wiretap duplicates it
        assert output_records == 2, 'Observed %d output records (expected 2)' % output_records

        # We have exactly one output record, check that it is a no-more-data event
        assert len(wiretap.output_records) == 1, 'Received %d event records (expected 1)' % len(wiretap.output_records)
        event_record = wiretap.output_records[0]
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
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)


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
        aws.delete_s3_data(aws.s3_bucket_name, s3_key)


@stub
@aws('s3')
def test_data_format_avro(sdc_builder, sdc_executor, aws):
    pass


@aws('s3')
@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ['UNIVOCITY', 'LEGACY_PARSER'])
def test_data_format_delimited(sdc_builder, sdc_executor, aws, csv_parser):
    DATA = "A,B,C\n" \
           "1,2,3\n" \
           "10,20,30\n"
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string()}/sdc'

    # Build pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')

    s3_origin.set_attributes(bucket=s3_bucket,
                             data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             csv_parser=csv_parser,
                             prefix_pattern=f'{s3_key}*')

    wiretap = builder.add_wiretap()

    s3_origin >> wiretap.destination

    s3_origin_pipeline = builder.build().configure_for_environment(aws)
    s3_origin_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # Insert objects into S3.
        client.put_object(Bucket=s3_bucket, Key=f'{s3_key}', Body=DATA)

        sdc_executor.start_pipeline(s3_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(s3_origin_pipeline, 'output_record_count', 2)
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        records = wiretap.output_records
        assert len(records) == 2

        assert records[0].field['A'] == "1"
        assert records[0].field['B'] == "2"
        assert records[0].field['C'] == "3"
        assert records[1].field['A'] == "10"
        assert records[1].field['B'] == "20"
        assert records[1].field['C'] == "30"
    finally:
        if sdc_executor.get_pipeline_status(s3_origin_pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(s3_origin_pipeline)
        # Clean up S3.
        aws.delete_s3_data(s3_bucket, s3_key)

@stub
@aws('s3')
def test_data_format_excel(sdc_builder, sdc_executor, aws):
    pass


@stub
@aws('s3')
def test_data_format_json(sdc_builder, sdc_executor, aws):
    pass


@stub
@aws('s3')
def test_data_format_log(sdc_builder, sdc_executor, aws):
    pass


@stub
@aws('s3')
def test_data_format_protobuf(sdc_builder, sdc_executor, aws):
    pass


@stub
@aws('s3')
def test_data_format_text(sdc_builder, sdc_executor, aws):
    pass


@stub
@aws('s3')
def test_data_format_sdc_record(sdc_builder, sdc_executor, aws):
    pass


@stub
@aws('s3')
def test_data_format_whole_file(sdc_builder, sdc_executor, aws):
    pass


@stub
@aws('s3')
def test_data_format_xml(sdc_builder, sdc_executor, aws):
    pass
