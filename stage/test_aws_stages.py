# Copyright 2017 StreamSets Inc.
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

from datetime import datetime
import json
import logging
import string
import time

from streamsets.sdk.models import Configuration
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'


@aws('kinesis')
def test_kinesis_consumer(sdc_builder, sdc_executor, aws):
    """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
    having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
    published at Kinesis client and what we read in the pipeline snapshot. The pipeline looks like:

    Kinesis Consumer pipeline:
        kinesis_consumer >> trash
    """
    # build consumer pipeline
    application_name = get_random_string(string.ascii_letters, 10)
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kinesis_consumer = builder.add_stage('Kinesis Consumer')
    kinesis_consumer.set_attributes(application_name=application_name, data_format='TEXT',
                                    initial_position='TRIM_HORIZON',
                                    stream_name=stream_name)

    trash = builder.add_stage('Trash')

    kinesis_consumer >> trash

    consumer_origin_pipeline = builder.build(title='Kinesis Consumer pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    # run pipeline and capture snapshot
    client = aws.kinesis
    try:
        logger.info('Creating %s Kinesis stream on AWS ...', stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')

        expected_messages = set('Message {0}'.format(i) for i in range(10))
        # not using PartitionKey logic and hence assign some temp key
        put_records = [{'Data': exp_msg, 'PartitionKey': '111'} for exp_msg in expected_messages]
        client.put_records(Records=put_records, StreamName=stream_name)

        # messages are published, read through the pipeline and assert
        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'].value
                          for record in snapshot[kinesis_consumer.instance_name].output]

        assert set(output_records) == expected_messages
    finally:
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


@aws('kinesis')
def test_kinesis_consumer_at_timestamp(sdc_builder, sdc_executor, aws):
    """Test for Kinesis consumer origin stage, with AT_TIMESTAMP option. We do so by:
        - 1. Publishing data to a test stream
        - 2. Wait some time and store current timestamp
        - 3. Publishing new data
        - 4. Using Kinesis client to attempt reading from stored timestamp, passing it to the AT_TIMESTAMP option
        - 5. Assert that only the newest data has been read

     The pipelines look like:

     Kinesis Consumer pipeline: kinesis_consumer >> trash
    """

    # build stream
    application_name = get_random_string()
    stream_name = f'{aws.kinesis_stream_prefix}_{get_random_string()}'

    client = aws.kinesis
    try:
        logger.info('Creating %s Kinesis stream on AWS ...', stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')

        # 1. Publish data to the stream
        put_records = [{'Data': f'First Message {i}', 'PartitionKey': '111'} for i in range(10)]
        client.put_records(Records=put_records, StreamName=stream_name)

        # 2. Wait and store timestamp
        time.sleep(10)
        timestamp = int(time.time())*1000

        # 3. Publish new data
        put_records = [{'Data': f'Second Message {i}', 'PartitionKey': '111'} for i in range(10)]
        client.put_records(Records=put_records, StreamName=stream_name)

        # 4. Build consumer pipeline using timestamp
        builder = sdc_builder.get_pipeline_builder()
        builder.add_error_stage('Discard')
        kinesis_consumer = builder.add_stage('Kinesis Consumer')
        kinesis_consumer.set_attributes(application_name=application_name, data_format='TEXT',
                                        initial_position='AT_TIMESTAMP',
                                        initial_timestamp=timestamp,
                                        stream_name=stream_name)
        trash = builder.add_stage('Trash')
        kinesis_consumer >> trash

        consumer_origin_pipeline = builder.build(title='Kinesis Consumer pipeline').configure_for_environment(aws)
        sdc_executor.add_pipeline(consumer_origin_pipeline)

        # 5. messages are published, read through the pipeline and assert
        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field for record in snapshot[kinesis_consumer.instance_name].output]

        assert all('Second' in str(output_record) for output_record in output_records)
    finally:
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


@aws('kinesis')
def test_kinesis_producer(sdc_builder, sdc_executor, aws):
    """Test for Kinesis producer target stage. We do so by publishing data to a test stream using Kinesis producer
    stage and then read the data from that stream using Kinesis client. We assert the data from the client to what has
    been ingested by the producer pipeline. The pipeline looks like:

    Kinesis Producer pipeline:
        dev_raw_data_source >> kinesis_producer
    """
    # build producer pipeline
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))
    raw_str = 'Hello World!'

    # Create Kinesis stream and capture the ShardId
    client = aws.kinesis
    try:
        logger.info('Creating %s Kinesis stream on AWS ...', stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')
        desc_response = client.describe_stream(StreamName=stream_name)
        shard_id = desc_response['StreamDescription']['Shards'][0]['ShardId']

        producer_dest_pipeline = get_kinesis_producer_pipeline(sdc_builder, aws, stream_name, raw_str)

        # add pipeline and capture pipeline messages to assert
        sdc_executor.add_pipeline(producer_dest_pipeline)
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(producer_dest_pipeline)

        history = sdc_executor.get_pipeline_history(producer_dest_pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

        # read data from Kinesis to assert it is what got ingested into the pipeline
        shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                                   ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')
        response = client.get_records(ShardIterator=shard_iterator['ShardIterator'])
        msgs_received = [response['Records'][i]['Data'].decode().strip()
                         for i in range(msgs_sent_count)]

        logger.debug('Number of messages received from Kinesis = %d', (len(msgs_received)))

        assert msgs_received == [raw_str] * msgs_sent_count
    finally:
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)


def get_kinesis_producer_pipeline(sdc_builder, aws, stream_name, message, pipeline_suffix=''):
    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=message)
    kinesis_producer = builder.add_stage('Kinesis Producer')
    kinesis_producer.set_attributes(data_format='TEXT', stream_name=stream_name)

    dev_raw_data_source >> kinesis_producer
    producer_dest_pipeline = builder.build(title=f'Kinesis Producer Pipeline {pipeline_suffix}').configure_for_environment(aws)

    return producer_dest_pipeline


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
    _run_test_s3_destination(sdc_builder, sdc_executor, aws, False)


@aws('s3', 'kms')
@sdc_min_version('3.9.0')
def test_s3_destination_sse_kms(sdc_builder, sdc_executor, aws):
    """Test for S3 target stage using SSE-KMS. We do so by running a dev raw data source generator to S3 destination
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been ingested by the pipeline; we also verify that the data was in fact encrypted in the server using the KMS. We
    use a record deduplicator processor in between dev raw data source origin and S3 destination in order to determine
    exactly what has been ingested. The pipeline looks like:

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_destination
                                                   >> to_error
    """
    _run_test_s3_destination(sdc_builder, sdc_executor, aws, True)


def _run_test_s3_destination(sdc_builder, sdc_executor, aws, sse_kms):
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
        assert snapshot[identity].output[0].get_field_data('/bucket') == aws.s3_bucket_name
        assert snapshot[identity].output[0].get_field_data('/recordCount') == 1

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_obj_key = client.get_object(Bucket=s3_bucket, Key=list_s3_objs['Contents'][0]['Key'])

        # We're comparing the logic structure (JSON) rather than byte-to-byte to allow for different ordering, ...
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert json.loads(s3_contents) == json.loads(raw_str)

        if sse_kms:
            # assert that the data was stored with SSE using the KMS
            assert s3_obj_key['ServerSideEncryption'] == 'aws:kms'
            assert s3_obj_key['SSEKMSKeyId'] == aws.kms_key_arn
    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


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
@sdc_min_version('2.6.0.1-0002')
def test_s3_executor_create_object(sdc_builder, sdc_executor, aws):
    """Test for S3 executor stage. We do so by running a dev raw data source generator to S3 executor
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been created by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to limit number of objects to one. The pipeline looks like the following:

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_executor
                                                   >> to_error
    """
    # setup test static
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'
    raw_str = f'{{"bucket": "{s3_bucket}", "company": "StreamSets Inc."}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CREATE_NEW_OBJECT',
                               object=s3_key,
                               content='${record:value("/company")}')

    dev_raw_data_source >> record_deduplicator >> s3_executor
    record_deduplicator >> to_error

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    client = aws.s3
    try:
        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_exec_pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(s3_exec_pipeline)

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == 1

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_contents = [client.get_object(Bucket=s3_bucket, Key=s3_content['Key'])['Body'].read().decode().strip()
                       for s3_content in list_s3_objs['Contents']]

        assert s3_contents[0] == 'StreamSets Inc.'
    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('s3')
@sdc_min_version('2.6.0.1-0002')
def test_s3_executor_tag_object(sdc_builder, sdc_executor, aws):
    """Test for S3 executor stage. We do so by running a dev raw data source generator to S3 destination
    sandbox bucket and then reading S3 bucket using STF client to assert data between the client to what has
    been created by the pipeline. We use a record deduplicator processor in between dev raw data source origin
    and S3 destination in order to limit number of objects to one. The pipeline looks like the following:

    S3 Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> s3_executor
                                                   >> to_error
    """
    s3_bucket = aws.s3_bucket_name
    s3_key = f'{S3_SANDBOX_PREFIX}/{get_random_string(string.ascii_letters, 10)}'
    raw_str = f'{{"bucket": "{s3_bucket}", "key": "{s3_key}"}}'

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_str)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CHANGE_EXISTING_OBJECT',
                               object='${record:value("/key")}',
                               tags=Configuration(property_key='key', company='${record:value("/company")}'))

    dev_raw_data_source >> record_deduplicator >> s3_executor
    record_deduplicator >> to_error

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    client = aws.s3
    try:
        # Pre-create the object so that it exists
        client.put_object(Body='Secret Data', Bucket=s3_bucket, Key=s3_key)

        # And run the pipeline for at least one record (rest will be removed by the de-dup)
        sdc_executor.start_pipeline(s3_exec_pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(s3_exec_pipeline)

        tags = client.get_object_tagging(Bucket=s3_bucket, Key=s3_key)['TagSet']
        assert len(tags) == 1

    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


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
                                                                                  raw_data=raw_str)

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CREATE_NEW_OBJECT',
                               object=s3_key,
                               content='${record:value("/company")}')

    dev_raw_data_source >> s3_executor

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    # Read snapshot of the pipeline
    snapshot = sdc_executor.capture_snapshot(s3_exec_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(s3_exec_pipeline)

    # All records should go to error stream.
    input_records = snapshot[dev_raw_data_source.instance_name].output
    stage = snapshot[s3_executor.instance_name]
    assert len(stage.error_records) == len(input_records)


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
                                                                                  raw_data=raw_str)

    s3_executor = builder.add_stage('Amazon S3', type='executor')
    s3_executor.set_attributes(bucket='${record:value("/bucket")}',
                               task='CHANGE_EXISTING_OBJECT',
                               object='${record:value("/key")}',
                               tags=Configuration(property_key='key', company='${record:value("/company")}'))

    dev_raw_data_source >> s3_executor

    s3_exec_pipeline = builder.build(title='Amazon S3 executor pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_exec_pipeline)

    # Read snapshot of the pipeline
    snapshot = sdc_executor.capture_snapshot(s3_exec_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(s3_exec_pipeline)

    # All records should go to error stream.
    input_records = snapshot[dev_raw_data_source.instance_name].output
    stage = snapshot[s3_executor.instance_name]
    assert len(stage.error_records) == len(input_records)


@aws('firehose', 's3')
def test_firehose_destination_to_s3(sdc_builder, sdc_executor, aws):
    """Test for Firehose target stage. This test assumes Firehose is destined to S3 bucket. We run a dev raw data source
    generator to Firehose destination which is pre-setup to put to S3 bucket. We then read S3 bucket using STF client
    to assert data between the client to what has been ingested into the pipeline. The pipeline looks like:

    Firehose Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> firehose_destination
                                                   >> to_error
    """
    s3_client = aws.s3
    firehose_client = aws.firehose

    # setup test static
    s3_bucket = aws.s3_bucket_name
    stream_name = aws.firehose_stream_name
    # json formatted string
    random_raw_str = '{{"text":"{0}"}}'.format(get_random_string(string.ascii_letters, 10))
    record_count = 1  # random_raw_str record size
    s3_put_keys = []

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=random_raw_str)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    firehose_destination = builder.add_stage('Kinesis Firehose')
    firehose_destination.set_attributes(stream_name=stream_name, data_format='JSON')

    dev_raw_data_source >> record_deduplicator >> firehose_destination
    record_deduplicator >> to_error

    firehose_dest_pipeline = builder.build(title='Amazon Firehose destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(firehose_dest_pipeline)

    try:
        # start pipeline and assert
        sdc_executor.start_pipeline(firehose_dest_pipeline).wait_for_pipeline_output_records_count(record_count)
        sdc_executor.stop_pipeline(firehose_dest_pipeline)

        # wait till data is available in S3. We do so by querying for buffer wait time and sleep till then
        resp = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)
        dests = resp['DeliveryStreamDescription']['Destinations'][0]
        wait_secs = dests['ExtendedS3DestinationDescription']['BufferingHints']['IntervalInSeconds']
        time.sleep(wait_secs+15)  # few seconds more to wait to make sure S3 gets the data

        # Firehose S3 object naming http://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html#s3-object-name
        # read data to assert
        list_s3_objs = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=datetime.utcnow().strftime("%Y/%m/%d"))
        for s3_content in list_s3_objs['Contents']:
            akey = s3_content['Key']
            aobj = s3_client.get_object(Bucket=s3_bucket, Key=akey)
            if aobj['Body'].read().decode().strip() == random_raw_str:
                s3_put_keys.append(akey)

        assert len(s3_put_keys) == record_count
    finally:
        # delete S3 objects related to this test
        if len(s3_put_keys) > 0:
            delete_keys = {'Objects': [{'Key': k} for k in s3_put_keys]}
            s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('provisionemr')
@sdc_min_version('3.4.0')
def test_provision_emr_origin_to_s3(sdc_builder, sdc_executor, aws):
    """Test for EMR origin stage. We do so by putting data to a test S3 bucket using AWS S3 client and
    having a pipeline which reads that data using EMR origin stage and writes to S3 destination. Data is then asserted
    from destination S3 bucket to what is put by S3 client.

    An Amazon EMR is created in the pipeline. The pipeline looks like:

    EMR origin pipeline:
        emr_origin >> s3_destination
    """
    # Set configurations so that pipeline can create Amazon EMR.
    pipeline_configs = {'amazonEMRConfig.provisionNewCluster': True,
                        'amazonEMRConfig.clusterPrefix': 'stf_test',
                        'amazonEMRConfig.terminateCluster': True,
                        'amazonEMRConfig.ec2SubnetId': aws.ec2_subnet_id,
                        'amazonEMRConfig.masterSecurityGroup': aws.ec2_security_group,
                        'amazonEMRConfig.slaveSecurityGroup': aws.ec2_security_group,
                        'amazonEMRConfig.instanceCount': 1,
                        'amazonEMRConfig.masterInstanceType': 'M4_LARGE',
                        'amazonEMRConfig.slaveInstanceType': 'M4_LARGE'}
    _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws, pipeline_configs)


@aws('existingemr')
@sdc_min_version('3.4.0')
def test_existing_emr_origin_to_s3(sdc_builder, sdc_executor, aws):
    """Test for EMR origin stage. We do so by putting data to a test S3 bucket using AWS S3 client and
    having a pipeline which reads that data using EMR origin stage and writes to S3 destination.
    Data is then asserted from destination S3 bucket to what is put by S3 client.

    The Amazon EMR with the passed aws-emr-cluster-id is used in the pipeline. The pipeline looks like:

    EMR origin pipeline:
        emr_origin >> s3_destination
    """
    pipeline_configs = {'amazonEMRConfig.clusterId': aws.emr_cluster_id}
    _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws, pipeline_configs)


def _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws, pipeline_configs):
    s3_bucket = aws.emr_s3_bucket_name
    s3_input_key = '{0}/{1}/input'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))
    s3_output_key = '{0}/{1}/output'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))

    s3_staging_bucket = aws.emr_s3_staging_bucket_name
    s3_staging_key = '{0}/{1}/sdc_staging'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))
    s3_logging_key = '{0}/{1}/sdc_logging'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))

    raw_str = 'Hello World!'
    s3_obj_count = 2  # keep it low, so as the number of MR jobs don't spin a lot and take a while lot of time

    logger.info('%s S3 bucket used with input key: %s output key: %s and object count: %s',
                s3_bucket, s3_input_key, s3_output_key, s3_obj_count)
    logger.info('%s S3 staging bucket used with EMR staging key: %s and EMR logging key: %s',
                s3_staging_bucket, s3_staging_key, s3_logging_key)

    # build pipeline
    builder = sdc_builder.get_pipeline_builder()

    emr_origin = builder.add_stage('Hadoop FS', type='origin')
    emr_origin.set_attributes(input_paths=[f's3a://{s3_bucket}/{s3_input_key}'], data_format='TEXT')

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket=s3_bucket, data_format='TEXT', partition_prefix=s3_output_key)

    emr_origin >> s3_destination

    pipeline = builder.build(title='Amazon EMR to S3 pipeline').configure_for_environment(aws)
    configs = {'executionMode': 'EMR_BATCH',
               'amazonEMRConfig.userRegion': aws.sdc_formatted_region,
               'amazonEMRConfig.accessKey': aws.aws_access_key_id,
               'amazonEMRConfig.secretKey': aws.aws_secret_access_key,
               'amazonEMRConfig.s3StagingUri': f's3://{s3_staging_bucket}/{s3_staging_key}',
               'amazonEMRConfig.s3LogUri': f's3://{s3_staging_bucket}/{s3_logging_key}',
               'amazonEMRConfig.enableEMRDebugging': False}
    configs.update(pipeline_configs)
    pipeline.configuration.update(configs)
    sdc_executor.add_pipeline(pipeline)

    client = aws.s3
    try:
        logger.info('Creating input S3 data ...')
        [client.put_object(Bucket=s3_staging_bucket, Key='{0}/{1}'.format(s3_input_key, i), Body=raw_str)
         for i in range(s3_obj_count)]

        # lets not wait for pipeline start, as the transition from START to RUNNING takes more time
        sdc_executor.start_pipeline(pipeline, wait=False).wait_for_finished(timeout_sec=1800)

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_output_key)
        assert len(list_s3_objs['Contents']) == s3_obj_count

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_contents = [client.get_object(Bucket=s3_bucket, Key=s3_content['Key'])['Body'].read().decode().strip()
                       for s3_content in list_s3_objs['Contents']]

        assert s3_contents == [raw_str] * s3_obj_count
    finally:
        logger.info('Deleting input S3 data ...')
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_input_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)

        logger.info('Deleting output S3 data ...')
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_output_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)

@aws('sqs')
@sdc_min_version('3.0.0.0')
def test_standard_sqs_consumer(sdc_builder, sdc_executor, aws):
    """Test for SQS consumer origin stage. We do so by publishing data to a test queue using SQS client and
    having a pipeline which reads that data using SQS consumer origin stage. Data is then asserted for what is
    published at SQS client and what we read in the pipeline snapshot. The pipeline looks like:

    Amazon SQS Consumer pipeline:
        amazon_sqs_consumer >> trash
    """
    queue_name = '{}_{}'.format(aws.sqs_queue_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()
    amazon_sqs_consumer = builder.add_stage('Amazon SQS Consumer')
    amazon_sqs_consumer.set_attributes(data_format='TEXT',
                                       queue_name_prefixes=[queue_name])
    trash = builder.add_stage('Trash')
    amazon_sqs_consumer >> trash

    consumer_origin_pipeline = builder.build(title='Amazon SQS Consumer pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    client = aws.sqs
    logger.info('Creating %s SQS queue on AWS ...', queue_name)
    queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']
    try:
        # note there is a limit of 10 messages only for sending in a batch
        number_of_messages = 10
        message_entries = [{'Id': str(i), 'MessageBody': 'Message {0}'.format(i)} for i in range(number_of_messages)]
        sent_response = client.send_message_batch(QueueUrl=queue_url, Entries=message_entries)
        if len(sent_response.get('Successful', [])) != number_of_messages:
            raise Exception('Test messages not successfully sent to the queue %s', queue_name)

        # messages are published, read through the pipeline and assert
        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        result_data = [str(record.field['text']) for record in snapshot[amazon_sqs_consumer.instance_name].output]
        assert sorted(result_data) == sorted([message['MessageBody'] for message in message_entries])
    finally:
        if queue_url:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


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
    target.set_attributes(bucket=aws.s3_bucket_name, data_format='WHOLE_FILE', partition_prefix=s3_dest_key, file_name_expression='output.txt')

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
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # Validate event generation
        assert len(snapshot[identity].output) == 1
        assert snapshot[identity].output[0].get_field_data('/targetFileInfo/bucket') == aws.s3_bucket_name
        assert snapshot[identity].output[0].get_field_data('/targetFileInfo/objectKey') == f'{s3_dest_key}sdc-output.txt'

        # We should have exactly one file on the destination side
        list_s3_objs = client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_dest_key)
        assert len(list_s3_objs['Contents']) == 1

        # With our secret message
        s3_obj_key = client.get_object(Bucket=aws.s3_bucket_name, Key=list_s3_objs['Contents'][0]['Key'])
        s3_contents = s3_obj_key['Body'].read().decode().strip()
        assert s3_contents == data
    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in
                                   client.list_objects_v2(Bucket=aws.s3_bucket_name, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=aws.s3_bucket_name, Delete=delete_keys)
