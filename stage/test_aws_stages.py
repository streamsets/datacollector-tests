# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

from datetime import datetime
import logging
import string
import time

from testframework.markers import aws
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'

@aws
def test_kinesis_consumer(sdc_builder, sdc_executor, aws):
    """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
    having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
    published at Kinesis client and what we read in the pipeline snapshot. The pipeline looks like:

    Kinesis Consumer pipeline:
        kinesis_consumer >> trash
    """
    #build consumer pipeline
    application_name = get_random_string(string.ascii_letters, 10)
    stream_name = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kinesis_consumer = builder.add_stage('Kinesis Consumer')
    kinesis_consumer.set_attributes(application_name=application_name, data_format='TEXT',
                                    initial_position_in_stream='TRIM_HORIZON',
                                    stream_name=stream_name)

    trash = builder.add_stage('Trash')

    kinesis_consumer >> trash

    consumer_origin_pipeline = builder.build(title='Kinesis Consumer pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    #run pipeline and capture snapshot
    client = aws.kinesis
    try:
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')

        expected_messages = set('Message {0}'.format(i) for i in range(10))
        # not using PartitionKey logic and hence assign some temp key
        put_records = [{'Data': exp_msg, 'PartitionKey': '111'} for exp_msg in expected_messages]
        client.put_records(Records=put_records, StreamName=stream_name)

        # messages are published, read through the pipeline and assert
        snapshot = sdc_executor.capture_snapshot(consumer_origin_pipeline,
                                                 start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.value['value']['text']['value']
                          for record in snapshot[kinesis_consumer.instance_name].output]

        assert set(output_records) == expected_messages
    finally:
        client.delete_stream(StreamName=stream_name) # Stream operations are done. Delete the stream.


@aws
def test_kinesis_producer(sdc_builder, sdc_executor, aws):
    """Test for Kinesis producer target stage. We do so by publishing data to a test stream using Kinesis producer
    stage and then read the data from that stream using Kinesis client. We assert the data from the client to what has
    been ingested by the producer pipeline. The pipeline looks like:

    Kinesis Producer pipeline:
        dev_raw_data_source >> kinesis_producer
    """
    # build producer pipeline
    stream_name = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    # Create Kinesis stream and capture the ShardId
    client = aws.kinesis
    try:
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')
        desc_response = client.describe_stream(StreamName=stream_name)
        shard_id = desc_response['StreamDescription']['Shards'][0]['ShardId']

        # Build the pipeline
        builder = sdc_builder.get_pipeline_builder()
        builder.add_error_stage('Discard')

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                      raw_data=raw_str)
        kinesis_producer = builder.add_stage('Kinesis Producer')
        kinesis_producer.set_attributes(data_format='TEXT', stream_name=stream_name)

        dev_raw_data_source >> kinesis_producer
        producer_dest_pipeline = builder.build(title='Kinesis Producer pipeline').configure_for_environment(aws)

        # add pipeline and capture pipeline messages to assert
        sdc_executor.add_pipeline(producer_dest_pipeline)
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(producer_dest_pipeline).wait_for_stopped()

        history = sdc_executor.pipeline_history(producer_dest_pipeline)
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
        client.delete_stream(StreamName=stream_name)


@aws('s3')
def test_s3_origin(sdc_builder, sdc_executor, aws):
    """Test for S3 origin stage. We do so by putting data to a test S3 bucket using AWS S3 client and
    having a pipeline which reads that data using S3 origin stage. Data is then asserted for what is
    put by S3 client and what we read in the pipeline snapshot. The pipeline looks like:

    S3 Origin pipeline:
        s3_origin >> trash
    """
    # setup test static
    s3_bucket = aws.s3_bucket_name
    s3_key = '{0}/{1}/sdc'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))
    raw_str = 'Hello World!'
    s3_obj_count = 10

    # build pipeline
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    s3_origin = builder.add_stage('Amazon S3', type='origin')
    # partition_prefix uses ant based pattern
    s3_origin.set_attributes(bucket=s3_bucket, data_format='TEXT', partition_prefix='{0}*'.format(s3_key))

    trash = builder.add_stage('Trash')

    s3_origin >> trash

    s3_origin_pipeline = builder.build(title='Amazon S3 origin pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_origin_pipeline)

    client = aws.s3
    try:
        # use S3 client to put test data
        [client.put_object(Bucket=s3_bucket, Key='{0}{1}'.format(s3_key, i), Body=raw_str) for i in range(s3_obj_count)]

        # read through the pipeline and assert
        snapshot = sdc_executor.capture_snapshot(s3_origin_pipeline,
                                                 start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(s3_origin_pipeline)

        output_records = [record.value['value']['text']['value']
                          for record in snapshot[s3_origin.instance_name].output]

        logger.debug('Number of messages captured by the pipeline = %s', len(output_records))

        assert output_records == [raw_str] * s3_obj_count
    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


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
    # setup test static
    s3_bucket = aws.s3_bucket_name
    s3_key = '{0}/{1}'.format(S3_SANDBOX_PREFIX, get_random_string(string.ascii_letters, 10))
    raw_str = 'Hello World!'
    record_count = 1 # raw_str record size

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=raw_str)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket=s3_bucket, data_format='TEXT', partition_prefix=s3_key)

    dev_raw_data_source >> record_deduplicator >> s3_destination
    record_deduplicator >> to_error

    s3_dest_pipeline = builder.build(title='Amazon S3 destination pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(s3_dest_pipeline)

    client = aws.s3
    try:
        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(s3_dest_pipeline).wait_for_pipeline_output_records_count(record_count)
        sdc_executor.stop_pipeline(s3_dest_pipeline).wait_for_stopped()

        # assert record count to S3 the size of the objects put
        list_s3_objs = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        assert len(list_s3_objs['Contents']) == record_count

        # read data from S3 to assert it is what got ingested into the pipeline
        s3_contents = [client.get_object(Bucket=s3_bucket, Key=s3_content['Key'])['Body'].read().decode().strip()
                       for s3_content in list_s3_objs['Contents']]

        assert s3_contents == [raw_str] * record_count
    finally:
        delete_keys = {'Objects': [{'Key': k['Key']}
                                   for k in client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']]}
        client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


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
    record_count = 1 # random_raw_str record size
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
        sdc_executor.stop_pipeline(firehose_dest_pipeline).wait_for_stopped()

        # wait till data is available in S3. We do so by querying for buffer wait time and sleep till then
        resp = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)
        dests = resp['DeliveryStreamDescription']['Destinations'][0]
        wait_secs = dests['ExtendedS3DestinationDescription']['BufferingHints']['IntervalInSeconds']
        time.sleep(wait_secs+15) # few seconds more to wait to make sure S3 gets the data

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
