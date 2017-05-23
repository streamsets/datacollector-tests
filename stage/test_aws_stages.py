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

import logging
import string

from testframework.markers import aws
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    been injected by the producer pipeline. The pipeline looks like:

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
