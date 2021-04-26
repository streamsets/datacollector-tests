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

import logging
import string

from streamsets.testframework.markers import aws
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

SERVICE_ENDPOINT_FORMAT = '{}.{}.amazonaws.com'


@aws('kinesis')
def test_kinesis_producer(sdc_builder, sdc_executor, aws):
    """Test for Kinesis producer target stage. We do so by publishing data to a test stream using Kinesis producer
    stage. Then we stop the pipeline and then read the data from that stream using Kinesis client. We assert the
    data from the client to what has been ingested by the producer pipeline. Then we add more data, stop the pipelina
    and we assert the second batch data was readed. The pipeline looks like:

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
        _ensure_pipeline_is_stopped(sdc_executor, producer_dest_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)


@aws('kinesis')
def test_kinesis_producer_other_region(sdc_builder, sdc_executor, aws):
    """Test for Kinesis producer target stage using other as region and service endpoint.
    We do so by publishing data to a test stream using Kinesis producer stage. Then we stop the pipeline and then
    read the data from that stream using Kinesis client.
    The region is set to other and the service endpoint for kinesis is used.
    We assert the data from the client to what has been ingested by the producer pipeline. Then we add more data, stop
    the pipeline and we assert the second batch data was readed. The pipeline looks like:

    Kinesis Producer pipeline:
        dev_raw_data_source >> kinesis_producer
    """
    endpoint = SERVICE_ENDPOINT_FORMAT.format('kinesis', aws.region)

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

        builder = sdc_builder.get_pipeline_builder()
        builder.add_error_stage('Discard')

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                      raw_data=raw_str)
        kinesis_producer = builder.add_stage('Kinesis Producer')
        kinesis_producer.set_attributes(data_format='TEXT', stream_name=stream_name)

        dev_raw_data_source >> kinesis_producer
        producer_dest_pipeline = builder.build().configure_for_environment(aws)
        kinesis_producer.set_attributes(region='OTHER', endpoint=endpoint)

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
        _ensure_pipeline_is_stopped(sdc_executor, producer_dest_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)


def _ensure_pipeline_is_stopped(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)


def get_kinesis_producer_pipeline(sdc_builder, aws, stream_name, message, pipeline_suffix=''):
    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=message)
    kinesis_producer = builder.add_stage('Kinesis Producer')
    kinesis_producer.set_attributes(data_format='TEXT', stream_name=stream_name)

    dev_raw_data_source >> kinesis_producer
    producer_dest_pipeline = builder.build(
        title=f'Kinesis Producer Pipeline {pipeline_suffix}').configure_for_environment(aws)

    return producer_dest_pipeline
