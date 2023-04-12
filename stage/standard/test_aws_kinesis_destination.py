# Copyright 2021 StreamSets Inc.
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

import pytest
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import aws
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# Reference https://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html
KINESIS_STREAM_NAMES = [
    ('minsize', get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', get_random_string(string.ascii_lowercase, 128)),
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('hypen', get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('underscore', get_random_string(string.ascii_lowercase) + '_' + get_random_string(string.ascii_lowercase)),
    ('period', get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('digits', get_random_string(string.digits))
]


@aws('kinesis')
def test_data_types(sdc_builder, sdc_executor, aws):
    pytest.skip("AWS Kinesis isn't a typed data store")


@aws('kinesis')
@pytest.mark.parametrize('test_name, stream_generator', KINESIS_STREAM_NAMES, ids=[i[0] for i in KINESIS_STREAM_NAMES])
def test_stream_name(sdc_builder, sdc_executor, aws, test_name, stream_generator, keep_data):
    """Test for Kinesis producer destination stage. We do so by generating data with dev_raw_data_source
    and publishing it to a Kinesis stream using Kinesis producer.
    Data is then asserted for what is produced by dev raw data source and what we read from the stream.
    The pipeline looks like:
        dev_raw_data_source >> kinesis_producer
    """
    builder = sdc_builder.get_pipeline_builder()

    # Create dev_raw_data_source with 10 messages
    expected_data = [f'Hello {i}' for i in range(10)]
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='TEXT',
                          raw_data='\n'.join(expected_data),
                          stop_after_first_batch=True)

    pipeline = None

    # Create Kinesis stream and capture the ShardId
    client = aws.kinesis
    try:

        # Create Kinesis Stream
        stream_name = f'{aws.kinesis_stream_prefix}_{stream_generator}'
        # if stream is longer than 128 then select first 128 characters
        stream_name = stream_name[0:128]
        logger.info('Creating %s Kinesis stream on AWS ...', stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')
        desc_response = client.describe_stream(StreamName=stream_name)
        shard_id = desc_response['StreamDescription']['Shards'][0]['ShardId']

        # Create Kinesis Producer
        kinesis_producer = builder.add_stage('Kinesis Producer')
        kinesis_producer.set_attributes(data_format='TEXT',
                                        stream_name=stream_name,
                                        record_separator='',
                                        preserve_record_order=True,
                                        kinesis_producer_configuration=[
                                            {'key': 'AggregationEnabled', 'value': 'false'}])

        source >> kinesis_producer

        pipeline = builder.build().configure_for_environment(aws)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # read data from Kinesis to assert it is what got ingested into the pipeline
        shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                                   ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')
        response = client.get_records(ShardIterator=shard_iterator['ShardIterator'])
        msgs_received = [rec['Data'].decode().strip() for rec in response['Records']]

        logger.debug('Number of messages received from Kinesis = %d', (len(msgs_received)))

        assert msgs_received == expected_data

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
            client.delete_stream(StreamName=stream_name)


@aws('kinesis')
def test_dataflow_events(sdc_builder, sdc_executor, aws):
    pytest.skip('Kinesis Destination does not produce events')


@aws('kinesis')
def test_multiple_batch(sdc_builder, sdc_executor, aws, keep_data):
    """Test for Kinesis producer destination stage. We do so by generating data with dev_data_generator
    and publishing it to a Kinesis stream using Kinesis producer.
    Data is then asserted for what is produced by dev_raw_data_generator using wiretap and what we read from the stream.
    The pipeline looks like:
        dev_raw_data_source >> [kinesis_producer, wiretap]
    """
    builder = sdc_builder.get_pipeline_builder()

    # Create dev_data_generator with 10 messages
    BATCH_SIZE = 3
    BATCHES = 2
    origin = builder.add_stage('Dev Data Generator')
    origin.set_attributes(batch_size=BATCH_SIZE, delay_between_batches=0,
                          fields_to_generate=[{
                              "type": "CODE_IMEI",
                              "field": "text"
                          }])

    # Create Kinesis stream and capture the ShardId
    client = aws.kinesis

    try:

        # Create Kinesis Stream
        stream_name = f'{aws.kinesis_stream_prefix}_{get_random_string()}'
        logger.info('Creating %s Kinesis stream on AWS ...', stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')
        desc_response = client.describe_stream(StreamName=stream_name)
        shard_id = desc_response['StreamDescription']['Shards'][0]['ShardId']

        # Create Kinesis Producer - Aggregation disabled avoids records to be compressed using protobuf
        # https://github.com/awslabs/amazon-kinesis-producer/issues/80 shows how to configure Kinesis
        # to avoid compression.
        kinesis_producer = builder.add_stage('Kinesis Producer')
        kinesis_producer.set_attributes(data_format='TEXT',
                                        binary_field_path='/text',
                                        stream_name=stream_name,
                                        record_separator='',
                                        preserve_record_order=True,
                                        kinesis_producer_configuration=[
                                            {'key': 'AggregationEnabled', 'value': 'false'}])

        wiretap = builder.add_wiretap()

        origin >> [kinesis_producer, wiretap.destination]

        pipeline = builder.build().configure_for_environment(aws)
        pipeline.configuration['rateLimit'] = 1

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', BATCH_SIZE * BATCHES, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # read data from Kinesis to assert it is what got ingested into the pipeline
        shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                                   ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')["ShardIterator"]

        out = client.get_records(ShardIterator=shard_iterator)

        # Records read using kinesis client are retrieved and decoded
        response = [rec['Data'].decode() for rec in out['Records']]

        logger.debug('Number of messages received from Kinesis = %d', (len(response)))
        assert response == [record.field['text'].value for record in wiretap.output_records]

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
            client.delete_stream(StreamName=stream_name)


@aws('kinesis')
def test_push_pull(sdc_builder, sdc_executor, cluster):
    pytest.skip(
        "We haven't re-implemented this test since Dev Data Generator (push) is part of test_multiple_batches and "
        "Dev Raw Data Source (pull) is part of test_data_types.")


@stub
def test_data_format_binary(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_delimited(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_json(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_protobuf(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_text(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_sdc_record(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_xml(sdc_builder, sdc_executor):
    pass
