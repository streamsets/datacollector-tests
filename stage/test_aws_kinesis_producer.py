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
import os
import string

import pytest
from streamsets.testframework.markers import aws, sdc_min_version, emr_external_id
from streamsets.testframework.utils import get_random_string, Version

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

        if Version(sdc_builder.version) < Version('6.1.0'):
            kinesis_producer.set_attributes(region='OTHER',
                                            endpoint=endpoint)
        else:
            kinesis_producer.set_attributes(region_definition_for_kinesis='SPECIFY_REGIONAL_ENDPOINT',
                                            regional_endpoint_for_kinesis=endpoint)

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
def test_kinesis_preserve_record_order(sdc_builder, sdc_executor, aws, keep_data):
    """
    Test the 'Preserve Record Order' property on Kinesis producer destination stages. We do so by generating data with
    Dev Raw Data Source and publishing it to a Kinesis stream using Kinesis Producer and checking it has been published
    in the same order.

    The pipeline looks like:
        dev_raw_data_source >> kinesis_producer
    """
    expected_data = [f'Hello {i}' for i in range(100)]
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data='\n'.join(expected_data),
        stop_after_first_batch=True
    )

    kinesis_producer = builder.add_stage('Kinesis Producer')
    kinesis_producer.set_attributes(
        data_format='TEXT',
        stream_name=stream_name,
        record_separator='',
        preserve_record_order=True,
        kinesis_producer_configuration=[{'key': 'AggregationEnabled', 'value': 'false'}]
    )

    dev_raw_data_source >> kinesis_producer
    pipeline = builder.build().configure_for_environment(aws)

    client = aws.kinesis
    try:
        logger.info(f'Creating a Kinesis Stream {stream_name} on AWS ...')
        client.create_stream(
            StreamName=stream_name,
            ShardCount=1
        )
        aws.wait_for_stream_status(
            stream_name=stream_name,
            status='ACTIVE'
        )
        desc_response = client.describe_stream(
            StreamName=stream_name
        )

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info(f'Reading the data from the Kinesis Stream ...')
        shard_iterator = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=desc_response['StreamDescription']['Shards'][0]['ShardId'],
            ShardIteratorType='TRIM_HORIZON'
        )
        response = client.get_records(
            ShardIterator=shard_iterator['ShardIterator']
        )
        received_data = [rec['Data'].decode().strip() for rec in response['Records']]

        logger.debug(f'Number of messages received from Kinesis = {len(received_data)}')
        assert received_data == expected_data

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, pipeline)
        if not keep_data:
            logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
            client.delete_stream(StreamName=stream_name)


@aws('kinesis')
def test_kinesis_too_large_record(sdc_builder, sdc_executor, aws, keep_data):
    """
    Test the Kinesis producer destination stage cannot produce a record too large and the pertinent error record is
    created in such case. We do so by generating a file with 2 correct records and one that is too large, reading them
    with a Directory stage and publishing it to a Kinesis stream using Kinesis Producer. We then check the correct
    records have been successfully produced and a record error has been created for the incorrect record.

    The pipeline looks like:
        Directory >> [Kinesis Producer, Wiretap]
    """
    record_1_content = 'Hello 1'
    record_2_content = 'Hello ' + '2' * 1024 * 1024
    record_3_content = 'Hello 3'
    file_content = f'{record_1_content}\n{record_2_content}\n{record_3_content}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data=file_content,
        stop_after_first_batch=True,
        max_line_length=len(record_2_content)
    )
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    kinesis_producer = pipeline_builder.add_stage('Kinesis Producer')
    kinesis_producer.set_attributes(
        data_format='TEXT',
        stream_name=stream_name,
        record_separator='',
        preserve_record_order=True,
        kinesis_producer_configuration=[{'key': 'AggregationEnabled', 'value': 'false'}]
    )

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [kinesis_producer, wiretap.destination]
    pipeline = pipeline_builder.build().configure_for_environment(aws)

    client = aws.kinesis
    try:
        logger.info(f'Creating a Kinesis Stream {stream_name} on AWS...')
        client.create_stream(
            StreamName=stream_name,
            ShardCount=1
        )
        aws.wait_for_stream_status(
            stream_name=stream_name,
            status='ACTIVE'
        )
        desc_response = client.describe_stream(
            StreamName=stream_name
        )

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info(f'Reading the data from the Kinesis Stream...')
        shard_iterator = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=desc_response['StreamDescription']['Shards'][0]['ShardId'],
            ShardIteratorType='TRIM_HORIZON'
        )
        response = client.get_records(
            ShardIterator=shard_iterator['ShardIterator']
        )
        received_data = [rec['Data'].decode().strip() for rec in response['Records']]
        assert len(received_data) == 2
        assert received_data[0] == record_1_content
        assert received_data[1] == record_3_content

        error_records = wiretap.error_records
        assert len(error_records) == 1
        assert error_records[0].header['errorCode'] == 'KINESIS_08'

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, pipeline)
        if not keep_data:
            logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
            client.delete_stream(StreamName=stream_name)


@aws('kinesis')
@emr_external_id
@sdc_min_version('5.9.0')
def test_kinesis_producer_assume_role_with_external_id(sdc_builder, sdc_executor, aws):
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

        builder = sdc_builder.get_pipeline_builder()
        builder.add_error_stage('Discard')

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                      raw_data=raw_str)
        kinesis_producer = builder.add_stage('Kinesis Producer')
        kinesis_producer.set_attributes(data_format='TEXT',
                                        stream_name=stream_name,
                                        set_session_tags=False)

        dev_raw_data_source >> kinesis_producer
        producer_dest_pipeline = builder.build(
            title=f'Kinesis Producer Pipeline using assume role with external ID').configure_for_environment(aws)

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

        assert msgs_received == [raw_str] * msgs_sent_count, "The number of messages received it isn't the same"
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, producer_dest_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)


@aws('kinesis')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('specify_region', [
    'use_region',
    'use_custom_region',
    'use_regional_endpoint',
    'use_regional_vpc_endpoint',
    'use_custom_endpoint_and_signing_region',
    'use_custom_endpoint_and_custom_signing_region'
])
def test_kinesis_producer_different_kinesis_region_definition_types(sdc_builder, sdc_executor, aws, specify_region):
    """Test for Kinesis producer target stage using kinesis region definition types

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

        builder = sdc_builder.get_pipeline_builder()
        builder.add_error_stage('Discard')

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                      raw_data=raw_str)
        kinesis_producer = builder.add_stage('Kinesis Producer')
        kinesis_producer.set_attributes(data_format='TEXT', stream_name=stream_name)

        dev_raw_data_source >> kinesis_producer
        producer_dest_pipeline = builder.build().configure_for_environment(aws)

        if specify_region == 'use_region':
            kinesis_producer.set_attributes(region_definition_for_kinesis='SPECIFY_REGION',
                                            region_for_kinesis=aws.formatted_region)
        if specify_region == 'use_custom_region':
            kinesis_producer.set_attributes(region_definition_for_kinesis='SPECIFY_REGION',
                                            region_for_kinesis='OTHER',
                                            custom_region_for_kinesis=aws.region)
        elif specify_region == 'use_regional_endpoint':
            kinesis_producer.set_attributes(region_definition_for_kinesis="SPECIFY_REGIONAL_ENDPOINT",
                                            regional_endpoint_for_kinesis=f'kinesis.{aws.region}.amazonaws.com')
        elif specify_region == 'use_regional_vpc_endpoint':
            kinesis_producer.set_attributes(region_definition_for_kinesis="SPECIFY_REGIONAL_ENDPOINT",
                                            regional_endpoint_for_kinesis=aws.aws_kinesis_vpce_endpoint)
        elif specify_region == 'use_custom_endpoint_and_signing_region':
            kinesis_producer.set_attributes(region_definition_for_kinesis="SPECIFY_NON_REGIONAL_ENDPOINT",
                                            custom_endpoint_for_kinesis=aws.aws_kinesis_vpce_endpoint,
                                            signing_region_for_kinesis=aws.formatted_region)
        elif specify_region == 'use_custom_endpoint_and_custom_signing_region':
            kinesis_producer.set_attributes(region_definition_for_kinesis="SPECIFY_NON_REGIONAL_ENDPOINT",
                                            custom_endpoint_for_kinesis=aws.aws_kinesis_vpce_endpoint,
                                            signing_region_for_kinesis="OTHER",
                                            custom_signing_region_for_kinesis=aws.region)

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
