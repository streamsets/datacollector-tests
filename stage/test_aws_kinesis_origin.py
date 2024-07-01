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
import time

import pytest
from streamsets.testframework.markers import aws, sdc_min_version, emr_external_id
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

SERVICE_ENDPOINT_FORMAT = '{}.{}.amazonaws.com'


@aws('kinesis')
def test_kinesis_consumer(sdc_builder, sdc_executor, aws):
    """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
    having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
    published at Kinesis client and what we read in the pipeline. The pipeline looks like:

    Kinesis Consumer pipeline:
        kinesis_consumer >> wiretap
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

    wiretap = builder.add_wiretap()

    kinesis_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Kinesis Consumer pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    # run pipeline
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
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


@aws('kinesis')
@sdc_min_version('3.19.0')
@pytest.mark.parametrize('additional_configurations', [
    [],
    [{'key': 'failoverTimeMillis', 'value': '10000'}],
    [{'key': 'taskBackoffTimeMillis', 'value': '500'}],
    [{'key': 'metricsBufferTimeMillis', 'value': '10000'}],
    [{'key': 'metricsMaxQueueSize', 'value': '10000'}],
    [{'key': 'validateSequenceNumberBeforeCheckpointing', 'value': 'true'}],
    [{'key': 'shutdownGraceMillis', 'value': '1'}],
    [{'key': 'billingMode', 'value': 'PROVISIONED'}],
    [{'key': 'timeoutInSeconds', 'value': '50'}],
    [{'key': 'retryGetRecordsInSeconds', 'value': '50'}],
    [{'key': 'maxGetRecordsThreadPool', 'value': '50'}],
    [{'key': 'maxLeaseRenewalThreads', 'value': '20'}],
    [{'key': 'logWarningForTaskAfterMillis', 'value': '50'}],
    [{'key': 'listShardsBackoffTimeInMillis', 'value': '1500'}],
    [{'key': 'maxListShardsRetryAttempts', 'value': '50'}],
    [{'key': 'userAgentPrefix', 'value': ''}],
    [{'key': 'userAgentSuffix', 'value': ''}],
    [{'key': 'maxConnections', 'value': '50'}],
    [{'key': 'requestTimeout', 'value': '0'}],
    [{'key': 'clientExecutionTimeout', 'value': '0'}],
    [{'key': 'throttleRetries', 'value': 'true'}],
    [{'key': 'connectionMaxIdleMillis', 'value': '60000'}],
    [{'key': 'validateAfterInactivityMillis', 'value': '5000'}],
    [{'key': 'useExpectContinue', 'value': 'true'}],
    [{'key': 'maxConsecutiveRetriesBeforeThrottling', 'value': '100'}],
    [{'key': 'retryMode', 'value': 'null'}],
    [{'key': 'cleanupLeasesUponShardCompletion', 'value': 'true'}],
    [{'key': 'a', 'value': '1'}]])
def test_kinesis_consumer_additional_properties(sdc_builder, sdc_executor, aws, additional_configurations):
    """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
    having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
    published at Kinesis client and what we read in the pipeline. The pipeline looks like:

    Kinesis Consumer pipeline:
        kinesis_consumer >> wiretap
    """
    invalid_config = False
    # build consumer pipeline
    application_name = get_random_string(string.ascii_letters, 10)
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kinesis_consumer = builder.add_stage('Kinesis Consumer')
    kinesis_consumer.set_attributes(application_name=application_name, data_format='TEXT',
                                    initial_position='TRIM_HORIZON',
                                    stream_name=stream_name,
                                    kinesis_configuration=additional_configurations)

    wiretap = builder.add_wiretap()

    kinesis_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Kinesis Consumer pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

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
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages
    except Exception as error:
        if additional_configurations[0]['key'] == 'a':
            assert 'KINESIS_24 - Invalid setting for \'' + additional_configurations[0]['key'] + \
                   '\' property' in error.message
            invalid_config = True
        else:
            raise error
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        if not invalid_config:
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

     Kinesis Consumer pipeline: kinesis_consumer >> wiretap
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
        timestamp = int(time.time()) * 1000

        # 3. Publish new data
        expected_messages = set('Second Message {0}'.format(i) for i in range(10))
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
        wiretap = builder.add_wiretap()
        kinesis_consumer >> wiretap.destination

        consumer_origin_pipeline = builder.build(title='Kinesis Consumer pipeline').configure_for_environment(aws)
        sdc_executor.add_pipeline(consumer_origin_pipeline)

        # 5. messages are published, read through the pipeline and assert
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


@aws('kinesis')
@pytest.mark.parametrize('no_of_msg', [1, 5, 10, 20, 35])
def test_kinesis_consumer_stop_resume(sdc_builder, sdc_executor, aws, no_of_msg):
    """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
    having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
    published at Kinesis client and what we read in the pipeline. The pipeline looks like:

    Kinesis Consumer pipeline:
        kinesis_consumer >> wiretap
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
    wiretap = builder.add_wiretap()
    kinesis_consumer >> wiretap.destination
    consumer_origin_pipeline = builder.build(
        title=f'Kinesis Consumer Stop Resume: {no_of_msg}').configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    client = aws.kinesis
    try:
        logger.info('Creating %s Kinesis stream on AWS ...', stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)
        aws.wait_for_stream_status(stream_name=stream_name, status='ACTIVE')

        expected_messages = set('Message {0}'.format(i) for i in range(no_of_msg))
        # not using PartitionKey logic and hence assign some temp key
        put_records = [{'Data': exp_msg, 'PartitionKey': '111'} for exp_msg in expected_messages]
        client.put_records(Records=put_records, StreamName=stream_name)

        # messages are published, read through the pipeline and assert
        # number of batches to be captured is the number of messages + 1
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(no_of_msg + 1)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages

        expected_messages = set('Message B {0}'.format(i) for i in range(no_of_msg))
        # not using PartitionKey logic and hence assign some temp key
        put_records = [{'Data': exp_msg, 'PartitionKey': '111'} for exp_msg in expected_messages]
        client.put_records(Records=put_records, StreamName=stream_name)

        # messages are published, read through the pipeline and assert
        # number of batches to be captured is the number of messages + 1
        wiretap.reset()
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(no_of_msg + 1)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field['text'].value
                          for record in wiretap.output_records]
        assert set(output_records) == expected_messages

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


@aws('kinesis')
def test_kinesis_consumer_other_region(sdc_builder, sdc_executor, aws):
    """Test for Kinesis consumer origin stage using other as region and service endpoint. We do so by publishing data to
     a test stream using Kinesis client and having a pipeline which reads that data using Kinesis consumer origin stage.
     The region is set to other, and the service endpoint for kinesis is used.
     Data is then asserted for what is published at Kinesis client and what we read in the pipeline.
     The pipeline looks like:

    Kinesis Consumer pipeline:
        kinesis_consumer >> wiretap
    """
    endpoint = SERVICE_ENDPOINT_FORMAT.format('kinesis', aws.region)

    # build consumer pipeline
    application_name = get_random_string(string.ascii_letters, 10)
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kinesis_consumer = builder.add_stage('Kinesis Consumer')
    kinesis_consumer.set_attributes(application_name=application_name, data_format='TEXT',
                                    initial_position='TRIM_HORIZON',
                                    stream_name=stream_name)

    wiretap = builder.add_wiretap()
    kinesis_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(aws)
    kinesis_consumer.set_attributes(region='OTHER', endpoint=endpoint)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

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
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)

@aws('kinesis')
@emr_external_id
@sdc_min_version('5.9.0')
def test_kinesis_consumer_assume_role_with_external_id(sdc_builder, sdc_executor, aws):
    # build consumer pipeline
    application_name = get_random_string(string.ascii_letters, 10)
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kinesis_consumer = builder.add_stage('Kinesis Consumer')
    kinesis_consumer.set_attributes(application_name=application_name, data_format='TEXT',
                                    initial_position='TRIM_HORIZON',
                                    stream_name=stream_name,
                                    set_session_tags=False)

    wiretap = builder.add_wiretap()

    kinesis_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build(title='Kinesis Consumer pipeline').configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    # run pipeline
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
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages, "Wrong expected messages for output_records v"
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


@aws('kinesis')
@sdc_min_version('5.10.0')
def test_kinesis_consumer_other_region_and_vpc_endpoint_only_for_kinesis_service(sdc_builder, sdc_executor, aws):
    """Test for Kinesis consumer origin stage using other as region and VPC endpoint. VPC endpoint allows the customer
    to privately connect to supported AWS services and VPC endpoint services. We do this by publishing data to
    a test stream using Kinesis client and having a pipeline which reads that data using Kinesis consumer origin stage.

    The region is set to "OTHER", and the service vpc endpoint service is used for kinesis. VPC endpoint only
    works with Kinesis, not with DynamoDB or CloudWatch. We need to enable use different connection for DynamoDB and
    CloudWatch and specify a new region for each one.
    Data is then asserted for what is published at Kinesis client and what we read in the pipeline.
    The pipeline looks like:

    Kinesis Consumer pipeline:
        kinesis_consumer >> wiretap
    """

    if not aws.kinesis_vpc_endpoint:
        pytest.skip("This test will only run with --aws-kinesis-vpc-endpoint")

    vpc_endpoint = aws.kinesis_vpc_endpoint

    # build consumer pipeline
    application_name = get_random_string(string.ascii_letters, 10)
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()

    kinesis_consumer = builder.add_stage('Kinesis Consumer')
    kinesis_consumer.set_attributes(application_name=application_name,
                                    data_format='TEXT',
                                    initial_position='TRIM_HORIZON',
                                    stream_name=stream_name)

    wiretap = builder.add_wiretap()
    kinesis_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(aws)
    kinesis_consumer.set_attributes(region='OTHER',
                                    assume_role=False,
                                    use_a_different_connection_for_dynamodb=True,
                                    use_a_different_connection_for_cloudwatch=True)

    sdc_executor.add_pipeline(consumer_origin_pipeline)

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
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


@aws('kinesis')
@sdc_min_version('5.10.0')
def test_kinesis_consumer_using_specific_connection_for_dynamodb_and_cloudwatch(sdc_builder, sdc_executor, aws):
    """Test for Kinesis consumer origin stage using other as region and VPC endpoint. VPC endpoint enables customer
        to privately connect to supported AWS services and VPC endpoint services. We do so by publishing data to
        a test stream using Kinesis client and having a pipeline which reads that data using Kinesis consumer origin stage.

    The region is set to "OTHER", and the service vpc endpoint service is used for kinesis. VPC endpoint only
    works with Kinesis, not with DynamoDB or CloudWatch. We need to enable use different connection for DynamoDB and
    CloudWatch and specify a new region set to "OTHER" and adding the corresponding endpoint for each service.
    Data is then asserted for what is published at Kinesis client and what we read in the pipeline.

    The pipeline looks like:

        Kinesis Consumer pipeline:
            kinesis_consumer >> wiretap
    """

    if not aws.kinesis_vpc_endpoint:
        pytest.skip("This test will only run with --aws-kinesis-vpc-endpoint")

    vpc_endpoint = aws.kinesis_vpc_endpoint
    endpoint = SERVICE_ENDPOINT_FORMAT.format('kinesis', aws.region)

    # build consumer pipeline
    application_name = get_random_string(string.ascii_letters, 10)
    stream_name = '{}_{}'.format(aws.kinesis_stream_prefix, get_random_string(string.ascii_letters, 10))

    builder = sdc_builder.get_pipeline_builder()

    kinesis_consumer = builder.add_stage('Kinesis Consumer')
    kinesis_consumer.set_attributes(application_name=application_name,
                                    data_format='TEXT',
                                    initial_position='TRIM_HORIZON',
                                    stream_name=stream_name)

    wiretap = builder.add_wiretap()
    kinesis_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(aws)
    kinesis_consumer.set_attributes(region='OTHER',
                                    assume_role=False,
                                    use_a_different_connection_for_dynamodb=True,
                                    dynamodb_region='OTHER',
                                    use_a_different_connection_for_cloudwatch=True,
                                    cloudwatch_region='OTHER')
    
    sdc_executor.add_pipeline(consumer_origin_pipeline)

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
        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)
        output_records = [record.field['text'].value
                          for record in wiretap.output_records]

        assert set(output_records) == expected_messages
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
        client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
        logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
        aws.dynamodb.delete_table(TableName=application_name)


def _ensure_pipeline_is_stopped(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)
