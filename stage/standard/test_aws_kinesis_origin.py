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
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


# Reference https://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html
KINESIS_STREAM_NAMES = [
    ('minsize', lambda: get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 128)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('uppercase', lambda: get_random_string(string.ascii_uppercase)),
    ('hypen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('underscore', lambda: get_random_string(string.ascii_lowercase) + '_' + get_random_string(string.ascii_lowercase)),
    ('period', lambda: get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits))
]


@aws('kinesis')
def test_data_types(sdc_builder, sdc_executor, aws):
    pytest.skip("AWS Kinesis doesn't talk to a structured system, so we don't need to test each data type.")


@aws('kinesis')
def test_dataflow_event(sdc_builder, sdc_executor, aws):
    pytest.skip('AWS Kinesis does not produce events.')


@aws('kinesis')
@pytest.mark.parametrize('test_name, stream_generator', KINESIS_STREAM_NAMES, ids=[i[0] for i in KINESIS_STREAM_NAMES])
def test_stream_name(sdc_builder, sdc_executor, aws, test_name, stream_generator):
        """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
        having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
        published at Kinesis client and what we read in the pipeline. The pipeline looks like:

        Kinesis Consumer pipeline:
            kinesis_consumer >> wiretap
        """
        # build consumer pipeline
        application_name = get_random_string(string.ascii_letters, 10)
        stream_name = f'{aws.kinesis_stream_prefix}_{stream_generator()}'

        # if stream is longer than 128 then select first 128 characters
        stream_name = stream_name[0:128]

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
def test_multiple_batches(sdc_builder, sdc_executor, aws):
        """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
        having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
        published at Kinesis client and what we read in the pipeline. Batch size is configured to deal with more than
        one batch. The pipeline looks like:

        Kinesis Consumer pipeline:
            kinesis_consumer >> wiretap

        """
        # build consumer pipeline
        application_name = get_random_string(string.ascii_letters, 10)
        stream_name = f'{aws.kinesis_stream_prefix}_{get_random_string(string.ascii_lowercase)}'


        builder = sdc_builder.get_pipeline_builder()
        builder.add_error_stage('Discard')

        kinesis_consumer = builder.add_stage('Kinesis Consumer')
        kinesis_consumer.set_attributes(application_name=application_name, data_format='TEXT',
                                        initial_position='TRIM_HORIZON',
                                        stream_name=stream_name,
                                        max_batch_size_in_messages=50)

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

            expected_messages = set('Message {0}'.format(i) for i in range(100))
            # not using PartitionKey logic and hence assign some temp key
            put_records = [{'Data': exp_msg, 'PartitionKey': '111'} for exp_msg in expected_messages]
            client.put_records(Records=put_records, StreamName=stream_name)

            # messages are published, read through the pipeline and assert
            sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(102)
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
def test_resume_offset(sdc_builder, sdc_executor, aws):
        """Test for Kinesis consumer origin stage. We do so by publishing data to a test stream using Kinesis client and
        having a pipeline which reads that data using Kinesis consumer origin stage. Data is then asserted for what is
        published at Kinesis client and what we read in the pipeline. Data in kinesis is added, pipeline is stopped,
        more data is added. The pipeline looks like:

        Kinesis Consumer pipeline:
            kinesis_consumer >> wiretap

        """
        # build consumer pipeline
        application_name = get_random_string(string.ascii_letters, 10)
        stream_name = f'{aws.kinesis_stream_prefix}_{get_random_string(string.ascii_lowercase)}'


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

            expected_messages_0 = set('Message {0}'.format(i) for i in range(10))
            # not using PartitionKey logic and hence assign some temp key
            put_records = [{'Data': exp_msg, 'PartitionKey': '111'} for exp_msg in expected_messages_0]
            client.put_records(Records=put_records, StreamName=stream_name)

            # messages are published, read through the pipeline
            sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
            sdc_executor.stop_pipeline(consumer_origin_pipeline)

            expected_messages_1 = set('Message {0}'.format(i) for i in range(10,20))
            # not using PartitionKey logic and hence assign some temp key
            put_records = [{'Data': exp_msg, 'PartitionKey': '111'} for exp_msg in expected_messages_1]
            client.put_records(Records=put_records, StreamName=stream_name)

            # messages are published, read through the pipeline and assert
            sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_output_records_count(11)
            sdc_executor.stop_pipeline(consumer_origin_pipeline)

            output_records = [record.field['text'].value
                              for record in wiretap.output_records]

            assert set(output_records) == expected_messages_0 | expected_messages_1
        finally:
            _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
            logger.info('Deleting %s Kinesis stream on AWS ...', stream_name)
            client.delete_stream(StreamName=stream_name)  # Stream operations are done. Delete the stream.
            logger.info('Deleting %s DynamoDB table on AWS ...', application_name)
            aws.dynamodb.delete_table(TableName=application_name)


def _ensure_pipeline_is_stopped(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)