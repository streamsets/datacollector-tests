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
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


# Reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html
QUEUE_NAMES = [
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('letters', get_random_string(string.ascii_letters)),
    ('digits', get_random_string(string.digits)),
    ('hexadecimal', get_random_string(string.hexdigits).lower()),
    ('hypen', get_random_string() + '-' + get_random_string()),
    ('start_hypen', '-' + get_random_string()),
    ('end_hypen', get_random_string() + '-'),
    ('underscore', get_random_string() + '_' + get_random_string()),
    ('start_underscore', get_random_string() + '_'),
    ('end_underscore', '_' + get_random_string()),
    ('max_length', get_random_string(string.ascii_lowercase, 80)),
]


@aws('sqs')
def test_data_types(sdc_builder, sdc_executor, aws):
    pytest.skip("SQS Origin doesn't talk to a structured system, so we don't need to test each data type.")


@aws('sqs')
@pytest.mark.parametrize('test_name,queue_name', QUEUE_NAMES, ids=[t[0] for t in QUEUE_NAMES])
def test_object_names_queue(sdc_builder, sdc_executor, aws, test_name, queue_name, keep_data):
    message = 'Hello World from SDC & DPM!'

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Amazon SQS Consumer')
    origin.data_format = 'TEXT'
    origin.queue_name_prefixes = [queue_name]

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.sqs
    logger.info('Creating %s SQS queue on AWS ...', queue_name)
    queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']
    try:
        _send_message(client, queue_url, message)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['text'] == message
    finally:
        if not keep_data:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


@aws('sqs')
def test_dataflow_events(sdc_builder, sdc_executor, cluster):
    pytest.skip('SQS Origin does not generate events.')


@aws('sqs')
def test_resume_offset(sdc_builder, sdc_executor, aws, keep_data):
    queue_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Amazon SQS Consumer')
    origin.data_format = 'TEXT'
    origin.queue_name_prefixes = [queue_name]

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.sqs
    logger.info('Creating %s SQS queue on AWS ...', queue_name)
    queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']
    try:
        _send_message(client, queue_url, "1")

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['text'] == "1"

        # Second try with a new message
        _send_message(client, queue_url, "2")
        wiretap.reset()

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['text'] == "2"
    finally:
        if not keep_data:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


@aws('sqs')
def test_data_format(sdc_builder, sdc_executor, aws, keep_data):
    pytest.skip("AWS SQS Origin doesn't deal with data formats")


@aws('sqs')
def test_multiple_batch(sdc_builder, sdc_executor, aws, keep_data):
    messages = 10_000
    queue_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Amazon SQS Consumer')
    origin.data_format = 'TEXT'
    origin.queue_name_prefixes = [queue_name]

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(pipeline)

    client = aws.sqs
    logger.info('Creating %s SQS queue on AWS ...', queue_name)
    queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']

    try:
        # Generate all the required messages
        entries = []
        for i in range(0, messages):
            entries.append({'Id': get_random_string(), 'MessageBody': str(i)})

        i = 0
        while i < messages:
            batch = entries[i:i+10]

            logger.info(f"Sending 10 messages starting with offset {i}")
            sent_response = client.send_message_batch(QueueUrl=queue_url, Entries=batch)
            if len(sent_response.get('Successful', [])) != len(batch):
                raise Exception(f'Test messages not successfully sent to the queue {queue_url}, only {len(sent_response.get("Successful", []))} received')

            i = i + 10

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', messages, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == messages

        # Verify each record
        def sortFunc(entry):
            return int(entry.field['text'].value)

        records.sort(key=sortFunc)

        expected_number = 0
        for record in records:
            assert record.field['text'] == str(expected_number)
            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


def _send_message(client, queue_url, message):
    sent_response = client.send_message_batch(QueueUrl=queue_url, Entries=[{'Id': get_random_string(), 'MessageBody': message}])
    if len(sent_response.get('Successful', [])) != 1:
        raise Exception('Test messages not successfully sent to the queue %s', queue_url)