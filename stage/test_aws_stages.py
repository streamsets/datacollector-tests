# Copyright 2019 StreamSets Inc.
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
import time
from datetime import datetime

import pytest
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import aws, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Sandbox prefix for S3 bucket
S3_SANDBOX_PREFIX = 'sandbox'

MIN_SDC_VERSION_WITH_EXECUTOR_EVENTS = Version('3.4.0')

SERVICE_ENDPOINT_FORMAT = '{}.{}.amazonaws.com'


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
        time.sleep(wait_secs + 15)  # few seconds more to wait to make sure S3 gets the data

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
        _ensure_pipeline_is_stopped(sdc_executor, firehose_dest_pipeline)
        # delete S3 objects related to this test
        if len(s3_put_keys) > 0:
            delete_keys = {'Objects': [{'Key': k} for k in s3_put_keys]}
            s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)


@aws('firehose', 's3')
def test_firehose_destination_to_s3_other_region(sdc_builder, sdc_executor, aws):
    """Test for Firehose target stage with other as region and service endpoint.
    This test assumes Firehose is destined to S3 bucket. We run a dev raw data source generator to Firehose destination
    which is pre-setup to put to S3 bucket.
    The region is set to other and the service endpoint for firehose is used.
    We then read S3 bucket using STF client to assert data between the client to what has been ingested into the pipeline.
    The pipeline looks like:

    Firehose Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> firehose_destination
                                                   >> to_error
    """
    endpoint = SERVICE_ENDPOINT_FORMAT.format('firehose', aws.region)

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

    firehose_dest_pipeline = builder.build().configure_for_environment(aws)
    firehose_destination.set_attributes(region='OTHER', endpoint=endpoint)
    sdc_executor.add_pipeline(firehose_dest_pipeline)

    try:
        # start pipeline and assert
        sdc_executor.start_pipeline(firehose_dest_pipeline).wait_for_pipeline_output_records_count(record_count)
        sdc_executor.stop_pipeline(firehose_dest_pipeline)

        # wait till data is available in S3. We do so by querying for buffer wait time and sleep till then
        resp = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)
        dests = resp['DeliveryStreamDescription']['Destinations'][0]
        wait_secs = dests['ExtendedS3DestinationDescription']['BufferingHints']['IntervalInSeconds']
        time.sleep(wait_secs + 15)  # few seconds more to wait to make sure S3 gets the data

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
        _ensure_pipeline_is_stopped(sdc_executor, firehose_dest_pipeline)
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
    _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws)


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
    _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws)


def _test_emr_origin_to_s3(sdc_builder, sdc_executor, aws):
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
    emr_origin.set_attributes(
        hadoop_fs_uri=f's3a://{s3_bucket}',
        input_paths=[f'/{s3_input_key}'],
        data_format='TEXT'
    )

    s3_destination = builder.add_stage('Amazon S3', type='destination')
    s3_destination.set_attributes(bucket=s3_bucket, data_format='TEXT', partition_prefix=s3_output_key)

    emr_origin >> s3_destination

    pipeline = builder.build(title='Amazon EMR to S3 pipeline').configure_for_environment(aws)
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
        logger.info('Deleting input S3 data from bucket %s with location %s ...', s3_bucket, s3_input_key)
        aws.delete_s3_data(s3_bucket, s3_input_key)

        logger.info('Deleting output S3 data from bucket %s with location %s ...', s3_bucket, s3_output_key)
        aws.delete_s3_data(s3_bucket, s3_output_key)

        logger.info('Deleting staging S3 data from bucket %s with location %s ...', s3_staging_bucket, s3_staging_key)
        aws.delete_s3_data(s3_staging_bucket, s3_staging_key)

        logger.info('Deleting logging S3 data from bucket %s with location %s ...', s3_staging_bucket, s3_logging_key)
        aws.delete_s3_data(s3_staging_bucket, s3_logging_key)


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
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        if queue_url:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


@aws('sqs')
@sdc_min_version('3.0.0.0')
# (10, 10) -> Messages = batch size
# (3. 5), (5, 10) -> batch size > number of messages
# (10, 5) -> batch size < number of messages but exactly divisible
# (5, 3) -> batch size > number of messages but not exactly divisible
@pytest.mark.parametrize('number_of_messages_sent_and_origin_batch_size',
                         [(10, 10), (10, 5), (5, 3), (3, 5), (5, 10), (100, 20)])
def test_standard_sqs_consumer_batch_size(sdc_builder, sdc_executor, aws,
                                          number_of_messages_sent_and_origin_batch_size, keep_data):
    """Test for SQS consumer origin stage with max batch size configuration. We do so by publishing data
     to a test queue using SQS client and having a pipeline which reads that data using SQS consumer origin stage.
     We assert the number of input/output and number of batches. The pipeline looks like:

    Amazon SQS Consumer pipeline:
        amazon_sqs_consumer >> trash
    """
    queue_name = '{}_{}'.format(aws.sqs_queue_prefix, get_random_string(string.ascii_letters, 10))

    number_of_messages, max_batch_size = number_of_messages_sent_and_origin_batch_size

    # number of batches will be 1 if max batch size >= number of messages
    # if number of messages > max_batch_size, we will have more than 1 batch
    # to decide the number of batches, we will basically divide to get the number of batches
    # and if it is not properly divisible (i.e modulo is non zero), we will add one more batch
    number_of_batches = number_of_messages // max_batch_size + int(number_of_messages % max_batch_size > 0)
    logger.info(f'Number of Messages : {number_of_messages}, Batch Size: {max_batch_size}, Number of batches be produced : {number_of_batches}')

    builder = sdc_builder.get_pipeline_builder()
    amazon_sqs_consumer = builder.add_stage('Amazon SQS Consumer')
    amazon_sqs_consumer.set_attributes(data_format='TEXT',
                                       max_batch_size_in_messages=max_batch_size,
                                       number_of_messages_per_request=10,
                                       queue_name_prefixes=[queue_name])

    wiretap = builder.add_wiretap()
    amazon_sqs_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    client = aws.sqs
    logger.info('Creating %s SQS queue on AWS ...', queue_name)
    queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']
    expected = []
    try:
        all_responses = []
        for batch in range(number_of_messages // 10 + int(number_of_messages % 10 > 0)):
            expected.extend([str(i) for i in range(batch * 10, min(number_of_messages, (batch + 1) * 10))])
            message_entries = [{'Id': str(i), f'MessageBody': str(i)}
                               for i in range(batch * 10, min(number_of_messages, (batch + 1) * 10))]
            sent_response = client.send_message_batch(QueueUrl=queue_url, Entries=message_entries)
            for message in sent_response.get('Successful', []):
                all_responses.append(message)
        if len(all_responses) != number_of_messages:
            raise Exception('Test messages not successfully sent to the queue %s', queue_name)

        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_batch_count(number_of_batches)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        # Verify that all records were properly read
        records = wiretap.output_records
        assert len(records) == number_of_messages

        assert sorted(expected) == sorted([r.field['text'].value for r in records])
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        if not keep_data and queue_url:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


@aws('sqs')
@sdc_min_version('3.19.0')
def test_sqs_no_read_access(sdc_builder, sdc_executor, aws):
    """Test for SQS consumer origin stage with a queue where we don't have read access to. The pipeline looks like:

    Amazon SQS Consumer pipeline:
        amazon_sqs_consumer >> trash
    """
    queue_name = f'{aws.sqs_queue_prefix}_{get_random_string()}'

    number_of_messages = 10
    max_batch_size = 10

    # number of batches will be 1 if max batch size >= number of messages
    # if number of messages > max_batch_size, we will have more than 1 batch
    # to decide the number of batches, we will basically divide to get the number of batches
    # and if it is not properly divisible (i.e modulo is non zero), we will add one more batch
    number_of_batches = number_of_messages // max_batch_size + int(number_of_messages % max_batch_size > 0)

    logger.info(f'Number of Messages : {number_of_messages}, Batch Size: {max_batch_size}, '
                f'Number of batches be produced : {number_of_batches}')

    builder = sdc_builder.get_pipeline_builder()
    amazon_sqs_consumer = builder.add_stage('Amazon SQS Consumer')
    amazon_sqs_consumer.set_attributes(data_format='TEXT',
                                       max_batch_size_in_messages=max_batch_size,
                                       number_of_messages_per_request=10,
                                       queue_name_prefixes=[queue_name])
    trash = builder.add_stage('Trash')
    amazon_sqs_consumer >> trash

    consumer_origin_pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    policy = {
        "Version": "2008-10-17",
        "Id": "__default_policy_ID",
        "Statement": [
            {
                "Sid": "__owner_statement",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "*"
                },
                "Action": "SQS:*",
                "Resource": "arn:aws:sqs:*:316386816690:" + queue_name
            },
            {
                "Sid": "Stmt1597739351356",
                "Effect": "Deny",
                "Principal": {
                    "AWS": "*"
                },
                "Action": "sqs:ReceiveMessage",
                "Resource": "arn:aws:sqs:*:316386816690:" + queue_name
            }
        ]
    }

    client = aws.sqs
    logger.info('Creating %s SQS queue on AWS ...', queue_name)

    try:
        queue_url = client.create_queue(QueueName=queue_name, Attributes={'Policy': json.dumps(policy)})['QueueUrl']
        client.tag_queue(
            QueueUrl=queue_url,
            Tags={
                'stf-env': 'nightly-tests',
                'managed-by': 'ep',
                'dept': 'eng'
        })

        all_responses = []
        for batch in range(number_of_messages // 10 + int(number_of_messages % 10 > 0)):
            message_entries = [{'Id': str(i), 'MessageBody': f'Message {i}'}
                               for i in range(batch * 10, min(number_of_messages, (batch + 1) * 10))]

            sent_response = client.send_message_batch(QueueUrl=queue_url, Entries=message_entries)
            for message in sent_response.get('Successful', []):
                all_responses.append(message)
        if len(all_responses) != number_of_messages:
            raise Exception('Test messages not successfully sent to the queue %s', queue_name)

        try:
            sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_batch_count(number_of_batches)
            sdc_executor.stop_pipeline(consumer_origin_pipeline)
            assert False

        except Exception as error:
            assert 'SQS_13' in str(error)
            assert 'AccessDenied' in str(error)
            assert queue_name in str(error)

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        if queue_url:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


@aws('sqs')
@sdc_min_version('3.19.0')
def test_sqs_specify_url_directly(sdc_builder, sdc_executor, aws):
    """Test for SQS consumer origin stage where instead of specifying the queue prefix, it uses the queue URL,
    that way the pipeline does not try to list them since could be a lack of permissions.
     The pipeline looks like:

    Amazon SQS Consumer pipeline:
        amazon_sqs_consumer >> trash
    """
    queue_name = f'{aws.sqs_queue_prefix}_{get_random_string(string.ascii_letters, 10)}'

    number_of_messages = 10
    max_batch_size = 10

    # number of batches will be 1 if max batch size >= number of messages
    # if number of messages > max_batch_size, we will have more than 1 batch
    # to decide the number of batches, we will basically divide to get the number of batches
    # and if it is not properly divisible (i.e modulo is non zero), we will add one more batch
    number_of_batches = number_of_messages // max_batch_size + int(number_of_messages % max_batch_size > 0)

    logger.info(f'Number of Messages : {number_of_messages}, Batch Size: {max_batch_size}, '
                f'Number of batches be produced : {number_of_batches}')

    client = aws.sqs
    queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']

    builder = sdc_builder.get_pipeline_builder()
    amazon_sqs_consumer = builder.add_stage('Amazon SQS Consumer')
    amazon_sqs_consumer.set_attributes(data_format='TEXT',
                                       max_batch_size_in_messages=max_batch_size,
                                       number_of_messages_per_request=10,
                                       specify_queue_url_directly=True,
                                       queue_urls=[queue_url])
    wiretap = builder.add_wiretap()
    amazon_sqs_consumer >> wiretap.destination

    consumer_origin_pipeline = builder.build().configure_for_environment(aws)
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    try:
        all_responses = []
        message_entries = None
        for batch in range(number_of_messages // 10 + int(number_of_messages % 10 > 0)):
            message_entries = [{'Id': str(i), 'MessageBody': f'Message {i}'}
                               for i in range(batch * 10, min(number_of_messages, (batch + 1) * 10))]
            sent_response = client.send_message_batch(QueueUrl=queue_url, Entries=message_entries)
            for message in sent_response.get('Successful', []):
                all_responses.append(message)
        if len(all_responses) != number_of_messages:
            raise Exception('Test messages not successfully sent to the queue %s', queue_name)

        sdc_executor.start_pipeline(consumer_origin_pipeline).wait_for_pipeline_batch_count(number_of_batches)
        sdc_executor.stop_pipeline(consumer_origin_pipeline)

        assert len(message_entries) == len(wiretap.output_records)
        assert [value in [record.field['text'] for record in wiretap.output_records] for value in message_entries]
    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        if queue_url:
            logger.info('Deleting %s SQS queue of %s URL on AWS ...', queue_name, queue_url)
            client.delete_queue(QueueUrl=queue_url)


@aws('sqs')
@pytest.mark.parametrize('delivery_guarantee', ['AT_LEAST_ONCE', 'AT_MOST_ONCE'])
def test_sqs_origin_delivery_guarantee(sdc_builder, sdc_executor, aws, delivery_guarantee):
    """
    In this test we want to make sure that the SQS consumer supports the At Least Once and At Most Once modes.
    If we receive a message but the record doesn't reach the destination for the At Least Once mode
    we expect the message stays in the queue.
    For the At Most Once mode we expect the message gets deleted from the queue even if a record doesn't reach
    a destination.
    To make sure a record doesn't reach a destination we put a JavaScript evaluator which always throws an error.
    This makes the pipeline to stop with the RUN_ERROR status.

    The pipeline is as follows:

    Amazon SQS Consumer >> JavaScript Evaluator >> Trash

    """

    queue_name = f'{aws.sqs_queue_prefix}_{get_random_string()}'
    message_id = get_random_string()
    error_message = get_random_string()
    message = "{'id': message_id}"
    client = aws.sqs

    builder = sdc_builder.get_pipeline_builder()
    amazon_sqs_consumer = builder.add_stage('Amazon SQS Consumer')
    amazon_sqs_consumer.set_attributes(data_format='TEXT',
                                       queue_name_prefixes=[queue_name])

    javascript_evaluator = builder.add_stage('JavaScript Evaluator')
    javascript_evaluator.script = f'throw new Error("{error_message}")'

    trash = builder.add_stage('Trash')
    amazon_sqs_consumer >> javascript_evaluator >> trash

    consumer_origin_pipeline = builder.build().configure_for_environment(aws)
    consumer_origin_pipeline.delivery_guarantee = delivery_guarantee
    sdc_executor.add_pipeline(consumer_origin_pipeline)

    queue_url = client.create_queue(QueueName=queue_name, Attributes={'VisibilityTimeout': '5'})['QueueUrl']
    try:
        message_entries = [{'Id': message_id, 'MessageBody': message}]
        sent_response = client.send_message_batch(QueueUrl=queue_url, Entries=message_entries)
        if len(sent_response.get('Successful', [])) != len(message_entries):
            raise Exception('Test messages not successfully sent to the queue %s', queue_name)

        sdc_executor.start_pipeline(consumer_origin_pipeline, wait=False).wait_for_status(status='RUN_ERROR', ignore_errors=True)
        status = sdc_executor.get_pipeline_status(consumer_origin_pipeline).response.json()

        response = client.receive_message(QueueUrl=queue_url, WaitTimeSeconds=8)
        if delivery_guarantee == 'AT_LEAST_ONCE':
            assert len(message_entries) == len(response['Messages'])
            assert message == response['Messages'][0]['Body']
        else:
            assert 'Messages' not in response

        assert 'SCRIPTING_06' in status['message']
        assert error_message in status['message']

    finally:
        _ensure_pipeline_is_stopped(sdc_executor, consumer_origin_pipeline)
        client.delete_queue(QueueUrl=queue_url)


def _ensure_pipeline_is_stopped(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)
