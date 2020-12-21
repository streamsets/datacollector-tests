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

import json
import logging
import time
import os
from string import ascii_letters, ascii_lowercase
from time import sleep
import pytest

from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string
from xlwt import Workbook

logger = logging.getLogger(__name__)

DEFAULT_COLUMN_FAMILY_NAME = 'cf'  # for Google Bigtable

# For Google pub/sub
MSG_DATA = 'Hello World from SDC and DPM!'
SNAPSHOT_TIMEOUT_SEC = 120


@gcp
@sdc_min_version('2.7.0.0')
def test_google_pubsub_publisher(sdc_builder, sdc_executor, gcp):
    """
    Send simple text messages to Google pub/sub from Dev Raw Data Source and
    confirm that Google pub/sub successfully reads them using Google pub/sub client from gcp.

    The pipeline looks like:
        dev_raw_data_source >> google_pubsub_publisher
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = MSG_DATA

    subscription_id = get_random_string(ascii_letters, 5)
    topic_name = get_random_string(ascii_letters, 5)
    google_pubsub_publisher = pipeline_builder.add_stage('Google Pub Sub Publisher', type='destination')
    google_pubsub_publisher.set_attributes(topic_id=topic_name, data_format='TEXT')

    dev_raw_data_source >> google_pubsub_publisher
    pipeline = pipeline_builder.build(title='Google Pub Sub Publisher Pipeline').configure_for_environment(gcp)
    pipeline.rate_limit = 1

    pubsub_publisher_client = gcp.pubsub_publisher_client
    pubsub_subscriber_client = gcp.pubsub_subscriber_client

    project_id = gcp.project_id
    topic_path = pubsub_publisher_client.topic_path(project_id, topic_name)
    subscription_path = pubsub_subscriber_client.subscription_path(project_id, subscription_id)
    try:
        # Using Google pub/sub client, create topic and subscription
        logger.info('Creating topic %s using Google pub/sub client ...', topic_name)

        topic = pubsub_publisher_client.create_topic(topic_path)

        subscription = pubsub_subscriber_client.create_subscription(subscription_path, topic_path)

        # Send messages using pipeline to Google pub/sub publisher Destination.
        logger.info('Starting Publisher pipeline and waiting for it to produce records ...')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)

        logger.info('Stopping Publisher pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info('No. of messages sent in the pipeline = %s ...', msgs_sent_count)

        msgs_to_be_received = msgs_sent_count
        results = []

        def callback(message):
            nonlocal msgs_to_be_received
            msgs_to_be_received = msgs_to_be_received - 1
            results.append(message)
            message.ack()

        # Open the subscription, passing the callback.
        future = pubsub_subscriber_client.subscribe(subscription_path, callback)

        timeout = 5  # in seconds
        start_time = time.time()
        while time.time() < start_time + timeout and msgs_to_be_received > 0:
            sleep(0.5)
            logger.info('To be received messages %d...', msgs_to_be_received)

        future.cancel()  # cancel the feature there by stopping subscribers

        # Verify
        msgs_received = [message.data.decode().rstrip('\n') for message in results]
        assert msgs_received == [dev_raw_data_source.raw_data] * msgs_sent_count

    finally:
        pubsub_subscriber_client.delete_subscription(subscription_path)
        pubsub_publisher_client.delete_topic(topic_path)


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigtable_destination(sdc_builder, sdc_executor, gcp):
    """
    Send text to Google bigtable from Dev Raw Data Source and
    confirm that Google bigtable successfully reads them using happybase connection from gcp.

    The pipeline looks like:
        dev_raw_data_source >> google_bigtable
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    # Data with 3 distinct TransactionIDs
    data = ['TransactionID,Type,UserID', '0003420301,01,1001', '0003420302,02,1002', '0003420303,03,1003',
            '0003420301,04,1004', '0003420302,05,1005', '0003420303,06,1006']
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(data))

    table_name = get_random_string(ascii_letters, 5)
    google_bigtable = pipeline_builder.add_stage('Google Bigtable', type='destination')
    # Field paths, name of columns, storage types
    fields_list = [{'source': '/TransactionID', 'storageType': 'TEXT', 'column': 'TransactionID'},
                   {'source': '/Type', 'storageType': 'TEXT', 'column': 'Type'},
                   {'source': '/UserID', 'storageType': 'TEXT', 'column': 'UserID'}]
    google_bigtable.set_attributes(create_table_and_column_families=True,
                                   default_column_family_name=DEFAULT_COLUMN_FAMILY_NAME,
                                   explicit_column_family_mapping=False,
                                   fields=fields_list,
                                   row_key='/TransactionID',
                                   table_name=table_name)

    dev_raw_data_source >> google_bigtable
    pipeline = pipeline_builder.build(title='Google Bigtable').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)
    pipeline.rate_limit = 1

    instance = gcp.bigtable_instance
    table = instance.table(table_name)

    try:
        # Produce Google Bigtable records using pipeline.
        logger.info('Starting Big table pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

        logger.info('Stopping Big table pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Reading contents from table %s ...', table_name)
        partial_rows = table.read_rows()
        partial_rows.consume_all()

        read_data = [(f'{row.cells["cf"]["TransactionID".encode()][0].value.decode()},'
                      f'{row.cells["cf"]["Type".encode()][0].value.decode()},'
                      f'{row.cells["cf"]["UserID".encode()][0].value.decode()}')
                     for row_key, row in partial_rows.rows.items()]

        logger.debug('read_data = {}'.format(read_data))

        # Verify: Note we expect only 3 rows since there are only 3 distinct TransactionIDs in data.
        # The result expected is the latest inserted data which are the last 3 rows.
        # Reason is in Google Bigtable, each row is indexed by a single row key resulting in
        # each row/column intersection can contain multiple cells at different timestamps,
        # providing a record of how the stored data has been altered over time.
        assert data[4:] == read_data
    finally:
        table.delete()


@gcp
@sdc_min_version('2.7.2.0')
def test_google_bigtable_destination_multiple_types(sdc_builder, sdc_executor, gcp):
    """Simple big table destination test with INSERT operation.
    The pipeline inserts 1000 records of multiple types.
    The pipeline should look like:
        dev_data_generator >> record_deduplicator >> google_bigquery
        record_deduplicator >> trash

    record_deduplicator is added to avoid duplications

    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'field1', 'type': 'STRING'},
        {'field': 'field2', 'type': 'INTEGER'}
    ]
    batch_size = 10000
    dev_data_generator.set_attributes(delay_between_batches=0, batch_size=batch_size)

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)

    google_bigtable = pipeline_builder.add_stage('Google Bigtable', type='destination')
    # Field paths, name of columns, storage types
    fields_list = [{'source': '/field1', 'storageType': 'TEXT', 'column': 'field1'},
                   {'source': '/field2', 'storageType': 'BINARY', 'column': 'field2'}
                   ]

    google_bigtable.set_attributes(create_table_and_column_families=True,
                                   default_column_family_name=DEFAULT_COLUMN_FAMILY_NAME,
                                   explicit_column_family_mapping=False,
                                   fields=fields_list,
                                   row_key='/field1',
                                   table_name=table_name)


    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    record_deduplicator.set_attributes(compare='SPECIFIED_FIELDS',
                                       fields_to_compare = ['/field1'])

    trash = pipeline_builder.add_stage('Trash')

    dev_data_generator >> record_deduplicator >> google_bigtable
    record_deduplicator >> trash

    pipeline = pipeline_builder.build(title='Google Bigtable Mult Types').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)
    pipeline.rate_limit = 1

    instance = gcp.bigtable_instance
    table = instance.table(table_name)

    try:
        # Produce Google Bigtable records using pipeline.
        logger.info('Starting Big table pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batch_size, timeout_sec=3600)


        snapshot = sdc_executor.capture_snapshot(pipeline).snapshot

        logger.info('Stopping Big tabñe Destination pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Reading contents from table %s ...', table_name)
        partial_rows = table.read_rows()
        partial_rows.consume_all()
        read_data = [row for row in partial_rows.rows.items()]

        # Verify quantity of records greater than batch_size
        assert len(read_data) > batch_size
        # Verify no errors were generated
        stage = snapshot[google_bigtable.instance_name]
        assert len(stage.error_records) == 0

    finally:
        table.delete()

@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_origin(sdc_builder, sdc_executor, gcp):
    """
    Write data to Google cloud storage using Storage Client
    and then check if Google Storage Origin receives them using snapshot.

    The pipeline looks like:
        google_cloud_storage_origin >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    trash = pipeline_builder.add_stage('Trash')

    google_cloud_storage >> trash

    pipeline = pipeline_builder.build(title='Google Cloud Storage').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = storage_client.create_bucket(bucket_name)
    try:
        data = [get_random_string(ascii_letters, length=100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and waiting for it to produce a snapshot ...')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [record.field['text']
                              for record in snapshot[google_cloud_storage].output]

        logger.debug(rows_from_snapshot)
        assert rows_from_snapshot == data
    finally:
        created_bucket.delete(force=True)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_origin_idle_start(sdc_builder, sdc_executor, gcp):
    """
    Start Google Cloud Storage pipeline with no previous offset and no data to consume.
    Then produce messages and check that the pipeline hasn't stopped and it consumes all the new data.

    The pipeline looks like:
        google_cloud_storage_origin >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(title='Google Cloud Storage').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = storage_client.create_bucket(bucket_name)
    try:
        logger.info('Starting GCS Origin pipeline and waiting for it to produce a snapshot ...')
        start_command = sdc_executor.start_pipeline(pipeline)

        # Wait for 5 seconds - in this time the pipeline should have ended
        time.sleep(5)

        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            data = [get_random_string(ascii_letters, length=100) for _ in range(10)]
            blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test.txt')
            blob.upload_from_string('\n'.join(data))

            start_command.wait_for_pipeline_output_records_count(10)
            sdc_executor.stop_pipeline(pipeline)

            rows = [record.field['text']
                                  for record in wiretap.output_records]

            assert rows == data
        else:
            pytest.fail('Pipeline stopped before data was produced in the bucket.')
    finally:
        created_bucket.delete(force=True)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_destination(sdc_builder, sdc_executor, gcp):
    """
    Send data to Google cloud storage from Dev Raw Data Source and
    confirm that Storage client successfully reads them.

    The pipeline looks like:
        dev_raw_data_source >> google_cloud_storage
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    data = [get_random_string(ascii_letters, length=100) for _ in range(10)]

    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       stop_after_first_batch=True,
                                       raw_data='\n'.join(data))
    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='destination')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        partition_prefix='${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}',
                                        data_format='TEXT')

    dev_raw_data_source >> google_cloud_storage

    pipeline = pipeline_builder.build(title='Google Cloud Storage').configure_for_environment(gcp)

    sdc_executor.add_pipeline(pipeline)

    created_bucket = storage_client.create_bucket(bucket_name)
    try:
        logger.info('Starting GCS Destination pipeline and waiting for it to produce records'
                    ' and transition to finished...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        blob_iter = created_bucket.list_blobs(max_results=1, prefix='gcs-test')
        blobs = [blob for blob in blob_iter]
        assert len(blobs) == 1
        blob = blobs[0]
        # Decode the byte array returned by storage client
        contents = blob.download_as_string().decode('ascii')
        # Strip out the lines which are empty (essentially the last line)
        lines = [line for line in contents.split('\n') if len(line) > 0]
        assert lines == data
    finally:
        created_bucket.delete(force=True)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_error(sdc_builder, sdc_executor, gcp):
    """Ensure that the error stage for Google Storage works properly"""
    bucket_name = get_random_string(ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'TEXT'
    origin.stop_after_first_batch = True
    origin.raw_data = 'Hello!'

    error_target = builder.add_stage('To Error')

    origin >> error_target

    gcs = builder.add_error_stage('Write to Google Cloud Storage')
    gcs.bucket = bucket_name
    gcs.common_prefix = 'gcs-test-error'

    pipeline = builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    storage_client = gcp.storage_client
    created_bucket = storage_client.create_bucket(bucket_name)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        blob_iter = created_bucket.list_blobs(max_results=1, prefix='gcs-test-error')
        blobs = [blob for blob in blob_iter]
        assert len(blobs) == 1
        blob = blobs[0]
        # Decode the byte array returned by storage client
        contents = blob.download_as_string()[1:].decode('ascii')
        logger.info(f"Loaded raw data: {contents}")
        sdc_json = json.loads(contents)

        assert sdc_json['value']['value']['text']['value'] == 'Hello!'
    finally:
        created_bucket.delete(force=True)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_destination_error_output_google_sub_pub(sdc_builder, sdc_executor, gcp):
    """
    Send data to Google cloud storage from Dev Raw Data Source
    bucket is not created and
    confirm that ten error records are generated.
    Send the errors to google PUB SUB.
    Retrieve the messages and check the code error.

    The pipeline looks like:
        dev_raw_data_source >> google_cloud_storage
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    subscription_id = get_random_string(ascii_letters, 5)
    topic_name = get_random_string(ascii_letters, 5)

    pubsub_subscriber_client = gcp.pubsub_subscriber_client
    pubsub_publisher_client = gcp.pubsub_publisher_client
    project_id = gcp.project_id

    topic_path = pubsub_publisher_client.topic_path(project_id, topic_name)
    subscription_path = pubsub_subscriber_client.subscription_path(project_id, subscription_id)

    # Using Google pub/sub client, create topic and subscription
    logger.info('Creating topic %s using Google pub/sub client ...', topic_name)
    topic = pubsub_publisher_client.create_topic(topic_path)

    subscription = pubsub_subscriber_client.create_subscription(subscription_path, topic_path)

    # Pipeline error configured to google pub sub topic and project
    write_to_google_pub_sub = pipeline_builder.add_error_stage('Write to Google Pub Sub')
    write_to_google_pub_sub.set_attributes(topic_id=topic_name,
                                           project_id=project_id)

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    data = [get_random_string(ascii_letters, length=100) for _ in range(10)]
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       stop_after_first_batch=True,
                                       raw_data='\n'.join(data))

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='destination')

    bucket_name = get_random_string(ascii_letters, 5)
    # Bucket name does not exists.
    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        partition_prefix='${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}',
                                        data_format='TEXT',
                                        stage_on_record_error='TO_ERROR')

    dev_raw_data_source >> google_cloud_storage

    pipeline = pipeline_builder.build(title='Google Cloud Storage').configure_for_environment(gcp)

    sdc_executor.add_pipeline(pipeline)

    logger.info('Starting GCS Destination pipeline and waiting for it to produce records'
                ' and transition to finished...')

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')

    stage = snapshot[google_cloud_storage.instance_name]
    assert len(stage.error_records) == 10
    for _ in range(0, 10):
        assert 'GCS_09' == stage.error_records[_].header['errorCode']

    msgs_to_be_received = 10
    results = []

    def callback(message):
        nonlocal msgs_to_be_received
        msgs_to_be_received = msgs_to_be_received - 1
        results.append(message)
        message.ack()

    # Open the subscription, passing the callback.
    future = pubsub_subscriber_client.subscribe(subscription_path, callback)

    timeout = 5  # in seconds
    start_time = time.time()
    while time.time() < start_time + timeout and msgs_to_be_received > 0:
        sleep(0.5)
        logger.info('To be received messages %d...', msgs_to_be_received)

    future.cancel()  # cancel the feature there by stopping subscribers

    # Verify
    msgs_received = [json.loads(message.data.decode(encoding='UTF-8', errors='ignore'))['header']['errorCode'] for
                     message in results]

    assert msgs_received == ['GCS_09'] * 10

    pubsub_subscriber_client.delete_subscription(subscription_path)
    pubsub_publisher_client.delete_topic(topic_path)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_origin_stop_resume(sdc_builder, sdc_executor, gcp):
    """
    Write data to Google cloud storage using Storage Client
    and then check if Google Storage Origin receives them using snapshot.
    Stop the pipeline, add more data, resume the pipline and check the new data.

    The pipeline looks like:
        google_cloud_storage_origin >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    trash = pipeline_builder.add_stage('Trash')

    google_cloud_storage >> trash

    pipeline = pipeline_builder.build(title='Google Cloud Storage').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = storage_client.create_bucket(bucket_name)
    try:
        data = [get_random_string(ascii_letters, length=100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test-1.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and waiting for it to produce a snapshot ...')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [record.field['text']
                              for record in snapshot[google_cloud_storage].output]

        logger.debug(rows_from_snapshot)
        assert rows_from_snapshot == data

        data = [get_random_string(ascii_letters, length=100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test-1.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting Second Time GCS Origin pipeline and waiting for it to produce a snapshot ...')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        rows_from_snapshot = [record.field['text']
                              for record in snapshot[google_cloud_storage].output]

        logger.debug(rows_from_snapshot)
        assert rows_from_snapshot == data


    finally:
        created_bucket.delete(force=True)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_small_batch_size(sdc_builder, sdc_executor, gcp):
    """
    Write Excel file to Google Cloud Storage, create an origin with a batch size smaller than the number of
    records in the file and check that no data is duplicated when reading the file.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap.destination
    """

    gcp_file_name = 'test_9_records.xls'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern=gcp_file_name,
                                        data_format='EXCEL',
                                        max_batch_size_in_records=1,
                                        excel_header_option='WITH_HEADER')

    pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher_executor.set_attributes(preconditions=['${record:eventType() == \'no-more-data\'}'],
                                              on_record_error='DISCARD')

    google_cloud_storage >= pipeline_finisher_executor

    wiretap = pipeline_builder.add_wiretap()
    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(title='Google Cloud Storage').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = storage_client.create_bucket(bucket_name)
    try:
        blob = created_bucket.blob('gcs-test/'+gcp_file_name)
        blob.upload_from_filename('resources/gcp/'+gcp_file_name)

        # Start the pipeline and make sure the timeout is defined, since the original bug involved
        # an infinite loop reading the same file over and over again
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=300)

        # Check that the file has been read through exactly once
        output_records = [record.field for record in wiretap.output_records]
        assert len(output_records) == 9
        # Check that no error records were generated
        error_records = [record.field for record in wiretap.error_records]
        assert len(error_records) == 0

    finally:
        created_bucket.delete(force=True)
