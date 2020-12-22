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
from string import ascii_letters, ascii_lowercase
from time import sleep

import pytest
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


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
