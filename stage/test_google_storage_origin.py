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
import time
from string import ascii_letters, ascii_lowercase

import pytest
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_origin(sdc_builder, sdc_executor, gcp):
    """
    Write data to Google cloud storage using Storage Client
    and then check if Google Storage Origin receives them using wiretap.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
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

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        data = [get_random_string(ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


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

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        logger.info('Starting GCS Origin pipeline...')
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
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
@sdc_min_version('3.0.0.0')
def test_google_storage_origin_stop_resume(sdc_builder, sdc_executor, gcp):
    """
    Write data to Google cloud storage using Storage Client
    and then check if Google Storage Origin receives them using wiretap.
    Stop the pipeline, add more data, resume the pipeline and check the new data.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
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

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        data = [get_random_string(ascii_letters, length=100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test-1.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and waiting for it to produce records...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data

        data = [get_random_string(ascii_letters, length=100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test-1.txt')
        blob.upload_from_string('\n'.join(data))

        wiretap.reset()

        logger.info('Starting Second Time GCS Origin pipeline and waiting for it to produce records again...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


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

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        blob = created_bucket.blob('gcs-test/' + gcp_file_name)
        blob.upload_from_filename('resources/gcp/' + gcp_file_name)

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
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
@sdc_min_version('3.22.0')
def test_google_storage_no_more_data(sdc_builder, sdc_executor, gcp):
    """
    SDC-15932
    Create bucket with no data to Google cloud storage and then check if
    Google Storage Origin sends no-more-data event with no records sent.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')

    pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher_executor.set_attributes(preconditions=['${record:eventType() == \'no-more-data\'}'])

    wiretap = pipeline_builder.add_wiretap()
    events_wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination
    google_cloud_storage >= [pipeline_finisher_executor, events_wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        logger.info('Starting GCS Origin with no data ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert 0 == len(wiretap.output_records)
        event_record = events_wiretap.output_records[0]
        event_type = event_record.header.values['sdc.event.type']
        assert event_type == 'no-more-data', 'Received %s as event type (expected no-more-data)' % event_type
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
@sdc_min_version('4.1.0')
@pytest.mark.parametrize('action', ['ARCHIVE', 'DELETE'])
def test_google_storage_post_processing(sdc_builder, sdc_executor, gcp, action):
    """Test file post-processing functionality in GCS

    Write data to Google cloud storage using Storage Client
    and then check if Google Storage Origin receives them using wiretap.
    Also, assert that processing works by checking paths existence.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT',
                                        post_processing_option=action)
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        data = [get_random_string(ascii_letters, 100) for _ in range(10)]
        file_path = 'gcs-test/a/b/c/d/e/'
        file_name = 'sdc-test.txt'
        blob = created_bucket.blob(file_path + file_name)
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        # If post processing option is enabled, old file path should not exist
        assert not storage_client.get_bucket(bucket_name).blob(file_path + file_name).exists()

        # If ARCHIVE, default prefix is empty, so it gets moved to root of bucket
        if action == 'ARCHIVE':
            assert storage_client.get_bucket(bucket_name).blob(file_name).exists()

        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)
