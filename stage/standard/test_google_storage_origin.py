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
import avro, avro.datafile
import io
import json
import logging
import os
import string
import time

import pytest
from streamsets.testframework.markers import gcp
from streamsets.testframework.utils import get_random_string
from xlwt import Workbook

logger = logging.getLogger(__name__)

TMPOUT = '/tmp/out/'
PROTOBUF_FILE_PATH = 'resources/protobuf/addressbook.desc'


@gcp
def test_data_types(sdc_builder, sdc_executor, gcp):
    pytest.skip("GCS stores objects without doing data type distinctions.")


#Reference: https://cloud.google.com/storage/docs/naming-buckets
GCS_BUCKET_NAMES = [
    ('max_size', get_random_string(string.ascii_lowercase, 63)),
    ('lowercase', get_random_string(string.ascii_lowercase, 20)),
    ('numbers', get_random_string(string.ascii_lowercase, 5) + "1234567890" + get_random_string(string.ascii_lowercase, 5)),
    ('special', get_random_string(string.ascii_lowercase, 5) + "-_" + get_random_string(string.ascii_lowercase, 5))
]


#Reference: https://cloud.google.com/storage/docs/naming-objects
GCS_OBJECT_NAMES = [
    ('max_size', get_random_string(string.ascii_lowercase, 1011)),
        # Max length for GCS object is 1024 chars, the full name is gcs-test/ + 1011 chars + .txt, 1024 in total
    ('min_size', get_random_string(string.ascii_lowercase, 1)),
    ('lowercase', get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20) + get_random_string(string.ascii_letters, 20)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "1234567890" + get_random_string(string.ascii_letters, 5)),
    ('special', get_random_string(string.ascii_letters, 5) + "!|@·#$%&/()='?¡¿[]`^+*{}´¨,.;:-_" + get_random_string(string.ascii_letters, 5))
]


@gcp
@pytest.mark.parametrize('test_name,bucket_name', GCS_BUCKET_NAMES, ids=[i[0] for i in GCS_BUCKET_NAMES])
def test_bucket_names(sdc_builder, sdc_executor, gcp, test_name, bucket_name):
    """
    Write data to Google cloud storage with different valid bucket names.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Google Cloud Storage Origin Bucket Names {test_name}').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/sdc-test.txt')
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
@pytest.mark.parametrize('test_name,object_name', GCS_OBJECT_NAMES, ids=[i[0] for i in GCS_OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, gcp, test_name, object_name):
    """
    Write data to Google cloud storage with different valid object (blob) names.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Google Cloud Storage Origin Object Names {test_name}').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/' + object_name + '.txt')
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
def test_dataflow_events(sdc_builder, sdc_executor, gcp):
    """
    Write data to Google cloud storage and check if the no-more-data event is generated.
    GCS currently generates no other types of events.

    The pipeline looks like:
        google_cloud_storage_origin >> trash
        google_cloud_storage_origin >= wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    wiretap = pipeline_builder.add_wiretap()

    trash = pipeline_builder.add_stage("Trash")
    google_cloud_storage >> trash

    google_cloud_storage >= wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Test Dataflow Events').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        # One event should be created
        records = wiretap.output_records
        assert len(records) == 1

        # Event is no-more-data
        assert records[0].header.values['sdc.event.type'] == 'no-more-data'
        assert records[0].field['record-count'] == 10
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_multiple_batches(sdc_builder, sdc_executor, gcp):
    """
    Test that we can produce multiple batches and the pipeline processes all the data exactly once.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    max_batch_size = 100
    number_of_batches = 10

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT',
                                        batch_wait_time_in_ms=20_000,
                                        max_batch_size_in_records=max_batch_size)
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Bucket Test Multiple Batches').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        data = [get_random_string(string.ascii_letters, 100) for _ in range(max_batch_size * number_of_batches)]
        blob = created_bucket.blob('gcs-test/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', max_batch_size * number_of_batches)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(rows_from_wiretap) == max_batch_size * number_of_batches
        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_data_format_avro(sdc_builder, sdc_executor, gcp):
    """
    Read data from Google cloud storage using Avro format.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    DATA = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}
    SCHEMA = {'namespace': 'example.avro',
              'type': 'record',
              'name': 'Employee',
              'fields': [{'name': 'name', 'type': 'string'},
                         {'name': 'age', 'type': 'int'},
                         {'name': 'emails', 'type': {'type': 'array', 'items': 'string'}},
                         {'name': 'boss', 'type': ['Employee', 'null']}]}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.avro',
                                        data_format='AVRO',
                                        avro_schema=json.dumps(SCHEMA),
                                        avro_schema_location='SOURCE')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format Avro').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        blob = created_bucket.blob('gcs-test/sdc-test.avro')

        binary_stream = io.BytesIO()
        datum_writer = avro.io.DatumWriter(avro.schema.Parse(json.dumps(SCHEMA)))
        with avro.datafile.DataFileWriter(writer=binary_stream, datum_writer=datum_writer,
                                          writer_schema=avro.schema.Parse(json.dumps(SCHEMA))) as data_file_writer:
            data_file_writer.append(DATA)
            data_file_writer.flush()
            raw_bytes = binary_stream.getvalue()

        blob.upload_from_string(raw_bytes)

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field for record in wiretap.output_records] == [DATA]
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
@pytest.mark.parametrize('csv_parser', ['LEGACY_PARSER', 'UNIVOCITY'])
def test_data_format_delimited(sdc_builder, sdc_executor, gcp, csv_parser):
    """
     Read data from Google cloud storage using delimited data format.

     The pipeline looks like:
         google_cloud_storage_origin >> wiretap
     """
    MESSAGE = 'Alex,Xavi,Tucu,Martin'
    EXPECTED_OUTPUT = {'0': 'Alex', '1': 'Xavi', '2': 'Tucu', '3': 'Martin'}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.del',
                                        data_format='DELIMITED',
                                        csv_parser=csv_parser)
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format Text').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        blob = created_bucket.blob('gcs-test/sdc-test.del')
        blob.upload_from_string(MESSAGE)

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field for record in wiretap.output_records] == [EXPECTED_OUTPUT]
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_data_format_excel(sdc_builder, sdc_executor, gcp):
    """
    Read data from Google cloud storage using Excel format.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    gcp_file_name = get_random_string(string.ascii_letters, 10)

    # Create the Excel file

    workbook = Workbook()
    sheet = workbook.add_sheet('0')

    colcount = 5
    rowcount = 10

    for col in range(colcount):
        for row in range(rowcount):
            sheet.write(row, col, 'TAB({row}, {col})'.format(row=row, col=col))

    workbook.save(gcp_file_name)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern=gcp_file_name,
                                        data_format='EXCEL',
                                        excel_header_option='NO_HEADER')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format Excel').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        blob = created_bucket.blob('gcs-test/' + gcp_file_name)
        blob.upload_from_filename(gcp_file_name)

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [record.field for record in wiretap.output_records]
        len_records = len(output_records)

        # Compare the results get from the output_records
        for row_res in range(len_records):
            for col_res in range(colcount):
                assert output_records[row_res][str(col_res)] == "TAB({row}, {col})".format(row=row_res, col=col_res)
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)
        os.remove(gcp_file_name)


@gcp
@pytest.mark.parametrize('data_type', ['ARRAY', 'ARRAY_OF_OBJECTS', 'OBJECT'])
def test_data_format_json(sdc_builder, sdc_executor, gcp, data_type):
    """
    Read data from Google cloud storage in JSON format.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    # We map data_type to input data as well as the expected output.
    DATA_TYPE = {'ARRAY': ['Alex', 'Xavi'],
                 'ARRAY_OF_OBJECTS': [{'Alex': 'Developer'}, {'Xavi': 'Developer'}],
                 'OBJECT': {'Alex': 'Developer', 'Xavi': 'Developer'}}
    data, expected_output = json.dumps(DATA_TYPE[data_type]), DATA_TYPE[data_type]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.json',
                                        data_format='JSON')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format JSON').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        blob = created_bucket.blob('gcs-test/sdc-test.json')
        blob.upload_from_string(data)

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field for record in wiretap.output_records] == [expected_output]
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


# complete later
@gcp
def test_data_format_protobuf(sdc_builder, sdc_executor, gcp):
    """
    Read data from Google cloud storage using protobuf data format.
    The file is created used a first pipeline. dev_raw -> local_fs
    The file is moved used a second pipeline.  local_fs -> gcp

    The final pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """

    message = '{"first_name": "Martin","last_name": "Balzamo"}'
    expected = json.loads(message)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*',
                                        data_format='PROTOBUF',
                                        message_type='Contact',
                                        protobuf_descriptor_file=PROTOBUF_FILE_PATH)
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format Protobuf').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)

        gcp_file = get_random_string(string.ascii_lowercase, 20)
        produce_messages_protobuf(gcp_file, sdc_builder, sdc_executor, message, gcp)
        move_directory_messages_protobuf_gcp(gcp_file, bucket_name, sdc_builder, sdc_executor, gcp)

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field == expected
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_data_format_sdc_record(sdc_builder, sdc_executor, gcp):
    """
    Read data from Google cloud storage using text format.
    The file is created used a first pipeline. dev_raw -> local_fs
    The file is moved used a second pipeline.  local_fs -> gcp

    The final pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    json_data = [{"field1": "abc", "field2": "def", "field3": "ghi"},
                 {"field1": "jkl", "field2": "mno", "field3": "pqr"}]
    raw_data = ''.join(json.dumps(record) for record in json_data)


    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*',
                                        data_format='SDC_JSON')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format SDC Record').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)

        gcp_file = get_random_string(string.ascii_lowercase, 20)
        produce_messages_SDC_Record(gcp_file, sdc_builder, sdc_executor, raw_data, gcp)
        move_directory_messages_SDC_record_gcp(gcp_file, bucket_name, sdc_builder, sdc_executor, gcp)


        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field == json_data[0]
        assert wiretap.output_records[1].field == json_data[1]

    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_data_format_text(sdc_builder, sdc_executor, gcp):
    """
    Read data from Google cloud storage using text format.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format Text').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]

        assert len(data) == len(output_records)
        assert output_records == data
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_data_format_whole_file(sdc_builder, sdc_executor, gcp):
    """
    Read data from Google cloud storage using whole file format.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='WHOLE_FILE')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format Whole File').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [record.field for record in wiretap.output_records]
        # Whole file mode only returns metadata, but no actual data, so we can only test that a single file was passed
        # and that it has the correct name
        assert len(output_records) == 1
        assert output_records[0]['fileInfo']['file'] == 'gcs-test/sdc-test.txt'

    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_data_format_log(sdc_builder, sdc_executor, gcp):
    """
    Read data from Google cloud storage using log format mode.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    MESSAGE = '200 [main] DEBUG org.StreamSets.Log4j unknown - This is a sample log message'
    EXPECTED_OUTPUT = {'category': 'org.StreamSets.Log4j',
                       'message': 'This is a sample log message',
                       'ndc': 'unknown',
                       'relativetime': '200',
                       'severity': 'DEBUG',
                       'thread': 'main'}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.log',
                                        data_format='LOG',
                                        log_format='LOG4J')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Data Format Text').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        blob = created_bucket.blob('gcs-test/sdc-test.log')
        blob.upload_from_string(MESSAGE)

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field for record in wiretap.output_records] == [EXPECTED_OUTPUT]
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_multithreading(sdc_builder, sdc_executor, gcp):
    pytest.skip("Multithreading not supported for GCS Origin")


@gcp
def test_resume_offset(sdc_builder, sdc_executor, gcp):
    """
    Create a pipeline that reads half the data on a Google Cloud Storage origin. Stop the pipeline, start it again and
    test that it resumes from the correct offset.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    max_batch_size = 100
    number_of_batches = 10
    half_batches = 5

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT',
                                        batch_wait_time_in_ms=20_000,
                                        max_batch_size_in_records=max_batch_size)
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Bucket Test Resume Offset').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        data1 = [get_random_string(string.ascii_letters, 100) for _ in range(max_batch_size * half_batches)]
        blob = created_bucket.blob('gcs-test/sdc-test.txt')
        blob.upload_from_string('\n'.join(data1))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', max_batch_size * half_batches)
        sdc_executor.stop_pipeline(pipeline)

        first_data_half = [record.field['text'] for record in wiretap.output_records]
        wiretap.reset()

        data2 = [get_random_string(string.ascii_letters, 100) for _ in range(max_batch_size * half_batches)]
        blob.upload_from_string('\n'.join(data2))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', max_batch_size * half_batches)
        sdc_executor.stop_pipeline(pipeline)

        second_data_half = [record.field['text'] for record in wiretap.output_records]

        joined_result = first_data_half + second_data_half

        assert len(first_data_half) + len(second_data_half) == max_batch_size * number_of_batches
        assert len(first_data_half) + len(second_data_half) == len(data1) + len(data2)
        assert joined_result == data1 + data2
    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_empty_objects(sdc_builder, sdc_executor, gcp):
    """
    Test that no records are generated if the origin is empty.

    The pipeline looks like:
        google_cloud_storage_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    bucket_name = get_random_string(string.ascii_lowercase, 20)

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')
    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build(
        title=f'Google Cloud Storage Origin Empty').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
        blob = created_bucket.blob('gcs-test/sdc-test.txt')

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)

        time.sleep(30)

        sdc_executor.stop_pipeline(pipeline)

        # Assert that no records were generated
        assert 0 == len(wiretap.output_records)

    finally:
        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


def produce_messages_protobuf(gcp_file, sdc_builder, sdc_executor, message, gcp):
    # Build a dev_raw > local_fs  pipeline to create a protobuf file.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=message, stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')

    local_fs.set_attributes(file_type='TEXT', files_prefix=gcp_file, directory_template=TMPOUT,
                            data_format='PROTOBUF', message_type='Contact',
                            protobuf_descriptor_file=PROTOBUF_FILE_PATH)

    dev_raw_data_source >> local_fs

    pipeline = builder.build(
        title='FS PROTOBUF pipeline - Producer').configure_for_environment(gcp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()


def move_directory_messages_protobuf_gcp(gcp_file, bucket_name, sdc_builder, sdc_executor, gcp):
    # Build a directory->gcp  pipeline to move a protobuf file to Google Cloud Storage.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern=f'{gcp_file}*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=TMPOUT,
                             process_subdirectories=False, read_order='TIMESTAMP')

    google_cloud_storage = builder.add_stage('Google Cloud Storage', type='destination')
    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        data_format='WHOLE_FILE',
                                        file_name_expression=gcp_file)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(
        stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    directory >> google_cloud_storage
    directory >= pipeline_finished_executor

    pipeline = builder.build(
        title='GCP PROTOBUF pipeline - Producer').configure_for_environment(gcp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()


def produce_messages_SDC_Record(gcp_file, sdc_builder, sdc_executor, message, gcp):
    # Build a dev_raw > local_fs  pipeline to create a SDC_record file.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=message, stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')

    local_fs.set_attributes(file_type='TEXT', files_prefix=gcp_file, directory_template=TMPOUT,
                            data_format='SDC_JSON')

    dev_raw_data_source >> local_fs

    pipeline = builder.build(
        title='FS SDC Record pipeline - Producer').configure_for_environment(gcp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()


def move_directory_messages_SDC_record_gcp(gcp_file, bucket_name, sdc_builder, sdc_executor, gcp):
    # Build a directory->gcp  pipeline to move a SDC_record file to Google Cloud Storage.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern=f'{gcp_file}*', file_name_pattern_mode='GLOB',
                             files_directory=TMPOUT, file_post_processing = 'DELETE',
                             process_subdirectories=False, read_order='TIMESTAMP')

    google_cloud_storage = builder.add_stage('Google Cloud Storage', type='destination')
    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        data_format='WHOLE_FILE',
                                        file_name_expression=gcp_file)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(
        stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    directory >> google_cloud_storage
    directory >= pipeline_finished_executor

    pipeline = builder.build(
        title='GCP SDC record pipeline - Producer').configure_for_environment(gcp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()