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

import base64
import json
import logging
import math
import time
import uuid
from datetime import datetime
from string import ascii_letters, ascii_lowercase
from time import sleep

from google.cloud.bigquery import Dataset, SchemaField, Table
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DEFAULT_COLUMN_FAMILY_NAME = 'cf'  # for Google Bigtable
# For Google pub/sub
MSG_DATA = 'Hello World from SDC and DPM!'
SNAPSHOT_TIMEOUT_SEC = 120
# For Google BigQuery, data to insert- needs to be in the sorted order by name.

bytes_column = base64.b64encode("dataAsBytes".encode('utf-8'))

ROWS_TO_INSERT = [(None, 'null ID', 12.3456, '99999999999999999999999999999.999999999', True,
                   '2019-02-05', '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', '08:39:01.123',
                   bytes_column),
                  (1, 'full record', 12.3456, '99999999999999999999999999999.999999999', True,
                   '2019-02-05', '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', '08:39:01.123',
                   bytes_column),
                  (2, 'null float', None, '99999999999999999999999999999.999999999', True,
                   '2019-02-05', '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', '08:39:01.123',
                   bytes_column),
                  (3, 'null numeric', 12.3456, None, True,
                   '2019-02-05', '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', '08:39:01.123',
                   bytes_column),
                  (4, 'null boolean', 12.3456, '99999999999999999999999999999.999999999', None,
                   '2019-02-05', '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', '08:39:01.123',
                   bytes_column),
                  (5, 'null date', 12.3456, '99999999999999999999999999999.999999999', True,
                   None, '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', '08:39:01.123', bytes_column),
                  (6, 'null datetime', 12.3456, '99999999999999999999999999999.999999999', True,
                   '2019-02-05', None, '2007-05-28 07:52:31.123 UTC', '08:39:01.123', bytes_column),
                  (7, 'null timestamp', 12.3456, '99999999999999999999999999999.999999999', True,
                   '2019-02-05', '2019-02-05 23:59:59.999', None, '08:39:01.123', bytes_column),
                  (8, 'null time', 12.3456, '99999999999999999999999999999.999999999', True,
                   '2019-02-05', '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', None, bytes_column),
                  (9, 'null bytes', 12.3456, '99999999999999999999999999999.999999999', True,
                   '2019-02-05', '2019-02-05 23:59:59.999', '2007-05-28 07:52:31.123 UTC', '08:39:01.123', None)]

ROWS_TO_INSERT_2 = [(11, 'full record 2', 12.3456, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (12, 'full record 3', None, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (13, 'full record 4', 12.3456, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (14, 'full record 5', 12.3456, '99999999999999999999999999999.999999995', False,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (15, 'full record 6', 12.3456, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (16, 'full record 7', 12.3456, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (17, 'full record 8', 12.3456, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (18, 'full record 9', 12.3456, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column),
                    (19, 'full record 10', 12.3456, '99999999999999999999999999999.999999995', True,
                     '2019-02-05', '2019-02-05 23:59:59.995', '2007-05-28 07:52:31.125 UTC', '08:39:01.125',
                     bytes_column)]

ROWS_EXPECTED = [('Cristiano Ronaldo', 32),
                 ('David Beckham', 32),
                 ('Gerard Pique', 30),
                 ('Lionel Messi', 30),
                 ('Mario Gotze', 25),
                 ('Neymar', 25),
                 ('Pele', 76),
                 ('Ronaldinho', 40),
                 ('Ronaldo', 40),
                 ('Zinedine Zidane', 42)]

CSV_DATA_TO_INSERT = ['full_name,age'] + [','.join(str(element) for element in row) for row in ROWS_EXPECTED]


@gcp
@sdc_min_version('2.7.2.0')
def test_google_bigquery_destination(sdc_builder, sdc_executor, gcp):
    """
    Send data to Google BigQuery from Dev Raw Data Source and
    confirm that Google BigQuery destination successfully recieves them using Google BigQuery client.

    This is achieved by using a deduplicator which assures that there is only one ingest to Google BigQuery.
    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> google_bigquery
                               record_deduplicator >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(CSV_DATA_TO_INSERT))

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> google_bigquery
    record_deduplicator >> trash

    pipeline = pipeline_builder.build(title='Google BigQuery Destination').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('full_name', 'STRING', mode='required'),
              SchemaField('age', 'INTEGER', mode='required')]
    dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

    try:
        logger.info('Creating dataset %s using Google BigQuery client ...', dataset_name)
        dataset = bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))

        logger.info('Starting BigQuery Destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

        logger.info('Stopping BigQuery Destination pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        # Verify by reading records using Google BigQuery client
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()
        logger.debug('read_data = {}'.format(data_from_bigquery))
        assert ROWS_EXPECTED == data_from_bigquery
    finally:
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
@sdc_min_version('2.7.2.0')
def test_google_bigquery_destination_multiple_types(sdc_builder, sdc_executor, gcp):
    """Simple big query destination test with INSERT operation.
    The pipeline inserts 1000 records of multiple types.
    A type converter is included to transform decimal to float.
    The pipeline should look like:
        dev_data_generator >> field_type_converter >> google_bigquery

    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'field1', 'type': 'STRING'},
        {'field': 'field2', 'type': 'DATETIME'},
        {'field': 'field3', 'type': 'INTEGER'},
        {'field': 'field4', 'precision': 10, 'scale': 2, 'type': 'DECIMAL'},
        {'field': 'field5', 'type': 'DOUBLE'}

    ]
    batch_size = 10000
    dev_data_generator.set_attributes(delay_between_batches=0, batch_size=batch_size)

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name,
                                   stage_on_record_error='TO_ERROR')

    # Create Field Type Converter
    conversions = [{'fields': ['/field4'],
                    'targetType': 'FLOAT'}]
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=conversions)

    dev_data_generator >> field_type_converter >> google_bigquery

    pipeline = pipeline_builder.build(title="BigQuery Destination multiple types")

    sdc_executor.add_pipeline(pipeline.configure_for_environment(gcp))

    # FLOAT64 is used because there is a bug with NUMERIC, in bigquery Client
    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('field1', 'STRING', mode='required'),
              SchemaField('field2', 'DATETIME', mode='required'),
              SchemaField('field3', 'INTEGER', mode='required'),
              SchemaField('field4', 'FLOAT64', mode='required'),
              SchemaField('field5', 'FLOAT', mode='required')
              ]
    dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

    try:
        logger.info('Creating dataset %s using Google BigQuery client ...', dataset_name)
        dataset = bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))

        logger.info('Starting BigQuery Destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batch_size, timeout_sec=3600)
        snapshot = sdc_executor.capture_snapshot(pipeline).snapshot

        logger.info('Stopping BigQuery Destination pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        # Verify by reading records using Google BigQuery client
        data_from_bigquery = [row for row in bigquery_client.list_rows(table)]
        assert len(data_from_bigquery) > batch_size
        stage = snapshot[google_bigquery.instance_name]
        assert len(stage.error_records) == 0

    finally:
        logger.info('Dropping table %s in Google Big Query database ...', table_name)
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
@sdc_min_version('3.11.0')
def test_google_bigquery_destination_empty_table_name_error(sdc_builder, sdc_executor, gcp):
    """Test that BigQuery API does not return a NullPointerException if asked for an empty table name

    Pipeline:


        dev_raw_data_source >> google_bigquery
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    json_data = {'table': ''}

    # Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format = 'JSON',
        raw_data = json.dumps(json_data),
        stop_after_first_batch = True
    )

    # Google BigQuery Destination
    dataset_name = 'dont_care'
    table_name = '${record:value(\'/table\')}'
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type = 'destination')
    google_bigquery.set_attributes(dataset = dataset_name,
                                   table_name = table_name,
                                   stage_on_record_error = 'TO_ERROR')

    # Implement pipeline topology
    dev_raw_data_source >> google_bigquery

    pipeline = pipeline_builder.build(title="BigQuery Destination empty table name")

    sdc_executor.add_pipeline(pipeline.configure_for_environment(gcp))

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline = True).snapshot

    stage = snapshot[google_bigquery.instance_name]

    # Verify that we have exactly one record
    assert len(stage.error_records) == 1
    # Verify that the error is indeed a BIGQUERY_18 (table name is empty or expression evaluates to empty)
    assert stage.error_records[0].header['errorCode'] == 'BIGQUERY_18'


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigquery_origin(sdc_builder, sdc_executor, gcp):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives them using snapshot.

    The pipeline looks like:
        google_bigquery >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY id'
    google_bigquery.set_attributes(query=query_str)

    trash = pipeline_builder.add_stage('Trash')
    google_bigquery >> trash
    pipeline = pipeline_builder.build(title='Google BigQuery').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('id', 'INTEGER', mode='NULLABLE'),
              SchemaField('name', 'STRING', mode='NULLABLE'),
              SchemaField('floatVal', 'FLOAT', mode='NULLABLE'),
              SchemaField('numericVal', 'NUMERIC', mode='NULLABLE'),
              SchemaField('booleanVal', 'BOOLEAN', mode='NULLABLE'),
              SchemaField('dateVal', 'DATE', mode='NULLABLE'),
              SchemaField('datetimeVal', 'DATETIME', mode='NULLABLE'),
              SchemaField('timestampVal', 'TIMESTAMP', mode='NULLABLE'),
              SchemaField('timeVal', 'TIME', mode='NULLABLE'),
              SchemaField('bytesVal', 'BYTES', mode='NULLABLE')]

    dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

    try:
        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        dataset = bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors

        # Start pipeline and verify correct rows are received using snaphot.
        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        get_value_by_index = lambda x: list(x[0].field.values())[x[1]].value
        rows_from_snapshot = [(int(get_value_by_index((record, 0)))
                               if get_value_by_index((record, 0)) is not None else None,
                               get_value_by_index((record, 1)),
                               float(get_value_by_index((record, 2)))
                               if get_value_by_index((record, 2)) is not None else None,
                               str(get_value_by_index((record, 3)))
                               if get_value_by_index((record, 3)) is not None else None,
                               get_value_by_index((record, 4)),
                               get_value_by_index((record, 5)).strftime("%Y-%m-%d")
                               if get_value_by_index((record, 5)) is not None else None,
                               get_value_by_index((record, 6)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                               if get_value_by_index((record, 6)) is not None else None,
                               '{} UTC'.format(get_value_by_index((record, 7)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                               if get_value_by_index((record, 7)) is not None else None,
                               get_value_by_index((record, 8)).strftime('%H:%M:%S.%f')[:-3]
                               if get_value_by_index((record, 8)) is not None else None,
                               get_value_by_index((record, 9)) if get_value_by_index((record, 9)) is not None else None)
                              for record in snapshot[google_bigquery].output]

        assert rows_from_snapshot == ROWS_TO_INSERT


    finally:
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigquery_origin_stop_resume(sdc_builder, sdc_executor, gcp):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives them using snapshot.
    Stop the pipeline, add more data, and check the second batch.

    The pipeline looks like:
        google_bigquery >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY id'
    google_bigquery.set_attributes(query=query_str)

    trash = pipeline_builder.add_stage('Trash')
    google_bigquery >> trash
    pipeline = pipeline_builder.build(title='Google BigQuery').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('id', 'INTEGER', mode='NULLABLE'),
              SchemaField('name', 'STRING', mode='NULLABLE'),
              SchemaField('floatVal', 'FLOAT', mode='NULLABLE'),
              SchemaField('numericVal', 'NUMERIC', mode='NULLABLE'),
              SchemaField('booleanVal', 'BOOLEAN', mode='NULLABLE'),
              SchemaField('dateVal', 'DATE', mode='NULLABLE'),
              SchemaField('datetimeVal', 'DATETIME', mode='NULLABLE'),
              SchemaField('timestampVal', 'TIMESTAMP', mode='NULLABLE'),
              SchemaField('timeVal', 'TIME', mode='NULLABLE'),
              SchemaField('bytesVal', 'BYTES', mode='NULLABLE')]

    dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

    try:
        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        dataset = bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors

        # Start pipeline and verify correct rows are received using snaphot.
        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        get_value_by_index = lambda x: list(x[0].field.values())[x[1]].value
        rows_from_snapshot = [(int(get_value_by_index((record, 0)))
                               if get_value_by_index((record, 0)) is not None else None,
                               get_value_by_index((record, 1)),
                               float(get_value_by_index((record, 2)))
                               if get_value_by_index((record, 2)) is not None else None,
                               str(get_value_by_index((record, 3)))
                               if get_value_by_index((record, 3)) is not None else None,
                               get_value_by_index((record, 4)),
                               get_value_by_index((record, 5)).strftime("%Y-%m-%d")
                               if get_value_by_index((record, 5)) is not None else None,
                               get_value_by_index((record, 6)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                               if get_value_by_index((record, 6)) is not None else None,
                               '{} UTC'.format(get_value_by_index((record, 7)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                               if get_value_by_index((record, 7)) is not None else None,
                               get_value_by_index((record, 8)).strftime('%H:%M:%S.%f')[:-3]
                               if get_value_by_index((record, 8)) is not None else None,
                               get_value_by_index((record, 9))
                               if get_value_by_index((record, 9)) is not None else None)
                              for record in snapshot[google_bigquery].output]

        assert rows_from_snapshot == ROWS_TO_INSERT

        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT_2)
        assert not errors

        snapshot_2 = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=2).snapshot
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        get_value_by_index = lambda x: list(x[0].field.values())[x[1]].value

        rows_from_snapshot = [(int(get_value_by_index((record, 0)))
                               if get_value_by_index((record, 0)) is not None else None,
                               get_value_by_index((record, 1)),
                               float(get_value_by_index((record, 2)))
                               if get_value_by_index((record, 2)) is not None else None,
                               str(get_value_by_index((record, 3)))
                               if get_value_by_index((record, 3)) is not None else None,
                               get_value_by_index((record, 4)),
                               get_value_by_index((record, 5)).strftime("%Y-%m-%d")
                               if get_value_by_index((record, 5)) is not None else None,
                               get_value_by_index((record, 6)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                               if get_value_by_index((record, 6)) is not None else None,
                               '{} UTC'.format(get_value_by_index((record, 7)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                               if get_value_by_index((record, 7)) is not None else None,
                               get_value_by_index((record, 8)).strftime('%H:%M:%S.%f')[:-3]
                               if get_value_by_index((record, 8)) is not None else None,
                               get_value_by_index((record, 9))
                               if get_value_by_index((record, 9)) is not None else None)
                              for batch in snapshot_2.snapshot_batches
                              for record in batch.stage_outputs[google_bigquery.instance_name].output]

        assert rows_from_snapshot == ROWS_TO_INSERT + ROWS_TO_INSERT_2

    finally:
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


# SDC-10836. Google BigQuery is not respecting batchSize and at the same time is duplicating records
@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigquery_origin_batch_handling(sdc_builder, sdc_executor, gcp):
    """Verify proper batch handling by the BigQuery origin.

    In this test, we write 8 records to BigQuery with a batch size of 3, verifying that each batch is of
    size 3 and that 8 total records are captured. The small numbers are used because of the limitations
    described in SDC-8765.
    """
    MAX_BATCH_SIZE = 3
    TOTAL_RECORDS = 8

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY number'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    google_bigquery.set_attributes(query=query_str, max_batch_size_in_records=MAX_BATCH_SIZE)
    trash = pipeline_builder.add_stage('Trash')
    google_bigquery >> trash

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('number', 'STRING', mode='required'),
              SchemaField('value', 'STRING', mode='required')]

    data = [dict(number=str(i), value=get_random_string()) for i in range(TOTAL_RECORDS)]
    dataset = Dataset(bigquery_client.dataset(dataset_name))

    try:
        # Using Google bigquery client, create dataset, table and data inside table.
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset)
        table = bigquery_client.create_table(Table(dataset.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, data)
        assert not errors

        # Ceiling division is needed to capture all the complete batches along with the last partial one.
        snapshot = sdc_executor.capture_snapshot(pipeline,
                                                 start_pipeline=True,
                                                 batches=math.ceil(TOTAL_RECORDS / MAX_BATCH_SIZE)).snapshot

        # Assert that the batch size is being respected in each batch (including the last). In this case,
        # we'd expect batch sizes of 3, 3, and 2.
        for i, batch in enumerate(snapshot.snapshot_batches, start=1):
            #  for 8 records, we'd expect batch sizes of 3, 3, and 2.
            assert (len(batch.stage_outputs[google_bigquery.instance_name].output) == MAX_BATCH_SIZE
                    if i * MAX_BATCH_SIZE <= TOTAL_RECORDS
                    else TOTAL_RECORDS % MAX_BATCH_SIZE)

        all_records = [record.field
                       for batch in snapshot.snapshot_batches
                       for record in batch.stage_outputs[google_bigquery.instance_name].output]
        assert all_records == data
    finally:
        bigquery_client.delete_dataset(dataset, delete_contents=True)


@gcp
@sdc_min_version('2.7.0.0')
def test_google_pubsub_subscriber(sdc_builder, sdc_executor, gcp):
    """Publish messages using Google pub/sub client and then check if Google pub/sub
    subscriber receives them using snapshot.

    The pipeline looks like:
        google_pubsub_subscriber >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    subscription_id = get_random_string(ascii_letters, 5)
    topic_name = get_random_string(ascii_letters, 5)
    google_pubsub_subscriber = pipeline_builder.add_stage('Google Pub Sub Subscriber', type='origin')
    google_pubsub_subscriber.set_attributes(batch_wait_time_in_ms=20000,
                                            data_format='TEXT',
                                            max_batch_size_in_records=10,
                                            num_pipeline_runners=1,
                                            subscriber_thread_pool_size=1,
                                            subscription_id=subscription_id)

    trash = pipeline_builder.add_stage('Trash')
    google_pubsub_subscriber >> trash
    pipeline = pipeline_builder.build(title='Google Pub Sub Subscriber Pipeline').configure_for_environment(gcp)

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

        # Start pipeline and verify messages are received using snapshot.
        logger.info('Starting pipeline and snapshot')
        sdc_executor.add_pipeline(pipeline)
        snapshot_pipeline_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        logger.info('Publishing messages to topic %s using Google pub/sub client ...', topic_name)
        encoded_msg_data = MSG_DATA.encode()
        num = 10
        for _ in range(num):
            pubsub_publisher_client.publish(topic_path, encoded_msg_data)

        logger.debug('Finish the snapshot and verify')
        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC).snapshot
        sdc_executor.stop_pipeline(pipeline)
        rows_from_snapshot = [record.field['text'].value
                              for record in snapshot[google_pubsub_subscriber].output]

        logger.debug(rows_from_snapshot)
        assert rows_from_snapshot == [MSG_DATA] * num
    finally:
        pubsub_subscriber_client.delete_subscription(subscription_path)
        pubsub_publisher_client.delete_topic(topic_path)


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
def test_google_storage_destination_error(sdc_builder, sdc_executor, gcp):
    """
    Send data to Google cloud storage from Dev Raw Data Source
    bucket is not created and
    confirm that ten error records are generated.

    The pipeline looks like:
        dev_raw_data_source >> google_cloud_storage
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    data = [get_random_string(ascii_letters, length=100) for _ in range(10)]

    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       stop_after_first_batch=True,
                                       raw_data='\n'.join(data))
    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='destination')

    google_cloud_storage.set_attributes(bucket='X',
                                        common_prefix='gcs-test',
                                        partition_prefix='${YYYY()}/${MM()}/${DD()}/${hh()}/${mm()}',
                                        data_format='TEXT',
                                        file_suffix='txt',
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
        assert 'CONTAINER_0001' == stage.error_records[_].header['errorCode']


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
                                        file_suffix='txt',
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
        assert 'CONTAINER_0001' == stage.error_records[_].header['errorCode']

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

    assert msgs_received == ['CONTAINER_0001'] * 10

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
