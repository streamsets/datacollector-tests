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

import base64
import logging
import string
from decimal import Decimal
from datetime import datetime

import pytest
from google.cloud.bigquery import Dataset, SchemaField, Table
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

bytes_column = base64.b64encode("dataAsBytes".encode('utf-8'))

SCHEMA = [SchemaField('id', 'INTEGER', mode='NULLABLE'),
          SchemaField('name', 'STRING', mode='NULLABLE'),
          SchemaField('floatVal', 'FLOAT', mode='NULLABLE'),
          SchemaField('numericVal', 'NUMERIC', mode='NULLABLE'),
          SchemaField('booleanVal', 'BOOLEAN', mode='NULLABLE'),
          SchemaField('dateVal', 'DATE', mode='NULLABLE'),
          SchemaField('datetimeVal', 'DATETIME', mode='NULLABLE'),
          SchemaField('timestampVal', 'TIMESTAMP', mode='NULLABLE'),
          SchemaField('timeVal', 'TIME', mode='NULLABLE'),
          SchemaField('bytesVal', 'BYTES', mode='NULLABLE')]

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


@gcp
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# https://docs.streamsets.com/portal/#datacollector/latest/help/datacollector/UserGuide/Origins/BigQuery.html
# For every data type supported
@pytest.mark.parametrize('data_type, data, expected_data', [
    ('INTEGER', 34, 34),
    ('STRING', 'gcp standard test 123', 'gcp standard test 123'),
    ('FLOAT', 2424.2424, 2424.2424),
    ('NUMERIC', -999999999999999.999999995, Decimal(-999999999999999.999999995)),
    ('BOOLEAN', True, True),
    ('DATE', '2019-02-05', datetime(2019, 2, 5, 0, 0)),
    ('DATETIME', '2019-02-05 23:59:59.999', datetime(2019, 2, 5, 23, 59, 59, 999000)),
    ('TIMESTAMP', '2007-05-28 07:52:31.123 UTC', datetime(2007, 5, 28, 7, 52, 31, 123000)),
    ('TIME', '08:39:01.123', datetime(1970, 1, 1, 8, 39, 1, 123000)),
    ('BYTES', bytes_column, bytes_column)
    ])
def test_data_type(sdc_builder, sdc_executor, gcp, data_type, data, expected_data):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives them using wiretap with one data format each.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name}'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('id', 'INTEGER', mode='NULLABLE'),
              SchemaField('data', data_type, mode='NULLABLE')]

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, [(None, data)])
        assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1, f'Expected 1 record, received {len(wiretap.output_records)}'
        received_data = wiretap.output_records[0].field['data']

        assert received_data == expected_data
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
# https://cloud.google.com/bigquery/docs/tables#table_naming
@pytest.mark.parametrize('table_name', [
    'table-01',  # kind of standard name
    '92TABLE',  # begin with numeric characters
    'myTABLE_upperANDlowerCaSEs',  # case sensitiveness
    ' EVEN THIS AND THAT',  # allowed blank spaces even at the start
    'ग्राहक',  # more characters
    '00_お客様',  # more characters
    'étudiant'  # accents
])
def test_object_names_tables(sdc_builder, sdc_executor, gcp, table_name):
    """
    Create data using Google BigQuery client with specific table names
    and then check if Google BigQuery origin receives them using wiretap.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f"SELECT * FROM `{dataset_name}`.`{table_name}` ORDER BY id"
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == len(ROWS_TO_INSERT),\
            f'Expected {len(ROWS_TO_INSERT)} records, received {len(wiretap.output_records)}'
        rows_from_wiretap = get_rows_from_wiretap(wiretap)
        assert rows_from_wiretap == ROWS_TO_INSERT
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
# https://cloud.google.com/bigquery/docs/datasets#dataset-naming
@pytest.mark.parametrize('dataset_name', [
    'table_01',  # alphanumeric with underscore
    '92TABLE',  # begin with numeric characters
    'myTABLE_upperANDlowerCaSEs'  # case sensitiveness
])
def test_object_names_datasets(sdc_builder, sdc_executor, gcp, dataset_name):
    """
    Create data using Google BigQuery client with specific table names
    and then check if Google BigQuery origin receives them using wiretap.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    table_name = get_random_string(string.ascii_letters, 5)
    dataset_name = f'{dataset_name}_{get_random_string(string.ascii_letters, 5)}'
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f"SELECT * FROM `{dataset_name}`.`{table_name}` ORDER BY id"
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == len(ROWS_TO_INSERT),\
            f'Expected {len(ROWS_TO_INSERT)} records, received {len(wiretap.output_records)}'
        rows_from_wiretap = get_rows_from_wiretap(wiretap)
        assert rows_from_wiretap == ROWS_TO_INSERT
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
# https://cloud.google.com/bigquery/docs/schemas#column_names
@pytest.mark.parametrize('column_name', [
    'column',  # kind of standard name
    'column_name',  # with underscore
    '_column_name',  # starting with underscore
    'myCOlUMNnaMe',  # case sensitiveness
    'column24'  # numbers
])
def test_object_names_columns(sdc_builder, sdc_executor, gcp, column_name):
    """
    Create data using Google BigQuery client with specific column names
    and then check if Google BigQuery origin receives them using wiretap.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name}'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('id', 'INTEGER', mode='NULLABLE'),
              SchemaField(column_name, 'STRING', mode='NULLABLE')]

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, [(None, 'data')])
        assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1, f'Expected 1 record, received {len(wiretap.output_records)}'
        received_data = wiretap.output_records[0].field[column_name]

        assert received_data == 'data'
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
def test_dataflow_event_big_query_success(sdc_builder, sdc_executor, gcp):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives them using wiretap, including a query successfully read event.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY id'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()
    events_wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination
    google_bigquery >= events_wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == len(ROWS_TO_INSERT),\
            f'Expected {len(ROWS_TO_INSERT)} records, received {len(wiretap.output_records)}'
        rows_from_wiretap = get_rows_from_wiretap(wiretap)
        assert rows_from_wiretap == ROWS_TO_INSERT

        # We have exactly one output record, check that it is a big-query-success event
        assert len(events_wiretap.output_records) == 1, f'Expected 1 records, received {len(events_wiretap.output_records)}'
        event_record = events_wiretap.output_records[0]
        event_type = event_record.header.values['sdc.event.type']
        assert event_type == 'big-query-success', 'Received %s as event type (expected new-file)' % event_type
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
@pytest.mark.parametrize('number_batches', [1, 20, 100])
def test_multiple_batch(sdc_builder, sdc_executor, gcp, number_batches):
    """
    Create data using Google BigQuery client with specific column names
    and then check if Google BigQuery origin receives them using wiretap.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    number_records = 100
    batch_size = number_records//number_batches

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    google_bigquery.set_attributes(max_batch_size_in_records=batch_size)
    query_str = f"SELECT * FROM {dataset_name}.{table_name} ORDER BY id"
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('id', 'INTEGER', mode='NULLABLE'),
              SchemaField('data', 'STRING', mode='NULLABLE')]

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        input_values = []
        for i in range(0, number_records):
            input_values += [(i, get_random_string(string.ascii_letters, 5))]
            errors = bigquery_client.insert_rows(table, [(i, get_random_string(string.ascii_letters, 5))])
            assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == number_records,\
            f'Expected {number_records} record, received {len(wiretap.output_records)}'
        records = [record.field for record in wiretap.output_records]
        assert len(records) >= number_batches

        batches = wiretap.batches
        len_records_in_batches = 0
        for bat in batches:
            len_records_in_batches = len_records_in_batches + len(bat)
        assert len(records) == len_records_in_batches
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
def test_data_format(sdc_builder, sdc_executor, gcp):
    pytest.skip("Google BigQuery Origin doesn't deal with data formats")


@gcp
def test_resume_offset(sdc_builder, sdc_executor, gcp):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives them using wiretap.
    Stop the pipeline, add more data, and check the second batch.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY id'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == len(ROWS_TO_INSERT),\
            f'Expected {len(ROWS_TO_INSERT)} records, received {len(wiretap.output_records)}'
        rows_from_wiretap = get_rows_from_wiretap(wiretap)
        assert rows_from_wiretap == ROWS_TO_INSERT

        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT_2)
        assert not errors, 'Errors found when creating data using bigquery client'

        wiretap.reset()
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records_count = len(ROWS_TO_INSERT) + len(ROWS_TO_INSERT_2)
        assert len(wiretap.output_records) == records_count,\
            f'Expected {records_count} records, received {len(wiretap.output_records)}'
        rows_from_wiretap_2 = get_rows_from_wiretap(wiretap)
        assert rows_from_wiretap_2 == ROWS_TO_INSERT + ROWS_TO_INSERT_2
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
def test_empty_object_data(sdc_builder, sdc_executor, gcp):
    """
    Create empty data using Google BigQuery client
    and then check if Google BigQuery origin receives them using wiretap.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name}'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('id', 'INTEGER', mode='NULLABLE'),
              SchemaField('name', 'STRING', mode='NULLABLE')]

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, [(None, '')])
        assert not errors, 'Errors found when creating data using bigquery client'

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1, f'Expected 1 record, received {len(wiretap.output_records)}'
        received_data = wiretap.output_records[0].field['name']

        assert received_data == ''
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


def get_rows_from_wiretap(wiretap):
    get_value_by_index = lambda x: list(x[0].field.values())[x[1]].value
    return [(int(get_value_by_index((record, 0)))
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
            for record in wiretap.output_records]
