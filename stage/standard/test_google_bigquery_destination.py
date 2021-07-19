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
import datetime
from isodate import UTC

import pytest
from google.cloud.bigquery import Dataset, SchemaField, Table
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.category('standard')]

bytes_column = base64.b64encode("dataAsBytes".encode('utf-8'))

SCHEMA = [SchemaField('full_name', 'STRING', mode='required'),
          SchemaField('age', 'INTEGER', mode='required')]

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
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# https://docs.streamsets.com/portal/#datacollector/latest/help/datacollector/UserGuide/Destinations/BigQuery.html
# For every data type supported
@pytest.mark.parametrize('data_type, data, expected_data', [
    ('INTEGER', 34, 34),
    ('STRING', 'gcp standard test 123', 'gcp standard test 123'),
    ('FLOAT', 2424.2424, 2424.2424),
    ('NUMERIC', -999999999999999.999999995, Decimal(-999999999999999.999999995)),
    ('BOOLEAN', True, True),
    ('DATE', '2019-02-05', datetime.date(2019, 2, 5)),
    ('DATETIME', '2019-02-05 23:59:59.999', datetime.datetime(2019, 2, 5, 23, 59, 59, 999000)),
    ('TIMESTAMP', '2007-05-28 07:52:31.123 UTC', datetime.datetime(2007, 5, 28, 7, 52, 31, 123000, tzinfo=UTC)),
    ('TIME', '08:39:01.123',  datetime.time(8, 39, 1, 123000)),
    ('BYTES', 'ZGF0YUFzQnl0ZXM=', b'dataAsBytes')
])
def test_data_type(sdc_builder, sdc_executor, gcp, data_type, data, expected_data):
    """
    Create data using Google BigQuery destination with different data types
    and then check if it has been correctly created using BigQuery client.

    The pipeline looks like:
        dev_raw_data_source >> google_bigquery
    """
    data_to_insert = [(1, data)]
    rows_to_insert = ['id,data'] + [','.join(str(element) for element in row) for row in data_to_insert]
    data_to_expect = [(1, expected_data)]
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       stop_after_first_batch=True,
                                       raw_data='\n'.join(rows_to_insert))

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)

    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name)

    dev_raw_data_source >> google_bigquery

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

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        assert data_to_expect == data_from_bigquery
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
    Create data using Google BigQuery destination with different table names
    and then check if it has been correctly created using BigQuery client.

    The pipeline looks like:
        dev_raw_data_source >> google_bigquery
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       stop_after_first_batch=True,
                                       raw_data='\n'.join(CSV_DATA_TO_INSERT))

    # If tests fail for any reason, leftovers with equal names might lead to more errors
    table_name = f'{table_name}_{get_random_string(string.ascii_letters, 5)}'
    dataset_name = get_random_string(string.ascii_letters, 5)

    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name)

    dev_raw_data_source >> google_bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        assert ROWS_EXPECTED == data_from_bigquery
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
    Create data using Google BigQuery destination with different dataset names
    and then check if it has been correctly created using BigQuery client.

    The pipeline looks like:
        dev_raw_data_source >> google_bigquery
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       stop_after_first_batch=True,
                                       raw_data='\n'.join(CSV_DATA_TO_INSERT))

    table_name = get_random_string(string.ascii_letters, 5)
    # If tests fail for any reason, leftovers with equal names might lead to more errors
    dataset_name = f'{dataset_name}_{get_random_string(string.ascii_letters, 5)}'

    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name)

    dev_raw_data_source >> google_bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        assert ROWS_EXPECTED == data_from_bigquery
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
    Create data using Google BigQuery destination with different column names
    and then check if it has been correctly created using BigQuery client.

    The pipeline looks like:
        dev_raw_data_source >> google_bigquery
    """
    data_to_insert = [(1, 'data')]
    rows_to_insert = [f'id,{column_name}'] + [','.join(str(element) for element in row) for row in data_to_insert]
    data_to_expect = [(1, 'data')]
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       stop_after_first_batch=True,
                                       raw_data='\n'.join(rows_to_insert))

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)

    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name)

    dev_raw_data_source >> google_bigquery

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

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        assert data_to_expect == data_from_bigquery
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
def test_dataflow_event(sdc_builder, sdc_executor, gcp):
    pytest.skip('Google BigQuery does not produce events.')


@gcp
@pytest.mark.parametrize('number_batches', [1, 20, 100])
def test_multiple_batch(sdc_builder, sdc_executor, gcp, number_batches):
    """
    Create data using Google BigQuery destination using different batch sizes
    and then check if it has been correctly created using BigQuery client.

    The pipeline looks like:
        dev_raw_data_source >> google_bigquery
    """
    number_records = 1000
    batch_size = number_records//number_batches

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Data Generator')
    dev_raw_data_source.set_attributes(batch_size=batch_size,
                                       fields_to_generate=[
                                           {"type": "INTEGER",  "field": "age"},
                                           {"type": "STRING",  "field": "full_name"}
                                       ])

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name)

    dev_raw_data_source >> google_bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client

    try:
        dataset_ref = Dataset(bigquery_client.dataset(dataset_name))

        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=SCHEMA))

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(number_records)
        sdc_executor.stop_pipeline(pipeline)

        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        assert data_from_bigquery == data_from_bigquery

        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        assert len(data_from_bigquery) == records
    finally:
        if dataset_ref:
            bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
def test_data_format(sdc_builder, sdc_executor, gcp):
    pytest.skip("Google BigQuery Destination doesn't deal with data formats")


@gcp
def test_push_pull(sdc_builder, sdc_executor, gcp):
    pytest.skip("Both strategies already used in other standard tests with Dev Raw Data Source and Dev Data Generator.")
