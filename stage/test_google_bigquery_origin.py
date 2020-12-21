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

import base64
import logging
from string import ascii_letters

from google.cloud.bigquery import Dataset, SchemaField, Table
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

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


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigquery_origin(sdc_builder, sdc_executor, gcp):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives the data.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY id'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)
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
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        get_value_by_index = lambda x: list(x[0].field.values())[x[1]].value
        rows_from_wiretap = [(int(get_value_by_index((record, 0)))
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
                             for record in wiretap.output_records]

        assert rows_from_wiretap == ROWS_TO_INSERT
    finally:
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True)


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigquery_origin_stop_resume(sdc_builder, sdc_executor, gcp):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives them using wiretap.
    Stop the pipeline, add more data, and check the second batch.

    The pipeline looks like:
        google_bigquery >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY id'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
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
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT)
        assert not errors

        # Start pipeline and verify correct rows are received.
        logger.info('Starting pipeline')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        get_value_by_index = lambda x: list(x[0].field.values())[x[1]].value
        rows_from_wiretap = [(int(get_value_by_index((record, 0)))
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

        assert rows_from_wiretap == ROWS_TO_INSERT

        errors = bigquery_client.insert_rows(table, ROWS_TO_INSERT_2)
        assert not errors

        wiretap.reset()
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        get_value_by_index = lambda x: list(x[0].field.values())[x[1]].value

        rows_from_wiretap_2 = [(int(get_value_by_index((record, 0)))
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

        assert rows_from_wiretap_2 == ROWS_TO_INSERT + ROWS_TO_INSERT_2
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

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    schema = [SchemaField('number', 'STRING', mode='required'), SchemaField('value', 'STRING', mode='required')]

    data = [dict(number=str(i), value=get_random_string()) for i in range(TOTAL_RECORDS)]
    dataset = Dataset(bigquery_client.dataset(dataset_name))

    try:
        # Using Google bigquery client, create dataset, table and data inside table.
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset)
        table = bigquery_client.create_table(Table(dataset.table(table_name), schema=schema))
        errors = bigquery_client.insert_rows(table, data)
        assert not errors

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)

        # Assert that the batch size is being respected in each batch (including the last). In this case,
        # we'd expect batch sizes of 3, 3, and 2.
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == 3
        assert history.latest.metrics.histogram('stage.GoogleBigQuery_01.outputRecords.histogramM5')._data['count'] == 3
        assert history.latest.metrics.histogram('stage.GoogleBigQuery_01.outputRecords.histogramM5')._data['max'] == 3
        assert history.latest.metrics.histogram('stage.GoogleBigQuery_01.outputRecords.histogramM5')._data['min'] == 2

        records = [record.field for record in wiretap.output_records]
        assert len(records) == len(data)
        assert records == data
    finally:
        bigquery_client.delete_dataset(dataset, delete_contents=True)
