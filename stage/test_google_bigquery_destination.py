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
import json
import logging
from string import ascii_letters

import pytest
from google.cloud.bigquery import Dataset, SchemaField, Table
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

name=DESTINATION_STAGE_NAME = 'com_streamsets_pipeline_stage_bigquery_destination_BigQueryDTarget'

pytestmark = [pytest.mark.category('nonstandard')]

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
    google_bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME, type='destination')
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
        bigquery_client.create_dataset(dataset_ref)
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
        dev_data_generator >> field_type_converter >> [google_bigquery, wiretap.destination]
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
    batch_size = 1000
    dev_data_generator.set_attributes(delay_between_batches=1000, batch_size=batch_size)

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME, type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name,
                                   stage_on_record_error='TO_ERROR')

    # Create Field Type Converter
    conversions = [{'fields': ['/field4'],
                    'targetType': 'FLOAT'}]
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=conversions)

    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> field_type_converter >> [google_bigquery, wiretap.destination]

    pipeline = pipeline_builder.build()

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
        bigquery_client.create_dataset(dataset_ref)
        table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))

        logger.info('Starting BigQuery Destination pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1_000, timeout_sec=60)
        sdc_executor.stop_pipeline(pipeline)

        # Verify by reading records using Google BigQuery client
        data_from_bigquery = [{"field1" : row.values()[0],
                               "field2" : row.values()[1].replace(microsecond = row.values()[1].microsecond * 1000),
                               "field3" : row.values()[2],
                               "field4" : row.values()[3],
                               "field5" : row.values()[4]}
                              for row in bigquery_client.list_rows(table)]

        data_from_wiretap = [rec.field for rec in wiretap.output_records]
        assert len(data_from_bigquery) >= batch_size
        assert len(wiretap.error_records) == 0

        assert len(data_from_bigquery) == len(data_from_wiretap)
        assert all([element in data_from_bigquery for element in data_from_wiretap])
        
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
        data_format='JSON',
        raw_data=json.dumps(json_data),
        stop_after_first_batch=True
    )

    # Google BigQuery Destination
    dataset_name = 'dont_care'
    table_name = '${record:value(\'/table\')}'
    google_bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME, type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name,
                                   stage_on_record_error='TO_ERROR')

    wiretap = pipeline_builder.add_wiretap()

    # Implement pipeline topology
    dev_raw_data_source >> [google_bigquery, wiretap.destination]

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline.configure_for_environment(gcp))

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Verify that we have exactly one record
    assert len(wiretap.error_records) == 1
    # Verify that the error is indeed a BIGQUERY_18 (table name is empty or expression evaluates to empty)
    assert wiretap.error_records[0].header['errorCode'] == 'BIGQUERY_18'
