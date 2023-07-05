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
from string import ascii_lowercase

import pytest
from google.cloud.bigquery import Dataset, SchemaField, Table, TableReference
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string, wait_for_condition
from streamsets.testframework.decorators import stub
from stage.utils.common import cleanup

logger = logging.getLogger(__name__)

DESTINATION_STAGE_NAME = 'com_streamsets_pipeline_stage_bigquery_destination_BigQueryDTarget'

pytestmark = [pytest.mark.category('nonstandard')]


@stub
@pytest.mark.parametrize('stage_attributes', [{'credentials_provider': 'JSON'}])
def test_credentials_file_content_in_json(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'credentials_provider': 'JSON_PROVIDER'}])
def test_credentials_file_path_in_json(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'credentials_provider': 'DEFAULT_PROVIDER'},
                                              {'credentials_provider': 'JSON'},
                                              {'credentials_provider': 'JSON_PROVIDER'}])
def test_credentials_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_dataset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_invalid_column': False}, {'ignore_invalid_column': True}])
def test_ignore_invalid_column(sdc_builder, sdc_executor, stage_attributes):
    pass


@gcp
def test_insert_id_expression(sdc_builder, sdc_executor, gcp, cleanup):
    """
        NOTE: When you supply insertId for an inserted row, BigQuery uses this ID to support best effort de-duplication
        for up to one minute De-duplication offered by BigQuery is best effort, and it should not be relied upon as
        a mechanism to guarantee the absence of duplicates in your data. Additionally, BigQuery might degrade the
        quality of best effort de-duplication at any time in order to guarantee higher reliability and availability
        for your data. https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery#dataconsistency
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    rows = [{'a': 1, "info1": 11, "info2": 111},
            {'a': 2, "info1": 22, "info2": 222},
            {'a': 1, "info1": 33, "info2": 333}]

    DATA = '\n'.join(json.dumps(rec) for rec in rows)

    schema = [SchemaField('a', 'STRING', mode='REQUIRED'),
              SchemaField('info1', 'INTEGER', mode='REQUIRED'),
              SchemaField('info2', 'INTEGER', mode='REQUIRED')]

    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=DATA,
                                       stop_after_first_batch=True)

    dataset_name = get_random_string(string.ascii_letters, 5)
    table_name = get_random_string(string.ascii_letters, 5)
    row_id_expression = "${record:value(\'/a\')}"
    google_bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME, type='destination')
    google_bigquery.set_attributes(dataset=dataset_name,
                                   table_name=table_name,
                                   insert_id_expression=row_id_expression)

    dev_raw_data_source >> google_bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    bigquery_client = gcp.bigquery_client
    dataset_ref = Dataset(bigquery_client.dataset(dataset_name))
    cleanup.callback(bigquery_client.delete_dataset, dataset_ref, delete_contents=True)

    # Using Google bigquery client, create dataset, table and data inside table
    logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
    bigquery_client.create_dataset(dataset_ref)
    logger.info('Creating table %s using Google bigquery client', table_name)
    table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=schema))

    # Start pipeline and verify correct rows are received.
    logger.info('Starting pipeline')
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    cleanup.callback(sdc_executor.stop_pipeline, pipeline, wait=False)

    def condition():
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()
        return len(data_from_bigquery) == 2

    def failure(timeout):
        data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()
        logger.info('Timed out after `{}` seconds waiting for data from bigquery `{}` to reach `{}` '
                .format(timeout, len(data_from_bigquery), 2))
    wait_for_condition(condition=condition, timeout=120, failure=failure)

    data_from_bigquery = [tuple(row.values()) for row in bigquery_client.list_rows(table)]
    data_from_bigquery.sort()

    # We need two check two conditions for the GCP best effort de-duplication.
    assert len(data_from_bigquery) == 2 or len(data_from_bigquery) == len(rows)


@gcp
@sdc_min_version("3.11.0")
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, gcp, stage_attributes):
    """Test that BigQuery API does not return a NullPointerException if asked for an empty table name
    Pipeline:
        dev_raw_data_source >> [google_bigquery, wiretap]
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Dev Raw Data Source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')

    data = {'table': ''}
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json.dumps(data),
                                       stop_after_first_batch=True)

    dataset = get_random_string(ascii_lowercase, 10)
    table_name = '${record:value(\'/table\')}'
    google_bigquery = pipeline_builder.add_stage(name=DESTINATION_STAGE_NAME, type='destination')
    google_bigquery.set_attributes(dataset=dataset,
                                   table_name=table_name,
                                   stage_on_record_error=stage_attributes['on_record_error'])

    wiretap = pipeline_builder.add_wiretap()

    # Implement pipeline topology
    dev_raw_data_source >> [google_bigquery, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
    except Exception as e:
        if stage_attributes['on_record_error'] == 'STOP_PIPELINE':
            logger.info("Verify that pipeline transitioned to RUN_ERROR state...")
            status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
            assert status == 'RUN_ERROR'
            assert 'BIGQUERY_18' in e.response['message']
    finally:
        if stage_attributes['on_record_error'] == 'TO_ERROR':
            logger.info("Verify that destination stage produced 1 error record...")
            # Verify that we have exactly one record
            assert len(wiretap.error_records) == 1
            assert wiretap.error_records[0].header['errorCode'] == 'BIGQUERY_18'
            status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
            assert status == 'FINISHED'


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_project_id(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_table_cache_size(sdc_builder, sdc_executor):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass
