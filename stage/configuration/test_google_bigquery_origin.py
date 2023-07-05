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
import pytest
import logging
from string import ascii_letters
import os
import tempfile

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import gcp
from streamsets.testframework.utils import get_random_string
from google.cloud.bigquery import Dataset, SchemaField, Table

logger = logging.getLogger(__name__)


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
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_project_id(sdc_builder, sdc_executor):
    pass


@stub
def test_query(sdc_builder, sdc_executor):
    pass


@stub
def test_query_timeout_in_sec(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_cached_query_results': False}, {'use_cached_query_results': True}])
def test_use_cached_query_results(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_legacy_sql': False}, {'use_legacy_sql': True}])
def test_use_legacy_sql(sdc_builder, sdc_executor, stage_attributes):
    pass


@gcp
def test_credentials_file_path_in_json_not_found(sdc_builder, sdc_executor, gcp):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY id'
    google_bigquery.set_attributes(query=query_str)

    wiretap = pipeline_builder.add_wiretap()

    google_bigquery >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    # Create a file
    file_path = tempfile.gettempdir()
    file_name = f'{get_random_string(ascii_letters, 10)}.json'

    def _create_file(file_content):
        builder = sdc_executor.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(
            data_format='TEXT',
            raw_data=file_content,
            stop_after_first_batch=True
        )

        local_fs = builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(
            data_format='TEXT',
            directory_template=file_path,
            files_prefix=file_name,
            files_suffix=''
        )

        dev_raw_data_source >> local_fs
        file_pipeline = builder.build(f'Generate files pipeline {file_path}')
        sdc_executor.add_pipeline(file_pipeline)

        sdc_executor.start_pipeline(file_pipeline).wait_for_finished()
        sdc_executor.remove_pipeline(file_pipeline)

    credentials_file_path = file_path + '/' + file_name
    _create_file('Hello World')

    google_bigquery.set_attributes(credentials_provider='JSON_PROVIDER',
                                   credentials_file_path_in_json=credentials_file_path)
    sdc_executor.add_pipeline(pipeline)

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

    dataset = Dataset(bigquery_client.dataset(dataset_name))

    try:
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client.create_dataset(dataset)
        bigquery_client.create_table(Table(dataset.table(table_name), schema=schema))

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.stop_pipeline(pipeline, force=True)
        assert False, 'Should not reach here.'

    except Exception as error:
        assert 'GOOGLE_01' in error.message
