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
from string import ascii_lowercase

import pytest
from google.cloud.bigquery import Dataset, SchemaField, Table
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.decorators import stub

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
def test_dataset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_invalid_column': False}, {'ignore_invalid_column': True}])
def test_ignore_invalid_column(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_insert_id_expression(sdc_builder, sdc_executor):
    pass


@gcp
@sdc_min_version("3.11.0")
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, gcp, stage_attributes):
    """Test that BigQuery API does not return a NullPointerException if asked for an empty table name
    Pipeline:
        dev_raw_data_source >> google_bigquery
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
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='destination')
    google_bigquery.set_attributes(dataset=dataset,
                                   table_name=table_name,
                                   stage_on_record_error=stage_attributes['on_record_error'])

    # Implement pipeline topology
    dev_raw_data_source >> google_bigquery

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')
    except Exception as e:
        if stage_attributes['on_record_error'] == 'STOP_PIPELINE':
            logger.info("Verify that pipeline transitioned to RUN_ERROR state...")
            status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
            assert status == 'RUN_ERROR'
            assert 'BIGQUERY_18' in e.response['message']
    finally:
        if stage_attributes['on_record_error'] == 'TO_ERROR':
            stage = snapshot[google_bigquery.instance_name]
            logger.info("Verify that destination stage produced 1 error record...")
            # Verify that we have exactly one record
            assert len(stage.error_records) == 1
            assert stage.error_records[0].header['errorCode'] == 'BIGQUERY_18'
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

