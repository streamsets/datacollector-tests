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

import pytest
import json

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import sdc_min_version


@pytest.mark.parametrize('stage_attributes', [
    {'record_error': None, 'error': None, 'data': {"value": "new_field_name(some"}, 'aggregation_expression': '${fields[0]}', 'expression': '/${record:value("/value")}'},
    {'record_error': True, 'error': 'EXPRESSION_EVALUATION_FAILURE_03', 'data': {"value": True}, 'aggregation_expression': '${0/record:value("/value")}', 'expression': '/${record:value("/value")}'},
    {'record_error': False, 'error': 'INVALID_EXPRESSION_04', 'data': {"value": "value"}, 'aggregation_expression': '${fields', 'expression': '/${record:value("/value")}'},
])
@sdc_min_version('3.8.0')
def test_aggregation_expression(sdc_builder, sdc_executor, stage_attributes):
    """
    Here we want to test that
    1. A record error should be added if a aggregation_expression cannot be evaluated.
    2. If a aggregation_expression is invalid the pipeline should be aborted.
    3. If a aggregation_expression evaluates to a valid value a corresponding field should be modified as needed.

    The pipeline is as follows:

    Dev Raw Data Source >> Field Mapper >> Wiretap
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.json_content = 'MULTIPLE_OBJECTS'
    dev_raw_data_source.raw_data = json.dumps(stage_attributes['data'])
    dev_raw_data_source.stop_after_first_batch = True

    mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    mapper.operate_on = 'FIELD_PATHS'
    mapper.mapping_expression = stage_attributes['expression']
    mapper.aggregation_expression = stage_attributes['aggregation_expression']

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> mapper >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status(
        status='FINISHED' if stage_attributes['record_error'] is None or stage_attributes['record_error'] else 'START_ERROR',
        ignore_errors=True
    )

    if stage_attributes['record_error'] is None:
        assert [record.field for record in wiretap.output_records] == [{
            stage_attributes['data']['value']: stage_attributes['data']['value']
        }]
    elif stage_attributes['record_error']:
        assert len(wiretap.output_records) == 0
        assert (len(wiretap.error_records) == 1
                and stage_attributes['error'] in wiretap.error_records[0].header['errorMessage'])
    else:
        status = sdc_executor.get_pipeline_status(pipeline).response.json()
        assert stage_attributes['error'] in status['message']


@stub
@pytest.mark.parametrize('stage_attributes', [{'append_list_values': False, 'operate_on': 'FIELD_NAMES'},
                                              {'append_list_values': True, 'operate_on': 'FIELD_NAMES'},
                                              {'append_list_values': False, 'operate_on': 'FIELD_PATHS'},
                                              {'append_list_values': True, 'operate_on': 'FIELD_PATHS'}])
def test_append_list_values(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [
    {'record_error': None, 'error': None, 'data': {"value": "new_field_name"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '/${record:value("/value")}', 'operate_on': 'FIELD_PATHS'},
    {'record_error': True, 'error': 'EXPRESSION_EVALUATION_FAILURE_03', 'data': {"value": True}, 'conditional_expression': '${0/record:value("/value")}', 'expression': '/${record:value("/value")}', 'operate_on': 'FIELD_PATHS'},
    {'record_error': False, 'error': 'INVALID_EXPRESSION_04', 'data': {"value": "new_field_name"}, 'conditional_expression': '${f:name() == "value"', 'expression': '/${record:value("/value")}', 'operate_on': 'FIELD_PATHS'},
    {'record_error': None, 'error': None, 'data': {"value": "new_field_name"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_NAMES'},
    {'record_error': True, 'error': 'EXPRESSION_EVALUATION_FAILURE_03', 'data': {"value": True}, 'conditional_expression': '${0/record:value("/value")}', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_NAMES'},
    {'record_error': False, 'error': 'INVALID_EXPRESSION_04', 'data': {"value": "new_field_name"}, 'conditional_expression': '${f:name() == "value"', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_NAMES'},
    {'record_error': None, 'error': None, 'data': {"value": "new_field_name"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_VALUES'},
    {'record_error': True, 'error': 'EXPRESSION_EVALUATION_FAILURE_03', 'data': {"value": True}, 'conditional_expression': '${0/record:value("/value")}', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_VALUES'},
    {'record_error': False, 'error': 'INVALID_EXPRESSION_04', 'data': {"value": "new_field_name"}, 'conditional_expression': '${f:name() == "value"', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_VALUES'}
])
@sdc_min_version('3.8.0')
def test_conditional_expression(sdc_builder, sdc_executor, stage_attributes):
    """
    Here we want to test that
    1. A record error should be added if a conditional_expression cannot be evaluated.
    2. If a conditional_expression is invalid the pipeline should be aborted.
    3. If a conditional_expression evaluates to a valid value a corresponding field should be modified as needed.

    The pipeline is as follows:

    Dev Raw Data Source >> Field Mapper >> Wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.json_content = 'MULTIPLE_OBJECTS'
    dev_raw_data_source.raw_data = json.dumps(stage_attributes['data'])
    dev_raw_data_source.stop_after_first_batch = True

    mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    mapper.operate_on = stage_attributes['operate_on']
    mapper.mapping_expression = stage_attributes['expression']
    mapper.conditional_expression = stage_attributes['conditional_expression']
    mapper.aggregation_expression = ''

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> mapper >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status(
        status='FINISHED' if stage_attributes['record_error'] is None or stage_attributes['record_error'] else 'START_ERROR',
        ignore_errors=True
    )

    if stage_attributes['record_error'] is None:
        assert [record.field for record in wiretap.output_records] == [{
            "value" if stage_attributes['operate_on'] == 'FIELD_VALUES' else stage_attributes['data']['value']: stage_attributes['data']['value']
        }]
    elif stage_attributes['record_error']:
        assert len(wiretap.output_records) == 0
        assert (len(wiretap.error_records) == 1
                and stage_attributes['error'] in wiretap.error_records[0].header['errorMessage'])
    else:
        status = sdc_executor.get_pipeline_status(pipeline).response.json()
        assert stage_attributes['error'] in status['message']


@stub
@pytest.mark.parametrize('stage_attributes', [{'maintain_original_paths': False, 'operate_on': 'FIELD_NAMES'},
                                              {'maintain_original_paths': True, 'operate_on': 'FIELD_NAMES'},
                                              {'maintain_original_paths': False, 'operate_on': 'FIELD_PATHS'},
                                              {'maintain_original_paths': True, 'operate_on': 'FIELD_PATHS'}])
def test_maintain_original_paths(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [
    {'record_error': None, 'error': None, 'data': {"value": "new_field_name(some"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '/${record:value("/value")}', 'operate_on': 'FIELD_PATHS'},
    {'record_error': True, 'error': 'INVALID_EVALUATED_FIELD_PATH_02', 'data': {"value": "new_field_name(\'some"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '/${record:value("/value")}', 'operate_on': 'FIELD_PATHS'},
    {'record_error': True, 'error': 'EXPRESSION_EVALUATION_FAILURE_03', 'data': {"value": True}, 'conditional_expression': '${f:name() == "value"}', 'expression': '/${0/record:value("/value")}', 'operate_on': 'FIELD_PATHS'},
    {'record_error': False, 'error': 'INVALID_EXPRESSION_04', 'data': {"value": "value"}, 'conditional_expression': '${f:name() == "value"', 'expression': '/${record:value("/value")}', 'operate_on': 'FIELD_PATHS'},
    {'record_error': None, 'error': None, 'data': {"value": "new_field_name(\'some"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_NAMES'},
    {'record_error': True, 'error': 'EXPRESSION_EVALUATION_FAILURE_03', 'data': {"value": True}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${0/record:value("/value")}', 'operate_on': 'FIELD_NAMES'},
    {'record_error': False, 'error': 'INVALID_EXPRESSION_04', 'data': {"value": "value"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${record:value("/value")', 'operate_on': 'FIELD_NAMES'},
    {'record_error': None, 'error': None, 'data': {"value": "new_field_name(\'some"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${record:value("/value")}', 'operate_on': 'FIELD_VALUES'},
    {'record_error': True, 'error': 'EXPRESSION_EVALUATION_FAILURE_03', 'data': {"value": True}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${0/record:value("/value")}', 'operate_on': 'FIELD_VALUES'},
    {'record_error': False, 'error': 'INVALID_EXPRESSION_04', 'data': {"value": "value"}, 'conditional_expression': '${f:name() == "value"}', 'expression': '${record:value("/value")', 'operate_on': 'FIELD_VALUES'}
])
@sdc_min_version('3.8.0')
def test_mapping_expression(sdc_builder, sdc_executor, stage_attributes):
    """
    Here we want to test that
    1. if mapping_expression evaluates to an invalid field name, e.g. a field name containing a quote,
    a record error is added instead of a stage error as it was before.
    2. A record error should also be added if a mapping_expression cannot be evaluated.
    3. If a mapping_expression is invalid the pipeline should be aborted.
    4. If a mapping_expression evaluates to a valid field name or value the field is modified as needed.

    Please notice, cases 1 is applicable only for the FIELD_PATHS operation mode.

    The pipeline is as follows:

    Dev Raw Data Source >> Field Mapper >> Wiretap
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.json_content = 'MULTIPLE_OBJECTS'
    dev_raw_data_source.raw_data = json.dumps(stage_attributes['data'])
    dev_raw_data_source.stop_after_first_batch = True

    mapper = pipeline_builder.add_stage('Field Mapper', type='processor')
    mapper.operate_on = stage_attributes['operate_on']
    mapper.mapping_expression = stage_attributes['expression']
    mapper.conditional_expression = stage_attributes['conditional_expression']
    mapper.aggregation_expression = ''

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> mapper >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status(
        status='FINISHED' if stage_attributes['record_error'] is None or stage_attributes['record_error'] else 'START_ERROR',
        ignore_errors=True
    )

    if stage_attributes['record_error'] is None:
        assert [record.field for record in wiretap.output_records] == [{
            "value" if stage_attributes['operate_on'] == 'FIELD_VALUES' else stage_attributes['data']['value']: stage_attributes['data']['value']
        }]
    elif stage_attributes['record_error']:
        assert len(wiretap.output_records) == 0
        assert (len(wiretap.error_records) == 1
                and stage_attributes['error'] in wiretap.error_records[0].header['errorMessage'])
    else:
        status = sdc_executor.get_pipeline_status(pipeline).response.json()
        assert stage_attributes['error'] in status['message']


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'operate_on': 'FIELD_NAMES'},
                                              {'operate_on': 'FIELD_PATHS'},
                                              {'operate_on': 'FIELD_VALUES'}])
def test_operate_on(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'operate_on': 'FIELD_NAMES', 'structure_change_allowed': False},
                                              {'operate_on': 'FIELD_NAMES', 'structure_change_allowed': True},
                                              {'operate_on': 'FIELD_PATHS', 'structure_change_allowed': False},
                                              {'operate_on': 'FIELD_PATHS', 'structure_change_allowed': True}])
def test_structure_change_allowed(sdc_builder, sdc_executor, stage_attributes):
    pass

