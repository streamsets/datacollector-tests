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
import json

import pytest
from streamsets.sdk.sdc_api import RunError

@pytest.mark.parametrize('stage_attributes', [{'field_does_not_exist': 'ADD_FIELD'},
                                              {'field_does_not_exist': 'CONTINUE'},
                                              {'field_does_not_exist': 'TO_ERROR'}])
def test_field_does_not_exist(sdc_builder, sdc_executor, stage_attributes):
    """Validate behavior when the field that is being operated on does not exist within a record."""

    DATA = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}

    EXPECTED_OUTPUT_IF_ADD_FIELD = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.', 'age': '60'}
    EXPECTED_OUTPUT_IF_CONTINUE = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}
    EXPECTED_OUTPUT_IF_TO_ERROR = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}

    field_does_not_exist = stage_attributes['field_does_not_exist']

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.set_attributes(replacement_rules=[{'setToNull': False, 'fields': '/age', 'replacement': '60'}],
                                  **stage_attributes)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    if field_does_not_exist == 'ADD_FIELD':
        record = wiretap.output_records[0]
        assert record.field == EXPECTED_OUTPUT_IF_ADD_FIELD and not wiretap.error_records
    elif field_does_not_exist == 'CONTINUE':
        record = wiretap.output_records[0]
        assert record.field == EXPECTED_OUTPUT_IF_CONTINUE and not wiretap.error_records
    elif field_does_not_exist == 'TO_ERROR':
        record = wiretap.error_records[0]
        assert record.field == EXPECTED_OUTPUT_IF_TO_ERROR and not wiretap.output_records


@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    """Validate behavior when a record generates an error."""

    DATA = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}
    on_record_error = stage_attributes['on_record_error']

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.set_attributes(replacement_rules=[{'setToNull': False, 'fields': '/age'}],
                                  field_does_not_exist='TO_ERROR',
                                  **stage_attributes)

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)

    if on_record_error == 'DISCARD':
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert not wiretap.error_records and not wiretap.output_records

    elif on_record_error == 'STOP_PIPELINE':
        try:
            sdc_executor.start_pipeline(pipeline).wait_for_status('RUN_ERROR')

            assert False, 'An exception should have been thrown'
        except RunError:

            assert not wiretap.error_records and not wiretap.output_records

    elif on_record_error == 'TO_ERROR':
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        record = wiretap.error_records[0]
        assert record.field == DATA and not wiretap.output_records


@pytest.mark.parametrize('precondition_field_present', [True, False])
def test_preconditions(sdc_builder, sdc_executor, precondition_field_present):
    """Validate behavior when preconditions match and do not match a record."""

    DATA = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}

    if precondition_field_present:
        DATA['age'] = 60

    EXPECTED_OUTPUT_IF_PRECONDITION_FIELD_PRESENT = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.', 'age': 60}
    EXPECTED_OUTPUT_IF_PRECONDITION_FIELD_NOT_PRESENT = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.set_attributes(replacement_rules=[],
                                  preconditions=['${record:value("/age") != NULL}'])

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    if precondition_field_present:
        record = wiretap.output_records[0]
        assert record.field == EXPECTED_OUTPUT_IF_PRECONDITION_FIELD_PRESENT and not wiretap.error_records
    else:
        record = wiretap.error_records[0]
        assert record.field == EXPECTED_OUTPUT_IF_PRECONDITION_FIELD_NOT_PRESENT and not wiretap.output_records


@pytest.mark.parametrize('replacement_rule_property', ['SET_TO_NULL', 'NEW_VALUE'])
def test_replacement_rules(sdc_builder, sdc_executor, replacement_rule_property):
    """Validate replacement rule output on records."""

    DATA = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}

    EXPECTED_OUTPUT_IF_SET_TO_NULL = {'name': 'Al Gore', 'birthplace': None}
    EXPECTED_OUTPUT_IF_NEW_VALUE = {'name': 'Al Gore', 'birthplace': 'Houston'}

    if replacement_rule_property == 'SET_TO_NULL':
        replacement_rules = [{'setToNull': True, 'fields': '/birthplace'}]
    elif replacement_rule_property == 'NEW_VALUE':
        replacement_rules = [{'setToNull': False, 'fields': '/birthplace', 'replacement': 'Houston'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.replacement_rules = replacement_rules

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    if replacement_rule_property == 'SET_TO_NULL':
        record = wiretap.output_records[0]
        assert record.field == EXPECTED_OUTPUT_IF_SET_TO_NULL
    elif replacement_rule_property == 'NEW_VALUE':
        record = wiretap.output_records[0]
        assert record.field == EXPECTED_OUTPUT_IF_NEW_VALUE


@pytest.mark.parametrize('required_field_present', [True, False])
def test_required_fields(sdc_builder, sdc_executor, required_field_present):
    """Validate behavior when a required field does and does not exist within a record."""

    DATA = {'name': 'Al Gore', 'birthplace': 'Washington, D.C.'}

    if required_field_present:
        DATA['age'] = 60

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.set_attributes(replacement_rules=[],
                                  required_fields = ['/age'])

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination

    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    if required_field_present:
        record = wiretap.output_records[0]
        assert record.field == DATA and not wiretap.error_records
    else:
        record = wiretap.error_records[0]
        assert record.field == DATA and not wiretap.output_records
