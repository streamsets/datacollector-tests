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

import json

import pytest
from streamsets.testframework.decorators import stub


def test_fields_to_rename(sdc_builder, sdc_executor):
    DATA = dict(name='Al Gore', birthplace='Washington, D.C.')
    EXPECTED_RENAMED_DATA = dict(internetInventor='Al Gore', birthplace='Washington, D.C.')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True
    field_renamer = pipeline_builder.add_stage('Field Renamer')
    field_renamer.fields_to_rename = [{'fromFieldExpression': '/name', 'toFieldExpression': '/internetInventor'}]
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_renamer >> wiretap.destination
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    record = wiretap.output_records[0]
    assert record.field == EXPECTED_RENAMED_DATA


@stub
@pytest.mark.parametrize('stage_attributes', [{'multiple_source_field_matches': 'CONTINUE'},
                                              {'multiple_source_field_matches': 'TO_ERROR'}])
def test_multiple_source_field_matches(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'source_field_does_not_exist': 'CONTINUE'},
                                              {'source_field_does_not_exist': 'TO_ERROR'}])
def test_source_field_does_not_exist(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'target_field_already_exists': 'APPEND_NUMBERS'},
                                              {'target_field_already_exists': 'CONTINUE'},
                                              {'target_field_already_exists': 'REPLACE'},
                                              {'target_field_already_exists': 'TO_ERROR'}])
def test_target_field_already_exists(sdc_builder, sdc_executor, stage_attributes):
    pass

