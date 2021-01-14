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

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_table_and_column_families': False},
                                              {'create_table_and_column_families': True}])
def test_create_table_and_column_families(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_default_column_family_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'explicit_column_family_mapping': False},
                                              {'explicit_column_family_mapping': True}])
def test_explicit_column_family_mapping(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_missing_data_values': False},
                                              {'ignore_missing_data_values': True}])
def test_ignore_missing_data_values(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_instance_id(sdc_builder, sdc_executor):
    pass


@stub
def test_number_of_records_to_buffer(sdc_builder, sdc_executor):
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
def test_project_id(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'row_key_is_composite': False}])
def test_row_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'row_key_is_composite': True}])
def test_row_key_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'row_key_is_composite': False}, {'row_key_is_composite': True}])
def test_row_key_is_composite(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'time_basis': 'BATCH_START'},
                                              {'time_basis': 'FROM_RECORD'},
                                              {'time_basis': 'PIPELINE_START'},
                                              {'time_basis': 'SYSTEM_TIME'}])
def test_time_basis(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'time_basis': 'FROM_RECORD'}])
def test_time_stamp_field_name(sdc_builder, sdc_executor, stage_attributes):
    pass

