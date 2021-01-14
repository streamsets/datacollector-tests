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
@pytest.mark.parametrize('stage_attributes', [{'copy_all_fields': False}, {'copy_all_fields': True}])
def test_copy_all_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'field_does_not_exist': 'CONTINUE'},
                                              {'field_does_not_exist': 'TO_ERROR'}])
def test_field_does_not_exist(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_pivot(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'save_original_field_name': True}])
def test_original_field_name_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'copy_all_fields': True}])
def test_pivoted_items_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'copy_all_fields': True,
                                               'pivoted_items_path': 'true',
                                               'remove_pivot_field': False},
                                              {'copy_all_fields': True,
                                               'pivoted_items_path': 'true',
                                               'remove_pivot_field': True}])
def test_remove_pivot_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'save_original_field_name': False}, {'save_original_field_name': True}])
def test_save_original_field_name(sdc_builder, sdc_executor, stage_attributes):
    pass

