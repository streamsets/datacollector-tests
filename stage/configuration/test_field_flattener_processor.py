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
@pytest.mark.parametrize('stage_attributes', [{'collision_field_action': 'DISCARD',
                                               'flatten': 'SPECIFIC_FIELDS',
                                               'flatten_in_place': False},
                                              {'collision_field_action': 'OVERRIDE',
                                               'flatten': 'SPECIFIC_FIELDS',
                                               'flatten_in_place': False},
                                              {'collision_field_action': 'TO_ERROR',
                                               'flatten': 'SPECIFIC_FIELDS',
                                               'flatten_in_place': False}])
def test_collision_field_action(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'flatten': 'SPECIFIC_FIELDS'}])
def test_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'flatten': 'ENTIRE_RECORD'}, {'flatten': 'SPECIFIC_FIELDS'}])
def test_flatten(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'flatten': 'SPECIFIC_FIELDS', 'flatten_in_place': False},
                                              {'flatten': 'SPECIFIC_FIELDS', 'flatten_in_place': True}])
def test_flatten_in_place(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_name_separator(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'flatten': 'SPECIFIC_FIELDS',
                                               'flatten_in_place': False,
                                               'remove_flattened_field': False},
                                              {'flatten': 'SPECIFIC_FIELDS',
                                               'flatten_in_place': False,
                                               'remove_flattened_field': True}])
def test_remove_flattened_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'flatten': 'SPECIFIC_FIELDS', 'flatten_in_place': False}])
def test_target_field(sdc_builder, sdc_executor, stage_attributes):
    pass

