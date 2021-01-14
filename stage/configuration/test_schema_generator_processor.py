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
@pytest.mark.parametrize('stage_attributes', [{'enable_cache': True}])
def test_cache_key_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_cache': True}])
def test_cache_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_default_precision(sdc_builder, sdc_executor):
    pass


@stub
def test_default_scale(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'default_to_nullable': False,
                                               'nullable_fields': True,
                                               'schema_type': 'AVRO'},
                                              {'default_to_nullable': True,
                                               'nullable_fields': True,
                                               'schema_type': 'AVRO'}])
def test_default_to_nullable(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nullable_fields': False, 'schema_type': 'AVRO'}])
def test_default_values_for_types(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'schema_type': 'AVRO'}])
def test_doc(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_cache': False}, {'enable_cache': True}])
def test_enable_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'expand_types': False, 'schema_type': 'AVRO'},
                                              {'expand_types': True, 'schema_type': 'AVRO'}])
def test_expand_types(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_header_attribute(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'schema_type': 'AVRO'}])
def test_namespace(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'nullable_fields': False, 'schema_type': 'AVRO'},
                                              {'nullable_fields': True, 'schema_type': 'AVRO'}])
def test_nullable_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_precision_field_attribute(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_scale_field_attribute(sdc_builder, sdc_executor):
    pass


@stub
def test_schema_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'schema_type': 'AVRO'}])
def test_schema_type(sdc_builder, sdc_executor, stage_attributes):
    pass

