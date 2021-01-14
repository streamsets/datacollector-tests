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
@pytest.mark.parametrize('stage_attributes', [{'hash_entire_record': True, 'use_field_separator': True},
                                              {'use_field_separator': True}])
def test_field_separator(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'hash_entire_record': False}, {'hash_entire_record': True}])
def test_hash_entire_record(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_hash_in_place(sdc_builder, sdc_executor):
    pass


@stub
def test_hash_to_target(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'hash_entire_record': True, 'hash_type': 'MD5'},
                                              {'hash_entire_record': True, 'hash_type': 'MURMUR3_128'},
                                              {'hash_entire_record': True, 'hash_type': 'SHA1'},
                                              {'hash_entire_record': True, 'hash_type': 'SHA256'},
                                              {'hash_entire_record': True, 'hash_type': 'SHA512'}])
def test_hash_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'hash_entire_record': True}])
def test_header_attribute(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'hash_entire_record': True, 'include_record_header': False},
                                              {'hash_entire_record': True, 'include_record_header': True}])
def test_include_record_header(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_field_issue': 'CONTINUE'}, {'on_field_issue': 'TO_ERROR'}])
def test_on_field_issue(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'hash_entire_record': True}])
def test_target_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'hash_entire_record': True, 'use_field_separator': False},
                                              {'hash_entire_record': True, 'use_field_separator': True},
                                              {'use_field_separator': False},
                                              {'use_field_separator': True}])
def test_use_field_separator(sdc_builder, sdc_executor, stage_attributes):
    pass

