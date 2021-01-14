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
def test_account_fqdn(sdc_builder, sdc_executor):
    pass


@stub
def test_advanced_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_application_id(sdc_builder, sdc_executor):
    pass


@stub
def test_application_key(sdc_builder, sdc_executor):
    pass


@stub
def test_auth_token_endpoint(sdc_builder, sdc_executor):
    pass


@stub
def test_file_path(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'move_file': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'move_file': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_move_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_acls': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_acls': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_acls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_ownership': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_group(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'move_file': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_new_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'rename': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_new_name(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_ownership': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_owner(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_permissions': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_permissions': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_new_permissions(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'rename': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'rename': True, 'task': 'CHANGE_EXISTING_FILE'}])
def test_rename(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_acls': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_acls': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_acls': False, 'task': 'CREATE_EMPTY_FILE'},
                                              {'set_acls': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_set_acls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_ownership': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_ownership': False, 'task': 'CREATE_EMPTY_FILE'},
                                              {'set_ownership': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_set_ownership(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'set_permissions': False, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_permissions': True, 'task': 'CHANGE_EXISTING_FILE'},
                                              {'set_permissions': False, 'task': 'CREATE_EMPTY_FILE'},
                                              {'set_permissions': True, 'task': 'CREATE_EMPTY_FILE'}])
def test_set_permissions(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'CHANGE_EXISTING_FILE'},
                                              {'task': 'CREATE_EMPTY_FILE'},
                                              {'task': 'REMOVE_FILE'}])
def test_task(sdc_builder, sdc_executor, stage_attributes):
    pass

