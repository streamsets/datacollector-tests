# Copyright 2017 StreamSets Inc.
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

import logging

import pytest
from streamsets.sdk.exceptions import InternalServerError

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# CONTAINER_01200 - doesn't have permissions on pipeline.
ERROR_CODE_PIPELINE_PERMISSIONS = 'CONTAINER_01200'


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_user('testuser', roles=['manager'], groups=['contractor'])
        data_collector.sdc_properties['pipeline.access.control.enabled'] = 'true'

    return hook


@pytest.fixture(scope='module')
def pipeline(sdc_executor):
    pipeline_builder = sdc_executor.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    yield pipeline


def test_validate_default_acl(sdc_executor, pipeline):
    acl_response = sdc_executor.get_pipeline_acl(pipeline)
    assert acl_response['resourceOwner'] == 'admin'
    assert acl_response['resourceType'] == 'PIPELINE'
    assert acl_response['resourceId'] == pipeline.id
    assert any(i['subjectType'] == 'USER' for i in acl_response.permissions)
    for i in acl_response.permissions:
        if i['subjectType'] == 'USER':
            assert i['subjectId'] == 'admin'
            assert i['actions'] == ['READ', 'WRITE', 'EXECUTE']


def test_validate_default_permissions(sdc_executor, pipeline):
    perm_response = sdc_executor.get_pipeline_permissions(pipeline)
    assert any(i['subjectType'] == 'USER' for i in perm_response.permissions)
    for i in perm_response.permissions:
        if i['subjectType'] == 'USER':
            assert i['subjectId'] == 'admin'
            assert i['actions'] == ['READ', 'WRITE', 'EXECUTE']


def test_modified_acl(sdc_executor, pipeline):
    acl_mod = sdc_executor.get_pipeline_acl(pipeline)
    for permission in acl_mod.permissions:
        if permission['subjectType'] == 'USER':
            permission['subjectId'] = 'testuser'
            permission['actions'] = ['READ', 'WRITE']
    sdc_executor.set_pipeline_acl(pipeline, pipeline_acl=acl_mod)

    sdc_executor.set_user('testuser')
    sdc_executor.dump_log_on_error = False
    with pytest.raises(InternalServerError) as exception_info:
        sdc_executor.start_pipeline(pipeline)
    sdc_executor.dump_log_on_error = True
    assert ERROR_CODE_PIPELINE_PERMISSIONS in exception_info.value.text
