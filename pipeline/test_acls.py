# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import pytest

from testframework import sdc
from testframework.common_exceptions import *  # pylint: disable=wildcard-import

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# CONTAINER_01200 - doesn't have permissions on pipeline.
ERROR_CODE_PIPELINE_PERMISSIONS = 'CONTAINER_01200'

@pytest.fixture(scope='module')
def data_collector():
    dc = sdc.DataCollector()
    dc.add_user('testuser', roles=['manager'], groups=['contractor'])
    dc.sdc_properties['pipeline.access.control.enabled'] = 'true'
    dc.start()
    yield dc
    if dc.tear_down_on_exit:
        dc.tear_down()

@pytest.fixture(scope='module')
def pipeline(data_collector):
    pipeline_builder = data_collector.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    data_collector.add_pipeline(pipeline)
    yield pipeline

# Validate default ACL for a pipeline.
def test_validate_default_acl(data_collector, pipeline):
    acl_response = data_collector.get_pipeline_acl(pipeline)
    assert acl_response['resourceOwner'] == 'admin'
    assert acl_response['resourceType'] == 'PIPELINE'
    assert acl_response['resourceId'] == pipeline.id
    assert any(i['subjectType'] == 'USER' for i in acl_response.permissions)
    for i in acl_response.permissions:
        if i['subjectType'] == 'USER':
            assert i['subjectId'] == 'admin'
            assert i['actions'] == ['READ', 'WRITE', 'EXECUTE']

# Validate default permissions for a pipeline.
def test_validate_default_permissions(data_collector, pipeline):
    perm_response = data_collector.get_pipeline_permissions(pipeline)
    assert any(i['subjectType'] == 'USER' for i in perm_response.permissions)
    for i in perm_response.permissions:
        if i['subjectType'] == 'USER':
            assert i['subjectId'] == 'admin'
            assert i['actions'] == ['READ', 'WRITE', 'EXECUTE']

# Validate ACL modification for a pipeline.
def test_modified_acl(data_collector, pipeline):
    acl_mod = data_collector.get_pipeline_acl(pipeline)
    for permission in acl_mod.permissions:
        if permission['subjectType'] == 'USER':
            permission['subjectId'] = 'testuser'
            permission['actions'] = ['READ', 'WRITE']
    data_collector.set_pipeline_acl(pipeline, pipeline_acl=acl_mod)

    data_collector.set_user('testuser')
    with pytest.raises(InternalServerError) as exception_info:
        data_collector.start_pipeline(pipeline)
    assert ERROR_CODE_PIPELINE_PERMISSIONS in exception_info.value.text
