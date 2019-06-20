# Copyright 2019 StreamSets Inc.
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

logger = logging.getLogger(__name__)

# CONTAINER_0200 - Pipeline '{}' does not exist
ERROR_CODE_PIPELINE_NOT_FOUND = 'CONTAINER_0200'


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_user('creator', roles=['creator'], groups=['creator'])
        data_collector.sdc_properties['pipeline.access.control.enabled'] = 'true'

    return hook


@pytest.fixture
def pipeline(sdc_executor):
    """
    Creates a simple pipeline in draft mode using a non-admin user.
    """
    sdc_executor.set_user('creator')
    pipeline_builder = sdc_executor.get_pipeline_builder(draft=True)
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'
    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline, draft=True)
    yield pipeline


def test_draft_pipeline(sdc_executor, pipeline):
    sdc_executor.dump_log_on_error = False
    with pytest.raises(InternalServerError) as exception_info:
        sdc_executor.get_pipeline(pipeline)
    sdc_executor.dump_log_on_error = True
    assert ERROR_CODE_PIPELINE_NOT_FOUND in exception_info.value.text


def test_draft_pipeline_acl(sdc_executor, pipeline):
    sdc_executor.dump_log_on_error = False
    with pytest.raises(InternalServerError) as exception_info:
        sdc_executor.get_pipeline_acl(pipeline)
    sdc_executor.dump_log_on_error = True
    assert ERROR_CODE_PIPELINE_NOT_FOUND in exception_info.value.text


def test_draft_pipeline_permissions(sdc_executor, pipeline):
    sdc_executor.dump_log_on_error = False
    with pytest.raises(InternalServerError) as exception_info:
        sdc_executor.get_pipeline_permissions(pipeline)
    sdc_executor.dump_log_on_error = True
    assert ERROR_CODE_PIPELINE_NOT_FOUND in exception_info.value.text
