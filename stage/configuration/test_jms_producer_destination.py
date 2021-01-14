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
def test_additional_jms_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_clientid': True}])
def test_client_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_jms_destination_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'jms_destination_type': 'QUEUE'},
                                              {'jms_destination_type': 'TOPIC'},
                                              {'jms_destination_type': 'UNKNOWN'}])
def test_jms_destination_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_jms_initial_context_factory(sdc_builder, sdc_executor):
    pass


@stub
def test_jms_provider_url(sdc_builder, sdc_executor):
    pass


@stub
def test_jndi_connection_factory(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_clientid': False}, {'use_clientid': True}])
def test_use_clientid(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

