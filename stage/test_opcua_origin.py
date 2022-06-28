# Copyright 2022 StreamSets Inc.
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
from streamsets.testframework.markers import opcua, sdc_min_version

pytestmark = [opcua]

logger = logging.getLogger(__name__)


@pytest.mark.parametrize('security_policy', ['NONE', 'BASIC_128_RSA_15', 'BASIC_256', 'BASIC_256_SHA_256'])
def test_security_policies(sdc_builder, sdc_executor, opcua, security_policy):
    builder = sdc_builder.get_pipeline_builder()
    opcua_client_origin = builder.add_stage("OPC UA Client", type="origin")
    opcua_client_origin.set_attributes(processing_mode='BROWSE_NODES', nodeid_fetch_mode='BROWSE',
                                       root_node_identifier_type='NUMERIC',
                                       root_node_identifier='0',
                                       root_node_namespace_index=0,
                                       security_policy=security_policy)
    wiretap = builder.add_wiretap()
    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    opcua_client_origin >> [wiretap.destination, pipeline_finisher]

    pipeline = builder.build().configure_for_environment(opcua)
    opcua_client_origin.set_attributes(client_private_key_alias='test')
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    records = wiretap.output_records
    assert len(records) == 1


@sdc_min_version("5.1.0")
@pytest.mark.parametrize('use_username_and_password', [True, False])
@pytest.mark.parametrize('security_policy', ['NONE', 'BASIC_128_RSA_15', 'BASIC_256', 'BASIC_256_SHA_256'])
def test_username_and_password(sdc_builder, sdc_executor, opcua, use_username_and_password, security_policy):
    builder = sdc_builder.get_pipeline_builder()
    opcua_client_origin = builder.add_stage("OPC UA Client", type="origin")
    opcua_client_origin.set_attributes(processing_mode='BROWSE_NODES', nodeid_fetch_mode='BROWSE',
                                       root_node_identifier_type='NUMERIC',
                                       root_node_identifier='0',
                                       root_node_namespace_index=0,
                                       security_policy=security_policy,
                                       use_username_and_password=use_username_and_password,
                                       username="user",
                                       password="password1")

    wiretap = builder.add_wiretap()
    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    opcua_client_origin >> [wiretap.destination, pipeline_finisher]

    pipeline = builder.build().configure_for_environment(opcua)
    opcua_client_origin.set_attributes(client_private_key_alias='test')
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    records = wiretap.output_records
    assert len(records) == 1
