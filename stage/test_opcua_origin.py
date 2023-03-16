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
from streamsets.sdk.exceptions import ValidationError

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

def test_invalid_opcua_url(sdc_builder, sdc_executor, opcua):
    builder = sdc_builder.get_pipeline_builder()
    opcua_client_origin = builder.add_stage("OPC UA Client", type="origin")
    opcua_client_origin.set_attributes(processing_mode='BROWSE_NODES', nodeid_fetch_mode='BROWSE',
                                       root_node_identifier_type='NUMERIC',
                                       root_node_identifier='0',
                                       root_node_namespace_index=0,
                                       security_policy='NONE')
    wiretap = builder.add_wiretap()
    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    opcua_client_origin >> [wiretap.destination, pipeline_finisher]

    pipeline = builder.build().configure_for_environment(opcua)
    opcua_client_origin.set_attributes(client_private_key_alias='test')
    opcua_client_origin.set_attributes(resource_url='opc.tcp://localhost:12686/invalid')
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.validate_pipeline(pipeline)
    except ValidationError as e:
        assert 'OPC_UA_02' in e.issues['stageIssues']['OPCUAClient_01'][0]['message']
    else:
        assert False, 'The start_pipeline must fail'

def test_browse_nodes_mode(sdc_builder, sdc_executor, opcua):
    builder = sdc_builder.get_pipeline_builder()
    opcua_client_origin = builder.add_stage("OPC UA Client", type="origin")
    opcua_client_origin.set_attributes(processing_mode='BROWSE_NODES', 
                                       nodeid_fetch_mode='BROWSE',
                                       root_node_identifier_type='NUMERIC',
                                       root_node_identifier='0',
                                       root_node_namespace_index=0,
                                       security_policy='NONE')
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

def test_polling_mode(sdc_builder, sdc_executor, opcua):
    builder = sdc_builder.get_pipeline_builder()
    opcua_client_origin = builder.add_stage("OPC UA Client", type="origin")
    opcua_client_origin.set_attributes(processing_mode='POLLING', 
                                       nodeid_fetch_mode='MANUAL',
                                       root_node_identifier_type='NUMERIC',
                                       root_node_identifier='0',
                                       root_node_namespace_index=0,
                                       security_policy='NONE')
    nodeids_config_dict = [x for x in filter(lambda x: x['name'] == 'conf.nodeIdConfigs', opcua_client_origin._data['configuration'])][0]
    nodeids_config_dict['value'] = [
        {'field': 'Boolean', 'identifier': 'HelloWorld/ScalarTypes/Boolean', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Byte', 'identifier': 'HelloWorld/ScalarTypes/Byte', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'SByte', 'identifier': 'HelloWorld/ScalarTypes/SByte', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Int16', 'identifier': 'HelloWorld/ScalarTypes/Int16', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Int32', 'identifier': 'HelloWorld/ScalarTypes/Int32', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Int64', 'identifier': 'HelloWorld/ScalarTypes/Int64', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UInt16', 'identifier': 'HelloWorld/ScalarTypes/UInt16', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UInt32', 'identifier': 'HelloWorld/ScalarTypes/UInt32', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UInt64', 'identifier': 'HelloWorld/ScalarTypes/UInt64', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Float', 'identifier': 'HelloWorld/ScalarTypes/Float', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Double', 'identifier': 'HelloWorld/ScalarTypes/Double', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'String', 'identifier': 'HelloWorld/ScalarTypes/String', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'DateTime', 'identifier': 'HelloWorld/ScalarTypes/DateTime', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Guid', 'identifier': 'HelloWorld/ScalarTypes/Guid', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'ByteString', 'identifier': 'HelloWorld/ScalarTypes/ByteString',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'XmlElement', 'identifier': 'HelloWorld/ScalarTypes/XmlElement',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'LocalizedText', 'identifier': 'HelloWorld/ScalarTypes/LocalizedText',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'NodeId', 'identifier': 'HelloWorld/ScalarTypes/NodeId', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Duration', 'identifier': 'HelloWorld/ScalarTypes/Duration', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UtcTime', 'identifier': 'HelloWorld/ScalarTypes/UtcTime', 'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'OnlyAdminCanRead', 'identifier': 'HelloWorld/OnlyAdminCanRead',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'BooleanArray', 'identifier': 'HelloWorld/ArrayTypes/BooleanArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'ByteArray', 'identifier': 'HelloWorld/ArrayTypes/ByteArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'SByteArray', 'identifier': 'HelloWorld/ArrayTypes/SByteArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Int16Array', 'identifier': 'HelloWorld/ArrayTypes/Int16Array',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Int32Array', 'identifier': 'HelloWorld/ArrayTypes/Int32Array',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'Int64Array', 'identifier': 'HelloWorld/ArrayTypes/Int64Array',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UInt16Array', 'identifier': 'HelloWorld/ArrayTypes/UInt16Array',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UInt32Array', 'identifier': 'HelloWorld/ArrayTypes/UInt32Array',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UInt64Array', 'identifier': 'HelloWorld/ArrayTypes/UInt64Array',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'FloatArray', 'identifier': 'HelloWorld/ArrayTypes/FloatArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'DoubleArray', 'identifier': 'HelloWorld/ArrayTypes/DoubleArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'StringArray', 'identifier': 'HelloWorld/ArrayTypes/StringArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'DateTimeArray', 'identifier': 'HelloWorld/ArrayTypes/DateTimeArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'GuidArray', 'identifier': 'HelloWorld/ArrayTypes/GuidArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'ByteStringArray', 'identifier': 'HelloWorld/ArrayTypes/ByteStringArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'XmlElementArray', 'identifier': 'HelloWorld/ArrayTypes/XmlElementArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'LocalizedTextArray', 'identifier': 'HelloWorld/ArrayTypes/LocalizedTextArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'NodeIdArray', 'identifier': 'HelloWorld/ArrayTypes/NodeIdArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'DurationArray', 'identifier': 'HelloWorld/ArrayTypes/DurationArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
        {'field': 'UtcTimeArray', 'identifier': 'HelloWorld/ArrayTypes/UtcTimeArray',
            'identifierType': 'STRING', 'namespaceIndex': 2},
    ]

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

def test_subscribe_mode(sdc_builder, sdc_executor, opcua):
    builder = sdc_builder.get_pipeline_builder()
    opcua_client_origin = builder.add_stage("OPC UA Client", type="origin")
    opcua_client_origin.set_attributes(processing_mode='SUBSCRIBE', nodeid_fetch_mode='MANUAL',
                                       root_node_identifier_type='NUMERIC',
                                       root_node_identifier='0',
                                       root_node_namespace_index=0,
                                       security_policy='NONE')
    nodeids_config_dict = [x for x in filter(lambda x: x['name'] == 'conf.nodeIdConfigs', opcua_client_origin._data['configuration'])][0]
    nodeids_config_dict['value'] = [
        {'field': 'Boolean', 'identifier': 'HelloWorld/ScalarTypes/Boolean', 'identifierType': 'STRING', 'namespaceIndex': 2},
    ]

    wiretap = builder.add_wiretap()
    opcua_client_origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(opcua)
    opcua_client_origin.set_attributes(client_private_key_alias='test')
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)


    records = wiretap.output_records
    assert len(records) == 1