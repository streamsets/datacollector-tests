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

import json
import logging

logger = logging.getLogger(__name__)

WEBSOCKETS_KEYSTORE_FILE_PATH = 'resources/websocket/websocket_keystore.p12'
WEBSOCKETS_TRUSTSTORE_FILE_PATH = 'resources/websocket/websocket_truststore.jks'


def test_websocket(sdc_builder, sdc_executor):
    # Client pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()
    input_data = [{'name': 'Rafa Nadal', 'country': 'Spain'},
                  {'name': 'Roger Federer', 'country': 'Switzerland'},
                  {'name': 'Pete Sampras', 'country': 'USA'}]
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(
        raw_data='\n'.join(json.dumps(rec) for rec in input_data),
        data_format='JSON',
        stop_after_first_batch=True)

    javascript_evaluator = pipeline_builder.add_stage('JavaScript Evaluator')

    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='destination').set_attributes(
        resource_url='ws://localhost:9999',
        headers=[{'key': 'X-SDC-APPLICATION-ID', 'value': 'APPLICATION_ID'}])

    dev_raw_data_source >> javascript_evaluator >> websocket_client
    client_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(client_pipeline)

    # Server pipeline creation
    pipeline_builder = sdc_builder.get_pipeline_builder()
    websocket_server = pipeline_builder.add_stage('WebSocket Server').set_attributes(websocket_listening_port=9999,
                                                                                     application_id='APPLICATION_ID',
                                                                                     data_format='JSON')

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    wiretap = pipeline_builder.add_wiretap()

    websocket_server >> expression_evaluator >> wiretap.destination
    server_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(server_pipeline)

    try:
        # Start WebSocket Server pipeline.
        sdc_executor.start_pipeline(server_pipeline)

        # Start WebSocket Client pipeline.
        sdc_executor.start_pipeline(client_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(server_pipeline, 'input_record_count', len(input_data))

        output_data = [{'name': rec.field['name'].value, 'country': rec.field['country'].value}
                       for rec in wiretap.output_records]
        assert sorted(input_data, key=lambda x: x['name']) == sorted(output_data, key=lambda x: x['name'])
    finally:
        # Stop the pipeline and verify pipeline's status
        sdc_executor.stop_pipeline(server_pipeline)


def test_websocket_tls(sdc_builder, sdc_executor):
    """Test sending messages between Websocket Client and Server stages with TLS enabled.

    We do so just sending some records with a Websocket Client stage and checking the Websocket Server stage
    correctly receives them. We use two pipelines, one for each websocket stage, defined in the respective
    fixtures.

    """
    # Creation of client pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    input_data = [{'name': 'Rafa Nadal', 'country': 'Spain'},
                  {'name': 'Roger Federer', 'country': 'Switzerland'},
                  {'name': 'Pete Sampras', 'country': 'USA'}]
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(
        raw_data='\n'.join(json.dumps(rec) for rec in input_data),
        data_format='JSON',
        stop_after_first_batch=True)

    javascript_evaluator = pipeline_builder.add_stage('JavaScript Evaluator')

    websocket_client = pipeline_builder.add_stage('WebSocket Client', type='destination').set_attributes(
        resource_url='wss://localhost:9999',
        headers=[{'key': 'X-SDC-APPLICATION-ID', 'value': 'APPLICATION_ID'}],
        use_tls=True,
        truststore_file=WEBSOCKETS_TRUSTSTORE_FILE_PATH,
        truststore_type='JKS',
        truststore_password='streamsets')

    pipeline_builder.add_wiretap()

    dev_raw_data_source >> javascript_evaluator >> websocket_client
    client_pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(client_pipeline)

    # Creation of server pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    websocket_server = pipeline_builder.add_stage('WebSocket Server').set_attributes(websocket_listening_port=9999,
                                                                                     application_id='APPLICATION_ID',
                                                                                     data_format='JSON',
                                                                                     use_tls=True,
                                                                                     keystore_file=WEBSOCKETS_KEYSTORE_FILE_PATH,
                                                                                     keystore_type='PKCS12',
                                                                                     keystore_password='streamsets')

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    wiretap = pipeline_builder.add_wiretap()

    websocket_server >> expression_evaluator >> wiretap.destination
    server_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(server_pipeline)

    try:
        sdc_executor.start_pipeline(server_pipeline)
        sdc_executor.start_pipeline(client_pipeline).wait_for_finished()

        sdc_executor.wait_for_pipeline_metric(server_pipeline, 'input_record_count', len(input_data))

        output_data = [{'name': rec.field['name'].value, 'country': rec.field['country'].value}
                       for rec in wiretap.output_records]
        assert sorted(input_data, key=lambda x: x['name']) == sorted(output_data, key=lambda x: x['name'])

    finally:
        sdc_executor.stop_pipeline(server_pipeline)
