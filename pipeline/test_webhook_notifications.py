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
import logging
import pytest

logger = logging.getLogger(__name__)

HTTP_LISTENING_PORT = 8234

@pytest.mark.parametrize('http_method', ['POST', 'PUT'])
def test_webhook_notifications(sdc_builder,sdc_executor, http_method):

    service_pipeline_builder = sdc_builder.get_pipeline_builder()
    rest_service_source = service_pipeline_builder.add_stage('REST Service')
    service_trash = service_pipeline_builder.add_stage('Trash')
    rest_service_source.http_listening_port = HTTP_LISTENING_PORT
    rest_service_source >> service_trash
    service_pipeline = service_pipeline_builder.build('Service Pipeline')
    sdc_executor.add_pipeline(service_pipeline)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> trash
    webhook_pipeline = pipeline_builder.build('Webhook Configured Pipeline')
    webhookUrl = f'http://{sdc_executor.server_host}:{HTTP_LISTENING_PORT}'
    webhook_pipeline.configuration['notifyOnStates'] = ['RUNNING']
    webhook_pipeline.configuration['webhookConfigs'] = [{'webhookUrl': webhookUrl, 'httpMethod' : http_method}]
    sdc_executor.add_pipeline(webhook_pipeline)

    try:
        snapshot_command = sdc_executor.capture_snapshot(pipeline=service_pipeline, start_pipeline=True, wait=False)
        sdc_executor.start_pipeline(webhook_pipeline)
        snapshot_command.wait_for_finished()
        snapshot = snapshot_command.snapshot

        assert len(snapshot[rest_service_source].output) == 1
        record = snapshot[rest_service_source].output[0]
        logger.info(f"Record : {record}")
        httpMethod = record.header.values['method']
        assert httpMethod == http_method

    finally:
        sdc_executor.stop_pipeline(webhook_pipeline)
        sdc_executor.stop_pipeline(service_pipeline)