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

from streamsets.sdk.sdc_models import MetricRule

logger = logging.getLogger(__name__)

HTTP_LISTENING_PORT = 8234

@pytest.mark.parametrize('http_method', ['GET', 'HEAD', 'POST', 'PUT', 'DELETE'])
def test_webhook_alerts(sdc_builder,sdc_executor, http_method):
    """This test is to validate the invocation of configured alert webhook.Created two pipelines -
    One Pipeline with webhook configuration and another as a service pipeline to listen to the request.
    Pipeline is configured with metric rule to send the alert webhook.
    """

    service_pipeline_builder = sdc_builder.get_pipeline_builder()
    rest_service_source = service_pipeline_builder.add_stage('REST Service')
    wiretap = service_pipeline_builder.add_wiretap()
    rest_service_source.http_listening_port = HTTP_LISTENING_PORT
    rest_service_source >> wiretap.destination
    service_pipeline = service_pipeline_builder.build('Service Pipeline for method: ' + http_method)
    sdc_executor.add_pipeline(service_pipeline)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> trash

    pipeline_builder.add_metric_rule(MetricRule(
        alert_text='Alert to invoke webhook',
        metric_type='COUNTER',
        metric_id='pipeline.batchOutputRecords.counter',
        metric_element='COUNTER_COUNT',
        condition='${value() > 1}',
        send_email=False,
        active=True
    ))
    webhook_pipeline = pipeline_builder.build('Alert Webhook Pipeline for method: ' + http_method)
    webhookUrl = f'http://{sdc_executor.server_host}:{HTTP_LISTENING_PORT}'
    config_list  = webhook_pipeline._data['pipelineRules']['configuration']
    for data in config_list:
        if data['name'] == 'webhookConfigs':
            data['value'] = [{'webhookUrl': webhookUrl, 'httpMethod' : http_method}]
    sdc_executor.add_pipeline(webhook_pipeline)

    try:
        sdc_executor.start_pipeline(service_pipeline)
        sdc_executor.start_pipeline(webhook_pipeline)
        sdc_executor.wait_for_pipeline_metric(service_pipeline, 'input_record_count', 1)

        output_records = wiretap.output_records
        assert len(output_records) == 1
        webhook_record = output_records[0]
        logger.info('Record : %s', webhook_record)
        logger.info('Record Headers: %s', webhook_record.header.values)
        httpMethod = webhook_record.header.values['method']
        assert httpMethod == http_method

    finally:
        sdc_executor.stop_pipeline(webhook_pipeline)
        sdc_executor.stop_pipeline(service_pipeline)