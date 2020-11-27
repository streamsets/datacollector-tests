# Copyright 2020 StreamSets Inc.
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
import http.client as httpclient
import logging
import string

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import credentialstore, elasticsearch
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@elasticsearch
@credentialstore
def test_dev_data_generator_to_elastic_search(sdc_builder, sdc_executor, elasticsearch):
    """Simple Dev data generator to Elastic search.
    Pipeline will send records to elastic search and then we will query elastic search to verify the data

    The pipeline looks like:
        dev_data_generator >> elastic_search
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower()  # Elasticsearch indexes must be lower case
    es_mapping = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(
        fields_to_generate=[{"type": "STRING", "field": "doc_id"}],
        batch_size=1, number_of_threads=5, delay_between_batches=10)

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation = 'INDEX'
    target.document_id = "${record:value('/doc_id')}"
    target.index = es_index
    target.mapping = es_mapping

    wiretap = builder.add_wiretap()

    dev_data_generator >> [target, wiretap.destination]

    pipeline = builder.build().configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(es_index)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(100)
        sdc_executor.stop_pipeline(pipeline)

        output_records = wiretap.output_records

        responses = elasticsearch.client.search(es_index)
        assert len(responses) == len(output_records)

        for response in responses:
            assert response['_index'] == es_index
            assert response['_type'] == es_mapping
        assert set([response['_id'] for response in responses]) == set([r.field['doc_id'] for r in output_records])
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        # Clean up test data in ES
        elasticsearch.client.delete_index(es_index)


@elasticsearch
@credentialstore
def test_http_to_elastic_search(sdc_builder, sdc_executor, elasticsearch):
    """Simple Http Server to Elastic search.
        Pipeline will send records to elastic search and then we will query elastic search to verify the data

    The pipeline looks like:
        http_server >> elastic_search
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower()  # Elasticsearch indexes must be lower case
    es_mapping = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()

    http_server = builder.add_stage('HTTP Server')
    http_server.set_attributes(data_format='JSON')
    http_server.http_listening_port = 9999

    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.17.0'):
        http_server.list_of_application_ids = [{"appId": 'admin'}]
    elif Version(sdc_builder.version) >= Version('3.17.0'):
        http_server.list_of_application_ids = [{"credential": 'admin'}]
    else:
        http_server.application_id = 'admin'

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation = 'INDEX'
    target.document_id = "${record:value('/doc_id')}"
    target.index = es_index
    target.mapping = es_mapping

    wiretap = builder.add_wiretap()

    http_server >> [target, wiretap.destination]

    pipeline = builder.build().configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(es_index)

        # Run pipeline and read from Elasticsearch to assert
        start_command = sdc_executor.start_pipeline(pipeline)

        for i in range(10):
            logger.info('Posting message number %s', i)
            data = {'doc_id': get_random_string(string.ascii_letters, 10)}
            http_res = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
            http_res.request('POST', '/', json.dumps(data), {'X-SDC-APPLICATION-ID': 'admin'})
            resp = http_res.getresponse()
            assert resp.status == 200

        start_command.wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(pipeline)

        output_records = wiretap.output_records

        responses = elasticsearch.client.search(es_index)
        assert len(responses) == len(output_records)

        for response in responses:
            assert response['_index'] == es_index
            assert response['_type'] == es_mapping
        assert set([response['_id'] for response in responses]) == set([r.field['doc_id'] for r in output_records])
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        # Clean up test data in ES
        elasticsearch.client.delete_index(es_index)
