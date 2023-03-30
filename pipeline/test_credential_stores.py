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

import http.client as httpclient
import json
import logging
import string
import time

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import elasticsearch
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ELASTICSEARCH_VERSION_8 = 8


@elasticsearch
def test_dev_data_generator_to_elastic_search(sdc_builder, sdc_executor, elasticsearch, credential_store):
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

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(default_operation='INDEX',
                                                                                   document_id="${record:value('/doc_id')}",
                                                                                   index=es_index,
                                                                                   mapping=es_mapping)

    wiretap = builder.add_wiretap()

    dev_data_generator >> [target, wiretap.destination]

    pipeline = builder.build().configure_for_environment(elasticsearch, credential_store)
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
            if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                assert response['_type'] == es_mapping

        assert set([response['_id'] for response in responses]) == set([r.field['doc_id'] for r in output_records])
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        # Clean up test data in ES
        elasticsearch.client.delete_index(es_index)


@elasticsearch
def test_http_to_elastic_search(sdc_builder, sdc_executor, elasticsearch, credential_store):
    """Simple Http Server to Elastic search.
        Pipeline will send records to elastic search and then we will query elastic search to verify the data

    The pipeline looks like:
        http_server >> elastic_search
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower()  # Elasticsearch indexes must be lower case
    es_mapping = get_random_string(string.ascii_letters, 10)
    http_port = 9999

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()

    http_server = builder.add_stage('HTTP Server')
    http_server.set_attributes(data_format='JSON')
    http_server.http_listening_port = http_port

    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.17.0'):
        http_server.list_of_application_ids = [{"appId": 'admin'}]
    elif Version(sdc_builder.version) >= Version('3.17.0'):
        http_server.list_of_application_ids = [{"credential": 'admin'}]
    else:
        http_server.application_id = 'admin'

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(default_operation='INDEX',
                                                                                   document_id="${record:value('/doc_id')}",
                                                                                   index=es_index,
                                                                                   mapping=es_mapping)

    wiretap = builder.add_wiretap()

    http_server >> [target, wiretap.destination]

    pipeline = builder.build().configure_for_environment(elasticsearch, credential_store)
    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(es_index)

        # Run pipeline and read from Elasticsearch to assert
        start_command = sdc_executor.start_pipeline(pipeline)

        num_requests = 10
        failed_requests = 0

        for i in range(num_requests):
            logger.info('Posting message number %s', i)
            data = {'doc_id': get_random_string(string.ascii_letters, 10)}
            http_res = httpclient.HTTPConnection(sdc_executor.server_host, http_port)
            try:
                http_res.request('POST', '/', json.dumps(data), {'X-SDC-APPLICATION-ID': 'admin'})
            except ConnectionRefusedError as ce:
                logger.error(f"Message number {i} could not be posted: {ce}")
                failed_requests += 1
                continue
            resp = http_res.getresponse()
            tries = 0
            # Waiting for the server to start
            while resp.status != 200 and tries <= 5:
                time.sleep(1)
                resp = http_res.getresponse()
                logger.info('HTTP response number of tries = %s, status = %s', tries, resp.status)
                tries = tries + 1

            assert resp.status == 200

        assert failed_requests < num_requests, "The test set up failed. No HTTP POST requests were successful."

        start_command.wait_for_pipeline_batch_count(num_requests - failed_requests)
        sdc_executor.stop_pipeline(pipeline)

        output_records = wiretap.output_records

        responses = elasticsearch.client.search(es_index)
        assert len(responses) == len(output_records)

        for response in responses:
            assert response['_index'] == es_index
            if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                assert response['_type'] == es_mapping

        assert set([response['_id'] for response in responses]) == set([r.field['doc_id'] for r in output_records])
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        # Clean up test data in ES
        elasticsearch.client.delete_index(es_index)
