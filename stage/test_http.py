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

import http.client as httpclient
import json
import logging
import os
import pytest
import requests
import shutil
import ssl
import string
import tempfile
import time
import urllib

from collections import namedtuple
from pretenders.common.constants import FOREVER
from requests_gssapi import HTTPSPNEGOAuth
from streamsets.sdk import sdc_api
from streamsets.sdk.utils import Version
from streamsets.testframework.constants import (CREDENTIAL_STORE_EXPRESSION, CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION,
                                                STF_TESTCONFIG_DIR)
from streamsets.testframework.credential_stores.jks import JKSCredentialStore
from streamsets.testframework.markers import http, sdc_min_version, spnego
from streamsets.testframework.utils import get_random_string


logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, redefined-outer-name, too-many-locals

#
# To start the mock HTTP server, run
#
#    ste start HTTP
#
# or, without ste
#
#    docker run -d --name myhttpmockserver --net=cluster pretenders/pretenders:1.4
#


@pytest.fixture(scope='module')
def http_server_pipeline(sdc_builder, sdc_executor):
    """HTTP Server pipeline fixture."""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    http_server = pipeline_builder.add_stage('HTTP Server')
    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.17.0'):
        http_server.list_of_application_ids = [{"appId": '${APPLICATION_ID}'}]
    elif Version(sdc_builder.version) >= Version('3.17.0'):
        http_server.list_of_application_ids = [{"credential": '${APPLICATION_ID}'}]
    else:
        http_server.application_id = '${APPLICATION_ID}'
    http_server.data_format = 'JSON'
    http_server.http_listening_port = '${HTTP_PORT}'

    javascript_evaluator = pipeline_builder.add_stage('JavaScript Evaluator')
    javascript_evaluator.script = """for(var i = 0; i < records.length; i++) {
                                       try {
                                         records[i].value['${NEW_FIELD_NAME}'] = ${NEW_FIELD_VALUE}
                                         output.write(records[i]);
                                       } catch (e) {
                                         // Send record to error
                                         error.write(records[i], e);
                                       }
                                     }
                                  """

    trash = pipeline_builder.add_stage('Trash')

    http_server >> javascript_evaluator >> trash
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(HTTP_PORT='8000',
                            APPLICATION_ID='test',
                            NEW_FIELD_NAME='javscriptField',
                            NEW_FIELD_VALUE='5000')

    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    yield namedtuple('Pipeline', ['pipeline', 'http_server', 'javascript_evaluator'])(pipeline,
                                                                                      http_server,
                                                                                      javascript_evaluator)


@pytest.fixture(scope='module')
def http_client_pipeline(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.raw_data = '${RAW_DATA}'
    dev_raw_data_source.data_format = 'JSON'

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')

    http_client = pipeline_builder.add_stage('HTTP Client', type='destination')
    http_client.resource_url = '${RESOURCE_URL}'
    http_client.headers = [{'key': 'X-SDC-APPLICATION-ID', 'value': '${APPLICATION_ID}'}]
    http_client.one_request_per_batch = True

    dev_raw_data_source >> expression_evaluator >> http_client
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(RAW_DATA='{"f1": "abc"}{"f1": "xyz"}',
                            RESOURCE_URL='http://localhost:8000',
                            APPLICATION_ID='test')

    sdc_executor.add_pipeline(pipeline)

    yield namedtuple('Pipeline', ['pipeline',
                                  'dev_raw_data_source',
                                  'expression_evaluator',
                                  'http_client'])(pipeline,
                                                  dev_raw_data_source,
                                                  expression_evaluator,
                                                  http_client)


@http
def test_http(sdc_executor, http_server_pipeline, http_client_pipeline):
    # Start HTTP Server pipeline.
    server_runtime_parameters = {'HTTP_PORT': 9999,
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID',
                                 'NEW_FIELD_NAME': 'javscriptField',
                                 'NEW_FIELD_VALUE': 5000}

    sdc_executor.start_pipeline(http_server_pipeline.pipeline, server_runtime_parameters)
    pipeline_status = sdc_executor.get_pipeline_status(http_server_pipeline.pipeline).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes.get('RUNTIME_PARAMETERS').get('APPLICATION_ID') == 'HTTP_APPLICATION_ID'

    # Start and capture snapshot for HTTP Client pipeline.
    client_runtime_parameters = {'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
                                 'RESOURCE_URL': 'http://localhost:9999',
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID'}

    snapshot = sdc_executor.capture_snapshot(http_client_pipeline.pipeline, start_pipeline=True,
                                             runtime_parameters=client_runtime_parameters).snapshot
    origin_data = snapshot[http_client_pipeline.dev_raw_data_source.instance_name]
    processor_data = snapshot[http_client_pipeline.expression_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].field['f1'] == 'abc'
    assert origin_data.output[1].field['f1'] == 'xyz'

    # Capture snapshot for HTTP Server pipeline.
    snapshot = sdc_executor.capture_snapshot(http_server_pipeline.pipeline).snapshot
    origin_data = snapshot[http_server_pipeline.http_server.instance_name]
    processor_data = snapshot[http_server_pipeline.javascript_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].field['f1'] == 'abc'
    assert origin_data.output[1].field['f1'] == 'xyz'
    assert processor_data.output[0].field['f1'] == 'abc'
    assert processor_data.output[0].field['javscriptField'] == 5000
    assert processor_data.output[1].field['f1'] == 'xyz'
    assert processor_data.output[1].field['javscriptField'] == 5000

    # Stop the pipelines.
    sdc_executor.stop_pipeline(http_client_pipeline.pipeline)
    sdc_executor.stop_pipeline(http_server_pipeline.pipeline)


@http
def test_http_client_target_wrong_host(sdc_executor, http_client_pipeline):
    # Start HTTP Client pipeline with invalid resource URL.
    client_runtime_parameters = {'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
                                 'RESOURCE_URL': 'http://localhost:9999',
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID'}
    snapshot = sdc_executor.capture_snapshot(http_client_pipeline.pipeline, start_pipeline=True,
                                             runtime_parameters=client_runtime_parameters).snapshot
    origin_data = snapshot[http_client_pipeline.dev_raw_data_source.instance_name]
    processor_data = snapshot[http_client_pipeline.expression_evaluator.instance_name]
    assert len(origin_data.output) == 2
    assert len(processor_data.output) == 2
    assert origin_data.output[0].field['f1'] == 'abc'
    assert origin_data.output[1].field['f1'] == 'xyz'

    # Since resource URL is invalid, all the records should go to error in target stage.
    target_data = snapshot[http_client_pipeline.http_client.instance_name]
    assert len(target_data.error_records) == 2

    # Stop the pipeline.
    sdc_executor.stop_pipeline(http_client_pipeline.pipeline)


@http
@sdc_min_version("3.11.0")
def test_http_processor_multiple_records(sdc_builder, sdc_executor, http_client):
    """Test HTTP Lookup Processor for HTTP GET method and split the obtained result
    in different records:

        dev_raw_data_source >> http_client_processor >> trash
    """
    #The data returned by the HTTP mock server
    dataArr = [{'A':i,'C':i+1,'G':i+2,'T':i+3} for i in range(10)]

    expected_data = json.dumps(dataArr)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='dummy')
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', http_method='GET',
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}',
                                             multiple_values_behavior='SPLIT_INTO_MULTIPLE_RECORDS')
        trash = builder.add_stage('Trash')

        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup GET Processor Split Multiple Records pipeline')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # ensure HTTP GET result has 10 different records
        assert len(snapshot[http_client_processor.instance_name].output) == 10
        # check each
        for x in range(10):
            assert snapshot[http_client_processor.instance_name].output[x].field[record_output_field]['A'] == x
            assert snapshot[http_client_processor.instance_name].output[x].field[record_output_field]['C'] == x+1
            assert snapshot[http_client_processor.instance_name].output[x].field[record_output_field]['G'] == x+2
            assert snapshot[http_client_processor.instance_name].output[x].field[record_output_field]['T'] == x+3

    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("3.11.0")
def test_http_processor_list(sdc_builder, sdc_executor, http_client):
    """Test HTTP Lookup Processor for HTTP GET method and split the obtained result
    in different elements of the same list stored in just one record:

        dev_raw_data_source >> http_client_processor >> trash
    """
    #The data returned by the HTTP mock server
    dataArr = [{'A':i,'C':i+1,'G':i+2,'T':i+3} for i in range(10)]

    expected_data = json.dumps(dataArr)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='dummy')
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', http_method='GET',
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}',
                                             multiple_values_behavior='ALL_AS_LIST')
        trash = builder.add_stage('Trash')

        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup GET Processor All As List pipeline')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # ensure HTTP GET result has 1 record (The list containing the 10 elements)
        assert len(snapshot[http_client_processor.instance_name].output) == 1
        # check each element of the list
        for x in range(10):
            assert snapshot[http_client_processor.instance_name].output[0].field[record_output_field][x]['A'] == x+0
            assert snapshot[http_client_processor.instance_name].output[0].field[record_output_field][x]['C'] == x+1
            assert snapshot[http_client_processor.instance_name].output[0].field[record_output_field][x]['G'] == x+2
            assert snapshot[http_client_processor.instance_name].output[0].field[record_output_field][x]['T'] == x+3

    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("3.17.0")
def test_http_processor_response_action_stage_error(sdc_builder, sdc_executor, http_client):
    """
    Test when the http processor stage has the response action set up with the "Cause Stage to fail" option.
    To test this we force the URL to be a not available so we get a 404 response from the mock http server. An
    exception should be risen that shows the stage error.

    We use the pipeline:
    dev_raw_data_source >> http_client_processor >> trash

    """
    mock_path = get_random_string(string.ascii_letters, 10)
    fake_mock_path = get_random_string(string.ascii_letters, 10)
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    record_output_field = 'result'
    http_mock = http_client.mock()
    try:
        http_mock.when(
            rule=f'GET /{mock_path}'
        ).reply(
            body="Example",
            status=200,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{fake_mock_path}'
        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method='GET', request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')
        http_client_processor.per_status_actions=[
            {
              'statusCode':404,
              'action':'STAGE_ERROR'
            },
        ]
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions')
        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(sdc_api.RunError) as exception_info:
            sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        assert 'HTTP_14 - ' in f'{exception_info.value}'
    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@sdc_min_version("3.17.0")
def test_http_processor_response_action_record_error(sdc_builder, sdc_executor, http_client):
    """
    Test when the http processor stage has the response action set up with the "Generate Error Record" option.
    To test this we force the URL to be a not available so we get a 404 response from the mock http server. The output
    should be one error record containing the right error code.

    We use the pipeline:
         dev_raw_data_source >> http_client_processor >> trash
"""
    mock_path = get_random_string(string.ascii_letters, 10)
    fake_mock_path = get_random_string(string.ascii_letters, 10)
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    record_output_field = 'result'
    http_mock = http_client.mock()
    try:
        http_mock.when(
            rule=f'GET /{mock_path}'
        ).reply(
            body="Example",
            status=200,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{fake_mock_path}'
        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method='GET', request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')
        http_client_processor.per_status_actions=[
            {
                'statusCode':404,
                'action':'ERROR_RECORD'
            },
        ]
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions')
        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        assert len(snapshot[http_client_processor.instance_name].error_records) == 1
        assert snapshot[http_client_processor.instance_name].error_records[0].field['text'].value == raw_data

    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@sdc_min_version("3.17.0")
def test_http_processor_propagate_error_records(sdc_builder, sdc_executor, http_client):
    """
        Test when the http processor stage has the config option "Records for remaining statuses" set. To test this we
        force the URL to be a not available so we get a 404 response from the mock http server. The output should be
        one record containing the "Error Response Body Field" with the error message from the mock server.

        We use the pipeline:
             dev_raw_data_source >> http_client_processor >> trash
    """
    mock_path = get_random_string(string.ascii_letters, 10)
    fake_mock_path = get_random_string(string.ascii_letters, 10)
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    record_output_field = 'result'
    http_mock = http_client.mock()
    try:
        http_mock.when(
            rule=f'GET /{mock_path}'
        ).reply(
            body="Example",
            status=200,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{fake_mock_path}'
        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method='GET', request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')
        http_client_processor.records_for_remaining_statuses = True
        http_client_processor.error_response_body_field = 'errorField'
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions')
        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        assert len(snapshot[http_client_processor.instance_name].output) == 1
        assert snapshot[http_client_processor.instance_name].output[0].field['result']['errorField'].value == 'No matching preset response'
    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@sdc_min_version("3.17.0")
def test_http_processor_batch_wait_time_not_enough(sdc_builder, sdc_executor, http_client):
    """
        When the Batch Wait Time is not big enough and there is a retry action configured it can be the batch time
        expires before the number of retries is finished yet. In this case an stage error must be raised explaining
        the reason. We force the error to appear by configuring the pipeline to stop when it finds an stage error.

        We use the pipeline:
             dev_raw_data_source >> http_client_processor >> trash
    """
    mock_path = get_random_string(string.ascii_letters, 10)
    fake_mock_path = get_random_string(string.ascii_letters, 10)
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    record_output_field = 'result'
    http_mock = http_client.mock()
    try:
        http_mock.when(
            rule=f'GET /{mock_path}'
        ).reply(
            body="Example",
            status=200,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{fake_mock_path}'
        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method='GET', request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')
        http_client_processor.records_for_remaining_statuses = False
        http_client_processor.batch_wait_time_in_ms = 150
        http_client_processor.multiple_values_behavior='ALL_AS_LIST'
        http_client_processor.per_status_actions=[
            {
                'statusCode':404,
                'action':'RETRY_LINEAR_BACKOFF',
                'backoffInterval':100,
                'maxNumRetries':10
            },
        ]
        http_client_processor.on_record_error='STOP_PIPELINE'

        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions '
                                       'Max wait Time is not enough stage error')
        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(sdc_api.RunError) as exception_info:
            sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        assert 'HTTP_67 - ' in f'{exception_info.value}'

    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@pytest.mark.parametrize('retry_action,pagination_option', [
    ('RETRY_LINEAR_BACKOFF', 'BY_PAGE'),
    ('RETRY_LINEAR_BACKOFF', 'BY_OFFSET'),
    ('RETRY_LINEAR_BACKOFF', 'LINK_HEADER'),
    ('RETRY_LINEAR_BACKOFF', 'LINK_FIELD'),
    ('RETRY_EXPONENTIAL_BACKOFF', 'BY_PAGE'),
    ('RETRY_EXPONENTIAL_BACKOFF', 'BY_OFFSET'),
    ('RETRY_EXPONENTIAL_BACKOFF', 'LINK_HEADER'),
    ('RETRY_EXPONENTIAL_BACKOFF', 'LINK_FIELD'),
    ('RETRY_IMMEDIATELY', 'BY_PAGE'),
    ('RETRY_IMMEDIATELY', 'BY_OFFSET'),
    ('RETRY_IMMEDIATELY', 'LINK_HEADER'),
    ('RETRY_IMMEDIATELY', 'LINK_FIELD'),
])
@sdc_min_version("3.17.0")
def test_http_processor_pagination_and_retry_action(sdc_builder, sdc_executor, http_client, retry_action, pagination_option):
    """
        Test when a pagination option is set up and a retry action is set up and the maximum number
        of retries is exhausted then the error saying the number of retries is exceeded is risen.

        We use the pipeline:
             dev_raw_data_source >> http_client_processor >> trash
    """
    rand_pipeline_name = get_random_string(string.ascii_letters, 10)
    mock_path = get_random_string(string.ascii_letters, 10)
    fake_mock_path = get_random_string(string.ascii_letters, 10)
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    record_output_field = 'result'
    http_mock = http_client.mock()
    try:
        http_mock.when(
            rule=f'GET /{mock_path}'
        ).reply(
            body="Example",
            status=200,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{fake_mock_path}'
        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method='GET', request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')

        http_client_processor.records_for_remaining_statuses = False
        http_client_processor.batch_wait_time_in_ms = 500000
        http_client_processor.pagination_mode = pagination_option;
        http_client_processor.per_status_actions=[
            {
                'statusCode':404,
                'action':retry_action,
                'backoffInterval':100,
                'maxNumRetries':3
            },
        ]
        http_client_processor.result_field_path='/'
        http_client_processor.next_page_link_field='/foo'
        http_client_processor.stop_condition='1==1'
        http_client_processor.multiple_values_behavior='ALL_AS_LIST'
        #Must do it like this because the attribute name has the '/' char
        setattr(http_client_processor, 'initial_page/offset', 1)

        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline_title = f'HTTP Lookup Processor pipeline Response Actions with Pagination {rand_pipeline_name}'
        pipeline = builder.build(title=pipeline_title)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status(status='RUN_ERROR', ignore_errors=False)
        snapshots = sdc_executor.get_snapshots(pipeline)

        assert len(snapshots)==1
        log_line = next(
            (
                line for line in reversed(sdc_executor.get_logs()._data)
                if 'HTTP_19 - ' in line.get('message') and pipeline_title in line.get('s-entity')
            ),
            None
        )
        assert log_line is not None

    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@pytest.mark.parametrize(('method'), [
    'POST',
    # Testing of SDC-10809
    'PATCH'
])
def test_http_processor(sdc_builder, sdc_executor, http_client, method):
    """Test HTTP Lookup Processor for various HTTP methods. We do so by
    sending a request to a pre-defined HTTP server endpoint
    (testPostJsonEndpoint) and getting expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_processor >> trash
    """
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    expected_dict = dict(latitude='37.7576948', longitude='-122.4726194')
    # PATCH requests typically receive a 204 response with no body
    if method == 'POST':
        expected_data = json.dumps(expected_dict)
        expected_status = 200
    elif method == 'PATCH':
        expected_data = ''
        expected_status = 204
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(
            rule=f'{method} /{mock_path}',
            body=raw_data
        ).reply(
            body=expected_data,
            status=expected_status,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method=method, request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')
        trash = builder.add_stage('Trash')

        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title=f'HTTP Lookup {method} Processor pipeline')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # ensure HTTP POST/PATCH result is only stored to one record and assert the data
        assert len(snapshot[http_client_processor.instance_name].output) == 1
        record = snapshot[http_client_processor.instance_name].output[0].field
        if expected_data:
            assert record[record_output_field]['latitude'] == expected_dict['latitude']
            assert record[record_output_field]['longitude'] == expected_dict['longitude']
    finally:
        http_mock.delete_mock()


@http
@pytest.mark.parametrize(('method'), [
    'POST',
    # Testing of SDC-10809
    'PATCH'
])
@pytest.mark.parametrize(('request_option'), [
    'one_request_per_batch',
    # Testing of SDC-10809
    'one_request_per_record'
])
def test_http_destination(sdc_builder, sdc_executor, http_client, method, request_option):
    """Test HTTP Client Destination for HTTP POST/PATCH method. We do so by posting to a pre-defined
    HTTP server endpoint (testPostJsonEndpoint) and get expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_destination
    """
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    expected_dict = dict(latitude='37.7576948', longitude='-122.4726194')
    expected_data = json.dumps(expected_dict)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'{method} /{mock_path}', body=raw_data).reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)
        http_client_destination = builder.add_stage('HTTP Client', type='destination')
        # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
        http_client_destination.set_attributes(data_format='JSON',
                                               headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                               http_method=method,
                                               resource_url=mock_uri,
                                               one_request_per_batch=(request_option=='one_request_per_batch'))

        dev_raw_data_source >> http_client_destination
        pipeline = builder.build(title=f'HTTP {method} Destination pipeline')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Check that the HTTP server got the expected data
        r = http_mock.get_request(0)
        assert r
        assert r.method == method
        assert r.url == f'/{mock_path}'
        assert r.body
        assert json.loads(r.body.decode("utf-8")) == raw_dict
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("3.8.0")
def test_http_server_method_restriction(sdc_executor, http_server_pipeline):
    """HTTP Server Origin should actively disallow TRACE and TRACK HTTP request methods"""
    server_runtime_parameters = {'HTTP_PORT': 9999,
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID',
                                 'NEW_FIELD_NAME': 'javscriptField',
                                 'NEW_FIELD_VALUE': 5000}

    sdc_executor.start_pipeline(http_server_pipeline.pipeline, server_runtime_parameters)
    pipeline_status = sdc_executor.get_pipeline_status(http_server_pipeline.pipeline).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes.get('RUNTIME_PARAMETERS').get('APPLICATION_ID') == 'HTTP_APPLICATION_ID'

    # Try to push records using prohibited HTTP methods. Expecting HTTP status: 405 Method Not Allowed
    h1 = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
    h1.request('TRACE', '/', '{"f1": "abc"}{"f1": "xyz"}', {'X-SDC-APPLICATION-ID': 'HTTP_APPLICATION_ID'})
    resp = h1.getresponse()
    assert resp.status == 405

    h2 = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
    h2.request('TRACK', '/', '{"f1": "abc"}{"f1": "xyz"}', {'X-SDC-APPLICATION-ID': 'HTTP_APPLICATION_ID'})
    resp = h2.getresponse()
    assert resp.status == 405

    sdc_executor.stop_pipeline(http_server_pipeline.pipeline)


@http
@sdc_min_version("3.14.0")
def test_http_server_no_application_id(sdc_executor, http_server_pipeline):
    """HTTP Server Origin with no Application-ID must accept any request that does not contain any Application-ID"""
    server_runtime_parameters = {'HTTP_PORT': 9999,
                                 'APPLICATION_ID': ''}
    sdc_executor.start_pipeline(http_server_pipeline.pipeline, server_runtime_parameters)

    try:
        # Try a GET request using sample data with no application ID and we should expect a 200 response.
        http_res = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
        http_res.request('GET', '/', '{"f1": "abc"}{"f1": "xyz"}')
        resp = http_res.getresponse()
        assert resp.status == 200
    finally:
        sdc_executor.stop_pipeline(http_server_pipeline.pipeline)


@http
@sdc_min_version("3.14.0")
def test_http_server_multiple_application_ids(sdc_builder, sdc_executor):
    """HTTP Server Origin with some valid Application-ID must accept any request that contains
     a valid Application-ID"""

    pipeline_builder = sdc_builder.get_pipeline_builder()

    http_server = pipeline_builder.add_stage('HTTP Server')

    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.17.0'):
        http_server.list_of_application_ids = [{"appId": 'TEST_ID_FIRST'}, {"appId": 'TEST_ID_SECOND'}]
    elif Version(sdc_builder.version) >= Version('3.17.0'):
        http_server.list_of_application_ids = [{"credential": 'TEST_ID_FIRST'}, {"credential": 'TEST_ID_SECOND'}]

    http_server.data_format = 'JSON'
    http_server.http_listening_port = 9999

    trash = pipeline_builder.add_stage('Trash')

    http_server >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)

        # Try a GET request using sample data with a valid Application-ID and we should expect a 200 response.
        http_res = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
        http_res.request('GET', '/', '{"f1": "abc"}{"f1": "xyz"}',{'X-SDC-APPLICATION-ID': 'TEST_ID_FIRST'})
        resp = http_res.getresponse()
        assert resp.status == 200

        # Try a GET request using sample data with another valid Application-ID and we should expect a 200 response.
        http_res = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
        http_res.request('GET', '/', '{"f1": "abc"}{"f1": "xyz"}',{'X-SDC-APPLICATION-ID': 'TEST_ID_SECOND'})
        resp = http_res.getresponse()
        assert resp.status == 200

        # Try a GET request using sample data with a non valid Application-ID and we should expect a 403 response.
        http_res = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
        http_res.request('GET', '/', '{"f1": "abc"}{"f1": "xyz"}',{'X-SDC-APPLICATION-ID': 'TEST_ID_THIRD'})
        resp = http_res.getresponse()
        assert resp.status == 403
    finally:
        sdc_executor.stop_pipeline(pipeline)


@http
@sdc_min_version("3.19.0")
def test_http_client_origin_keep_all_fields_not_repeating_records(sdc_builder, sdc_executor, http_client):
    """HTTP Client Origin using pagination with Keep All Fields config enabled writing on a LocalFS must
    not repeat records on the file obtained. This tests the issue on ESC-999 (SDC-15893)"""
    dataArr = {'metadata': 'Example', 'next_page':None, 'data':[
        {'id': 0, 'name':"INDURAIN"},{'id': 1, 'name':"PANTANI"},{'id': 2, 'name':"ULRICH"} ]}
    expected_data = json.dumps(dataArr)
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    logger.info('Temp directory is %s ...', tmp_directory)

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        http_source = builder.add_stage('HTTP Client', type='origin')
        http_source.set_attributes(data_format='JSON', http_method='GET',
                                   resource_url=mock_uri,
                                   mode='BATCH',
                                   pagination_mode='LINK_FIELD',
                                   next_page_link_field="/next_page",
                                   stop_condition="${record:value('/next_page') == null }",
                                   result_field_path="/data",
                                   keep_all_fields=True)
        localfs = builder.add_stage('Local FS', type='destination')
        localfs.set_attributes(data_format='JSON',
                                json_content='MULTIPLE_OBJECTS',
                                directory_template=tmp_directory,
                                file_type='TEXT',
                                files_prefix='example',
                                files_suffix='txt')

        http_source >> localfs

        pipeline = builder.build(title='HTTP Client Origin Keep All Fields not repeating records when writing LocalFS')
        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # Check the output on the snapshot
        for value in snapshot.snapshot_batches[0][http_source.instance_name].output_lanes.values():
            assert len(value) == 3
            for i in range(3):
                assert value[i].field['metadata']=='Example'
                assert value[i].field['data']['id']==i
                assert value[i].field['data']['name']==dataArr['data'][i]['name']

        logger.info("Creating the second pipeline")

        # 2nd pipeline to read the file
        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory', type='origin')
        directory.set_attributes(batch_wait_time_in_secs=1,
                             data_format='JSON',
                             files_directory=tmp_directory,
                             file_name_pattern='example_*',
                             file_name_pattern_mode='GLOB',
                             json_content='MULTIPLE_OBJECTS',
                             batch_size_in_recs=10)

        trash = pipeline_builder.add_stage('Trash')

        directory >> trash

        pipeline_directory = pipeline_builder.build(title='HTTP Client Origin Keep All Fields not repeating records when writing LocalFS'
                                       ' (Read the file)')
        sdc_executor.add_pipeline(pipeline_directory)
        snapshot = sdc_executor.capture_snapshot(pipeline_directory, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline_directory)
        records = snapshot.snapshot_batches[0][directory.instance_name].output
        assert len(value) == 3
        for i in range(3):
            assert records[i].field['metadata']=='Example'
            assert records[i].field['data']['id']==i
            assert records[i].field['data']['name']==dataArr['data'][i]['name']
    finally:
        http_mock.delete_mock()
        logger.info("Removing tmp folder: %s", tmp_directory)
        if os.path.exists(tmp_directory) and os.path.isdir(tmp_directory):
            shutil.rmtree(tmp_directory)


@http
@sdc_min_version("3.16.0")
def test_http_client_wrong_pagination_field(sdc_builder, sdc_executor, http_client):
    """HTTP Client Origin with some an invalid page link field must throw an StageException HTTP_66"""
    dataArr = {'Name': f'Example', 'data':[{'id': 2, 'foo':2}]}

    expected_data = json.dumps(dataArr)
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        http_source = builder.add_stage('HTTP Client', type='origin')
        http_source.set_attributes(data_format='JSON', http_method='GET',
                                             resource_url=mock_uri,
                                             mode='POLLING',
                                             pagination_mode='LINK_FIELD',
                                             next_page_link_prefix=f'{mock_uri}&starting_after=',
                                             next_page_link_field="/pageField",
                                             stop_condition="1==0",
                                             result_field_path="/data")
        trash = builder.add_stage('Trash')

        http_source >> trash
        pipeline = builder.build(title='HTTP Client Origin wrong page field')
        sdc_executor.add_pipeline(pipeline)

        # Pipeline should stop with StageExcception
        with pytest.raises(Exception):
            sdc_executor.start_pipeline(pipeline)
            time.sleep(10)
            sdc_executor.stop_pipeline(pipeline)

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        message = sdc_executor.get_pipeline_status(pipeline).response.json().get('message')
        assert 'RUN_ERROR' == status
        assert 'HTTP_66 -' in message

    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("3.16.0")
def test_http_client_propagate_all_records(sdc_builder, sdc_executor, http_client):
    """HTTP Client Origin with the config 'Records for Remaining Statuses' set generates a record when gets a response
    different than the 200 OK HTTP Status. In this test we will simulate it gets a 404 HTTP Status and we will
    check a record is created"""
    dataArr = {'Name': f'Example'}

    expected_data = json.dumps(dataArr)
    mock_path = get_random_string(string.ascii_letters, 10)
    mock_wrong_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_wrong_uri = f'{http_mock.pretend_url}/{mock_wrong_path}'

        builder = sdc_builder.get_pipeline_builder()
        http_source = builder.add_stage('HTTP Client', type='origin')
        http_source.set_attributes(data_format='JSON', http_method='GET',
                                   resource_url=mock_wrong_uri,
                                   mode='POLLING',
                                   records_for_remaining_statuses=True
                                   )
        trash = builder.add_stage('Trash')

        http_source >> trash
        pipeline = builder.build(title='HTTP Client Origin propagates 404 record')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # ensure HTTP GET result has 1 records
        assert len(snapshot[http_source.instance_name].output) == 1
        assert snapshot[http_source.instance_name].output[0].header.values['HTTP-Status']=='404'

    finally:
        http_mock.delete_mock()

@http
@sdc_min_version("3.16.0")
def test_http_client_http_status_on_header(sdc_builder, sdc_executor, http_client):
    """HTTP Client Origin with the config 'Records for Remaining Statuses' set generates a record when gets a response
    different than the 200 OK HTTP Status. In this test we will simulate it gets a 404 HTTP Status and we will
    check a record is created"""
    dataArr = {'Name': f'Example'}

    expected_data = json.dumps(dataArr)
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        http_source = builder.add_stage('HTTP Client', type='origin')
        http_source.set_attributes(data_format='JSON', http_method='GET',
                                   resource_url=mock_uri,
                                   mode='POLLING',
                                   records_for_remaining_statuses=True
                                   )
        trash = builder.add_stage('Trash')

        http_source >> trash
        pipeline = builder.build(title='HTTP Client Origin HTTP-Status on header')
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # ensure HTTP GET result has at least 1 record
        num_of_els = len(snapshot[http_source.instance_name].output)
        assert num_of_els > 0
        # it has the HTTP-Status on header
        for x in range(num_of_els):
            assert 'HTTP-Status' in snapshot[http_source.instance_name].output[x].header.values

    finally:
        http_mock.delete_mock()


@http
@spnego
@sdc_min_version("3.16.0")
def test_http_server_with_spnego(sdc_builder, sdc_executor, http_client):
    # The goal of this test is to verify the http server origin which is configured to use SPNEGO/Kerberos
    # authentication. As part of the test, a HTTP Server stage is configured with SPENGO/Kerberos authentication and the
    # pipeline started. A connection is attempted where the client has not yet authenticated with kerberos. This should
    # fail with a 401 response. The next step is to login on the client (in this case the STF container) and then do a
    # get and a post (with some random data) and verify that the response code is a 200.

    # skip this test if incoming http_client fixture does not have kerberos enabled.
    if not http_client.kerberos_enabled:
        pytest.skip('Skipping test because Kerberos is not enabled for the HTTP fixture.')

    try:
        builder = sdc_builder.get_pipeline_builder()
        http_server = builder.add_stage('HTTP Server', type='origin')
        http_server.set_attributes(use_kerberos_with_spnego_authentication=True,
                                   kerberos_realm=http_client.kerberos_realm,
                                   http_spnego_principal=http_client.service_principal,
                                   keytab_file=http_client.service_keytab_path,
                                   data_format='JSON')
        trash = builder.add_stage('Trash')
        http_server >> trash
        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        gssapi_auth = HTTPSPNEGOAuth(mech=http_client.spnego_auth_mechanism)

        server_url = 'http://{}:8000'.format(sdc_executor.fqdn)

        # Test connection without credentials. This should fail with 401.
        response = requests.get(server_url, auth=gssapi_auth)
        assert response.status_code == 401

        # Get Kerberos credentials.
        http_client.kerberos_login()

        # Test connections after getting credentials.
        response = requests.get(server_url, auth=gssapi_auth)
        assert response.status_code == 200

        response = requests.post(server_url, auth=gssapi_auth, data='{"foo": "bar"}')
        assert response.status_code == 200

    finally:
        sdc_executor.stop_pipeline(pipeline)


@http
@sdc_min_version("3.17.0")
def test_http_client_remote_vault(sdc_builder, sdc_executor, http_client, credential_store):
    # skip the test if the http client isn't ssl enabled.
    if not credential_store or not http_client.ssl_enabled:
        pytest.skip('Skipping since credential_store is not defined or ssl-reverse-proxy-url is not specified.')

    if credential_store and isinstance(credential_store, JKSCredentialStore):
        pytest.skip('Skipping for JKS - as it does not apply to store webserver certificate')

    expected_message = {'msg': 'hello'}
    try:
        mock = http_client.mock()
        mock.when('GET /hello').reply(json.dumps(expected_message), times=FOREVER)

        builder = sdc_builder.get_pipeline_builder()
        http_client_origin = builder.add_stage('HTTP Client', type='origin')
        pretend_url = f'{mock.pretend_url}/hello'

        cert_expression = CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION.format(credential_store.store_id,
                                                                          credential_store.group_id,
                                                                          'webserver-certificate',
                                                                          'credentialType=certificate')
        http_client_origin.set_attributes(resource_url=http_client.ssl_url(pretend_url),
                                          use_tls=True,
                                          use_remote_truststore=True,
                                          mode='BATCH',
                                          trusted_certificates=[{'credential': cert_expression}])

        trash = builder.add_stage('Trash')
        wiretap = builder.add_wiretap()

        http_client_origin >> [wiretap.destination, trash]

        pipeline = builder.build().configure_for_environment(credential_store)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        assert(wiretap.output_records[0].field == expected_message)

    finally:
        mock.delete_mock()


@http
@sdc_min_version("3.17.0")
def test_http_server_remote_vault(sdc_builder, sdc_executor, http_client, credential_store):

    # skip the test if the http client isn't ssl enabled.
    if not credential_store or not http_client.ssl_enabled:
        pytest.skip('Skipping since credential_store is not defined or ssl-reverse-proxy-url is not specified.')

    if credential_store and isinstance(credential_store, JKSCredentialStore):
        pytest.skip('Skipping for JKS - as it does not apply to store webserver certificate')

    try:
        builder = sdc_builder.get_pipeline_builder()
        http_server_origin = builder.add_stage('HTTP Server', type='origin')

        key_expression = CREDENTIAL_STORE_EXPRESSION.format(credential_store.store_id, credential_store.group_id,
                                                            'webserver-privatekey')
        cert_expression = CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION.format(credential_store.store_id,
                                                                          credential_store.group_id,
                                                                          'webserver-certificate',
                                                                          'credentialType=certificate')
        http_port = 9999
        http_server_origin.set_attributes(use_tls=True,
                                          data_format='JSON',
                                          http_listening_port=http_port,
                                          use_remote_keystore=True,
                                          private_key=key_expression,
                                          certificate_chain=[{'credential': cert_expression}])

        trash = builder.add_stage('Trash')
        wiretap = builder.add_wiretap()

        http_server_origin >> [wiretap.destination, trash]

        pipeline = builder.build().configure_for_environment(credential_store)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # Post to the server using a valid certificate.
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False  # Ignore the hostname
        # Add the certificate present in the test config directory. This is setup when the test environment is started.
        ssl_context.load_verify_locations(cafile=f'{STF_TESTCONFIG_DIR}/selfsigned.crt')

        response = urllib.request.urlopen(url=f'https://{sdc_executor.fqdn}:{http_port}/',
                                          data=bytes(json.dumps({'msg': 'hello'}).encode('utf-8')),
                                          context=ssl_context)

        assert(response.status == 200)

        # Post to the server again but this time using the default certificates.
        default_context = ssl.create_default_context()
        default_context.check_hostname = False
        default_context.load_default_certs()

        # An exception will be thrown because certificate verification will fail.
        try:
            response = urllib.request.urlopen(url=f'https://{sdc_executor.fqdn}:{http_port}/',
                                              data=bytes(json.dumps({'msg': 'hello'}).encode('utf-8')),
                                              context=default_context)
        except urllib.error.URLError as err:
            assert("certificate verify failed" in str(err))

    finally:
        sdc_executor.stop_pipeline(pipeline)


@http
@sdc_min_version("3.18.0")
@pytest.mark.parametrize(('miss_val_bh'), [
    'PASS_RECORD_ON',
    'SEND_TO_ERROR'
])
def test_http_processor_response_JSON_empty(sdc_builder, sdc_executor, http_client, miss_val_bh):
    """
    Test when the http processor stage has as a response an empty JSON.

    We use the pipeline:
    dev_raw_data_source >> http_client_processor >> trash

    Test for SDC-15335.
    """
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)

    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(
            rule=f'POST /{mock_path}',
            body=raw_data
        ).reply(
            body='[]',
            status=200,
            times=FOREVER
        )
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')

        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method='POST', request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}',
                                             missing_values_behavior=miss_val_bh)
        trash = builder.add_stage('Trash')

        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title=f'HTTP Lookup Processor pipeline {miss_val_bh}')
        sdc_executor.add_pipeline(pipeline)

        snapshot_cmd = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=1,
                                                     batch_size=1)
        snapshot = snapshot_cmd.wait_for_finished().snapshot

        # ensure HTTP POST result produce 0 records and status STOPPED
        if miss_val_bh == 'SEND_TO_ERROR':
            stage = snapshot[http_client_processor.instance_name]
            logger.info('Error record %s ...', stage.error_records)
            assert 1 == len(stage.error_records)
            assert len(stage.output) == 0
            assert 'HTTP_68' == stage.error_records[0].header['errorCode']

        else:
            assert len(snapshot[http_client_processor.instance_name].output) == 1
            stage = snapshot[http_client_processor.instance_name]
            assert len(stage.output) == 1
            assert stage.output[0].field['text'].value == '{"city": "San Francisco"}'

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert 'FINISHED' == status

    finally:
        http_mock.delete_mock()


# SDC-16431:  Allow sending body with DELETE and other HTTP methods in HTTP components
@http
@sdc_min_version("3.11.0")
@pytest.mark.parametrize('method', [
    'GET',
    'PUT',
    'POST',
    'DELETE',
    'HEAD',
    'PATCH'
])
def test_http_processor_with_body(sdc_builder, sdc_executor, method, http_client, keep_data):
    expected_data = json.dumps({'A': 1})
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='TEXT', raw_data='dummy')
        origin.stop_after_first_batch = True

        processor = builder.add_stage('HTTP Client', type='processor')
        processor.set_attributes(data_format='JSON', http_method=method,
                                 resource_url=mock_uri,
                                 output_field='/result',
                                 request_data="{'something': 'here'}")
        wiretap = builder.add_wiretap()

        origin >> processor >> wiretap.destination
        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 1

        # The mock server won't return body on HEAD (rightfully so), but we can still send body to it though
        if method != 'HEAD':
            assert records[0].field['result'] == {'A': 1}
    finally:
        if not keep_data:
            http_mock.delete_mock()


# SDC-16431:  Allow sending body with DELETE and other HTTP methods in HTTP components
@http
@sdc_min_version("3.11.0")
@pytest.mark.parametrize('method', [
    'PUT',
    'POST',
    'DELETE',
])
def test_http_client_with_body(sdc_builder, sdc_executor, method, http_client, keep_data):
    expected_data = json.dumps({'A': 1})
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('HTTP Client', type='origin')
        origin.set_attributes(data_format='JSON', http_method=method,
                              resource_url=mock_uri,
                              mode='BATCH',
                              request_body="{'something': 'here'}")
        wiretap = builder.add_wiretap()

        origin >> wiretap.destination
        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 1

        assert records[0].field['A'] == 1
    finally:
        if not keep_data:
            http_mock.delete_mock()


# SDC-16431:  Allow sending body with DELETE and other HTTP methods in HTTP components
@http
@sdc_min_version("3.11.0")
@pytest.mark.parametrize('method', [
    'GET',
    'PUT',
    'POST',
    'DELETE',
    'HEAD',
    'PATCH'
])
def test_http_destination_with_body(sdc_builder, sdc_executor, method, http_client, keep_data):
    expected_data = json.dumps({'A': 1})
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON', raw_data='{"A": 1}')
        origin.stop_after_first_batch = True

        target = builder.add_stage('HTTP Client', type='destination')
        target.set_attributes(data_format='JSON', http_method=method, resource_url=mock_uri)

        origin >> target
        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Check that the HTTP server got the expected data
        request = http_mock.get_request(0)
        assert request
        assert request.method == method
        assert request.url == f'/{mock_path}'

        # The mock server won't persist body of GET and HEAD
        if method != 'GET' and method != 'HEAD':
            assert request.body
            assert json.loads(request.body.decode("utf-8")) == {"A": 1}
    finally:
        if not keep_data:
            http_mock.delete_mock()