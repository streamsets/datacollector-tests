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
import pytest

from collections import namedtuple
from pretenders.common.constants import FOREVER
from requests_gssapi import HTTPSPNEGOAuth
from streamsets.sdk.sdc_api import RunError
from streamsets.sdk import sdc_api
from streamsets.sdk.utils import Version
from streamsets.testframework.constants import (CREDENTIAL_STORE_EXPRESSION, CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION,
                                                STF_TESTCONFIG_DIR)
from streamsets.testframework.credential_stores.jks import JKSCredentialStore
from streamsets.testframework.markers import http, sdc_min_version, spnego
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@http
@sdc_min_version("3.11.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_multiple_records(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    """Test HTTP Lookup Processor for HTTP GET method and split the obtained result
    in different records:

        dev_raw_data_source >> http_client_processor >> wiretap
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

    # The data returned by the HTTP mock server
    data_array = [{'A': i, 'C': i + 1, 'G': i + 2, 'T': i + 3} for i in range(10)]

    expected_data = json.dumps(data_array)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='dummy',
                                           stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', http_method='GET',
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}',
                                             multiple_values_behavior='SPLIT_INTO_MULTIPLE_RECORDS',
                                             **one_request_per_batch_option)

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title='HTTP Lookup GET Processor Split Multiple Records pipeline')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # ensure HTTP GET result has 10 different records
        assert len(wiretap.output_records) == 10
        # check each
        for x in range(10):
            assert wiretap.output_records[x].field[record_output_field]['A'] == x
            assert wiretap.output_records[x].field[record_output_field]['C'] == x+1
            assert wiretap.output_records[x].field[record_output_field]['G'] == x+2
            assert wiretap.output_records[x].field[record_output_field]['T'] == x+3

    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("3.11.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_list(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    """Test HTTP Lookup Processor for HTTP GET method and split the obtained result
    in different elements of the same list stored in just one record:

        dev_raw_data_source >> http_client_processor >> wiretap
    """

    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

    # The data returned by the HTTP mock server
    data_array = [{'A': i, 'C': i + 1, 'G': i + 2, 'T': i + 3} for i in range(10)]

    expected_data = json.dumps(data_array)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='dummy',
                                           stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', http_method='GET',
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}',
                                             multiple_values_behavior='ALL_AS_LIST',
                                             **one_request_per_batch_option)
        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title='HTTP Lookup GET Processor All As List pipeline')
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.output_records) == 1
        # check each element of the list
        for x in range(10):
            assert wiretap.output_records[0].field[record_output_field][x]['A'] == x+0
            assert wiretap.output_records[0].field[record_output_field][x]['C'] == x+1
            assert wiretap.output_records[0].field[record_output_field][x]['G'] == x+2
            assert wiretap.output_records[0].field[record_output_field][x]['T'] == x+3

    finally:
        try:
            logger.info("Deleting http mock")
            http_mock.delete_mock()
        except:
            logger.info("Deleting http mock failed")


@http
@sdc_min_version("3.17.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_response_action_stage_error(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    """
    Test when the http processor stage has the response action set up with the "Cause Stage to fail" option.
    To test this we force the URL to be a not available so we get a 404 response from the mock http server. An
    exception should be risen that shows the stage error.

    We use the pipeline:
    dev_raw_data_source >> http_client_processor >> wiretap

    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

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
                                             output_field=f'/{record_output_field}',
                                             **one_request_per_batch_option)
        http_client_processor.per_status_actions = [
            {
              'statusCode': 404,
              'action': 'STAGE_ERROR'
            },
        ]
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions')
        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(sdc_api.RunError) as exception_info:
            sdc_executor.start_pipeline(pipeline)

        assert 'HTTP_14 - ' in f'{exception_info.value}'
    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@sdc_min_version("3.17.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_response_action_record_error(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    """
    Test when the http processor stage has the response action set up with the "Generate Error Record" option.
    To test this we force the URL to be a not available so we get a 404 response from the mock http server. The output
    should be one error record containing the right error code.

    We use the pipeline:
         dev_raw_data_source >> http_client_processor >> wiretap
"""
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

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
                                             output_field=f'/{record_output_field}',
                                             **one_request_per_batch_option)

        http_client_processor.per_status_actions = [
            {
                'statusCode': 404,
                'action': 'ERROR_RECORD'
            },
        ]
        wiretap = builder.add_wiretap()
        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.error_records) == 1
        assert wiretap.error_records[0].field['text'].value == raw_data

    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@sdc_min_version("3.17.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_propagate_error_records(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    """
        Test when the http processor stage has the config option "Records for remaining statuses" set. To test this we
        force the URL to be a not available so we get a 404 response from the mock http server. The output should be
        one record containing the "Error Response Body Field" with the error message from the mock server.

        We use the pipeline:
             dev_raw_data_source >> http_client_processor >> wiretap
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

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
                                             output_field=f'/{record_output_field}',
                                             **one_request_per_batch_option)
        http_client_processor.records_for_remaining_statuses = True
        http_client_processor.error_response_body_field = 'errorField'

        wiretap = builder.add_wiretap()
        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['result']['errorField'].value == 'No matching preset response'
    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@http
@sdc_min_version("3.17.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_batch_wait_time_not_enough(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    """
        When the Batch Wait Time is not big enough and there is a retry action configured it can be the batch time
        expires before the number of retries is finished yet. In this case an stage error must be raised explaining
        the reason. We force the error to appear by configuring the pipeline to stop when it finds an stage error.

        We use the pipeline:
             dev_raw_data_source >> http_client_processor >> trash
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

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
                                             output_field=f'/{record_output_field}',
                                             **one_request_per_batch_option)

        http_client_processor.records_for_remaining_statuses = False
        http_client_processor.batch_wait_time_in_ms = 150
        http_client_processor.multiple_values_behavior = 'ALL_AS_LIST'
        http_client_processor.per_status_actions = [
            {
                'statusCode': 404,
                'action': 'RETRY_LINEAR_BACKOFF',
                'backoffInterval': 100,
                'maxNumRetries': 10
            },
        ]
        http_client_processor.on_record_error = 'STOP_PIPELINE'

        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline = builder.build(title='HTTP Lookup Processor pipeline Response Actions '
                                       'Max wait Time is not enough stage error')
        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(sdc_api.RunError) as exception_info:
            sdc_executor.start_pipeline(pipeline)
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
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_pagination_and_retry_action(sdc_builder, sdc_executor, http_client, retry_action,
                                                    pagination_option, one_request_per_batch):
    """
        Test when a pagination option is set up and a retry action is set up and the maximum number
        of retries is exhausted then the error saying the number of retries is exceeded is risen.

        We use the pipeline:
             dev_raw_data_source >> http_client_processor >> trash
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

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
                                             output_field=f'/{record_output_field}',
                                             **one_request_per_batch_option)

        http_client_processor.records_for_remaining_statuses = False
        http_client_processor.batch_wait_time_in_ms = 500000
        http_client_processor.pagination_mode = pagination_option;
        http_client_processor.per_status_actions = [
            {
                'statusCode': 404,
                'action': retry_action,
                'backoffInterval': 100,
                'maxNumRetries': 3
            },
        ]
        http_client_processor.result_field_path = '/'
        http_client_processor.next_page_link_field = '/foo'
        http_client_processor.stop_condition = '1==1'
        http_client_processor.multiple_values_behavior = 'ALL_AS_LIST'
        # Must do it like this because the attribute name has the '/' char
        setattr(http_client_processor, 'initial_page/offset', 1)

        trash = builder.add_stage('Trash')
        dev_raw_data_source >> http_client_processor >> trash
        pipeline_title = f'HTTP Lookup Processor pipeline Response Actions with Pagination {rand_pipeline_name}'
        pipeline = builder.build(title=pipeline_title)
        sdc_executor.add_pipeline(pipeline)
        try:
            sdc_executor.start_pipeline(pipeline, wait=False)
        except Exception as e:
            assert 'HTTP_19 - ' in str(e)
    finally:
        logger.info("Deleting http mock")
        http_mock.delete_mock()


@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_wrong_url(sdc_builder, sdc_executor, one_request_per_batch):
    """Test HTTP Lookup Processor for a wrong URL. This should produce one
    error record. This test ensures there are no multiple error records created
    for each request. That is solved on SDC-16691

        dev_raw_data_source >> http_client_processor >> wiretap
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch}

    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    mock_path = get_random_string(string.ascii_letters, 5)

    mock_uri = f'http://fake_url_{mock_path}'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    http_client_processor = builder.add_stage('HTTP Client', type='processor')

    http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                         http_method='GET',
                                         resource_url=mock_uri,
                                         output_field=f'/result',
                                         **one_request_per_batch_option)


    wiretap = builder.add_wiretap()

    dev_raw_data_source >> http_client_processor >> wiretap.destination
    pipeline = builder.build(title=f'HTTP Lookup Wrong URL Processor pipeline')
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert len(wiretap.error_records) == 1
    assert 'HTTP_03' == wiretap.error_records[0].header['errorCode']
    assert 'UnknownHostException' in wiretap.error_records[0].header['errorMessage']


@http
@pytest.mark.parametrize('method', [
    'POST',
    # Testing of SDC-10809
    'PATCH'
])
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor(sdc_builder, sdc_executor, http_client, method, one_request_per_batch):
    """Test HTTP Lookup Processor for various HTTP methods. We do so by
    sending a request to a pre-defined HTTP server endpoint
    (testPostJsonEndpoint) and getting expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_processor >> wiretap
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

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
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method=method, request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}',
                                             **one_request_per_batch_option)

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title=f'HTTP Lookup {method} Processor pipeline')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # ensure HTTP POST/PATCH result is only stored to one record and assert the data
        assert len(wiretap.output_records) == 1
        record = wiretap.output_records[0].field
        if expected_data:
            assert record[record_output_field]['latitude'] == expected_dict['latitude']
            assert record[record_output_field]['longitude'] == expected_dict['longitude']
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_additional_security_header(sdc_builder, sdc_executor, http_client):
    """Test HTTP Lookup Processor using the new headers property. We do so by
    sending a request to a pre-defined HTTP server endpoint
    (testPostJsonEndpoint) and getting expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_processor >> wiretap
    """

    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
    expected_dict = dict(latitude='37.7576948', longitude='-122.4726194')
    # PATCH requests typically receive a 204 response with no body
    method = 'POST'
    expected_data = json.dumps(expected_dict)
    expected_status = 200
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
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
        http_client_processor.set_attributes(data_format='JSON', default_request_content_type='application/text',
                                             additional_security_headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                             http_method=method, request_data="${record:value('/text')}",
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title='test_http_additional_security_header')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # ensure HTTP POST/PATCH result is only stored to one record and assert the data
        assert len(wiretap.output_records) == 1
        record = wiretap.output_records[0].field
        if expected_data:
            assert record[record_output_field]['latitude'] == expected_dict['latitude']
            assert record[record_output_field]['longitude'] == expected_dict['longitude']
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("3.18.0")
@pytest.mark.parametrize('miss_val_bh', [
    'PASS_RECORD_ON',
    'SEND_TO_ERROR'
])
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_response_json_empty(sdc_builder, sdc_executor, http_client, miss_val_bh, one_request_per_batch):
    """
    Test when the http processor stage has as a response an empty JSON.

    We use the pipeline:
    dev_raw_data_source >> http_client_processor >> wiretap

    Test for SDC-15335.
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

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
                                             missing_values_behavior=miss_val_bh,
                                             **one_request_per_batch_option)

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title=f'HTTP Lookup Processor pipeline {miss_val_bh}')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # ensure HTTP POST result produce 0 records
        if miss_val_bh == 'SEND_TO_ERROR':
            assert 1 == len(wiretap.error_records)
            assert len(wiretap.output_records) == 0
            assert 'HTTP_68' == wiretap.error_records[0].header['errorCode']

        else:
            # ensure HTTP POST result produce 1 record
            assert 0 == len(wiretap.error_records)
            assert len(wiretap.output_records) == 1
            assert wiretap.output_records[0].field['text'].value == '{"city": "San Francisco"}'

        # ensure status is finished
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
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_with_body(sdc_builder, sdc_executor, method, http_client, keep_data, one_request_per_batch):
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

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
                                 request_data="{'something': 'here'}",
                                 **one_request_per_batch_option)

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


@http
@sdc_min_version("4.4.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_oauth2_token_retry(sdc_builder, sdc_executor, http_client, keep_data, one_request_per_batch):
    """
    Test the error when the http processor stage has more than the allowed number of consecutive invalid oauth2 tokens
    (1 for now, but test is suitable for any number as we just mock infinite invalid tokens).
    There are similar tests for origin and destination.

    We use the pipeline:
        dev_raw_data_source >> http_client_processor >> wiretap

    """
    mock_oauth_token = {
        "access_token": "MTQ0NjJkZmQ5OTM2NDE1ZTZjNGZmZjI3",
        "token_type": "Bearer",
        "expires_in": 100,
        "refresh_token": "IwOGYzYTlmM2YxOTQ5MGE3YmNmMDFkNTVk",
        "scope": "create"
    }
    mock_oauth_token_data = json.dumps(mock_oauth_token)
    mock_path = get_random_string(string.ascii_letters, 10)
    oauth_mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()
    oauth_http_mock = http_client.mock()

    try:
        http_mock.when(f'POST /{mock_path}').reply(status=403, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        oauth_http_mock.when(f'POST /{oauth_mock_path}').reply(mock_oauth_token_data, times=FOREVER)
        oauth_mock_uri = f'{oauth_http_mock.pretend_url}/{oauth_mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='dummy')
        dev_raw_data_source.stop_after_first_batch = True

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', http_method='POST',
                                             resource_url=mock_uri,
                                             output_field='/result',
                                             request_data="{'something': 'here'}",
                                             use_oauth_2=True,
                                             credentials_grant_type='CLIENT_CREDENTIALS',
                                             token_url=oauth_mock_uri,
                                             client_id='-',
                                             client_secret='-')

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        pytest.fail('Test should have raised an Exception with HTTP_32 - HTTP_38 exception, but did not')
    except RunError as e:
        assert 'HTTP_38' in e.message
    finally:
        if not keep_data:
            http_mock.delete_mock()
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


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
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_duplicate_requests(sdc_builder, sdc_executor, method, http_client, keep_data,
                                           one_request_per_batch):
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

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
                                 request_data="{'something': 'here'}",
                                 multiple_values_behavior='SPLIT_INTO_MULTIPLE_RECORDS',
                                 **one_request_per_batch_option)

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

        # Finally, check that only one request has been made
        assert len(http_mock.get_request()) == 1

    finally:
        if not keep_data:
            http_mock.delete_mock()


@sdc_min_version("4.0.0")
@http
@pytest.mark.parametrize('timeout_mode',
                         [
                             'connection',
                             'read',
                             'request',
                             'record',
                         ])
@pytest.mark.parametrize('timeout_action',
                         [
                             'RETRY_IMMEDIATELY',
                             'RETRY_LINEAR_BACKOFF',
                             'RETRY_EXPONENTIAL_BACKOFF',
                             'STAGE_ERROR',
                             'ERROR_RECORD'
                         ])
@pytest.mark.parametrize('pass_record',
                         [
                             True,
                             False
                         ])
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_client_processor_timeout(sdc_builder,
                                       sdc_executor,
                                       http_client,
                                       timeout_mode,
                                       timeout_action,
                                       pass_record,
                                       one_request_per_batch):
    """
        Test timeout handling for HTTP Client Processor.
        We get a Connection Timeout using a non-routable IP in resource_url
        We get a Read Timeout using an extremely low read_timeout
        We get a Request Timeout using an extremely low maximum_request_time_in_sec
        We get a Record Processing Timeout using an extremely low batch_wait_time_in_ms
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

    try:

        logger.info(f'Running test: {timeout_mode} - {timeout_action} - {pass_record}')

        non_routable_ip = '192.168.255.255'
        record_output_field = 'oteai'
        one_millisecond = 1000
        wait_seconds = 10
        retries = 2
        interval = 5000
        no_time = 0
        short_time = 1
        long_time = (one_millisecond * wait_seconds * (retries + 2)) * 100

        http_mock_server = http_client.mock()
        http_mock_path = get_random_string(string.ascii_letters, 10)
        http_mock_content = dict(kisei='Kobayashi Koichi', meijin='Ishida Yoshio', honinbo='Takemiya Masaki')
        http_mock_data = json.dumps(http_mock_content)

        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=200,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=FOREVER)

        http_mock_url_ok = f'{http_mock_server.pretend_url}/{http_mock_path}'
        http_mock_url_ko = http_mock_url_ok.replace(http_mock_server.host, non_routable_ip)

        if timeout_mode == 'connection':
            resource_url = http_mock_url_ko
            connect_timeout = short_time
            read_timeout = long_time
            maximum_request_time_in_sec = long_time
            batch_wait_time_in_ms = long_time
        elif timeout_mode == 'read':
            resource_url = http_mock_url_ok
            connect_timeout = long_time
            read_timeout = short_time
            maximum_request_time_in_sec = long_time
            batch_wait_time_in_ms = long_time
        elif timeout_mode == 'request':
            resource_url = http_mock_url_ok
            connect_timeout = long_time
            read_timeout = long_time
            maximum_request_time_in_sec = short_time
            batch_wait_time_in_ms = long_time
        elif timeout_mode == 'record':
            resource_url = http_mock_url_ok
            connect_timeout = long_time
            read_timeout = long_time
            maximum_request_time_in_sec = long_time
            batch_wait_time_in_ms = short_time
        else:
            resource_url = http_mock_url_ko
            connect_timeout = no_time
            read_timeout = no_time
            maximum_request_time_in_sec = no_time
            batch_wait_time_in_ms = no_time

        pipeline_name = f'{timeout_mode} - {timeout_action} - {pass_record} - {get_random_string(string.ascii_letters, 10)}'
        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=http_mock_data,
                                                  stop_after_first_batch=True)

        http_client_processor = pipeline_builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             resource_url=resource_url,
                                             http_method='GET',
                                             default_request_content_type='application/json',
                                             request_data="${record:value('/honinbo')}",
                                             output_field=f'/{record_output_field}',
                                             connect_timeout=connect_timeout,
                                             read_timeout=read_timeout,
                                             maximum_request_time_in_sec=maximum_request_time_in_sec,
                                             batch_wait_time_in_ms=batch_wait_time_in_ms,
                                             action_for_timeout=timeout_action,
                                             base_backoff_interval_in_ms=interval,
                                             max_retries=retries,
                                             pass_record=pass_record,
                                             records_for_remaining_statuses=False,
                                             missing_values_behavior='SEND_TO_ERROR',
                                             **one_request_per_batch_option)


        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> http_client_processor >> wiretap.destination

        pipeline_title = f'HTTP Client Processor Timeout Test Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title)
        pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
        sdc_executor.add_pipeline(pipeline)

        if timeout_action == 'STAGE_ERROR':
            if timeout_mode == 'record':
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
            else:
                with pytest.raises(Exception) as exception:
                    sdc_executor.start_pipeline(pipeline).wait_for_finished()
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if timeout_action == 'STAGE_ERROR':
            expected_output = 0
        else:
            if timeout_mode == 'record':
                expected_output = 0
            else:
                if pass_record:
                    expected_output = 1
                else:
                    expected_output = 0

        if timeout_action == 'STAGE_ERROR':
            if timeout_mode == 'record':
                expected_error = 0
                expected_message = 1
            else:
                expected_error = 0
                expected_message = 0
        else:
            if timeout_mode == 'record':
                expected_error = 0
                expected_message = 1
            else:
                if expected_output == 0:
                    expected_error = 1
                    expected_error_code = 'HTTP_102'
                    expected_message = 0
                else:
                    expected_error = 0
                    expected_message = 1

        try:
            pipeline_metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
            error_metric = f'stage.{http_client_processor.instance_name}.stageErrors.counter'
            error_counter = pipeline_metrics.counter(error_metric).count
        except:
            logger.warning('Error reading metrics...')
            error_counter = 0

        logger.info(
            f'Finishing test: {timeout_mode} - {timeout_action} - {pass_record} - '
            f'{expected_output} vs {len(wiretap.output_records)} - '
            f'{expected_error} vs {len(wiretap.error_records)} - '
            f'{expected_message} vs {error_counter}')

        assert len(wiretap.output_records) == expected_output, 'Unexpected number of output records'
        assert len(wiretap.error_records) == expected_error, 'Unexpected number of error records'
        if (expected_error > 0):
            assert expected_error_code in wiretap.error_records[0].header['errorMessage']
        assert error_counter == expected_message, 'Unexpected number of stage errors'

        pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if timeout_action == 'STAGE_ERROR':
            if timeout_mode == 'record':
                assert pipeline_status == 'FINISHED'
            else:
                assert pipeline_status == 'RUN_ERROR'
        else:
            assert pipeline_status == 'FINISHED'

    finally:

        http_mock_server.delete_mock()


@sdc_min_version("4.0.0")
@http
@pytest.mark.parametrize('http_status',
                         [
                            200,
                            404,
                            500
                         ])
@pytest.mark.parametrize('exhausted_action',
                         [
                             'RETRY_IMMEDIATELY',
                             'RETRY_LINEAR_BACKOFF',
                             'RETRY_EXPONENTIAL_BACKOFF',
                             'STAGE_ERROR',
                             'ERROR_RECORD'
                         ])
@pytest.mark.parametrize('pass_record',
                         [
                             True,
                             False
                         ])
@pytest.mark.parametrize('pass_record_other_status',
                         [
                             True,
                             False
                         ])
@pytest.mark.parametrize("one_request_per_batch",
                         [
                            True,
                            False
                         ])
def test_http_client_processor_passthrough(sdc_builder,
                                           sdc_executor,
                                           http_client,
                                           http_status,
                                           exhausted_action,
                                           pass_record,
                                           pass_record_other_status,
                                           one_request_per_batch):
    """
        Test exhausted handling for HTTP Client Processor.
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

    logger.info(f'Running test: {http_status} - {exhausted_action} - {pass_record} - {pass_record_other_status}')

    record_output_field = 'oteai'
    one_millisecond = 1000
    wait_seconds = 1
    retries = 2
    interval = 2000
    long_time = (one_millisecond * wait_seconds * (retries + 2)) * 10

    http_mock_server = http_client.mock()
    http_mock_path = get_random_string(string.ascii_letters, 10)
    http_mock_content = dict(kisei='Kobayashi Koichi', meijin='Ishida Yoshio', honinbo='Takemiya Masaki')
    http_mock_data = json.dumps(http_mock_content)

    http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                               body=http_mock_data,
                                                               status=http_status,
                                                               headers={'Content-Type': 'application/json'},
                                                               times=FOREVER)

    http_mock_url = f'{http_mock_server.pretend_url}/{http_mock_path}'

    resource_url = http_mock_url
    connect_timeout = long_time
    read_timeout = long_time
    maximum_request_time_in_sec = long_time
    batch_wait_time_in_ms = long_time

    try:
        pipeline_name = f'{http_status} - {exhausted_action} - {pass_record} - {pass_record_other_status}' \
                        f' - {get_random_string(string.ascii_letters, 10)}'
        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=http_mock_data,
                                                  stop_after_first_batch=True)

        http_client_processor = pipeline_builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             resource_url=resource_url,
                                             http_method='GET',
                                             default_request_content_type='application/json',
                                             request_data="${record:value('/honinbo')}",
                                             output_field=f'/{record_output_field}',
                                             connect_timeout=connect_timeout,
                                             read_timeout=read_timeout,
                                             maximum_request_time_in_sec=maximum_request_time_in_sec,
                                             batch_wait_time_in_ms=batch_wait_time_in_ms,
                                             action_for_timeout='STAGE_ERROR',
                                             records_for_remaining_statuses=pass_record_other_status,
                                             missing_values_behavior='SEND_TO_ERROR',
                                             **one_request_per_batch_option)

        http_client_processor.per_status_actions = [{
            'statusCode': 500,
            'action': exhausted_action,
            'backoffInterval': interval,
            'maxNumRetries': retries,
            'passRecord': pass_record
        }]

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> http_client_processor >> wiretap.destination

        pipeline_title = f'HTTP Client Processor Passthrough Test Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title)
        pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
        sdc_executor.add_pipeline(pipeline)

        if exhausted_action == 'STAGE_ERROR' and http_status == 500:
            with pytest.raises(Exception) as exception:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if http_status == 200:
            expected_output = 1
            expected_error = 0
            expected_message = 0
        elif http_status == 404:
            if pass_record_other_status:
                expected_output = 1
                expected_error = 0
                expected_message = 0
            else:
                expected_output = 0
                expected_error = 1
                expected_error_code = 'HTTP_01'
                expected_message = 0
        elif http_status == 500:
            if exhausted_action == 'STAGE_ERROR':
                expected_output = 0
                expected_error = 0
                expected_message = 0
            else:
                if pass_record:
                    expected_output = 1
                    expected_error = 0
                    expected_message = 1
                else:
                    expected_output = 0
                    expected_error = 1
                    expected_error_code = 'HTTP_101'
                    expected_message = 0

        try:
            pipeline_metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
            error_metric = f'stage.{http_client_processor.instance_name}.stageErrors.counter'
            error_counter = pipeline_metrics.counter(error_metric).count
        except:
            logger.warning('Error reading metrics...')
            error_counter = 0

        logger.info(
            f'Finishing test: {http_status} - {exhausted_action} - {pass_record} - {pass_record_other_status} - '
            f'{expected_output} vs {len(wiretap.output_records)} - '
            f'{expected_error} vs {len(wiretap.error_records)} - '
            f'{expected_message} vs {error_counter}')

        assert len(wiretap.output_records) == expected_output, 'Unexpected number of output records'
        assert len(wiretap.error_records) == expected_error, 'Unexpected number of error records'
        if (expected_error > 0):
            assert expected_error_code in wiretap.error_records[0].header['errorMessage']
        assert error_counter == expected_message, 'Unexpected number of stage errors'

        pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if exhausted_action == 'STAGE_ERROR' and http_status == 500:
            assert pipeline_status == 'RUN_ERROR'
        else:
            assert pipeline_status == 'FINISHED'

    finally:
        http_mock_server.delete_mock()


@sdc_min_version("4.0.0")
@http
@pytest.mark.parametrize('exhausted_action',
                         [
                             'RETRY_IMMEDIATELY',
                             'RETRY_LINEAR_BACKOFF',
                             'RETRY_EXPONENTIAL_BACKOFF',
                             'STAGE_ERROR',
                             'ERROR_RECORD'
                         ])
@pytest.mark.parametrize('pass_record',
                         [
                             True,
                             False
                         ])
@pytest.mark.parametrize('pass_record_other_status',
                         [
                             True,
                             False
                         ])
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_client_processor_alternating_status(sdc_builder,
                                                  sdc_executor,
                                                  http_client,
                                                  exhausted_action,
                                                  pass_record,
                                                  pass_record_other_status,
                                                  one_request_per_batch):
    """
        Test exhausted handling for HTTP Client Processor with alternating status.
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

    try:

        logger.info(f'Running test: {exhausted_action} - {pass_record} - {pass_record_other_status}')

        record_output_field = 'oteai'
        one_millisecond = 1000
        wait_seconds = 1
        retries = 2
        interval = 2000
        long_time = (one_millisecond * wait_seconds * (retries + 2)) * 100

        http_mock_server = http_client.mock()
        http_mock_path = get_random_string(string.ascii_letters, 10)
        http_mock_content = dict(kisei='Kobayashi Koichi', meijin='Ishida Yoshio', honinbo='Takemiya Masaki')
        http_mock_data = json.dumps(http_mock_content)

        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=404,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=404,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=404,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data,
                                                                   status=404,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)

        http_mock_url = f'{http_mock_server.pretend_url}/{http_mock_path}'

        resource_url = http_mock_url
        connect_timeout = long_time
        read_timeout = long_time
        maximum_request_time_in_sec = long_time
        batch_wait_time_in_ms = long_time

        pipeline_name = f'{exhausted_action} - {pass_record} - {pass_record_other_status}' \
                        f' - {get_random_string(string.ascii_letters, 10)}'
        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=http_mock_data,
                                                  stop_after_first_batch=True)

        http_client_processor = pipeline_builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             resource_url=resource_url,
                                             http_method='GET',
                                             default_request_content_type='application/json',
                                             request_data="${record:value('/honinbo')}",
                                             output_field=f'/{record_output_field}',
                                             connect_timeout=connect_timeout,
                                             read_timeout=read_timeout,
                                             maximum_request_time_in_sec=maximum_request_time_in_sec,
                                             batch_wait_time_in_ms=batch_wait_time_in_ms,
                                             action_for_timeout='STAGE_ERROR',
                                             records_for_remaining_statuses=pass_record_other_status,
                                             missing_values_behavior='SEND_TO_ERROR',
                                             **one_request_per_batch_option)
        http_client_processor.per_status_actions = [
            {
                'statusCode': 404,
                'action': exhausted_action,
                'backoffInterval': interval,
                'maxNumRetries': retries,
                'passRecord': pass_record
            },
            {
                'statusCode': 500,
                'action': exhausted_action,
                'backoffInterval': interval,
                'maxNumRetries': retries,
                'passRecord': pass_record
            }
        ]

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> http_client_processor >> wiretap.destination

        pipeline_title = f'HTTP Client Processor Passthrough Test Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title)
        pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
        sdc_executor.add_pipeline(pipeline)

        if exhausted_action == 'STAGE_ERROR':
            with pytest.raises(Exception) as exception:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if exhausted_action == 'STAGE_ERROR':
            expected_output = 0
            expected_error = 0
            expected_message = 0
        else:
            if pass_record:
                expected_output = 1
                expected_error = 0
                expected_message = 1
            else:
                expected_output = 0
                expected_error = 1
                expected_error_code = 'HTTP_101'
                expected_message = 0

        try:
            pipeline_metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
            error_metric = f'stage.{http_client_processor.instance_name}.stageErrors.counter'
            error_counter = pipeline_metrics.counter(error_metric).count
        except:
            logger.warning('Error reading metrics...')
            error_counter = 0

        logger.info(
            f'Finishing test: {exhausted_action} - {pass_record} - {pass_record_other_status} - '
            f'{expected_output} vs {len(wiretap.output_records)} - '
            f'{expected_error} vs {len(wiretap.error_records)} - '
            f'{expected_message} vs {error_counter}')

        assert len(wiretap.output_records) == expected_output, 'Unexpected number of output records'
        assert len(wiretap.error_records) == expected_error, 'Unexpected number of error records'
        if (expected_error > 0):
            assert expected_error_code in wiretap.error_records[0].header['errorMessage']
        assert error_counter == expected_message, 'Unexpected number of stage errors'

        pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if exhausted_action == 'STAGE_ERROR':
            assert pipeline_status == 'RUN_ERROR'
        else:
            assert pipeline_status == 'FINISHED'

    finally:

        http_mock_server.delete_mock()


@sdc_min_version("4.0.0")
@http
@pytest.mark.parametrize('exhausted_action',
                         [
                             'RETRY_IMMEDIATELY',
                             'RETRY_LINEAR_BACKOFF',
                             'RETRY_EXPONENTIAL_BACKOFF',
                             'STAGE_ERROR',
                             'ERROR_RECORD'
                         ])
@pytest.mark.parametrize('pass_record',
                         [
                             True,
                             False
                         ])
@pytest.mark.parametrize('pass_record_other_status',
                         [
                             True,
                             False
                         ])
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_client_processor_alternating_status_timeout(sdc_builder,
                                                          sdc_executor,
                                                          http_client,
                                                          exhausted_action,
                                                          pass_record,
                                                          pass_record_other_status,
                                                          one_request_per_batch):
    """
        Test exhausted handling for HTTP Client Processor with alternating status and timeout.
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

    try:

        logger.info(f'Running test: {exhausted_action} - {pass_record} - {pass_record_other_status}')

        record_output_field = 'oteai'
        one_millisecond = 1000
        wait_seconds_ok = 1
        wait_seconds_ko = 10
        retries = 2
        interval = 2000
        short_time = 5000
        long_time = (one_millisecond * wait_seconds_ko * (retries + 2)) * 300

        http_mock_server = http_client.mock()
        http_mock_path = get_random_string(string.ascii_letters, 10)
        http_mock_content = dict(kisei='Kobayashi Koichi', meijin='Ishida Yoshio', honinbo='Takemiya Masaki')
        http_mock_data = json.dumps(http_mock_content)

        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds_ok,
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds_ko,
                                                                   body=http_mock_data,
                                                                   status=200,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds_ko,
                                                                   body=http_mock_data,
                                                                   status=200,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds_ok,
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds_ko,
                                                                   body=http_mock_data,
                                                                   status=200,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds_ok,
                                                                   body=http_mock_data,
                                                                   status=500,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds_ko,
                                                                   body=http_mock_data,
                                                                   status=200,
                                                                   headers={'Content-Type': 'application/json'},
                                                                   times=1)

        http_mock_url = f'{http_mock_server.pretend_url}/{http_mock_path}'

        resource_url = http_mock_url
        connect_timeout = long_time
        read_timeout = short_time
        maximum_request_time_in_sec = long_time
        batch_wait_time_in_ms = long_time

        pipeline_name = f'{exhausted_action} - {pass_record} - {pass_record_other_status}' \
                        f' - {get_random_string(string.ascii_letters, 10)}'
        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=http_mock_data,
                                                  stop_after_first_batch=True)

        http_client_processor = pipeline_builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             resource_url=resource_url,
                                             http_method='GET',
                                             default_request_content_type='application/json',
                                             request_data="${record:value('/honinbo')}",
                                             output_field=f'/{record_output_field}',
                                             connect_timeout=connect_timeout,
                                             read_timeout=read_timeout,
                                             maximum_request_time_in_sec=maximum_request_time_in_sec,
                                             batch_wait_time_in_ms=batch_wait_time_in_ms,
                                             base_backoff_interval_in_ms=interval,
                                             max_retries=retries,
                                             pass_record=pass_record,
                                             action_for_timeout='RETRY_IMMEDIATELY',
                                             records_for_remaining_statuses=pass_record_other_status,
                                             missing_values_behavior='SEND_TO_ERROR',
                                             **one_request_per_batch_option)
        http_client_processor.per_status_actions = [
            {
                'statusCode': 500,
                'action': exhausted_action,
                'backoffInterval': interval,
                'maxNumRetries': retries,
                'passRecord': pass_record
            },
            {
                'statusCode': 404,
                'action': exhausted_action,
                'backoffInterval': interval,
                'maxNumRetries': retries,
                'passRecord': pass_record
            }
        ]

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> http_client_processor >> wiretap.destination

        pipeline_title = f'HTTP Client Processor Passthrough Test Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title)
        pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
        sdc_executor.add_pipeline(pipeline)

        if exhausted_action == 'STAGE_ERROR':
            with pytest.raises(Exception) as exception:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if exhausted_action == 'STAGE_ERROR':
            expected_output = 0
            expected_error = 0
            expected_message = 0
        else:
            if pass_record:
                expected_output = 1
                expected_error = 0
                expected_message = 1
            else:
                expected_output = 0
                expected_error = 1
                expected_message = 0
                if exhausted_action == 'ERROR_RECORD':
                    expected_error_code = 'HTTP_101'
                else:
                    expected_error_code = 'HTTP_102'

        try:
            pipeline_metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
            error_metric = f'stage.{http_client_processor.instance_name}.stageErrors.counter'
            error_counter = pipeline_metrics.counter(error_metric).count
        except:
            logger.warning('Error reading metrics...')
            error_counter = 0

        logger.info(
            f'Finishing test: {exhausted_action} - {pass_record} - {pass_record_other_status} - '
            f'{expected_output} vs {len(wiretap.output_records)} - '
            f'{expected_error} vs {len(wiretap.error_records)} - '
            f'{expected_message} vs {error_counter}')

        assert len(wiretap.output_records) == expected_output, 'Unexpected number of output records'
        assert len(wiretap.error_records) == expected_error, 'Unexpected number of error records'
        if (expected_error > 0):
            assert expected_error_code in wiretap.error_records[0].header['errorMessage']
        assert error_counter == expected_message, 'Unexpected number of stage errors'

        pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if exhausted_action == 'STAGE_ERROR':
            assert pipeline_status == 'RUN_ERROR'
        else:
            assert pipeline_status == 'FINISHED'

    finally:

        http_mock_server.delete_mock()


@http
@pytest.mark.parametrize('pagination_mode',
                         [
                             'BY_PAGE',
                             'BY_OFFSET',
                             'LINK_HEADER',
                             'LINK_FIELD'
                         ])
@pytest.mark.parametrize('pagination_end_mode',
                         [
                             'empty',
                             'void',
                             'vacuum',
                             'unexisting',
                             'nothing',
                             'null'
                         ])
@pytest.mark.parametrize('stop_condition',
                         [
                             'value',
                             'existence'
                         ])
@sdc_min_version("4.0.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_pagination_with_empty_response(sdc_builder,
                                                       sdc_executor,
                                                       http_client,
                                                       pagination_mode,
                                                       pagination_end_mode,
                                                       stop_condition,
                                                       one_request_per_batch):
    """
        Test when a pagination option is set up and last page is empty.
    """
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

    try:

        logger.info(f'Running test: {pagination_mode} - {pagination_end_mode} - {stop_condition}')

        record_output_field = 'oteai'
        one_millisecond = 1000
        wait_seconds = 1
        retries = 10
        interval = 2000
        long_time = (one_millisecond * wait_seconds * (retries + 2)) * 300

        if stop_condition == 'value':
            condition = '${record:value(\'/current_page\') == 4}'
        else:
            condition = '${!record:exists(\'/current_page\')}'

        http_mock_content = dict(type='tournaments', mode='verbose')
        http_mock_data = json.dumps(http_mock_content)

        http_mock_server = http_client.mock()
        http_mock_path = get_random_string(string.ascii_letters, 10)
        http_mock_url = f'{http_mock_server.pretend_url}/{http_mock_path}?page=${{startAt}}&offset=${{startAt}}'
        http_mock_simple_url = f'{http_mock_server.pretend_url}/{http_mock_path}?page=1&offset=1'
        http_mock_content_01 = \
            {
                'tournaments':
                [
                    {'title': 'Kisei',   'player': 'Kobayashi Koichi'},
                    {'title': 'Meijin',  'player': 'Ishida Yoshio'},
                    {'title': 'Honinbo', 'player': 'Takemiya Masaki'}
                ],
                'current_page': 1,
                'next_page': http_mock_simple_url
            }
        http_mock_content_02 = \
            {
                'tournaments':
                [
                    {'title': 'Judan',  'player': 'Otake Hideo'},
                    {'title': 'Tengen', 'player': 'Rin Kaiho'},
                    {'title': 'Gosei',  'player': 'Cho Chikun'}
                ],
                'current_page': 2,
                'next_page': http_mock_simple_url
            }

        http_mock_content_03 = \
            {
                'tournaments':
                [
                    {'title': 'Oza',     'player': 'Kato Masao'},
                    {'title': 'NHK Cup', 'player': 'Go Seigen'},
                    {'title': 'NEC Cup', 'player': 'Kitani Minoru'}
                ],
                'current_page': 3,
                'next_page': http_mock_simple_url
            }
        if pagination_mode == 'LINK_FIELD' and stop_condition == 'value':
            if pagination_end_mode == 'empty':
                http_mock_content_04 = \
                    {
                        'tournaments':
                        [
                        ],
                        'current_page': 4,
                        'next_page': http_mock_simple_url
                    }
                http_mock_data_04 = json.dumps(http_mock_content_04)
            elif pagination_end_mode == 'void':
                http_mock_data_04 = f'{{[], \'current_page\': 4, \'next_page\': \'{http_mock_simple_url}\'}}'
            elif pagination_end_mode == 'vacuum':
                http_mock_data_04 = f'[], \'current_page\': 4, \'next_page\': \'{http_mock_simple_url}\''
            elif pagination_end_mode == 'unexisting':
                http_mock_content_04 = \
                    {
                        'titles':
                            [
                                {'title': 'Ryusei', 'player': 'Takeo Kajiwara'},
                                {'title': 'Okage',  'player': 'Fujisawa Shuko'},
                                {'title': 'Okan',   'player': 'Sakata Eio'}
                            ],
                        'current_page': 4,
                        'next_page': http_mock_simple_url
                    }
                http_mock_data_04 = json.dumps(http_mock_content_04)
            elif pagination_end_mode == 'nothing':
                http_mock_content_04 = \
                    {
                        'current_page': 4,
                        'next_page': http_mock_simple_url
                    }
                http_mock_data_04 = json.dumps(http_mock_content_04)
            elif pagination_end_mode == 'null':
                http_mock_data_04 = f'\'current_page\': 4, \'next_page\': \'{http_mock_simple_url}\''
        else:
            if pagination_end_mode == 'empty':
                http_mock_content_04 = \
                    {
                        'tournaments':
                        [
                        ]
                    }
                http_mock_data_04 = json.dumps(http_mock_content_04)
            elif pagination_end_mode == 'void':
                http_mock_data_04 = '{[]}'
            elif pagination_end_mode == 'vacuum':
                http_mock_data_04 = '[]'
            elif pagination_end_mode == 'unexisting':
                http_mock_content_04 = \
                    {
                        'titles':
                            [
                                {'title': 'Ryusei', 'player': 'Takeo Kajiwara'},
                                {'title': 'Okage',  'player': 'Fujisawa Shuko'},
                                {'title': 'Okan',   'player': 'Sakata Eio'}
                            ]
                    }
                http_mock_data_04 = json.dumps(http_mock_content_04)
            elif pagination_end_mode == 'nothing':
                http_mock_content_04 = {}
                http_mock_data_04 = json.dumps(http_mock_content_04)
            elif pagination_end_mode == 'null':
                http_mock_data_04 = ''

        http_mock_data_01 = json.dumps(http_mock_content_01)
        http_mock_data_02 = json.dumps(http_mock_content_02)
        http_mock_data_03 = json.dumps(http_mock_content_03)

        header_content_type_value = f'application/json'
        header_link_value = f'<{http_mock_simple_url}>; rel=next'

        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_01,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value,
                                                                            'Link': header_link_value},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_02,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value,
                                                                            'Link': header_link_value},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_03,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value,
                                                                            'Link': header_link_value},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_04,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value},
                                                                   times=1)

        resource_url = http_mock_url
        connect_timeout = long_time
        read_timeout = long_time
        maximum_request_time_in_sec = long_time
        batch_wait_time_in_ms = long_time

        pipeline_name = f'{pagination_mode}' \
                        f' - {get_random_string(string.ascii_letters, 10)}'
        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=http_mock_data,
                                                  stop_after_first_batch=True)

        http_client_processor = pipeline_builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             resource_url=resource_url,
                                             http_method='GET',
                                             default_request_content_type='application/json',
                                             request_data='token',
                                             output_field=f'/{record_output_field}',
                                             connect_timeout=connect_timeout,
                                             read_timeout=read_timeout,
                                             maximum_request_time_in_sec=maximum_request_time_in_sec,
                                             batch_wait_time_in_ms=batch_wait_time_in_ms,
                                             base_backoff_interval_in_ms=interval,
                                             max_retries=retries,
                                             pass_record=False,
                                             action_for_timeout='RETRY_IMMEDIATELY',
                                             records_for_remaining_statuses=False,
                                             missing_values_behavior='SEND_TO_ERROR',
                                             pagination_mode=pagination_mode,
                                             result_field_path='/tournaments',
                                             multiple_values_behavior='ALL_AS_LIST',
                                             next_page_link_field='/next_page',
                                             stop_condition=f'{condition}',
                                             **one_request_per_batch_option)

        # Must do it like this because the attribute name has the '/' char
        setattr(http_client_processor, 'initial_page/offset', 1)

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> http_client_processor >> wiretap.destination

        pipeline_title = f'HTTP Client Processor Void Pagination Test Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title)
        pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if pagination_end_mode == 'void' or pagination_end_mode == 'null':
            expected_output = 0
            expected_error = 1
            expected_message = 0
            if pagination_end_mode == 'null' and (pagination_mode != 'LINK_FIELD' or
                                                  pagination_mode == 'LINK_FIELD' and stop_condition == 'existence'):
                expected_error_code = 'HTTP_34'
            else:
                expected_error_code = 'HTTP_00'
        elif pagination_end_mode == 'vacuum':
            if pagination_mode == 'LINK_FIELD' and stop_condition == 'value':
                expected_output = 0
                expected_error = 1
                expected_error_code = 'HTTP_00'
                expected_message = 0
            else:
                expected_output = 1
                expected_error = 0
                expected_message = 0
        else:
            expected_output = 1
            expected_error = 0
            expected_message = 0

        try:
            pipeline_metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
            error_metric = f'stage.{http_client_processor.instance_name}.stageErrors.counter'
            error_counter = pipeline_metrics.counter(error_metric).count
        except:
            logger.warning('Error reading metrics...')
            error_counter = 0

        logger.info(
            f'Finishing test: {pagination_mode} - {pagination_end_mode} - {stop_condition} - '
            f'{expected_output} vs {len(wiretap.output_records)} - '
            f'{expected_error} vs {len(wiretap.error_records)} - '
            f'{expected_message} vs {error_counter}')

        assert len(wiretap.output_records) == expected_output, 'Unexpected number of output records'
        assert len(wiretap.error_records) == expected_error, 'Unexpected number of error records'
        if (expected_error > 0):
            assert expected_error_code in wiretap.error_records[0].header['errorMessage']
        assert error_counter == expected_message, 'Unexpected number of stage errors'

        pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert pipeline_status == 'FINISHED'

    finally:

        http_mock_server.delete_mock()


@http
@sdc_min_version("4.2.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_metrics(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

    expected_data = json.dumps({'A': 1})
    mock_path = get_random_string(string.ascii_letters, 10)
    mock_wrong_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()
    method = 'GET'

    # Times:
    one_millisecond = 1000
    wait_seconds = 10
    short_time = 1
    long_time = (one_millisecond * wait_seconds)

    try:
        http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
        resource_url = f'{http_mock.pretend_url}/{mock_path}'
        timeout_time = long_time

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='TEXT', raw_data='dummy')
        origin.stop_after_first_batch = True

        processor = builder.add_stage('HTTP Client', type='processor')
        processor.set_attributes(data_format='JSON', http_method=method,
                                 resource_url=resource_url,
                                 read_timeout=timeout_time,
                                 output_field='/result',
                                 request_data="{'something': 'here'}",
                                 multiple_values_behavior='SPLIT_INTO_MULTIPLE_RECORDS',
                                 **one_request_per_batch_option)

        wiretap = builder.add_wiretap()

        origin >> processor >> wiretap.destination
        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        metrics = _get_metrics(history, 'correct')

        records = wiretap.output_records
        assert len(records) == 1
        # The mock server won't return body on HEAD (rightfully so), but we can still send body to it though
        assert records[0].field['result'] == {'A': 1}
        # Finally, check that only one request has been made
        assert len(http_mock.get_request()) == 1

        # Right correlation between mean time for every step of process
        assert metrics['records_processed_mean'] >= metrics['success_requests_mean']
        assert metrics['success_requests_mean'] >= metrics['requests_mean']

        # Same amount of records processed than successful request
        assert metrics['records_processed_count'] <= metrics['success_requests_count']
        assert metrics['requests_count'] == metrics['success_requests_count']
        # Same amount of status response OK (200) than successful request
        assert metrics['status']['200'] == metrics['success_requests_count']

    finally:
        http_mock.delete_mock()


@http
@pytest.mark.parametrize('run_mode',
                         [
                             'timeout_error',
                             'status_error'
                         ])
@sdc_min_version("4.2.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_metrics_errors(sdc_builder, sdc_executor, http_client, run_mode, one_request_per_batch):
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}


    for i in range(3):
        expected_data = json.dumps({'A': 1})
        mock_path = get_random_string(string.ascii_letters, 10)
        mock_wrong_path = get_random_string(string.ascii_letters, 10)
        http_mock = http_client.mock()
        method = 'GET'

        # Times:
        one_millisecond = 1000
        wait_seconds = 10
        short_time = 1
        long_time = (one_millisecond * wait_seconds)

        try:
            if run_mode == 'timeout_error':
                http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
                resource_url = f'{http_mock.pretend_url}/{mock_path}'
                timeout_time = short_time
            elif run_mode == 'status_error':
                http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
                resource_url = f'{http_mock.pretend_url}/{mock_wrong_path}'
                timeout_time = long_time

            builder = sdc_builder.get_pipeline_builder()

            origin = builder.add_stage('Dev Raw Data Source')
            origin.set_attributes(data_format='TEXT', raw_data='dummy')
            origin.stop_after_first_batch = True

            processor = builder.add_stage('HTTP Client', type='processor')
            processor.set_attributes(data_format='JSON', http_method=method,
                                     resource_url=resource_url,
                                     read_timeout=timeout_time,
                                     output_field='/result',
                                     request_data="{'something': 'here'}",
                                     multiple_values_behavior='SPLIT_INTO_MULTIPLE_RECORDS',
                                     **one_request_per_batch_option)

            wiretap = builder.add_wiretap()

            origin >> processor >> wiretap.destination
            pipeline = builder.build()
            sdc_executor.add_pipeline(pipeline)

            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            raise Exception('The pipeline should have failed')

        except Exception as e:
            history = sdc_executor.get_pipeline_history(pipeline)
            try:
                metrics = _get_metrics(history, run_mode)
                if run_mode == 'timeout_error':
                    # Same amount of timeout's than retries
                    assert metrics['errors']['Timeout Read'] >= metrics['retries']['Retries for timeout']
                elif run_mode == 'status_error':
                    # Same amount of status errors than 404 status
                    assert metrics['status']['404'] == metrics['errors']['Http status']
                else:
                    raise Exception

                break
            except Exception:
                print('Retry the test...')
        finally:
            http_mock.delete_mock()


@http
@sdc_min_version("4.2.0")
@pytest.mark.parametrize("one_request_per_batch", [True, False])
def test_http_processor_pagination_metrics(sdc_builder, sdc_executor, http_client, one_request_per_batch):
    one_request_per_batch_option = {}
    if Version(sdc_builder.version) < Version("4.4.0"):
        if one_request_per_batch:
            pytest.skip("Test skipped because oneRequestPerBatch option is only available from SDC 4.4.0 version")
    else:
        one_request_per_batch_option = {"one_request_per_batch": one_request_per_batch, "request_data_format": "TEXT"}

    pagination_mode='BY_PAGE'

    try:
        record_output_field = 'oteai'
        one_millisecond = 1000
        wait_seconds = 1
        retries = 10
        interval = 2000
        no_time = 0
        short_time = 5000
        long_time = (one_millisecond * wait_seconds * (retries + 2)) * 300

        condition = '${record:value(\'/current_page\') == 4}'

        http_mock_content = dict(type='tournaments', mode='verbose')
        http_mock_data = json.dumps(http_mock_content)

        http_mock_server = http_client.mock()
        http_mock_path = get_random_string(string.ascii_letters, 10)
        http_mock_url = f'{http_mock_server.pretend_url}/{http_mock_path}?page=${{startAt}}&offset=${{startAt}}'
        http_mock_simple_url = f'{http_mock_server.pretend_url}/{http_mock_path}?page=1&offset=1'
        http_mock_content_01 = \
            {
                'tournaments':
                    [
                        {'title': 'Kisei', 'player': 'Kobayashi Koichi'},
                        {'title': 'Meijin', 'player': 'Ishida Yoshio'},
                        {'title': 'Honinbo', 'player': 'Takemiya Masaki'}
                    ],
                'current_page': 1,
                'next_page': http_mock_simple_url
            }
        http_mock_content_02 = \
            {
                'tournaments':
                    [
                        {'title': 'Judan', 'player': 'Otake Hideo'},
                        {'title': 'Tengen', 'player': 'Rin Kaiho'},
                        {'title': 'Gosei', 'player': 'Cho Chikun'}
                    ],
                'current_page': 2,
                'next_page': http_mock_simple_url
            }

        http_mock_content_03 = \
            {
                'tournaments':
                    [
                        {'title': 'Oza', 'player': 'Kato Masao'},
                        {'title': 'NHK Cup', 'player': 'Go Seigen'},
                        {'title': 'NEC Cup', 'player': 'Kitani Minoru'}
                    ],
                'current_page': 3,
                'next_page': http_mock_simple_url
            }

        http_mock_content_04 = \
            {
                'tournaments':
                    [
                    ]
            }
        http_mock_data_04 = json.dumps(http_mock_content_04)

        http_mock_data_01 = json.dumps(http_mock_content_01)
        http_mock_data_02 = json.dumps(http_mock_content_02)
        http_mock_data_03 = json.dumps(http_mock_content_03)

        header_content_type_value = f'application/json'
        header_link_value = f'<{http_mock_simple_url}>; rel=next'

        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_01,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value,
                                                                            'Link': header_link_value},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_02,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value,
                                                                            'Link': header_link_value},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_03,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value,
                                                                            'Link': header_link_value},
                                                                   times=1)
        http_mock_server.when(rule=f'GET /{http_mock_path}').reply(after=wait_seconds,
                                                                   body=http_mock_data_04,
                                                                   status=200,
                                                                   headers={'Content-Type': header_content_type_value},
                                                                   times=1)

        resource_url = http_mock_url
        connect_timeout = long_time
        read_timeout = long_time
        maximum_request_time_in_sec = long_time
        batch_wait_time_in_ms = long_time

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source_origin = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source_origin.set_attributes(data_format='JSON',
                                                  raw_data=http_mock_data,
                                                  stop_after_first_batch=True)

        http_client_processor = pipeline_builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             resource_url=resource_url,
                                             http_method='GET',
                                             default_request_content_type='application/json',
                                             request_data='token',
                                             output_field=f'/{record_output_field}',
                                             connect_timeout=connect_timeout,
                                             read_timeout=read_timeout,
                                             maximum_request_time_in_sec=maximum_request_time_in_sec,
                                             batch_wait_time_in_ms=batch_wait_time_in_ms,
                                             base_backoff_interval_in_ms=interval,
                                             max_retries=retries,
                                             pass_record=False,
                                             action_for_timeout='RETRY_IMMEDIATELY',
                                             records_for_remaining_statuses=False,
                                             missing_values_behavior='SEND_TO_ERROR',
                                             pagination_mode=pagination_mode,
                                             result_field_path='/tournaments',
                                             multiple_values_behavior='ALL_AS_LIST',
                                             next_page_link_field='/next_page',
                                             stop_condition=f'{condition}',
                                             **one_request_per_batch_option)

        # Must do it like this because the attribute name has the '/' char
        setattr(http_client_processor, 'initial_page/offset', 1)

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source_origin >> http_client_processor >> wiretap.destination

        pipeline = pipeline_builder.build('Http Client Processor Metrics')
        pipeline.configuration['errorRecordPolicy'] = 'STAGE_RECORD'
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        metrics = _get_metrics(history, 'with_pagination')

        assert len(wiretap.output_records) == 1

        # Right correlation between mean time for every step of process
        assert metrics['records_processed_mean'] >= metrics['success_requests_mean']
        assert metrics['success_requests_mean'] >= metrics['requests_mean']

        # Same amount of records processed than successful request for each page (4)
        assert metrics['records_processed_count'] == metrics['success_requests_count']/4
        assert metrics['requests_count'] == metrics['success_requests_count']
        # Same amount of status response OK (200) than successful request
        assert metrics['status']['200'] == metrics['success_requests_count']

        # Same amount of successful request than pages processed
        assert metrics['initial_page'] + metrics['subsequent_pages'] == metrics['success_requests_count']

    finally:
        http_mock_server.delete_mock()


def _get_metrics(history, run_mode):
    # Timers
    record_processing_counter_from_metrics = history.latest.metrics.timer(
        'custom.HTTPClient_01.Record Processing.0.timer').count
    record_processing_timers_from_metrics = history.latest.metrics.timer(
        'custom.HTTPClient_01.Record Processing.0.timer')._data.get('mean')

    request_counter_from_metrics = history.latest.metrics.timer(
        'custom.HTTPClient_01.Request.0.timer').count
    request_timers_from_metrics = history.latest.metrics.timer(
        'custom.HTTPClient_01.Request.0.timer')._data.get('mean')

    request_successful_counters_from_metrics = history.latest.metrics.timer(
        'custom.HTTPClient_01.Successful Request.0.timer').count
    request_successful_timers_from_metrics = history.latest.metrics.timer(
        'custom.HTTPClient_01.Successful Request.0.timer')._data.get('mean')

    metrics = {'records_processed_count': record_processing_counter_from_metrics,
               'records_processed_mean': record_processing_timers_from_metrics,
               'requests_count': request_counter_from_metrics,
               'requests_mean': request_timers_from_metrics,
               'success_requests_count': request_successful_counters_from_metrics,
               'success_requests_mean': request_successful_timers_from_metrics}

    # Counters

    if run_mode == 'timeout_error':
        metrics['errors'] = history.latest.metrics.gauge(
            'custom.HTTPClient_01.Communication Errors.0.gauge').value
        try:
            metrics['retries'] = history.latest.metrics.gauge(
                'custom.HTTPClient_01.Retries.0.gauge').value
        except:
            logger.info('No retry option')
    elif run_mode == 'status_error':
        metrics['errors'] = history.latest.metrics.gauge(
            'custom.HTTPClient_01.Communication Errors.0.gauge').value
        metrics['status'] = history.latest.metrics.gauge(
            'custom.HTTPClient_01.Http Status.0.gauge').value
    else:
        metrics['status'] = history.latest.metrics.gauge(
            'custom.HTTPClient_01.Http Status.0.gauge').value

    if run_mode == 'with_pagination':
        metrics['initial_page'] = history.latest.metrics.timer(
            'custom.HTTPClient_01.Initial Page Resolution.0.timer').count
        metrics['subsequent_pages'] = history.latest.metrics.timer(
            'custom.HTTPClient_01.Subsequent Pages Resolution.0.timer').count

    return metrics


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_json(sdc_builder, sdc_executor, http_client):
    """ Test that the batch is sent correctly and the response record is generated properly,
    when the singleRequestPerBatch is set to true. """

    expected_response = {"mocked_response": "ok"}
    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(json.dumps(expected_response), times=FOREVER)

    try:
        record_output_field = 'result'
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}')

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure when there is a single request per batch and it works fine, only one response record is generated
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field[record_output_field] == expected_response

        # Ensure the request was done with the entire batch
        assert len(http_mock.get_request()) == 1

        assert json.loads(http_mock.get_request(0).body.decode("utf-8")) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_multipage(sdc_builder, sdc_executor, http_client):
    """ Test that when the singleRequestPerBatch is set to true and the stage should navigate through multiple pages,
    it does correctly and send the whole same batch for every page. """

    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)

    http_mock.when(f'POST /{mock_path}\\?p=[0-1]').reply(
        headers={'Content-Type': 'application/json'},
        body=json.dumps({"result": [{"status": "ok"}]}),
        times=FOREVER)
    http_mock.when(f'POST /{mock_path}\\?p=2').reply(
        headers={'Content-Type': 'application/json'},
        body=json.dumps({"result": []}),
        times=FOREVER)

    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        resource_url = f"{mock_uri}?p=${{startAt}}"
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             pagination_mode="BY_PAGE",
                                             result_field_path="/result",  # pagination result field path
                                             multiple_values_behavior="SPLIT_INTO_MULTIPLE_RECORDS",
                                             resource_url=resource_url,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}')
        setattr(http_client_processor, 'initial_page/offset', 0)

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # There is one response per page, so two records because we set the configuration to split results into
        # multiple records.
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field[record_output_field] == {"status": "ok"}

        # Should be in total 3 request. The last response is void and so it stops paginating
        assert len(http_mock.get_request()) == 3

        # All requests should be with the whole batch even the last one that does not provide any more data
        for i in range(3):
            assert json.loads(http_mock.get_request(i).body.decode("utf-8")) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_action_retry(sdc_builder, sdc_executor, http_client):
    """ Test the stage produce the output record properly when there is a retry action and the first request fails and
    the singleRequestPerBatch is true. """

    expected_response = {"mocked_response": "ok"}
    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=500,
                                               body=json.dumps(expected_response),
                                               headers={'Content-Type': 'application/json'},
                                               times=1)
    http_mock.when(f'POST /{mock_path}').reply(status=200,
                                               body=json.dumps(expected_response),
                                               headers={'Content-Type': 'application/json'},
                                               times=1)

    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}',
                                             per_status_actions=[
                                                 {
                                                     "statusCode": 500,
                                                     "action": "RETRY_IMMEDIATELY",
                                                     "backoffInterval": 1,
                                                     "passRecord": False,
                                                     "maxNumRetries": 1
                                                 }
                                             ])

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure both requests was done with the entire batch
        assert len(http_mock.get_request()) == 2
        for i in range(2):
            assert json.loads(http_mock.get_request(i).body.decode("utf-8")) == raw_data

        # Ensure when there is a single request per batch and it works fine, only one response record is generated
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field[record_output_field] == expected_response

    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_response_204(sdc_builder, sdc_executor, http_client):
    """ Test that when there is a 204 response and the response body is null and the singleRequestPerBatch is true, the
    output record contains all the input records data as a string analogously with what happens when the
    singleRequestPerBatch option is set to false. """

    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=204,
                                               body="",
                                               times=FOREVER)
    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}')

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # When there is a 204 the response body can be void, in this case the stage returns all the records
        # data in the same response record as a list of fields.
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field[record_output_field] == raw_data

        # Ensure the request was done with the entire batch
        assert len(http_mock.get_request()) == 1
        assert json.loads(http_mock.get_request(0).body.decode("utf-8")) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_action_passthrough(sdc_builder, sdc_executor, http_client):
    """ Test that the records in the batch are sent correctly to the output when there is a retry action and it fails in
    all its retries and passRecord is set to true and the singleRequestPerBatch is set to true. """

    expected_response = {"mocked_response": "ok"}
    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=500,
                                               body=json.dumps(expected_response),
                                               headers={'Content-Type': 'application/json'},
                                               times=FOREVER)

    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}',
                                             per_status_actions=[
                                                 {
                                                     "statusCode": 500,
                                                     "action": "RETRY_IMMEDIATELY",
                                                     "backoffInterval": 1,
                                                     "passRecord": True,
                                                     "maxNumRetries": 1
                                                 }
                                             ])

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure both requests was done with the entire batch
        assert len(http_mock.get_request()) == 2
        for i in range(2):
            assert json.loads(http_mock.get_request(i).body.decode("utf-8")) == raw_data

        # Both requests failed but the records where passed to the next stage so that there are no error records
        assert len(wiretap.error_records) == 0

        # Ensure that those records were passed to the wiretap
        assert len(wiretap.output_records) == len(raw_data)
        assert wiretap.output_records[0].field == raw_data[0]
        assert wiretap.output_records[1].field == raw_data[1]
        assert wiretap.output_records[2].field == raw_data[2]
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_missing_values_behavior_to_error(sdc_builder, sdc_executor, http_client):
    """ Test that all records in the batch are sent to error when it is configured to do so on missing values on the
    response """
    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=200,
                                               body=json.dumps([]),
                                               headers={'Content-Type': 'application/json'},
                                               times=FOREVER)

    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}',
                                             missing_values_behavior="SEND_TO_ERROR")

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure all records in the batch are sent to error
        assert len(wiretap.output_records) == 0
        assert len(wiretap.error_records) == len(raw_data)
        assert wiretap.error_records[0].field == raw_data[0]
        assert wiretap.error_records[1].field == raw_data[1]
        assert wiretap.error_records[2].field == raw_data[2]

        # Ensure the request was done with the entire batch
        assert len(http_mock.get_request()) == 1
        assert json.loads(http_mock.get_request(0).body.decode("utf-8")) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_missing_values_behavior_passthrough(sdc_builder, sdc_executor, http_client):
    """ Test that all records in the batch are sent to the next stage when it is configured to do so on missing
    values on the response """
    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=200,
                                               body=json.dumps([]),
                                               headers={'Content-Type': 'application/json'},
                                               times=FOREVER)

    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}',
                                             missing_values_behavior="PASS_RECORD_ON")

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure all records in the batch are sent to the next stage instead of to the error
        assert len(wiretap.error_records) == 0
        assert len(wiretap.output_records) == len(raw_data)
        assert wiretap.output_records[0].field == raw_data[0]
        assert wiretap.output_records[1].field == raw_data[1]
        assert wiretap.output_records[2].field == raw_data[2]

        # Ensure the request was done with the entire batch
        assert len(http_mock.get_request()) == 1
        assert json.loads(http_mock.get_request(0).body.decode("utf-8")) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_error_passthrough(sdc_builder, sdc_executor, http_client):
    """ Test that all records are sent to the next stage when the status code of the response indicates that has been
     an error and there are no actions that handle it and the records_for_remaining_statuses is set to true"""
    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=500,
                                               body="There is an error",
                                               headers={'Content-Type': 'application/json'},
                                               times=1)

    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}',
                                             records_for_remaining_statuses=True,
                                             per_status_actions=[])

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure all records in the batch are sent to the next stage instead of to the error
        assert len(wiretap.error_records) == 0
        assert len(wiretap.output_records) == len(raw_data)
        assert wiretap.output_records[0].field == {**raw_data[0], "result": {"outErrorBody": "There is an error"}}
        assert wiretap.output_records[1].field == {**raw_data[1], "result": {"outErrorBody": "There is an error"}}
        assert wiretap.output_records[2].field == {**raw_data[2], "result": {"outErrorBody": "There is an error"}}

        # TODO I would like to check out the headers. e.g. the 'HTTP-Status' one.

        # Ensure the request was done with the entire batch
        assert len(http_mock.get_request()) == 1
        assert json.loads(http_mock.get_request(0).body.decode("utf-8")) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
def test_http_post_batch_error(sdc_builder, sdc_executor, http_client):
    """ Test that all records are sent to error when the status code of the response indicates that has been
     an error and there are no actions that handle it """
    record_output_field = 'result'

    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=500,
                                               body="There is an error",
                                               headers={'Content-Type': 'application/json'},
                                               times=1)

    try:
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'
        raw_data = [
            {"a": "dummy1"},
            {"b": "dummy1"},
            {"c": "dummy1"},
        ]

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             request_data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             one_request_per_batch=True,
                                             resource_url=mock_uri,
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field=f'/{record_output_field}',
                                             records_for_remaining_statuses=False,
                                             per_status_actions=[])

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure all records in the batch are sent to the next stage instead of to the error
        assert len(wiretap.output_records) == 0
        assert len(wiretap.error_records) == len(raw_data)
        assert wiretap.error_records[0].field == raw_data[0]
        assert wiretap.error_records[1].field == raw_data[1]
        assert wiretap.error_records[2].field == raw_data[2]

        # Ensure the request was done with the entire batch
        assert len(http_mock.get_request()) == 1
        assert json.loads(http_mock.get_request(0).body.decode("utf-8")) == raw_data
    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("4.4.0")
@pytest.mark.parametrize("max_num_retries, total_number_requests", [(1, 2), (10, 11)])
def test_action_max_retries(sdc_builder, sdc_executor, http_client, max_num_retries, total_number_requests):
    """ Test that the number of retries on error is at most, the maxRetriesCount """
    http_mock = http_client.mock()
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock.when(f'POST /{mock_path}').reply(status=500,
                                               body='{"error": 500}',
                                               headers={'Content-Type': 'application/json'},
                                               times=FOREVER)

    try:
        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps([{"a": "dummy1"}]),
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON',
                                             json_content='ARRAY_OBJECTS',
                                             http_method='POST',
                                             resource_url=f'{http_mock.pretend_url}/{mock_path}',
                                             headers=[{'key': 'Content-Type', 'value': 'application/json'}],
                                             output_field='/result',
                                             per_status_actions=[
                                                 {
                                                     "statusCode": 500,
                                                     "action": "RETRY_IMMEDIATELY",
                                                     "backoffInterval": 1,
                                                     "passRecord": True,
                                                     "maxNumRetries": max_num_retries
                                                 }
                                             ])

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Ensure the number of requests done
        assert len(http_mock.get_request()) == total_number_requests
    finally:
        http_mock.delete_mock()

@http
@sdc_min_version("3.11.0")
def test_http_processor_overwrite_field(sdc_builder, sdc_executor, http_client):
    """Test HTTP Lookup Processor for HTTP GET method that check out whether an existing record field is
    overwritten if specified as output field. It has no sense to test this with the one_request_per_batch
    option set since no input record is propagated.

        dev_raw_data_source >> http_client_processor >> wiretap
    """

    # The data returned by the HTTP mock server
    data_array = {'A': 1, 'C': 2, 'G': 3, 'T': 4}

    expected_data = json.dumps(data_array)
    record_output_field = 'result'
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data='{"field": "value", "result": "value"}',
                                           stop_after_first_batch=True)
        http_client_processor = builder.add_stage('HTTP Client', type='processor')
        http_client_processor.set_attributes(data_format='JSON', http_method='GET',
                                             resource_url=mock_uri,
                                             output_field=f'/{record_output_field}')

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination
        pipeline = builder.build(title='HTTP Lookup GET Processor Split Multiple Records pipeline')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # ensure HTTP GET result has 10 different records
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field == {"field": "value", "result": data_array}
    finally:
        http_mock.delete_mock()
@http
@sdc_min_version("5.0.0")
def test_http_processor_pagination_next_page_link_prefix_el(sdc_builder, sdc_executor, http_client):
    """
       Test Next Page Link Prefix EL record:value for Http Processor pagination mode: Link in Response Field.

       We use the pipeline:
       dev_raw_data_source >> http_client_processor >> wiretap

       Test for COLLECTOR-898.
       """

    record_output_field = 'results'

    number_records = 5
    number_pages = 5

    # Id values to be evaluate by the El Expression
    id_value = "\n".join([f'{{"id_value":"{i}"}}' for i in range(number_records)])

    http_mock = http_client.mock()
    http_mock_path = get_random_string(string.ascii_letters, 10)
    http_mock_url = f'{http_mock.pretend_url}/{http_mock_path}'
    http_mock_url_simple = http_mock.pretend_url

    http_mock_content_01 = \
        {
            'bands':
                [
                    {'band': 'Death', 'singer': 'Chuck Schuldiner'},
                    {'band': 'Morbid Angel', 'singer': 'Trey Azagthoth'},
                    {'band': 'Possessed', 'singer': 'Jeff Becerra'}
                ],
            'current_page': 1,
            'next_page': http_mock_path
        }

    http_initial_content = json.dumps(http_mock_content_01)

    try:

        http_mock.when(rule=f'GET /{http_mock_path}').reply(body=http_initial_content,
                                                            status=200,
                                                            headers={'Content-Type': 'application/json'},
                                                            times=FOREVER)

        total_elements = []
        record_fields = []

        for i in range(number_records):
            # Clear elements each time new records come in to match with the record output
            record_fields.clear()
            for x in range(number_pages):
                http_data = {'bands': [{'band': F'Death_{x}', 'singer': 'Chuck Schuldiner'},
                                       {'band': F'Morbid Angel_{x}', 'singer': 'Trey Azagthoth;'},
                                       {'band': F'Possessed_{x}', 'singer': 'Jeff Becerra'}],
                             'current_page': x + 2,
                             'next_page': http_mock_path}
                if x == number_pages - 1:
                    http_data = {'bands': [{'band': F'Death_{x}', 'singer': 'Chuck Schuldiner'},
                                           {'band': F'Morbid Angel_{x}', 'singer': 'Trey Azagthoth;'},
                                           {'band': F'Possessed_{x}', 'singer': 'Jeff Becerra'}],
                                 'current_page': x + 2,
                                 'next_page': 'null'}

                # Concatenate record fields between pages
                record_fields += http_data['bands']
                http_data_pagination = json.dumps(http_data)
                http_mock.when(F'GET /to_id={i}-after={http_mock_path}').reply(body=http_data_pagination,
                                                                               headers={
                                                                                   'Content-Type': 'application/json'},
                                                                               times=1)
            # Add the initial records to the total of elements
            total_elements.append(http_mock_content_01['bands'] + record_fields)

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')

        dev_raw_data_source.set_attributes(data_format='JSON',
                                           raw_data=f'{id_value}',
                                           stop_after_first_batch=True)

        http_client_processor = builder.add_stage('HTTP Client', type='processor')

        http_client_processor.set_attributes(data_format='JSON',
                                             http_method='GET',
                                             resource_url=http_mock_url,
                                             output_field=f'/{record_output_field}',
                                             default_request_content_type='application/json',
                                             missing_values_behavior='SEND_TO_ERROR',
                                             multiple_values_behavior='ALL_AS_LIST',
                                             pagination_mode='LINK_FIELD',
                                             next_page_link_prefix=http_mock_url_simple + "/to_id=${record:value('/id_value')}-after=",
                                             next_page_link_field="/next_page",
                                             stop_condition="${record:value('/next_page') == 'null' }",
                                             result_field_path="/bands",
                                             keep_all_fields=True)

        print(http_client_processor.next_page_link_prefix)

        wiretap = builder.add_wiretap()

        dev_raw_data_source >> http_client_processor >> wiretap.destination

        pipeline = builder.build(title='HTTP Processor EL in LinkFieldPrefix')

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = []
        # Assert for the content generated by the initial content
        for i in range(number_records):
            for j in http_mock_content_01['bands']:
                assert j in wiretap.output_records[i].field['results'][0]['bands']
        # Assert contents generated by the pagination
        for i in wiretap.output_records:
            output_records.append(i.field['results'][0]['bands'])
        assert total_elements == output_records

    finally:
        http_mock.delete_mock()


@http
@sdc_min_version("5.1.0")
def test_http_processor_json_list_root_with_pagination(sdc_builder, sdc_executor, http_client):
    """
    Test the HTTP Processor stage with pagination by offset enabled and the root of the response
    JSON list as the result field path. The pipeline looks like:

    dev_data_generator >> http_client_processor >> [wiretap.destination, trash_destination]

    Test for ESC-1629.
    """

    # The mock data to be returned by the HTTP mock server
    mock_data_1 = [{"field1": "value1"}, {"field2": "value2"}, {"field3": "value3"}]  # first request
    mock_data_2 = [{"field4": "value4"}, {"field5": "value5"}, {"field6": "value6"}]  # second request
    # Configuration parameters
    long_time = 10_000_000
    per_page = 3
    record_output_field = "/out"

    # Create and configure the HTTP mock server
    http_mock_server = http_client.mock()
    http_mock_path = get_random_string(string.ascii_letters, 10)
    http_mock_url = f"{http_mock_server.pretend_url}/{http_mock_path}?page=${{startAt}}&per_page={per_page}"
    # First and second responses will return an actual value, the third will return an empty body
    http_mock_server.when(f"GET /{http_mock_path}").reply(status=200, body=json.dumps(mock_data_1), times=1)
    http_mock_server.when(f"GET /{http_mock_path}").reply(status=200, body=json.dumps(mock_data_2), times=1)
    http_mock_server.when(f"GET /{http_mock_path}").reply(status=200, body="{}", times=1)

    # Test expectations
    expected_record_number = 6
    expected_data = mock_data_1 + mock_data_2
    expected_urls = [f"/{http_mock_path}?page={x}&per_page={per_page}" for x in (1, 1 + per_page, 1 + per_page * 2)]

    # Build pipeline stages
    pipeline_builder = sdc_builder.get_pipeline_builder()
    # Data generator
    dev_data_generator = pipeline_builder.add_stage("Dev Data Generator")
    dev_data_generator.set_attributes(
        batch_size=1, root_field_type="MAP", records_to_be_generated=0, fields_to_generate=[]
    )
    # HTTP Processor
    http_client_processor = pipeline_builder.add_stage("HTTP Client", type="processor")
    http_client_processor.set_attributes(
        # HTTP
        resource_url=http_mock_url,
        output_field=record_output_field,
        missing_values_behavior="PASS_RECORD_ON",
        multiple_values_behavior="SPLIT_INTO_MULTIPLE_RECORDS",
        http_method="GET",
        read_timeout=long_time,
        one_request_per_batch=False,
        per_status_actions=[
            {
                "statusCode": 404,
                "action": "RETRY_EXPONENTIAL_BACKOFF",
                "backoffInterval": 1000,
                "maxNumRetries": 10,
                "passRecord": False,
            }
        ],
        connect_timeout=long_time,
        maximum_request_time_in_sec=long_time,
        batch_wait_time_in_ms=long_time,
        # Pagination
        pagination_mode="BY_OFFSET",
        result_field_path="/",
        wait_time_between_pages_in_ms=500,
        keep_all_fields=True,
        # Data Format
        data_format="JSON",
        ignore_control_characters=True,
    )
    setattr(http_client_processor, "initial_page/offset", 1)
    # Wiretap and trash
    wiretap = pipeline_builder.add_wiretap()
    trash_destination = pipeline_builder.add_stage("Trash")

    # Connect the stages and build the pipeline
    dev_data_generator >> http_client_processor >> [wiretap.destination, trash_destination]
    pipeline = pipeline_builder.build(title="Http Processor JSON List Root With Pagination")

    # Run the pipeline
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline)
        # Wait for the expected number of records
        sdc_executor.wait_for_pipeline_metric(pipeline, "output_record_count", expected_record_number)
        sdc_executor.stop_pipeline(pipeline, force=True)

        output_records = wiretap.output_records
        # Assertion 1: the number of records is correct
        assert len(output_records) == expected_record_number, "Unexpected number of records"
        # Assertion 2: the content of the records is correct
        for i in range(expected_record_number):
            assert (
                output_records[i].field["out"] == expected_data[i]
            ), "Output records don't match expected records"
        # Assertion 3: the pagination was correct
        for i in range(len(expected_urls)):
            assert http_mock_server.get_request(i).url == expected_urls[i]

    finally:
        http_mock_server.delete_mock()
