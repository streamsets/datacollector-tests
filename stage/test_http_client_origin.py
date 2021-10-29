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
import shutil
import string
import tempfile
import time
import pytest

from pretenders.common.constants import FOREVER
from streamsets.testframework.constants import (CREDENTIAL_STORE_EXPRESSION, CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION,
                                                STF_TESTCONFIG_DIR)
from streamsets.testframework.credential_stores.jks import JKSCredentialStore
from streamsets.testframework.markers import http, sdc_min_version, spnego
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@http
@sdc_min_version("3.19.0")
def test_http_client_origin_keep_all_fields_not_repeating_records(sdc_builder, sdc_executor, http_client):
    """HTTP Client Origin using pagination with Keep All Fields config enabled writing on a LocalFS must
    not repeat records on the file obtained. This tests the issue on ESC-999 (SDC-15893)"""
    data_array = {'metadata': 'Example', 'next_page': None, 'data': [
        {'id': 0, 'name': "INDURAIN"}, {'id': 1, 'name': "PANTANI"}, {'id': 2, 'name': "ULRICH"}]}
    expected_data = json.dumps(data_array)
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
        wiretap = builder.add_wiretap()

        http_source >> [localfs, wiretap.destination]

        pipeline = builder.build(title='HTTP Client Origin Keep All Fields not repeating records when writing LocalFS')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Check the output on the wiretap
        records = wiretap.output_records
        assert len(records) == 3
        for i in range(3):
            assert records[i].field['metadata'] == 'Example'
            assert records[i].field['data']['id'] == i
            assert records[i].field['data']['name'] == data_array['data'][i]['name']

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

        wiretap_second = pipeline_builder.add_wiretap()

        directory >> wiretap_second.destination

        pipeline_directory = pipeline_builder.build(
            title='HTTP Client Origin Keep All Fields not repeating records when writing LocalFS (Read the file)')
        sdc_executor.add_pipeline(pipeline_directory)
        sdc_executor.start_pipeline(pipeline_directory)
        sdc_executor.wait_for_pipeline_metric(pipeline_directory, 'input_record_count', 3)
        sdc_executor.stop_pipeline(pipeline_directory)

        records = wiretap.output_records
        assert len(records) == 3
        for i in range(3):
            assert records[i].field['metadata'] == 'Example'
            assert records[i].field['data']['id'] == i
            assert records[i].field['data']['name'] == data_array['data'][i]['name']
    finally:
        http_mock.delete_mock()
        logger.info("Removing tmp folder: %s", tmp_directory)
        if os.path.exists(tmp_directory) and os.path.isdir(tmp_directory):
            shutil.rmtree(tmp_directory)


@http
@sdc_min_version("3.16.0")
def test_http_client_wrong_pagination_field(sdc_builder, sdc_executor, http_client):
    """HTTP Client Origin with some an invalid page link field must throw an StageException HTTP_66"""
    dataArr = {'Name': f'Example', 'data': [{'id': 2, 'foo': 2}]}

    expected_data = json.dumps(dataArr)
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    try:
        http_mock.when(f'GET /{mock_path}').reply(expected_data, times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        builder = sdc_builder.get_pipeline_builder()
        http_source = builder.add_stage('HTTP Client', type='origin')
        http_source.set_attributes(data_format='JSON',
                                   http_method='GET',
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
    data_array = {'Name': f'Example'}

    expected_data = json.dumps(data_array)
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
        wiretap = builder.add_wiretap()

        http_source >> wiretap.destination
        pipeline = builder.build(title='HTTP Client Origin propagates 404 record')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # ensure HTTP GET result has 1 records
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values['HTTP-Status'] == '404'

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
        wiretap = builder.add_wiretap()

        http_source >> wiretap.destination
        pipeline = builder.build(title='HTTP Client Origin HTTP-Status on header')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # ensure HTTP GET result has at least 1 record
        num_of_els = len(wiretap.output_records)
        assert num_of_els > 0
        # it has the HTTP-Status on header
        for x in range(num_of_els):
            assert 'HTTP-Status' in wiretap.output_records[x].header.values

    finally:
        http_mock.delete_mock()


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


@http
@pytest.mark.parametrize('run_mode',
                         [
                             'correct',
                             'timeout_error',
                             'status_error',
                             'with_pagination'
                         ])
@sdc_min_version("4.2.0")
def test_http_origin_metrics(sdc_builder, sdc_executor, http_client, run_mode):
    """ Test Metrics timers and gauge. Test the metrics in different type of Http Client origin configuration.
        The pipeline looks like:
        http_client_origin >> wiretap """
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
        if run_mode == 'correct':
            expected_data = json.dumps({'A': 1})
            pagination_mode = 'NONE'
            http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
            resource_url = f'{http_mock.pretend_url}/{mock_path}'
            timeout_time = long_time
        elif run_mode == 'timeout_error':
            expected_data = json.dumps({'A': 1})
            pagination_mode = 'NONE'
            http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
            resource_url = f'{http_mock.pretend_url}/{mock_path}'
            timeout_time = short_time
        elif run_mode == 'status_error':
            expected_data = json.dumps({'A': 1})
            pagination_mode = 'NONE'
            http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
            resource_url = f'{http_mock.pretend_url}/{mock_wrong_path}'
            timeout_time = long_time
        elif run_mode == 'with_pagination':
            expected_data = json.dumps({'metadata': 'Example', 'next_page': 2, 'data': [
                {'id': 0, 'name': "INDURAIN"}, {'id': 1, 'name': "PANTANI"}, {'id': 2, 'name': "ULRICH"}]})
            pagination_mode = 'LINK_FIELD'
            http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
            resource_url = f'{http_mock.pretend_url}/{mock_path}'
            timeout_time = long_time
        else:
            expected_data = json.dumps({'A': 1})
            pagination_mode = 'NONE'
            http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
            resource_url = f'{http_mock.pretend_url}/{mock_path}'
            timeout_time = long_time

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('HTTP Client', type='origin')
        origin.set_attributes(data_format='JSON', http_method=method,
                              resource_url=resource_url,
                              read_timeout=timeout_time,
                              request_body="{'something': 'here'}",
                              pagination_mode=pagination_mode,
                              next_page_link_field="/next_page",
                              stop_condition="${record:value('/next_page') == 2 }",
                              result_field_path="/data")

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination
        pipeline = builder.build('Http Client Origin Metrics')
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        time.sleep(2)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        if run_mode != 'with_pagination':
            assert records[0].field['A'] == 1
        else:
            assert records == expected_data

        history = sdc_executor.get_pipeline_history(pipeline)
        metrics = _get_metrics(history, run_mode)

        if run_mode == 'correct':
            # Right correlation between mean time for every step of process
            assert metrics['records_processed_mean'] >= metrics['success_requests_mean']
            assert metrics['success_requests_mean'] >= metrics['requests_mean']

            # Same amount of records processed than successful request
            assert metrics['records_processed_count'] <= metrics['success_requests_count']
            assert metrics['requests_count'] == metrics['success_requests_count']
            # Same amount of status response OK (200) than successful request
            assert metrics['status']['200'] == metrics['success_requests_count']
        elif run_mode == 'with_pagination':
            # Same amount of status response OK (200) than successful request
            assert metrics['status']['200'] == metrics['success_requests_count']
            # Same amount of successful request than records processed
            assert metrics['records_processed_count'] == metrics['initial_page']
        else:
            raise Exception('The pipeline should have failed')
    except Exception as e:
        history = sdc_executor.get_pipeline_history(pipeline)
        metrics = _get_metrics(history, run_mode)
        if run_mode == 'timeout_error':
            # Same amount of timeout's than retries
            assert metrics['errors']['Timeout Read'] == metrics['retries']['Retries for timeout']
        elif run_mode == 'status_error':
            # Same amount of status errors than 404 status
            assert metrics['status']['404'] == metrics['errors']['Http status']
        else:
            logger.error(f"Http Client Origin failed: {e}")

    finally:
        http_mock.delete_mock()


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
