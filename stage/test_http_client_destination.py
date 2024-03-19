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
import string
import pytest

from collections import namedtuple
from streamsets.sdk.exceptions import RunError
from pretenders.common.constants import FOREVER
from streamsets.testframework.markers import http, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def http_client_pipeline(sdc_builder, sdc_executor):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(raw_data='${RAW_DATA}', data_format='JSON', stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')

    http_client = pipeline_builder.add_stage('HTTP Client', type='destination')
    http_client.resource_url = '${RESOURCE_URL}'
    http_client.headers = [{'key': 'X-SDC-APPLICATION-ID', 'value': '${APPLICATION_ID}'}]
    http_client.one_request_per_batch = True

    wiretap_data_source = pipeline_builder.add_wiretap()
    wiretap_expression_evaluator = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [wiretap_data_source.destination, expression_evaluator]
    expression_evaluator >> [http_client, wiretap_expression_evaluator.destination]
    pipeline = pipeline_builder.build()
    pipeline.add_parameters(RAW_DATA='{"f1": "abc"}{"f1": "xyz"}',
                            RESOURCE_URL='http://localhost:8000',
                            APPLICATION_ID='test')

    sdc_executor.add_pipeline(pipeline)

    yield namedtuple('Pipeline', ['pipeline',
                                  'wiretap_data_source',
                                  'wiretap_expression_evaluator'])(pipeline,
                                                                   wiretap_data_source,
                                                                   wiretap_expression_evaluator)


@http
def test_http_client_target_wrong_host(sdc_executor, http_client_pipeline):
    # Start HTTP Client pipeline with invalid resource URL.
    client_runtime_parameters = {'RAW_DATA': '{"f1": "abc"}{"f1": "xyz"}',
                                 'RESOURCE_URL': 'http://localhost:9999',
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID'}
    sdc_executor.start_pipeline(http_client_pipeline.pipeline, client_runtime_parameters).wait_for_finished()

    origin_data = http_client_pipeline.wiretap_data_source.output_records
    processor_data = http_client_pipeline.wiretap_expression_evaluator.output_records
    assert len(origin_data) == 2
    assert len(processor_data) == 2
    assert origin_data[0].field['f1'] == 'abc'
    assert origin_data[1].field['f1'] == 'xyz'

    # Since resource URL is invalid, all the records should go to error in target stage.
    assert len(http_client_pipeline.wiretap_expression_evaluator.error_records) == 2


@http
@pytest.mark.parametrize('method', [
    'POST',
    # Testing of SDC-10809
    'PATCH'
])
@pytest.mark.parametrize('request_option', [
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
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
        http_client_destination = builder.add_stage('HTTP Client', type='destination')
        # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
        http_client_destination.set_attributes(data_format='JSON',
                                               headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                               http_method=method,
                                               resource_url=mock_uri,
                                               one_request_per_batch=(request_option == 'one_request_per_batch'))

        dev_raw_data_source >> http_client_destination
        pipeline = builder.build(title=f'HTTP {method} Destination pipeline')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Check that the HTTP server got the expected data
        r = http_mock.get_request(0)
        assert r
        assert r.method == method
        assert r.url == f'/{mock_path}'
        assert r.body
        assert json.loads(r.body.decode("utf-8")) == raw_dict
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


@http
@pytest.mark.parametrize('request_option', [
    'one_request_per_batch',
    'one_request_per_record'
])
@sdc_min_version("4.4.0")
def test_http_destination_oauth2_token_retry(sdc_builder, sdc_executor, http_client, request_option):
    """
    Test the error when the http destination stage has more than the allowed number of consecutive invalid
    oauth2 tokens (1 for now, but test is suitable for any number as we just mock infinite invalid tokens).
    There are similar tests for origin and processor.

    We use the pipeline:
        dev_data_generator >> http_client_destination

    """
    raw_dict = dict(city='San Francisco')
    raw_data = json.dumps(raw_dict)
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
        dev_data_generator = builder.add_stage('Dev Data Generator')
        dev_data_generator.set_attributes(batch_size=5, delay_between_batches=1000,
                                          records_to_be_generated=10)
        http_client_destination = builder.add_stage('HTTP Client', type='destination')
        http_client_destination.set_attributes(data_format='JSON',
                                               on_record_error='STOP_PIPELINE',
                                               headers=[{'key': 'content-length', 'value': f'{len(raw_data)}'}],
                                               http_method='POST',
                                               resource_url=mock_uri,
                                               one_request_per_batch=(request_option == 'one_request_per_batch'),
                                               use_oauth_2=True,
                                               credentials_grant_type='CLIENT_CREDENTIALS',
                                               token_url=oauth_mock_uri,
                                               client_id='-',
                                               client_secret='-')

        dev_data_generator >> http_client_destination
        pipeline = builder.build(title='HTTP POST Destination pipeline')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        pytest.fail('Test should have raised an Exception with HTTP_32 - HTTP_38 exception, but did not')
    except RunError as e:
        if Version(sdc_executor.version) >= Version('5.7.0'):
            # The change of the catch exception in 5.7 (COLLECTOR-4009) was only applied when "One Request per Batch"
            # is enabled. It was fixed in 5.8 (COLLECTOR-4117)
            if Version(sdc_executor.version) == Version('5.7.0') and request_option == 'one_request_per_batch':
                assert 'HTTP_41' in e.message
            assert 'HTTP_38' in str(sdc_executor.get_logs())
        else:
            assert 'HTTP_38' in e.message
    finally:
        http_mock.delete_mock()
        oauth_http_mock.delete_mock()
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@http
@sdc_min_version("4.2.0")
def test_http_target_metrics(sdc_builder, sdc_executor, http_client):
    """ Test Metrics timers and gauge. Test the metrics in different type of Http Client destination configuration.
        The pipeline looks like:
        dev_raw_data_source >> http_client_target """
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()
    method = 'POST'
    raw_data = [{'A': i, 'C': i + 1, 'G': i + 2, 'T': i + 3} for i in range(10)]
    expected_data = json.dumps(raw_data)

    # Times:
    one_millisecond = 1000
    wait_seconds = 10
    long_time = (one_millisecond * wait_seconds)

    try:
        http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
        resource_url = f'{http_mock.pretend_url}/{mock_path}'
        timeout_time = long_time

        builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data=expected_data, stop_after_first_batch=True)

        http_client_target = builder.add_stage('HTTP Client', type='destination')
        http_client_target.set_attributes(data_format='JSON',
                                          http_method=method,
                                          resource_url=resource_url,
                                          read_timeout=timeout_time)

        dev_raw_data_source >> http_client_target
        pipeline = builder.build('Http Client Destination Metrics')
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        metrics = _get_metrics(history, 'correct')

        # Check that the HTTP server got the expected data
        result = json.loads(http_mock.get_request(0).body.decode("utf-8"))
        assert raw_data == result

        # Right correlation between mean time for every step of process
        assert metrics['records_processed_mean'] >= metrics['success_requests_mean']
        assert metrics['success_requests_mean'] >= metrics['requests_mean']

        # Same amount of records processed than successful request
        assert metrics['records_processed_count'] == metrics['success_requests_count']
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
def test_http_target_metrics_errors(sdc_builder, sdc_executor, http_client, run_mode):
    """ Test Metrics timers and gauge. Test the metrics in timeout and status error configuration of Http Client
    destination.
        The pipeline looks like:
        dev_raw_data_source >> http_client_target """

    for i in range(3):
        mock_path = get_random_string(string.ascii_letters, 10)
        mock_path_wrong = get_random_string(string.ascii_letters, 10)
        http_mock = http_client.mock()
        method = 'GET'
        raw_data = [{'A': i, 'C': i + 1, 'G': i + 2, 'T': i + 3} for i in range(10)]
        expected_data = json.dumps(raw_data)

        # Times:
        one_millisecond = 1000
        wait_seconds = 10
        short_time = 1
        long_time = (one_millisecond * wait_seconds)

        try:
            if run_mode == 'timeout_error':
                http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=short_time)
                resource_url = f'{http_mock.pretend_url}/{mock_path}'
                timeout_time = short_time
            elif run_mode == 'status_error':
                http_mock.when(f'{method} /{mock_path}').reply(expected_data, times=FOREVER)
                resource_url = f'{http_mock.pretend_url}/{mock_path_wrong}'
                timeout_time = long_time

            builder = sdc_builder.get_pipeline_builder()

            dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
            dev_raw_data_source.set_attributes(data_format='JSON', raw_data=expected_data, stop_after_first_batch=True)

            http_client_target = builder.add_stage('HTTP Client', type='destination')
            http_client_target.set_attributes(data_format='JSON',
                                              http_method=method,
                                              resource_url=resource_url,
                                              read_timeout=timeout_time)

            dev_raw_data_source >> http_client_target
            pipeline = builder.build('Http Client Destination Metrics')
            sdc_executor.add_pipeline(pipeline)

            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            raise Exception('The pipeline should have failed')

        except Exception as e:
            history = sdc_executor.get_pipeline_history(pipeline)
            try:
                metrics = _get_metrics(history, 'timeout_error')

                if run_mode == 'timeout_error':
                    # Same amount of timeout's than retries
                    assert metrics['errors']['Timeout Read'] == 1
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
