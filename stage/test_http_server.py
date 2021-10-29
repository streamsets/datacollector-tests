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
import requests
import ssl
import urllib
import pytest

from collections import namedtuple
from requests_gssapi import HTTPSPNEGOAuth
from streamsets.sdk.utils import Version
from streamsets.testframework.constants import (CREDENTIAL_STORE_EXPRESSION, CREDENTIAL_STORE_WITH_OPTIONS_EXPRESSION,
                                                STF_TESTCONFIG_DIR)
from streamsets.testframework.credential_stores.jks import JKSCredentialStore
from streamsets.testframework.markers import http, sdc_min_version, spnego


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


@pytest.fixture(scope='function')
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

    wiretap_http_server = pipeline_builder.add_wiretap()
    wiretap_javscript_evaluator = pipeline_builder.add_wiretap()

    http_server >> [javascript_evaluator, wiretap_http_server.destination]
    javascript_evaluator >> wiretap_javscript_evaluator.destination

    pipeline = pipeline_builder.build()
    pipeline.add_parameters(HTTP_PORT='8000',
                            APPLICATION_ID='test',
                            NEW_FIELD_NAME='javscriptField',
                            NEW_FIELD_VALUE='5000')

    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    yield namedtuple('Pipeline', ['pipeline', 'wiretap_http_server',
                                  'wiretap_javscript_evaluator'])(pipeline,
                                                                  wiretap_http_server,
                                                                  wiretap_javscript_evaluator)


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
def test_http(sdc_executor, http_server_pipeline,  http_client_pipeline):
    # Start HTTP Server pipeline.
    server_runtime_parameters = {'HTTP_PORT': 9999,
                                 'APPLICATION_ID': 'HTTP_APPLICATION_ID',
                                 'NEW_FIELD_NAME': 'javscriptField',
                                 'NEW_FIELD_VALUE': 5000}

    sdc_executor.start_pipeline(http_server_pipeline.pipeline, server_runtime_parameters)
    pipeline_status = sdc_executor.get_pipeline_status(http_server_pipeline.pipeline).response.json()
    attributes = pipeline_status.get('attributes')
    assert attributes.get('RUNTIME_PARAMETERS').get('APPLICATION_ID') == 'HTTP_APPLICATION_ID'

    # Start HTTP Client pipeline.
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

    origin_data = http_server_pipeline.wiretap_http_server.output_records
    processor_data = http_server_pipeline.wiretap_javscript_evaluator.output_records
    assert len(origin_data) == 2
    assert len(processor_data) == 2
    assert origin_data[0].field['f1'] == 'abc'
    assert origin_data[1].field['f1'] == 'xyz'
    assert processor_data[0].field['f1'] == 'abc'
    assert processor_data[0].field['javscriptField'] == 5000
    assert processor_data[1].field['f1'] == 'xyz'
    assert processor_data[1].field['javscriptField'] == 5000

    # Stop the pipelines.
    sdc_executor.stop_pipeline(http_server_pipeline.pipeline)


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
        http_res.request('GET', '/', '{"f1": "abc"}{"f1": "xyz"}', {'X-SDC-APPLICATION-ID': 'TEST_ID_FIRST'})
        resp = http_res.getresponse()
        assert resp.status == 200

        # Try a GET request using sample data with another valid Application-ID and we should expect a 200 response.
        http_res = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
        http_res.request('GET', '/', '{"f1": "abc"}{"f1": "xyz"}', {'X-SDC-APPLICATION-ID': 'TEST_ID_SECOND'})
        resp = http_res.getresponse()
        assert resp.status == 200

        # Try a GET request using sample data with a non valid Application-ID and we should expect a 403 response.
        http_res = httpclient.HTTPConnection(sdc_executor.server_host, 9999)
        http_res.request('GET', '/', '{"f1": "abc"}{"f1": "xyz"}', {'X-SDC-APPLICATION-ID': 'TEST_ID_THIRD'})
        resp = http_res.getresponse()
        assert resp.status == 403
    finally:
        sdc_executor.stop_pipeline(pipeline)


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
