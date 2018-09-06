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

import logging
import socket
import ssl
from collections import namedtuple

import pytest
from streamsets.testframework.environment import TCPClient
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

# TODO: convert to pipeline param. seems to not work (see below)
TCP_PORT = 17892
TCP_SSL_FILE_PATH = './resources/tcp_server/file.txt'
# TCP keystore file path relative to $SDC_RESOURCES.
TCP_KEYSTORE_FILE_PATH = 'resources/tcp_server/keystore.jks'


@pytest.fixture(scope='module')
def tcp_server_pipeline(sdc_builder, sdc_executor):
    """Creates a pipeline with a TCP server origin using TEXT data with default separated records."""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server = pipeline_builder.add_stage('TCP Server')
    tcp_server.configuration.update({'conf.dataFormat': 'TEXT',
                                     # TODO: convert to param; this doesn't work
                                     # 'conf.ports': ['${TCP_LISTEN_PORT}'],
                                     'conf.ports': [str(TCP_PORT)],
                                     'conf.tcpMode': 'DELIMITED_RECORDS',
                                     'conf.recordProcessedAckMessage': 'record_${record:value(\'/text\')}'})

    trash = pipeline_builder.add_stage('Trash')

    tcp_server >> trash
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    yield namedtuple('Pipeline', ['pipeline', 'tcp_server'])(pipeline, tcp_server)


def test_tcp_server_simple(sdc_executor, tcp_server_pipeline):
    """Runs a test using the TCP server origin pipeline and asserts that the test record is created, with ack."""
    # Start TCP Server pipeline.
    expected_msg = 'hello_world'

    # Capture snapshot for HTTP Server pipeline.
    snapshot_cmd = sdc_executor.capture_snapshot(tcp_server_pipeline.pipeline, start_pipeline=True, batches=1,
                                                 batch_size=1, wait=False)
    # create TCP client and send the data
    tcp_client = TCPClient(sdc_executor.server_host, TCP_PORT)
    # default separator is newline
    record_ack1 = tcp_client.send_str_and_ack(f'{expected_msg}\n')

    snapshot = snapshot_cmd.wait_for_finished().snapshot
    origin_data = snapshot[tcp_server_pipeline.tcp_server.instance_name]
    assert len(origin_data.output) == 1
    assert origin_data.output[0].value['value']['text']['value'] == expected_msg
    assert record_ack1 == f'record_{expected_msg}'

    # Stop the pipelines.
    sdc_executor.stop_pipeline(tcp_server_pipeline.pipeline)


@sdc_min_version('3.4.2')
def test_tcp_server_ssl(sdc_builder, sdc_executor):
    """Runs a test using the TCP server origin pipeline with Enable TLS set and asserts that the file is received"""
    expected_msg = get_expected_message(TCP_SSL_FILE_PATH)

    # Start TCP server pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server = pipeline_builder.add_stage('TCP Server')
    tcp_server.set_attributes(data_format='TEXT',
                              port=[str(TCP_PORT)],
                              tcp_mode='DELIMITED_RECORDS',
                              use_tls=True,
                              keystore_file=TCP_KEYSTORE_FILE_PATH,
                              keystore_type='JKS',
                              keystore_password='password',
                              keystore_key_algorithm='SunX509',
                              use_default_protocols=True,
                              use_default_cipher_suites=True)

    trash = pipeline_builder.add_stage('Trash')

    tcp_server >> trash
    tcp_server_ssl_pipeline = pipeline_builder.build(title='TCP Server SSL pipeline')

    sdc_executor.add_pipeline(tcp_server_ssl_pipeline)

    try:
        # Capture snapshot for HTTP Server pipeline.
        snapshot_cmd = sdc_executor.capture_snapshot(tcp_server_ssl_pipeline, start_pipeline=True, batches=1,
                                                     batch_size=2, wait=False)

        # Send twice the data. Even though batch_size = 2, 2 batches are sent (1 for each connection).
        send_tcp_ssl_file(sdc_executor)
        send_tcp_ssl_file(sdc_executor)

        # Wait for snapshot to finish then stop the pipeline in order to get the summary later.
        snapshot = snapshot_cmd.wait_for_finished().snapshot
        sdc_executor.stop_pipeline(tcp_server_ssl_pipeline)

        # Verify the results. First check number of batches received is 2.
        history = sdc_executor.get_pipeline_history(tcp_server_ssl_pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == 2
        # Then check last batch received in snapshot contains the expected message.
        origin_data = snapshot[tcp_server.instance_name]
        assert len(origin_data.output) == 1
        assert str(origin_data.output[0].field['text']) in expected_msg.decode("utf-8")

    finally:
        if sdc_executor.get_pipeline_status(tcp_server_ssl_pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(tcp_server_ssl_pipeline)


def send_tcp_ssl_file(sdc_executor):
    """Sends a file through tcp using ssl"""
    hostname = sdc_executor.server_host
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    with socket.create_connection((hostname, TCP_PORT)) as sock:
        with context.wrap_socket(sock, server_hostname=hostname) as ssock:
            file_to_send = open(TCP_SSL_FILE_PATH, 'rb')
            ssock.sendfile(file_to_send)
            file_to_send.close()
            ssock.close()


def get_expected_message(file_path):
    file_to_read = open(file_path, 'rb')
    message = file_to_read.readline()
    file_to_read.close()
    return message
