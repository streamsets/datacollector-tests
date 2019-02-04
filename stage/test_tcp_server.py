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
import time
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


# SDC-10425
def test_stop_tcp_with_delay(sdc_builder, sdc_executor):
    """Make sure that the origin can properly be started after stopping it with long batch times."""
    builder = sdc_builder.get_pipeline_builder()

    tcp_server = builder.add_stage('TCP Server')
    tcp_server.configuration.update({'conf.dataFormat': 'TEXT',
                                     'conf.ports': [str(TCP_PORT)],
                                     'conf.tcpMode': 'DELIMITED_RECORDS',
                                     'conf.recordProcessedAckMessage': 'record_${record:value(\'/text\')}'})

    # Make sure that each batch takes at least 5 seconds
    delay = builder.add_stage('Delay')
    delay.delay_between_batches = 5 * 1000

    trash = builder.add_stage('Trash')

    tcp_server >> delay >> trash
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)

    # Let's start/stop the pipeline few times, it should always properly wait for graceful shutdown and subsequent
    # start of pipeline should be immediate.
    for _ in range(3):
        # Start the pipeline
        sdc_executor.start_pipeline(pipeline)

        # Send exactly one record
        tcp_client = TCPClient(sdc_executor.server_host, TCP_PORT)
        tcp_client.send_str_and_ack('Something not important\n')

        # Wait one second to make sure that the batch is 'processing' (it should take ~5 seconds to process that batch)
        time.sleep(1)

        # Stop the pipeline, the pipeline stop command should take time to finish. In 'zero' operation cost world that
        # would be around 4 seconds, but we don't want to let the test fail for random race conditions and thus we
        # verify at least 2 seconds.
        start = time.time()
        sdc_executor.stop_pipeline(pipeline)
        assert time.time() - start > 2

        # There should be exactly one record and batch processing time should be more than 5 seconds
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        #TODO: TLKT-167: Add access methods to metric objects
        assert history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('mean') >= 5


@sdc_min_version('3.7.0')
def test_tcp_server_read_timeout(sdc_builder, sdc_executor):
    """Runs a test using TCP Server Origin and setting Read Timeout to 20 seconds.
    Then checks connection is automatically closed after 20 seconds as the timeout is triggered.

    As the destination is not relevant for this stage the pipeline looks like:

    TCP Server >> trash
    """
    expected_message = 'testMessage\n'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server_stage = pipeline_builder.add_stage('TCP Server').set_attributes(port=[str(TCP_PORT)],
                                                                               tcp_mode='DELIMITED_RECORDS',
                                                                               data_format='TEXT',
                                                                               read_timeout_in_seconds=20)
    trash_stage = pipeline_builder.add_stage('Trash')

    tcp_server_stage >> trash_stage

    tcp_server_pipeline = pipeline_builder.build(title=f'TCP Server Origin Read Timeout')
    sdc_executor.add_pipeline(tcp_server_pipeline)

    try:
        snapshot_cmd = sdc_executor.capture_snapshot(tcp_server_pipeline, start_pipeline=True, wait=False,
                                                     batch_size=1000, batches=1)

        # Send message to test connection is open.
        tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_client_socket.connect((sdc_executor.server_host, TCP_PORT))
        messages_sent = 0
        while messages_sent < 1:
            messages_sent = tcp_client_socket.send(bytes(expected_message, 'utf-8'))

        # Trying to send read a message should return no data (== an empty bytes object) if socket is closed.
        data = tcp_client_socket.recv(1024)
        if data:
            pytest.fail('Could send message when timeout should have triggered and closed the connection')

        tcp_client_socket.close()
        snapshot = snapshot_cmd.wait_for_finished().snapshot
        output_records = [record.field['text'] for record in snapshot[tcp_server_stage.instance_name].output]
        assert len(output_records) == 1
        assert expected_message.rstrip('\n') == output_records[0]
    finally:
        sdc_executor.stop_pipeline(tcp_server_pipeline, wait=True, force=True)


def test_tcp_server_multiple_messages(sdc_builder, sdc_executor):
    """Runs a test using 4 pipelines with the TCP Server Origin writing to trash and checking all records sent by the
    client are correctly received by the TCP Server Origin. Pipeline configurations are:

    1) No record_processed_ack_message and No batch_completed_ack_message
    2) record_processed_ack_message and No batch_completed_ack_message
    3) No record_processed_ack_message and batch_completed_ack_message
    4)record_processed_ack_message and batch_completed_ack_message

    All pipelines look like below but with different configuration as explained above:
        TCP Server >> trash
    """

    expected_message = ' hello_world\n'

    # Build and test pipeline number 1.
    tcp_server_pipeline_1, tcp_server_stage_1 = add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                                   record_ack=False, batch_ack=False,
                                                                                   batch_timeout=1000, batch_size=10)
    run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_1, tcp_server_stage_1, expected_message, 3,
                                   [26, 25, 5], [0, 0, 5])

    # Build and test pipeline number 2.
    tcp_server_pipeline_2, tcp_server_stage_2 = add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                                   record_ack=True, batch_ack=False,
                                                                                   batch_timeout=1000, batch_size=10)
    run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_2, tcp_server_stage_2, expected_message, 3,
                                   [26, 25, 5], [0, 0, 5])

    # Build and test pipeline number 3.
    tcp_server_pipeline_3, tcp_server_stage_3 = add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                                   record_ack=False, batch_ack=True,
                                                                                   batch_timeout=1000, batch_size=10)
    run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_3, tcp_server_stage_3, expected_message, 3,
                                   [26, 25, 5], [0, 0, 5])

    # Build and test pipeline number 4.
    tcp_server_pipeline_4, tcp_server_stage_4 = add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                                   record_ack=True, batch_ack=True,
                                                                                   batch_timeout=1000, batch_size=10)
    run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_4, tcp_server_stage_4, expected_message, 3,
                                   [26, 25, 5], [0, 0, 5])


def add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor, record_ack, batch_ack, batch_timeout, batch_size):
    """Add a TCP Server to Trash pipeline to the given sdc_executor setting a record ack if record_ack is true or
    setting a batch ack batch_ack is true.
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server_stage = pipeline_builder.add_stage('TCP Server').set_attributes(port=[str(TCP_PORT)],
                                                                               tcp_mode='DELIMITED_RECORDS',
                                                                               data_format='TEXT',
                                                                               batch_wait_time_in_ms=batch_timeout,
                                                                               max_batch_size_in_messages=batch_size)
    if record_ack:
        tcp_server_stage.set_attributes(record_processed_ack_message='Record Processed')

    if batch_ack:
        tcp_server_stage.set_attributes(batch_completed_ack_message='Batch Completed')

    trash_stage = pipeline_builder.add_stage('Trash')

    tcp_server_stage >> trash_stage

    tcp_server_pipeline = pipeline_builder.build(
        title=f'TCP Server Origin {"with record ack" if record_ack else "no record ack"} '
        f'{"and batch ack" if batch_ack else "and no batch ack"}')

    sdc_executor.add_pipeline(tcp_server_pipeline)

    return [tcp_server_pipeline, tcp_server_stage]


def run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline, tcp_server_stage, expected_message, num_clients,
                                   num_messages_by_client, seconds_to_wait_before_close):
    """ Runs the given tcp_server_pipeline and sends num_messages_by_client messages for each client where each
    position in num_messages_by_client indicates the number of messages to send for the next client, for example: first
    client will send num_messages_by_client[0] messages and so on. The number of clients is num_clients, therefore
    num_clients must be equal to len(num_messages_by_client). After that it stops the  pipeline and checks that the
    number of messages received by the pipeline is the same as the sum of number of messages sent by each the client.
    seconds_to_wait_before_close indicates the number of seconds to wait before closing for each client. If
    seconds_to_wait_before_close is zero then there is no wait for that client. seconds_to_wait_before_close[0]
    indicates the time to wait for first client and so on.
    """
    # First assert lists sizes are correct, this is checking their size is equal to num_clients.
    assert len(num_messages_by_client) == len(seconds_to_wait_before_close) == num_clients

    try:
        # Run pipeline.
        snapshot_cmd = sdc_executor.capture_snapshot(tcp_server_pipeline, start_pipeline=True, wait=False,
                                                     batch_size=10, batches=7)
        total_num_messages = 0
        expected_messages_list = []
        # Process each client.
        for i in range(0, num_clients):
            # Create tcp client.
            tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_client_socket.connect((sdc_executor.server_host, TCP_PORT))

            # Send messages for this tcp client.
            for j in range(0, num_messages_by_client[i]):
                expected_message_bytes = bytes(f'{str(total_num_messages)}{expected_message}', 'utf-8')
                tcp_client_socket.sendall(expected_message_bytes)
                new_line_char = '\n'
                expected_messages_list.append(f'{str(total_num_messages)}{expected_message.rstrip(new_line_char)}')
                total_num_messages += 1

            # Sleep if necessary after sending messages so TCP Server Origin may send a batch due to timeout.
            if seconds_to_wait_before_close[i] > 0:
                time.sleep(seconds_to_wait_before_close[i])

            tcp_client_socket.close()

        snapshot = snapshot_cmd.wait_for_finished().snapshot
        output_records_values = [str(record.field['text'])
                                 for batch in snapshot.snapshot_batches
                                 for record in batch.stage_outputs[tcp_server_stage.instance_name].output]
        assert len(output_records_values) == total_num_messages
        assert sorted(output_records_values) == sorted(expected_messages_list)
    finally:
        sdc_executor.stop_pipeline(tcp_server_pipeline, wait=True, force=True)


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
