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
from threading import Thread

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

    wiretap = pipeline_builder.add_wiretap()

    tcp_server >> wiretap.destination
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)

    # Yield a namedtuple so that we can access instance names of the stages within the test.
    yield namedtuple('Pipeline', ['pipeline', 'wiretap'])(pipeline, wiretap)


def test_tcp_server_simple(sdc_executor, tcp_server_pipeline):
    """Runs a test using the TCP server origin pipeline and asserts that the test record is created, with ack."""
    # Start TCP Server pipeline.
    expected_msg = 'hello_world'

    sdc_executor.start_pipeline(tcp_server_pipeline.pipeline)
    # create TCP client and send the data
    tcp_client = TCPClient(sdc_executor.server_host, TCP_PORT)
    # default separator is newline
    record_ack1 = tcp_client.send_str_and_ack(f'{expected_msg}\n')

    sdc_executor.wait_for_pipeline_metric(tcp_server_pipeline.pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(tcp_server_pipeline.pipeline)

    read_data = tcp_server_pipeline.wiretap.output_records

    assert len(read_data) == 1
    assert read_data[0].field['text'].value == expected_msg
    assert record_ack1 == f'record_{expected_msg}'


# SDC-10425
@sdc_min_version('3.0.0.0')  # Need the delay processor
def test_stop_tcp_with_delay(sdc_builder, sdc_executor):
    """Make sure that the origin can properly be started after stopping it with long batch times."""
    builder = sdc_builder.get_pipeline_builder()

    message = 'Something not important'

    tcp_server = builder.add_stage('TCP Server')
    tcp_server.configuration.update({'conf.dataFormat': 'TEXT',
                                     'conf.ports': [str(TCP_PORT)],
                                     'conf.tcpMode': 'DELIMITED_RECORDS',
                                     'conf.recordProcessedAckMessage': 'record_${record:value(\'/text\')}'})

    # Make sure that each batch takes at least 5 seconds
    delay = builder.add_stage('Delay')
    delay.delay_between_batches = 5 * 1000

    wiretap = builder.add_wiretap()

    tcp_server >> delay >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)

    # Let's start/stop the pipeline few times, it should always properly wait for graceful shutdown and subsequent
    # start of pipeline should be immediate.
    for _ in range(3):
        wiretap.reset()
        # Start the pipeline
        sdc_executor.start_pipeline(pipeline)

        # Send exactly one record
        tcp_client = TCPClient(sdc_executor.server_host, TCP_PORT)
        tcp_client.send_str_and_ack(message + '\n')

        # Wait until at least 1 record is processed
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert [message] == [record.field['text'] for record in wiretap.output_records]


@sdc_min_version('3.7.0')
def test_tcp_server_read_timeout(sdc_builder, sdc_executor):
    """Runs a test using TCP Server Origin and setting Read Timeout to 20 seconds.
    Then checks connection is automatically closed after 20 seconds as the timeout is triggered.

    As the destination is not relevant for this stage the pipeline looks like:

    TCP Server >> wiretap
    """
    expected_message = 'testMessage'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server_stage = pipeline_builder.add_stage('TCP Server').set_attributes(port=[str(TCP_PORT)],
                                                                               tcp_mode='DELIMITED_RECORDS',
                                                                               data_format='TEXT',
                                                                               read_timeout_in_seconds=20)
    wiretap = pipeline_builder.add_wiretap()

    tcp_server_stage >> wiretap.destination

    tcp_server_pipeline = pipeline_builder.build(title=f'TCP Server Origin Read Timeout')
    sdc_executor.add_pipeline(tcp_server_pipeline)

    sdc_executor.start_pipeline(tcp_server_pipeline)

    # Send message to test connection is open.
    tcp_client = TCPClient(sdc_executor.server_host, TCP_PORT)
    tcp_client.send_str_and_ack(expected_message + '\n')

    sdc_executor.wait_for_pipeline_metric(tcp_server_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(tcp_server_pipeline)
    output_records = [record.field['text'] for record in wiretap.output_records]
    assert len(output_records) == 1
    assert expected_message == output_records[0]


def test_tcp_server_multiple_messages(sdc_builder, sdc_executor):
    """Runs a test using 4 pipelines with the TCP Server Origin writing to wiretap and checking all records sent by the
    client are correctly received by the TCP Server Origin. Pipeline configurations are:

    1) No record_processed_ack_message and No batch_completed_ack_message
    2) record_processed_ack_message and No batch_completed_ack_message
    3) No record_processed_ack_message and batch_completed_ack_message
    4)record_processed_ack_message and batch_completed_ack_message

    All pipelines look like below but with different configuration as explained above:
        TCP Server >> wiretap
    """
    # Build and test pipeline number 1.
    tcp_server_pipeline_1, wiretap_1 = _add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                           record_ack=False, batch_ack=False)
    _run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_1, wiretap_1)

    # Build and test pipeline number 2.
    tcp_server_pipeline_2, wiretap_2, = _add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                            record_ack=True, batch_ack=False)
    _run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_2, wiretap_2)

    # Build and test pipeline number 3.
    tcp_server_pipeline_3, wiretap_3 = _add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                           record_ack=False,
                                                                           batch_ack=True)
    _run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_3, wiretap_3)

    # Build and test pipeline number 4.
    tcp_server_pipeline_4, wiretap_4 = _add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor,
                                                                           record_ack=True,
                                                                           batch_ack=True)
    _run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline_4, wiretap_4)


@sdc_min_version('3.4.2')
def test_tcp_server_ssl(sdc_builder, sdc_executor):
    """Runs a test using the TCP server origin pipeline with Enable TLS set and asserts that the file is received"""
    expected_msg = _get_expected_message(TCP_SSL_FILE_PATH)

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

    wiretap = pipeline_builder.add_wiretap()

    tcp_server >> wiretap.destination
    tcp_server_ssl_pipeline = pipeline_builder.build(title='TCP Server SSL pipeline')

    sdc_executor.add_pipeline(tcp_server_ssl_pipeline)

    sdc_executor.start_pipeline(tcp_server_ssl_pipeline)

    # Send twice the data. Even though batch_size = 2, 2 batches are sent (1 for each connection).
    _send_tcp_ssl_file(sdc_executor)
    _send_tcp_ssl_file(sdc_executor)

    sdc_executor.wait_for_pipeline_metric(tcp_server_ssl_pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(tcp_server_ssl_pipeline)

    # Verify the results. First check number of batches received is 2.
    history = sdc_executor.get_pipeline_history(tcp_server_ssl_pipeline)
    assert history.latest.metrics.counter('pipeline.batchCount.counter').count == 2

    # Then check last batch received in wiretap contains the expected message.
    assert len(wiretap.output_records) == 2
    assert str(wiretap.output_records[0].field['text']) in expected_msg.decode("utf-8")
    assert str(wiretap.output_records[1].field['text']) in expected_msg.decode("utf-8")


def test_tcp_multiple_ports(sdc_builder, sdc_executor):
    """ Runs a test using TCP Server as Origin and Trash as destination. TCP Server will be listening to ports 55555 and
    44444. Two clients will be writing in parallel to one of these ports (each client to a different port). While
    clients are writing it will be checked no exception is thrown due to TCP Server pool exhausted.

    Pipeline looks like:

    TCP Server >> trash
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server_stage = pipeline_builder.add_stage('TCP Server').set_attributes(port=[str(55555), str(44444)],
                                                                               number_of_receiver_threads=5,
                                                                               tcp_mode='DELIMITED_RECORDS',
                                                                               max_batch_size_in_messages=100,
                                                                               batch_wait_time_in_ms=6000,
                                                                               max_message_size_in_bytes=40960,
                                                                               data_format='TEXT',
                                                                               max_line_length=10240)
    wiretap = pipeline_builder.add_wiretap()

    tcp_server_stage >> wiretap.destination

    tcp_server_pipeline = pipeline_builder.build(title='TCP Server Origin 20 threads 2 ports')
    sdc_executor.add_pipeline(tcp_server_pipeline)

    # Run pipeline.
    sdc_executor.start_pipeline(tcp_server_pipeline)

    expected_message = 'hello_world'
    total_num_messages = 0
    message_counter = 0
    expected_messages_list = []

    # Create tcp client listening to port 55555.
    tcp_client_socket_port_55555 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_client_socket_port_55555.connect((sdc_executor.server_host, 55555))

    # Create tcp client listening to port 44444.
    tcp_client_socket_port_44444 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_client_socket_port_44444.connect((sdc_executor.server_host, 44444))

    # Send messages for both tcp clients.
    for _ in range(0, 1000):
        expected_message_bytes = bytes(f'{message_counter}{expected_message}\n', 'utf-8')
        _send_asynchronous_message_multiple_clients([tcp_client_socket_port_55555, tcp_client_socket_port_44444],
                                                    expected_message_bytes)
        # Append twice the message to the list as two clients sending send message.
        expected_messages_list.append(f'{message_counter}{expected_message}')
        expected_messages_list.append(f'{message_counter}{expected_message}')
        message_counter += 1
        total_num_messages += 2

    # Close clients.
    tcp_client_socket_port_55555.close()
    tcp_client_socket_port_44444.close()

    sdc_executor.wait_for_pipeline_metric(tcp_server_pipeline, 'input_record_count', 1000)
    sdc_executor.stop_pipeline(tcp_server_pipeline)
    output_records_values = [str(record.field['text']) for record in wiretap.output_records]
    assert len(output_records_values) == total_num_messages
    assert sorted(output_records_values) == sorted(expected_messages_list)


def test_tcp_epoll_enabled(sdc_builder, sdc_executor):
    """ Run a pipeline with TCP Server Origin having Epoll Enabled as well as setting number of threads to 5 and
    validate it correctly starts and receives data from a client.

    Pipeline looks like:

    TCP Server >> wiretap
    """

    max_records = 500
    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server_stage = pipeline_builder.add_stage('TCP Server').set_attributes(port=[str(TCP_PORT)],
                                                                               number_of_receiver_threads=5,
                                                                               enable_native_transports_in_epoll=True,
                                                                               tcp_mode='DELIMITED_RECORDS',
                                                                               max_batch_size_in_messages=1000,
                                                                               batch_wait_time_in_ms=6000,
                                                                               max_message_size_in_bytes=40960,
                                                                               data_format='TEXT',
                                                                               max_line_length=10240)
    wiretap = pipeline_builder.add_wiretap()
    
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    precondition = f"${{record:value('/text') == '{max_records-1}_hello_world'}}"
    pipeline_finisher.set_attributes(preconditions=[precondition])

    tcp_server_stage >> [wiretap.destination, pipeline_finisher]

    tcp_server_pipeline = pipeline_builder.build(title='TCP Server Origin 5 threads 1 port Epoll Enabled')
    sdc_executor.add_pipeline(tcp_server_pipeline)

    # Run pipeline.
    status = sdc_executor.start_pipeline(tcp_server_pipeline)

    expected_message = 'hello_world'
    total_num_messages = 0
    expected_messages_list = []

    # Create tcp client.
    tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_client_socket.connect((sdc_executor.server_host, TCP_PORT))

    # Send messages for this tcp client.
    for _ in range(0, max_records):
        message = f'{total_num_messages}_{expected_message}'
        tcp_client_socket.sendall(bytes(message + '\n', 'utf-8'))
        expected_messages_list.append(message)
        total_num_messages += 1

    tcp_client_socket.close()

    status.wait_for_finished()

    output_records_values = [str(record.field['text']) for record in wiretap.output_records]
    assert len(output_records_values) == total_num_messages
    assert sorted(output_records_values) == sorted(expected_messages_list)


def _send_asynchronous_message_multiple_clients(client_socket_list, message_bytes):
    """ Sends message_bytes for each client in client_socket_list

    :param client_socket_list: The list of TCP client sockets
    :param message_bytes: the bytes of the message to send

    """
    threads_list = []
    for client_socket in client_socket_list:
        thread = Thread(target=_send_synchronous_message, args=(client_socket, message_bytes))
        threads_list.append(thread)
        thread.start()
    for thread in threads_list:
        thread.join()


def _send_synchronous_message(client_socket, message_bytes):
    client_socket.sendall(message_bytes)


def _send_tcp_ssl_file(sdc_executor):
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


def _get_expected_message(file_path):
    file_to_read = open(file_path, 'rb')
    message = file_to_read.readline()
    file_to_read.close()
    return message


def _add_tcp_pipeline_multiple_messages(sdc_builder, sdc_executor, record_ack, batch_ack):
    """Add a TCP Server to Trash pipeline to the given sdc_executor setting a record ack if record_ack is true or
    setting a batch ack batch_ack is true.
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    tcp_server_stage = pipeline_builder.add_stage('TCP Server').set_attributes(port=[str(TCP_PORT)],
                                                                               tcp_mode='DELIMITED_RECORDS',
                                                                               data_format='TEXT')
    if record_ack:
        tcp_server_stage.set_attributes(record_processed_ack_message='Record Processed')

    if batch_ack:
        tcp_server_stage.set_attributes(batch_completed_ack_message='Batch Completed')

    wiretap = pipeline_builder.add_wiretap()

    tcp_server_stage >> wiretap.destination

    tcp_server_pipeline = pipeline_builder.build(
        title=f'TCP Server Origin {"with record ack" if record_ack else "no record ack"} '
              f'{"and batch ack" if batch_ack else "and no batch ack"}')

    sdc_executor.add_pipeline(tcp_server_pipeline)

    return [tcp_server_pipeline, wiretap]


def _run_pipeline_send_tcp_messages(sdc_executor, tcp_server_pipeline, wiretap):
    """ Runs the given tcp_server_pipeline and sends num_messages_by_client messages for each client where each
    position in num_messages_by_client indicates the number of messages to send for the next client, for example: first
    client will send num_messages_by_client[0] messages and so on. The number of clients is num_clients, therefore
    num_clients must be equal to len(num_messages_by_client). After that it stops the  pipeline and checks that the
    number of messages received by the pipeline is the same as the sum of number of messages sent by each the client.
    seconds_to_wait_before_close indicates the number of seconds to wait before closing for each client. If
    seconds_to_wait_before_close is zero then there is no wait for that client. seconds_to_wait_before_close[0]
    indicates the time to wait for first client and so on.
    """
    message = 'hello world'

    # Run pipeline.
    sdc_executor.start_pipeline(tcp_server_pipeline)

    # Process each client.
    for _ in range(0, 3):
        # Create tcp client.
        tcp_client = TCPClient(sdc_executor.server_host, TCP_PORT)

        # Send messages for this tcp client.
        tcp_client.send_str_and_ack(message + '\n')

    sdc_executor.wait_for_pipeline_metric(tcp_server_pipeline, 'input_record_count', 3)
    sdc_executor.stop_pipeline(tcp_server_pipeline)
    output_records_values = [str(record.field['text']) for record in wiretap.output_records]
    assert len(output_records_values) == 3
    assert output_records_values == 3 * [message]
