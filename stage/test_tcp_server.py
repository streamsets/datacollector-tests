# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""These are tests for the TCP server origin.  They make use of the TCP client provided in the testframework
environment.
"""

import logging
from collections import namedtuple

import pytest

from testframework.environment import TCPClient

logger = logging.getLogger(__name__)

# TODO: convert to pipeline param. seems to not work (see below)
TCP_PORT = 17892

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
                                                 batch_size=1)
    sdc_executor.get_status_pipeline(tcp_server_pipeline.pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

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
    sdc_executor.stop_pipeline(tcp_server_pipeline.pipeline).wait_for_stopped()
