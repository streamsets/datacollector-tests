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

import json
import logging
import string

from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Specify a port for SDC RPC stages to use.
SDC_RPC_LISTENING_PORT = 20000


def test_sdcrpc_with_buffering_origin_target(sdc_builder, sdc_executor):
    """This test will test SDC RPC with buffering origin and SDC RPC target. The way we do that is to create 2 pipelines - one which creates
    RPC listener (SDC RPC with buffering origin pipeline) and another which writes to RPC (SDC RPC target pipeline). We then assert
    what we ingest at RPC target pipeline to what we find at snapshot of RPC origin pipeline. The pipelines would look
    like:

        SDC RPC with buffering origin pipeline:
            sdc_rpc_origin >> trash

        SDC RPC target pipeline:
            dev_raw_data_source >> sdc_rpc_destination
    """
    # test static
    sdc_rpc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    # Build the SDC RPC origin pipeline.
    builder = sdc_builder.get_pipeline_builder()

    sdc_rpc_origin = builder.add_stage('Dev SDC RPC with Buffering')
    sdc_rpc_origin.sdc_rpc_listening_port = SDC_RPC_LISTENING_PORT
    sdc_rpc_origin.sdc_rpc_id = sdc_rpc_id

    sdc_rpc_origin >> (builder.add_stage(label='Trash'))
    rpc_origin_pipeline = builder.build('SDC RPC origin pipeline')

    # Build the SDC RPC target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = raw_str

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.sdc_rpc_connection.append('{}:{}'.format(sdc_executor.server_host, SDC_RPC_LISTENING_PORT))
    sdc_rpc_destination.sdc_rpc_id = sdc_rpc_id

    dev_raw_data_source >> sdc_rpc_destination
    rpc_target_pipeline = builder.build('SDC RPC target pipeline')

    sdc_executor.add_pipeline(rpc_origin_pipeline, rpc_target_pipeline)

    # Run pipelines and assert data.
    sdc_executor.start_pipeline(rpc_origin_pipeline)
    sdc_executor.start_pipeline(rpc_target_pipeline).wait_for_pipeline_output_records_count(1)
    snapshot = sdc_executor.capture_snapshot(rpc_origin_pipeline).snapshot
    snapshot_data = snapshot[sdc_rpc_origin.instance_name].output[0].value['value']['text']['value']

    assert raw_str == snapshot_data

    sdc_executor.stop_pipeline(rpc_target_pipeline)
    sdc_executor.stop_pipeline(rpc_origin_pipeline)