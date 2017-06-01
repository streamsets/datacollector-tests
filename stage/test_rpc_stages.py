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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
import string

from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000

def test_sdcrpc_origin_target(sdc_builder, sdc_executor):
    """This test will test SDC RPC origin and target. The way we do that is to create 2 pipelines - one which creates
    RPC listener (SDC RPC origin pipeline) and another which writes to RPC (SDC RPC target pipeline). We then assert
    what we ingest at RPC target pipeline to what we find at snapshot of RPC origin pipeline. The pipelines would look
    like:

        SDC RPC origin pipeline:
            sdc_rpc_origin >> trash

        SDC RPC target pipeline:
            dev_raw_data_source >> sdc_rpc_destination
    """
    # test static
    rpc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    # Build the SDC RPC origin pipeline.
    builder = sdc_builder.get_pipeline_builder()

    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.rpc_port = SDC_RPC_PORT
    sdc_rpc_origin.rpc_id = rpc_id

    sdc_rpc_origin >> (builder.add_stage(label='Trash'))
    rpc_origin_pipeline = builder.build('SDC RPC origin pipeline')

    # Build the SDC RPC target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = raw_str

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host, SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = rpc_id

    dev_raw_data_source >> sdc_rpc_destination
    rpc_target_pipeline = builder.build('SDC RPC target pipeline')

    sdc_executor.add_pipeline(rpc_origin_pipeline, rpc_target_pipeline)

    # Run pipelines and assert data.
    sdc_executor.start_pipeline(rpc_origin_pipeline).wait_for_status('RUNNING')
    sdc_executor.start_pipeline(rpc_target_pipeline).wait_for_pipeline_output_records_count(1)
    snapshot = sdc_executor.capture_snapshot(rpc_origin_pipeline, start_pipeline=False).wait_for_finished().snapshot
    snapshot_data = snapshot[sdc_rpc_origin.instance_name].output[0].value['value']['text']['value']

    assert raw_str == snapshot_data

    sdc_executor.stop_pipeline(rpc_target_pipeline)
    sdc_executor.stop_pipeline(rpc_origin_pipeline)
