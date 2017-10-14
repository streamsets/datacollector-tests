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

import json
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
    sdc_executor.start_pipeline(rpc_origin_pipeline)
    sdc_executor.start_pipeline(rpc_target_pipeline).wait_for_pipeline_output_records_count(1)
    snapshot = sdc_executor.capture_snapshot(rpc_origin_pipeline, start_pipeline=False).snapshot
    snapshot_data = snapshot[sdc_rpc_origin.instance_name].output[0].value['value']['text']['value']

    assert raw_str == snapshot_data

    sdc_executor.stop_pipeline(rpc_target_pipeline)
    sdc_executor.stop_pipeline(rpc_origin_pipeline)


def test_write_to_another_pipeline_error_stage(sdc_builder, sdc_executor):
    """This test will test the Write to Another Pipeline error stage, which writes to an SDC RPC destination.
    We then take a snapshot in a separate pipeline with an SDC RPC origin:

    Write to Another Pipeline error stage pipeline:
        dev_raw_data_source >> to_error

    SDC RPC origin pipeline:
        sdc_rpc_origin >> trash
    """

    # Create some silly Tour de France-themed test data.
    tdf_did_not_starts = [dict(name='Peter Sagan', reason='DQ'),
                          dict(name='Mark Cavendish', reason='Abandoned')]
    raw_data = ''.join(json.dumps(dns) for dns in tdf_did_not_starts)

    sdc_rpc_id = get_random_string(string.ascii_letters, 10)

    # Build the Write to Another Pipeline error stage pipeline.
    builder = sdc_builder.get_pipeline_builder()
    write_to_another_pipeline = builder.add_error_stage('Write to Another Pipeline')
    write_to_another_pipeline.set_attributes(sdc_rpc_connection=[f'{sdc_executor.server_host}:{SDC_RPC_PORT}'],
                                             sdc_rpc_id=sdc_rpc_id)

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data)

    to_error = builder.add_stage('To Error')

    dev_raw_data_source >> to_error
    error_stage_pipeline = builder.build(title='Write to Another Pipeline error stage pipeline')

    # Build the SDC RPC origin pipeline.
    builder = sdc_builder.get_pipeline_builder()

    sdc_rpc_origin = builder.add_stage('SDC RPC', type='origin')
    sdc_rpc_origin.set_attributes(rpc_id=sdc_rpc_id,
                                  rpc_port=SDC_RPC_PORT)

    trash = builder.add_stage('Trash')

    sdc_rpc_origin >> trash
    sdc_rpc_origin_pipeline = builder.build(title='SDC RPC origin pipeline')

    sdc_executor.add_pipeline(error_stage_pipeline, sdc_rpc_origin_pipeline)
    sdc_executor.start_pipeline(sdc_rpc_origin_pipeline)
    sdc_executor.start_pipeline(error_stage_pipeline).wait_for_pipeline_error_records_count(2)
    snapshot = sdc_executor.capture_snapshot(sdc_rpc_origin_pipeline).snapshot

    sdc_executor.stop_pipeline(error_stage_pipeline)
    sdc_executor.stop_pipeline(sdc_rpc_origin_pipeline)

    assert [{key: value['value'] for key, value in record.value['value'].items()}
            for record in snapshot[sdc_rpc_origin.instance_name].output] == tdf_did_not_starts
