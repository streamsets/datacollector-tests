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

# Specify a port for SDC RPC stages to use.
SDC_RPC_LISTENING_PORT = 20000


def test_sdcrpc_origin_target(sdc_builder, sdc_executor):
    """This test will test SDC RPC origin and target. The way we do that is to create 2 pipelines - one which creates
    RPC listener (SDC RPC origin pipeline) and another which writes to RPC (SDC RPC target pipeline). We then assert
    what we ingest at RPC target pipeline to what we find at wiretap of RPC origin pipeline. The pipelines would look
    like:

        SDC RPC origin pipeline:
            sdc_rpc_origin >> wiretap

        SDC RPC target pipeline:
            dev_raw_data_source >> sdc_rpc_destination
    """
    # test static
    sdc_rpc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    # Build the SDC RPC origin pipeline.
    builder = sdc_builder.get_pipeline_builder()

    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.set_attributes(sdc_rpc_listening_port=SDC_RPC_LISTENING_PORT,
                                  sdc_rpc_id=sdc_rpc_id)

    wiretap = builder.add_wiretap()
    sdc_rpc_origin >> wiretap.destination
    rpc_origin_pipeline = builder.build('SDC RPC origin pipeline')

    # Build the SDC RPC target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_str,
                                       stop_after_first_batch=True)

    sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_LISTENING_PORT}']
    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.set_attributes(sdc_rpc_connection=sdc_rpc_connection,
                                       sdc_rpc_id=sdc_rpc_id)

    dev_raw_data_source >> sdc_rpc_destination
    rpc_target_pipeline = builder.build('SDC RPC target pipeline')

    sdc_executor.add_pipeline(rpc_origin_pipeline, rpc_target_pipeline)

    # Run pipelines and assert data.
    sdc_executor.start_pipeline(rpc_origin_pipeline)
    sdc_executor.start_pipeline(rpc_target_pipeline).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(rpc_origin_pipeline, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(rpc_origin_pipeline)

    records_data = wiretap.output_records[0].field['text'].value
    assert raw_str == records_data


def test_write_to_another_pipeline_error_stage(sdc_builder, sdc_executor):
    """This test will test the Write to Another Pipeline error stage, which writes to an SDC RPC destination.
    We then check wiretap in a separate pipeline with an SDC RPC origin:

    Write to Another Pipeline error stage pipeline:
        dev_raw_data_source >> to_error

    SDC RPC origin pipeline:
        sdc_rpc_origin >> wiretap
    """

    # Create some silly Tour de France-themed test data.
    tdf_did_not_starts = [dict(name='Peter Sagan', reason='DQ'),
                          dict(name='Mark Cavendish', reason='Abandoned')]
    raw_data = ''.join(json.dumps(dns) for dns in tdf_did_not_starts)

    sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_LISTENING_PORT}']
    sdc_rpc_id = get_random_string(string.ascii_letters, 10)

    # Build the Write to Another Pipeline error stage pipeline.
    builder = sdc_builder.get_pipeline_builder()
    write_to_another_pipeline = builder.add_error_stage('Write to Another Pipeline')
    write_to_another_pipeline.set_attributes(sdc_rpc_connection=sdc_rpc_connection,
                                             sdc_rpc_id=sdc_rpc_id)

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    to_error = builder.add_stage('To Error')

    dev_raw_data_source >> to_error
    error_stage_pipeline = builder.build(title='Write to Another Pipeline error stage pipeline')

    # Build the SDC RPC origin pipeline.
    builder = sdc_builder.get_pipeline_builder()

    sdc_rpc_origin = builder.add_stage('SDC RPC', type='origin')
    sdc_rpc_origin.set_attributes(sdc_rpc_id=sdc_rpc_id,
                                  sdc_rpc_listening_port=SDC_RPC_LISTENING_PORT)

    wiretap = builder.add_wiretap()

    sdc_rpc_origin >> wiretap.destination
    sdc_rpc_origin_pipeline = builder.build(title='SDC RPC origin pipeline')

    sdc_executor.add_pipeline(error_stage_pipeline, sdc_rpc_origin_pipeline)

    sdc_executor.start_pipeline(sdc_rpc_origin_pipeline)
    sdc_executor.start_pipeline(error_stage_pipeline).wait_for_finished()

    sdc_executor.wait_for_pipeline_metric(sdc_rpc_origin_pipeline, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(sdc_rpc_origin_pipeline)

    assert [{key: value.value for key, value in record.field.items()}
            for record in wiretap.output_records] == tdf_did_not_starts
