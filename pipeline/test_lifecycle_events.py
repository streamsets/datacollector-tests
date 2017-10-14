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

import logging

import pytest

from testframework.markers import sdc_min_version
from testframework import sdc_api

logger = logging.getLogger(__name__)

# Port for SDC RPC stages to exchange error records
SDC_RPC_PORT = 20000
SDC_RPC_ID = 'lifecycle'

@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
    return hook


@pytest.fixture(scope='module')
def sdc_executor_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
    return hook


@pytest.fixture(scope='function')
def generator_trash_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    trash = builder.add_stage('Trash')

    dev_data_generator >> trash

    yield builder


@pytest.fixture(scope='function')
def generator_finisher_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    finisher = builder.add_stage('Pipeline Finisher Executor')

    dev_data_generator >> finisher

    yield builder


@pytest.fixture(scope='function')
def generator_failure_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    jython = builder.add_stage('Jython Evaluator')
    jython.script = '1 / 0' # ~ throw exception and stop the pipeline

    trash = builder.add_stage('Trash')

    dev_data_generator >> jython >> trash

    yield builder


@pytest.fixture(scope='function')
def successful_receiver_pipeline(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('SDC RPC', type='origin')
    origin.rpc_port = SDC_RPC_PORT
    origin.rpc_id = SDC_RPC_ID

    trash = builder.add_stage('Trash')

    origin >> trash

    yield builder.build('Succeeding Lifecycle Receiver')


@pytest.fixture(scope='function')
def failing_receiver_pipeline(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('SDC RPC', type='origin')
    origin.rpc_port = SDC_RPC_PORT
    origin.rpc_id = SDC_RPC_ID

    jython = builder.add_stage('Jython Evaluator')
    jython.script = '1 / 0' # ~ throw exception and stop the pipeline
    trash = builder.add_stage('Trash')

    origin >> jython >> trash

    pipeline = builder.build('Failing Lifecycle Receiver')
    pipeline.configuration['shouldRetry'] = False

    yield pipeline


@sdc_min_version('2.7.0.0')
def test_start_event(generator_trash_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event on pipeline start."""
    start_stage = generator_trash_builder.add_start_event_stage('Write to Another Pipeline')
    start_stage.sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_PORT}']
    start_stage.sdc_rpc_id = SDC_RPC_ID

    start_event_pipeline = generator_trash_builder.build('Start Event')

    sdc_executor.add_pipeline(start_event_pipeline, successful_receiver_pipeline)

    try:
        # Since there will be exactly one event generated we need to make sure that:
        # * We catch the first batch on receiver side otherwise we miss the event
        # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start
        # * We block on the snapshot after generating pipeline started otherwise the snapshot won't return
        snapshot_command = sdc_executor.capture_snapshot(successful_receiver_pipeline, start_pipeline=True,
                                                         wait=False)
        sdc_executor.start_pipeline(start_event_pipeline)

        # And validate that the event arrived to the receiver pipeline
        snapshot = snapshot_command.wait_for_finished().snapshot
        record = snapshot[successful_receiver_pipeline.origin_stage].output[0]

        assert record != None
        assert record.header['sdc.event.type'] == 'pipeline-start'
        assert record.value['value']['user']['value'] == 'admin'

    finally:
        sdc_executor.stop_pipeline(successful_receiver_pipeline)
        sdc_executor.stop_pipeline(start_event_pipeline)


@sdc_min_version('2.7.0.0')
def test_stop_event_user_action(generator_trash_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event when pipeline is stopped by user."""
    stop_stage = generator_trash_builder.add_stop_event_stage('Write to Another Pipeline')
    stop_stage.sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_PORT}']
    stop_stage.sdc_rpc_id = SDC_RPC_ID

    stop_event_pipeline = generator_trash_builder.build('Stop Event - User Action')

    sdc_executor.add_pipeline(stop_event_pipeline, successful_receiver_pipeline)

    try:
        # Since there will be exactly one event generated we need to make sure that:
        # * We catch the first batch on receiver side otherwise we miss the event
        # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start
        # * We block on the snapshot after generating pipeline started otherwise the snapshot won't return
        snapshot_command = sdc_executor.capture_snapshot(successful_receiver_pipeline, start_pipeline=True,
                                                         wait=False)
        sdc_executor.start_pipeline(stop_event_pipeline)
        sdc_executor.stop_pipeline(stop_event_pipeline)

        # And validate that the event arrived to the receiver pipeline
        snapshot = snapshot_command.wait_for_finished().snapshot

        record = snapshot[successful_receiver_pipeline.origin_stage].output[0]

        assert record != None
        assert record.header['sdc.event.type'] == 'pipeline-stop'
        assert record.value['value']['reason']['value'] == 'USER_ACTION'

    finally:
        sdc_executor.stop_pipeline(successful_receiver_pipeline)


@sdc_min_version('2.7.0.0')
def test_stop_event_finished(generator_finisher_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event when pipeline finishes."""
    stop_stage = generator_finisher_builder.add_stop_event_stage('Write to Another Pipeline')
    stop_stage.sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_PORT}']
    stop_stage.sdc_rpc_id = SDC_RPC_ID

    stop_event_pipeline = generator_finisher_builder.build('Stop Event - Finished')

    sdc_executor.add_pipeline(stop_event_pipeline, successful_receiver_pipeline)

    try:
        # Since there will be exactly one event generated we need to make sure that:
        # * We catch the first batch on receiver side otherwise we miss the event
        # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start
        # * We block on the snapshot after generating pipeline started otherwise the snapshot won't return
        snapshot_command = sdc_executor.capture_snapshot(successful_receiver_pipeline, start_pipeline=True,
                                                         wait=False)
        sdc_executor.start_pipeline(stop_event_pipeline)

        # And validate that the event arrived to the receiver pipeline
        snapshot = snapshot_command.wait_for_finished().snapshot

        record = snapshot[successful_receiver_pipeline.origin_stage].output[0]

        assert record != None
        assert record.header['sdc.event.type'] == 'pipeline-stop'
        assert record.value['value']['reason']['value'] == 'FINISHED'

    finally:
        sdc_executor.stop_pipeline(successful_receiver_pipeline)


@sdc_min_version('2.7.0.0')
def test_stop_event_failure(generator_failure_builder, successful_receiver_pipeline, sdc_executor):
    """ Validate that we properly generate and process event when pipeline crashes."""
    stop_stage = generator_failure_builder.add_stop_event_stage('Write to Another Pipeline')
    stop_stage.sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_PORT}']
    stop_stage.sdc_rpc_id = SDC_RPC_ID

    stop_event_pipeline = generator_failure_builder.build('Stop Event - Failure')
    stop_event_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(stop_event_pipeline, successful_receiver_pipeline)

    try:
        # Since there will be exactly one event generated we need to make sure that:
        # * We catch the first batch on receiver side otherwise we miss the event
        # * The receiver pipeline is 'RUNNING' otherwise event generating pipeline will fail to start
        # * We block on the snapshot after generating pipeline started otherwise the snapshot won't return
        snapshot_command = sdc_executor.capture_snapshot(successful_receiver_pipeline, start_pipeline=True,
                                                         wait=False)
        sdc_executor.start_pipeline(stop_event_pipeline)

        # And validate that the event arrived to the receiver pipeline
        snapshot = snapshot_command.wait_for_finished().snapshot

        record = snapshot[successful_receiver_pipeline.origin_stage].output[0]

        assert record != None
        assert record.header['sdc.event.type'] == 'pipeline-stop'
        assert record.value['value']['reason']['value'] == 'FAILURE'

    finally:
        sdc_executor.stop_pipeline(successful_receiver_pipeline)


@sdc_min_version('2.7.0.0')
def test_start_event_handler_failure(generator_trash_builder, failing_receiver_pipeline, sdc_executor):
    """ Validate that failure to process start event will terminate the pipeline."""
    start_stage = generator_trash_builder.add_start_event_stage('Write to Another Pipeline')
    start_stage.sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_PORT}']
    start_stage.sdc_rpc_id = SDC_RPC_ID

    start_event_pipeline = generator_trash_builder.build('Start Event: Handler Failure')
    start_event_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(start_event_pipeline, failing_receiver_pipeline)

    # Start the event handling pipeline
    sdc_executor.start_pipeline(failing_receiver_pipeline, wait=False)

    # Start the actual event generating pipeline
    sdc_executor.start_pipeline(start_event_pipeline, wait=False)

    # Which should kill the receiver pipeline
    sdc_executor.get_pipeline_status(failing_receiver_pipeline).wait_for_status('RUN_ERROR', ignore_errors=True)

    # And that in turns will also kill the event generating pipeline
    sdc_executor.get_pipeline_status(start_event_pipeline).wait_for_status('START_ERROR', ignore_errors=True)

    # Validate history is as expected
    history = sdc_executor.pipeline_history(start_event_pipeline)
    entry = history.entries[0]
    assert entry['status'] == 'START_ERROR'


@sdc_min_version('2.7.0.0')
def test_stop_event_handler_failure(generator_trash_builder, failing_receiver_pipeline, sdc_executor):
    """ Validate that failure to process stop event will terminate the pipeline."""
    stop_stage = generator_trash_builder.add_stop_event_stage('Write to Another Pipeline')
    stop_stage.sdc_rpc_connection = [f'{sdc_executor.server_host}:{SDC_RPC_PORT}']
    stop_stage.sdc_rpc_id = SDC_RPC_ID

    stop_event_pipeline = generator_trash_builder.build('Stop Event: Handler Failure')
    stop_event_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(stop_event_pipeline, failing_receiver_pipeline)

    # Start the event handling pipeline
    sdc_executor.start_pipeline(failing_receiver_pipeline)

    # Start the actual event generating pipeline
    sdc_executor.start_pipeline(stop_event_pipeline)
    sdc_executor.stop_pipeline(stop_event_pipeline, wait=False)

    # Which should kill the receiver pipeline
    sdc_executor.get_pipeline_status(failing_receiver_pipeline).wait_for_status('RUNNING_ERROR', ignore_errors=True)

    # And that in turns will also kill the event generating pipeline
    sdc_executor.get_pipeline_status(stop_event_pipeline).wait_for_status('STOP_ERROR', ignore_errors=True)

    # Validate history is as expected
    history = sdc_executor.pipeline_history(stop_event_pipeline)
    entry = history.entries[0]
    assert entry['status'] == 'STOP_ERROR'
