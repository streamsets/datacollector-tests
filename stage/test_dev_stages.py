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
from time import sleep

import pytest

from testframework import sdc, sdc_api

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) # pylint: disable=C0103


@pytest.fixture(scope='module')
def data_collector():
    """Create data collector for the tests and tear it down after execution of all tests. """
    data_collector = sdc.DataCollector()
    data_collector.start()
    yield data_collector
    # After all tests or after an exception is encountered, stop and remove the SDC instance
    if data_collector.tear_down_on_exit:
        data_collector.tear_down()


@pytest.fixture(scope='module')
def pipeline(data_collector):
    """Create pipeline for the tests. """
    pipeline_builder = data_collector.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'

    trash = pipeline_builder.add_stage('Trash')
    pipeline_builder.add_error_stage('Discard')

    # Wire up the stages.
    dev_raw_data_source > trash

    pipeline = pipeline_builder.build()
    data_collector.add_pipeline(pipeline)
    yield pipeline


def test_pipeline_status(data_collector, pipeline):
    """For a running and a stopped pipeline,
       confirm that status returns appropriate values in both cases."""
    data_collector.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify running pipeline's status
    current_status = data_collector.api_client.get_pipeline_status(pipeline.id).response.json().get('status')
    assert current_status == 'RUNNING'

    # Stop the pipeline and verify pipeline's status
    data_collector.stop_pipeline(pipeline).wait_for_stopped()
    current_status = data_collector.api_client.get_pipeline_status(pipeline.id).response.json().get('status')
    assert current_status == 'STOPPED'


def test_pipeline_definitions(data_collector, pipeline):
    """For a running pipeline, confirm that definitions returns some values.
       Stop the pipeline and confirm that definitions return same values as before."""
    data_collector.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    running_pipeline_definitions = data_collector.api_client.get_definitions()
    assert running_pipeline_definitions is not None

    # Stop the pipeline and verify definitions do not change
    data_collector.stop_pipeline(pipeline).wait_for_stopped()
    stopped_pipeline_definitions = data_collector.api_client.get_definitions()
    assert stopped_pipeline_definitions is not None
    assert running_pipeline_definitions == stopped_pipeline_definitions


def test_pipeline_metrics(data_collector, pipeline):
    """For a running pipeline, confirm that metrics endpoint returns some values,
       which change after some time when again metrics are received,
       Stop the pipeline and confirm that metrics endpoint return empty."""
    data_collector.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    first_metrics_json = data_collector.api_client.get_pipeline_metrics(pipeline.id)
    assert first_metrics_json is not None
    sleep(15)
    second_metrics_json = data_collector.api_client.get_pipeline_metrics(pipeline.id)
    assert second_metrics_json is not None
    assert first_metrics_json != second_metrics_json

    data_collector.stop_pipeline(pipeline).wait_for_stopped()
    assert data_collector.api_client.get_pipeline_metrics(pipeline.id) == {}


def test_pipeline_snapshot(data_collector, pipeline):
    """For a running pipeline, confirm that snapshot returns expected values."""
    data_collector.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    snapshot = data_collector.capture_snapshot(pipeline).wait_for_finished().snapshot
    assert snapshot is not None
    snap_data = snapshot[pipeline.origin_stage.instance_name]
    assert len(snap_data.output) == 1
    assert snap_data.output[0].value['value']['emp_id']['value'] == '123456'

    data_collector.stop_pipeline(pipeline).wait_for_stopped()


def test_pipeline_preview(data_collector, pipeline):
    """Run preview and confirm that preview returns expected values
       and no issues are reported."""
    preview = data_collector.run_pipeline_preview(pipeline).wait_for_finished().preview
    assert preview is not None
    assert preview.issues.issues_count == 0
    preview_data = preview[pipeline.origin_stage.instance_name]
    assert len(preview_data.output) == 1
    assert preview_data.output[0].value['value']['emp_id']['value'] == '123456'


def test_invalid_execution_mode(data_collector, pipeline):
    """Set executionMode to invalid value for a pipeline,
       try starting it and confirm that it raises expected exception."""
    pipeline.configuration['executionMode'] = 'Invalid_Execution_Mode'
    pipeline.id = 'Invalid_Execution_Mode Pipeline'
    data_collector.add_pipeline(pipeline)

    with pytest.raises(sdc_api.StartError):
        data_collector.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)
