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

import json
import logging
from time import sleep
from os.path import dirname, join

import pytest

from testframework import sdc, sdc_api, sdc_models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) # pylint: disable=C0103


@pytest.fixture(scope='module')
def dc(args, pipeline): # pylint: disable=C0103
    """Create data collector for the tests and tear it down after execution of all tests. """
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_pipeline(pipeline)
    dc.start()
    yield dc
    # After all tests or after an exception is encountered, stop and remove the SDC instance
    dc.tear_down()


@pytest.fixture(scope='module')
def pipeline(): # pylint: disable=C0103
    """Create pipeline for the tests. """
    pipeline = sdc_models.Pipeline(join(dirname(__file__),
                                        'pipelines',
                                        'dev_raw_data_source_to_trash.json'))
    yield pipeline


def test_pipeline_status(dc, pipeline):
    """For a running and a stopped pipeline,
       confirm that status returns appropriate values in both cases."""
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    # Verify running pipeline's status
    current_status = dc.api_client.get_pipeline_status(pipeline.id).response.json().get('status')
    assert current_status == 'RUNNING'

    # Stop the pipeline and verify pipeline's status
    dc.stop_pipeline(pipeline).wait_for_stopped()
    current_status = dc.api_client.get_pipeline_status(pipeline.id).response.json().get('status')
    assert current_status == 'STOPPED'


def test_pipeline_definitions(dc, pipeline):
    """For a running pipeline, confirm that definitions returns some values.
       Stop the pipeline and confirm that definitions return same values as before."""
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    running_pipeline_definitions = dc.api_client.get_definitions()
    assert running_pipeline_definitions is not None

    # Stop the pipeline and verify definitions do not change
    dc.stop_pipeline(pipeline).wait_for_stopped()
    stopped_pipeline_definitions = dc.api_client.get_definitions()
    assert stopped_pipeline_definitions is not None
    assert running_pipeline_definitions == stopped_pipeline_definitions


def test_pipeline_metrics(dc, pipeline):
    """For a running pipeline, confirm that metrics endpoint returns some values,
       which change after some time when again metrics are received,
       Stop the pipeline and confirm that metrics endpoint return empty."""
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    first_metrics_json = dc.api_client.get_pipeline_metrics(pipeline.id)
    assert first_metrics_json is not None
    sleep(15)
    second_metrics_json = dc.api_client.get_pipeline_metrics(pipeline.id)
    assert second_metrics_json is not None
    assert first_metrics_json != second_metrics_json

    dc.stop_pipeline(pipeline).wait_for_stopped()
    assert dc.api_client.get_pipeline_metrics(pipeline.id) == {}


def test_pipeline_snapshot(dc, pipeline):
    """For a running pipeline, confirm that snapshot returns expected values."""
    dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)

    snapshot = dc.capture_snapshot(pipeline, 'kirtiSnapshot').wait_for_finished().snapshot
    assert snapshot is not None
    snap_data = snapshot['DevRawDataSource_01']
    assert len(snap_data.output) == 2
    assert snap_data.output[0].value['value']['emp_name']['value'] == 'sdc1'
    assert snap_data.output[0].value['value']['emp_id']['value'] == '123456'
    assert snap_data.output[1].value['value']['emp_name']['value'] == 'sdc2'

    dc.stop_pipeline(pipeline).wait_for_stopped()

def test_pipeline_preview(dc, pipeline):
    """Run preview and confirm that preview returns expected values
       and no issues are reported."""
    preview = dc.run_pipeline_preview(pipeline).wait_for_finished().preview
    assert preview is not None
    assert preview.issues.issues_count == 0
    preview_data = preview['DevRawDataSource_01']
    assert len(preview_data.output) == 2
    assert preview_data.output[0].value['value']['emp_name']['value'] == 'sdc1'
    assert preview_data.output[0].value['value']['emp_id']['value'] == '123456'
    assert preview_data.output[1].value['value']['emp_name']['value'] == 'sdc2'

def test_invalid_execution_mode(dc, pipeline):
    """Set executionMode to invalid value for a pipeline,
       try starting it and confirm that it raises expected exception."""
    pipeline.configuration['executionMode'] = 'Invalid_Execution_Mode'
    pipeline.id = 'Invalid_Execution_Mode Pipeline'
    dc.add_pipeline(pipeline)

    with pytest.raises(sdc_api.StartError):
        dc.start_pipeline(pipeline).wait_for_status(status='RUNNING', timeout_sec=300)
