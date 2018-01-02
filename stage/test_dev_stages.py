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
from time import sleep

import pytest

from testframework import sdc_api
from testframework.markers import parcelpackaging, rpmpackaging
from testframework.utils import Version

logger = logging.getLogger(__name__)

pytestmark = [parcelpackaging, rpmpackaging]


@pytest.fixture(scope='module')
def pipeline(sdc_builder, sdc_executor):
    """Create pipeline for the tests. """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'

    trash = pipeline_builder.add_stage('Trash')

    # Wire up the stages.
    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    yield pipeline


def test_pipeline_status(sdc_executor, pipeline):
    """For a running and a stopped pipeline,
       confirm that status returns appropriate values in both cases."""
    sdc_executor.start_pipeline(pipeline)

    # Verify running pipeline's status
    current_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
    assert current_status == 'RUNNING'

    # Stop the pipeline and verify pipeline's status
    sdc_executor.stop_pipeline(pipeline)
    current_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
    assert current_status == 'STOPPED'


def test_pipeline_definitions(sdc_executor, pipeline):
    """For a running pipeline, confirm that definitions returns some values.
       Stop the pipeline and confirm that definitions return same values as before."""
    sdc_executor.start_pipeline(pipeline)

    running_pipeline_definitions = sdc_executor.api_client.get_definitions()
    assert running_pipeline_definitions is not None

    # Stop the pipeline and verify definitions do not change
    sdc_executor.stop_pipeline(pipeline)
    stopped_pipeline_definitions = sdc_executor.api_client.get_definitions()
    assert stopped_pipeline_definitions is not None
    assert running_pipeline_definitions == stopped_pipeline_definitions


def test_pipeline_metrics(sdc_executor, pipeline):
    """For a running pipeline, confirm that metrics endpoint returns some values,
       which change after some time when again metrics are received,
       Stop the pipeline and confirm that metrics endpoint return empty."""
    sdc_executor.start_pipeline(pipeline)

    first_metrics_json = sdc_executor.api_client.get_pipeline_metrics(pipeline.id)
    assert first_metrics_json is not None
    sleep(15)
    second_metrics_json = sdc_executor.api_client.get_pipeline_metrics(pipeline.id)
    assert second_metrics_json is not None
    assert first_metrics_json != second_metrics_json

    sdc_executor.stop_pipeline(pipeline)
    assert sdc_executor.api_client.get_pipeline_metrics(pipeline.id) == {}


def test_pipeline_snapshot(sdc_executor, pipeline):
    """For a running pipeline, confirm that snapshot returns expected values."""
    sdc_executor.start_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline).snapshot
    assert snapshot is not None
    snap_data = snapshot[pipeline.origin_stage.instance_name]
    assert len(snap_data.output) == 1
    assert snap_data.output[0].value['value']['emp_id']['value'] == '123456'

    sdc_executor.stop_pipeline(pipeline)


def test_pipeline_preview(sdc_executor, pipeline):
    """Run preview and confirm that preview returns expected values
       and no issues are reported."""
    preview = sdc_executor.run_pipeline_preview(pipeline).preview
    assert preview is not None
    assert preview.issues.issues_count == 0
    preview_data = preview[pipeline.origin_stage.instance_name]
    assert len(preview_data.output) == 1
    assert preview_data.output[0].value['value']['emp_id']['value'] == '123456'


def test_invalid_execution_mode(sdc_executor, pipeline):
    """Set executionMode to invalid value for a pipeline,
       try starting it and confirm that it raises expected exception."""
    pipeline.configuration['executionMode'] = 'Invalid_Execution_Mode'
    pipeline.id = 'Invalid_Execution_Mode Pipeline'

    try:
        # Do a version check since execution_mode handling changed starting in the 2.7.0.0 version.
        if Version(sdc_executor.version) >= Version('2.7.0.0'):
            with pytest.raises(sdc_api.InternalServerError):
                sdc_executor.dump_log_on_error = False
                sdc_executor.add_pipeline(pipeline)
        else:
            sdc_executor.add_pipeline(pipeline)
            with pytest.raises(sdc_api.StartError):
                sdc_executor.dump_log_on_error = False
                sdc_executor.start_pipeline(pipeline)
    finally:
        sdc_executor.dump_log_on_error = True
