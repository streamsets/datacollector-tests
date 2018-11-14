# Copyright 2018 StreamSets Inc.
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

import pytest

from streamsets.testframework.markers import sdc_min_version


# SDC-6217
@sdc_min_version('3.6.1')
def test_capture_same_snapshot_twice(sdc_builder, sdc_executor):
    """Verify that creating the same snapshot twice will properly fail."""
    builder = sdc_builder.get_pipeline_builder()

    # Super simple cluster pipeline
    source = builder.add_stage('Dev Data Generator')
    trash = builder.add_stage('Trash')
    source >> trash

    pipeline = builder.build(title='Capture same snapshot twice')
    sdc_executor.add_pipeline(pipeline)

    # First snapshot needs to succeed
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, snapshot_name='name').snapshot
    sdc_executor.stop_pipeline(pipeline)
    assert snapshot is not None

    # Trying to capture the same snapshot second time should fail
    sdc_executor.capture_snapshot(pipeline, snapshot_name='name', start_pipeline=True, wait=False)
    sdc_executor.get_pipeline_status(pipeline).wait_for_status('START_ERROR', ignore_errors=True)

    status = sdc_executor.get_pipeline_status(pipeline).response.json()
    assert 'CONTAINER_0606' in status['message']
    assert 'Snapshot already exists' in status['message']

