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
from streamsets.sdk.sdc_api import StartError

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-cdh_5_15-lib')

    return hook


# SDC-5989
@sdc_min_version('3.6.1')
def test_start_and_capture_snapshot(sdc_builder, sdc_executor):
    """Verify that when starting pipeline with a snapshot, the pipeline properly
    fails and ends with START_ERROR stage.
    """
    builder = sdc_builder.get_pipeline_builder()

    # Super simple cluster pipeline that will fail if it will get to validation
    source = builder.add_stage('Hadoop FS', type='origin')
    source.data_format = 'TEXT'
    source.input_paths.append('/i/do/not/exists')

    trash = builder.add_stage('Trash')

    source >> trash

    pipeline = builder.build()
    pipeline.configuration['executionMode'] = 'CLUSTER_BATCH'

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.capture_snapshot(pipeline, start_pipeline=True)
    except StartError:
        # We are expecting error that we will further inspect via pipeline status
        pass
    else:
        assert False

    sdc_executor.get_pipeline_status(pipeline).wait_for_status('START_ERROR', ignore_errors=True)
    status = sdc_executor.get_pipeline_status(pipeline).response.json()
    assert 'Cluster mode does not support snapshots.' in status['message']

