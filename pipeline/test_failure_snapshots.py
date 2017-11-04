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

import pytest

from testframework.markers import sdc_min_version

@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')
    return hook


@pytest.fixture(scope='module')
def sdc_executor_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')
    return hook


@pytest.fixture(scope='function')
def pipeline_builder(sdc_builder):
    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    groovy = builder.add_stage('Groovy Evaluator')
    groovy.script = '1/0' # Script that will always throw Exception and thus terminate the pipeline
    trash = builder.add_stage('Trash')

    dev_data_generator >> groovy >> trash

    yield builder


@sdc_min_version('3.0.0.0')
def test_failure_snapshots_off(pipeline_builder, sdc_executor):
    """Ensure that no snapshots will be created when the failure snapshots are off."""
    pipeline = pipeline_builder.build('Failure Snapshots OFF')
    pipeline.configuration['shouldCreateFailureSnapshot'] = False
    pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_status('RUN_ERROR')

    snapshots = sdc_executor.get_snapshots(pipeline)

    assert len(snapshots) == 0


@sdc_min_version('3.0.0.0')
def test_failure_snapshots_on(pipeline_builder, sdc_executor):
    """Ensure that snapshot will be created when the failure snapshots are on."""
    pipeline = pipeline_builder.build('Failure Snapshots ON')
    pipeline.configuration['shouldCreateFailureSnapshot'] = True
    pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_status('RUN_ERROR')

    snapshots = sdc_executor.get_snapshots(pipeline)

    assert len(snapshots) == 1
