# Copyright 2020 StreamSets Inc.
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
import logging
import time

from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')

    return hook


# SDC-11777: provide way to easily see where a pipeline is when it is stuck in STARTING
@sdc_min_version('3.16.0')
def test_runner_metrics_for_init_and_destroy(sdc_builder, sdc_executor):
    """Ensure that we properly update metrics when the runner is in starting phase."""
    SCRIPT = "sleep(5*1000)"

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Super simple cluster pipeline
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator').set_attributes(init_script=SCRIPT,
                                                                                     destroy_script=SCRIPT,
                                                                                     script=SCRIPT)
    trash = pipeline_builder.add_stage('Trash')

    dev_data_generator >> groovy_evaluator >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    # Starting pipeline should take at least 5 seconds (because of the sleep script). We fail if we don't see
    # the metric we're looking for during that time.
    sdc_executor.start_pipeline(pipeline, wait=False)

    try:
        while sdc_executor.get_pipeline_status(pipeline).response.json().get('status') != 'RUNNING':
            metrics = sdc_executor.get_pipeline_metrics(pipeline)
            if metrics:
                logger.info('Detected runtime gauge state %s ...', metrics.gauge('runner.0.gauge').value['state'])
                if metrics.gauge('runner.0.gauge').value['state'] == 'Starting':
                    break
            time.sleep(0.5)
        else:
            pytest.fail('Pipeline reached RUNNING status without runner gauge state reaching "Starting"')
    finally:
        sdc_executor.stop_pipeline(pipeline)
