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
from time import sleep

from streamsets.testframework.markers import sdc_min_version
from streamsets.sdk.sdc_models import Metrics

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
    builder = sdc_builder.get_pipeline_builder()
    SLEEP_SCRIPT = "sleep(5*1000)"

    # Super simple cluster pipeline
    source = builder.add_stage('Dev Data Generator')

    groovy = builder.add_stage('Groovy Evaluator', type='processor')
    groovy.init_script = SLEEP_SCRIPT
    groovy.destroy_script = SLEEP_SCRIPT
    groovy.script = SLEEP_SCRIPT

    trash = builder.add_stage('Trash')

    source >> groovy >> trash
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    # Start the pipeline, it should take at least 5 seconds (since the sleep) and we check that at least once
    # we have seen the metrics we're looking for.
    sdc_executor.start_pipeline(pipeline, wait=False)

    count = 0
    while True:
        # TLKT-468: SDC object doesn't expose get_pipeline_metrics method
        metrics_json = sdc_executor.api_client.get_pipeline_metrics(pipeline.id)
        if metrics_json:
            metrics = Metrics(metrics_json)
            logger.info(f"Detected runtime gauge state {metrics.gauge('runner.0.gauge').value['state']}")
            if metrics.gauge('runner.0.gauge').value['state'] == 'Starting':
                count += 1

        status = sdc_executor.get_pipeline_status(pipeline).response.json()
        sleep(0.5)
        if status.get('status') == 'RUNNING':
            break

    assert count > 0

    sdc_executor.stop_pipeline(pipeline)


