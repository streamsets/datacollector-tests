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

import json
import logging
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

@sdc_min_version('3.2.0.0')
def test_idle_batches_are_generated(sdc_builder, sdc_executor):
    """Validate that framework will properly create "idle batches" when runners are
    not used for the configured time."""
    builder = sdc_builder.get_pipeline_builder()

    generator = builder.add_stage('Dev Data Generator')
    generator.delay_between_batches = 5000
    generator.number_of_threads = 2

    trash = builder.add_stage('Trash')

    generator >> trash

    pipeline = builder.build('Generate Idle Batches')
    pipeline.configuration['runnerIdleTIme'] = 1
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(10000, timeout_sec=60)
    sdc_executor.stop_pipeline(pipeline)

    status = sdc_executor.get_pipeline_status(pipeline).response.json()
    metrics = json.loads(status['metrics'])
    runtimeStats = metrics['gauges']['RuntimeStatsGauge.gauge']['value']

    # Verify that we've generated at least 10 normal batches (batch size is 1k and we're waiting on at least 10k records
    assert runtimeStats['batchCount'] >= 10

    # Batch wait time is 5 seconds, the idle time is 1 second and we have two threads (4 * 1 * 2)
    assert runtimeStats['idleBatchCount'] >= 8
