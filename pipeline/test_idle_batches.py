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

import logging
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


@sdc_min_version('3.2.0.0')
def test_idle_batches_are_generated(sdc_builder, sdc_executor):
    """Validate that framework will properly create idle batches when runners are not used for the configured time."""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator').set_attributes(delay_between_batches=5000,
                                                                                         number_of_threads=2)
    trash = pipeline_builder.add_stage('Trash')
    dev_data_generator >> trash

    pipeline = pipeline_builder.build()
    pipeline.configuration['runnerIdleTIme'] = 1 # Dima note: the capitalization of this config is hurting my heart.
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 10_000, timeout_sec=60)
    metrics = sdc_executor.get_pipeline_metrics(pipeline)
    sdc_executor.stop_pipeline(pipeline)

    # Verify that we've generated at least 10 normal batches (batch size is 1k and we're waiting on at least 10k records
    assert metrics.pipeline.data_batch_count >= 10

    # Batch wait time is 5 seconds, the idle time is 1 second and we have two threads (4 * 1 * 2)
    assert metrics.pipeline.idle_batch_count >= 8
