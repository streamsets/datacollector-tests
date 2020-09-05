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

import logging
import pytest

from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

delta = 0.5


@pytest.mark.parametrize('ignore_records', [{False, True}])
def test_delay_stage(sdc_builder, sdc_executor, ignore_records):
    """
    Tests if there is a delay when a batch contains records or is empty.

    The pipeline:
        Dev Raw Data Source>>Delay>>Trash

    In the both cases there should be a delay.
    """

    expected_record_count = 2
    expected_delay = 2

    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source')
    data_source.data_format = 'JSON'
    data_source.stop_after_first_batch = True
    data_source.raw_data = '{"data": "abc"}{"data": "xyz"}'

    delay = builder.add_stage('Delay')
    delay.delay_between_batches = 2000
    delay.on_record_error = 'DISCARD'

    trash = builder.add_stage('Trash')

    if ignore_records:
        # Ignore all incoming records to this stage, this is to give to DelayProcessor an empty batch.
        delay.preconditions = ["${false}"]
        expected_record_count = 0

    data_source >> delay >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
    sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')

    stage = snapshot[delay.instance_name]
    assert expected_record_count == len(stage.output)
    if not ignore_records:
        assert 'abc' == stage.output[0].field['data']
        assert 'xyz' == stage.output[1].field['data']

    history = sdc_executor.get_pipeline_history(pipeline)
    assert expected_delay <= history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('mean') <= expected_delay + delta


@sdc_min_version('3.20.0')
def test_skip_delay_on_empty_batch_is_ignored_when_batches_are_not_empty(sdc_builder, sdc_executor):
    """
    Tests if there is a delay when a record batch and an event batch contain records and skip_delay_on_empty_batch
    is true.

    The pipeline:
        Dev Raw Data Source>>Delay1>>Trash1
                          E->Delay2>>Trash2

    We expect that the pipeline finishes with a delay of >= ~3 secs.

    NOTE: The case when there is no delay if batches are empty and skip_delay_on_empty_batch is true is checked in
    stage/configuration/test_skip_delay_on_empty_batch.py
    """

    expected_delay = 3

    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source')
    data_source.data_format = 'JSON'
    data_source.stop_after_first_batch = True
    data_source.raw_data = '{"data": "abc"}'
    data_source.event_data = '{"data": "error"}'

    delay1 = builder.add_stage('Delay')
    delay1.delay_between_batches = 2000
    delay1.skip_delay_on_empty_batch = True

    delay2 = builder.add_stage('Delay')
    delay2.delay_between_batches = 1000
    delay2.skip_delay_on_empty_batch = True

    trash1 = builder.add_stage('Trash')
    trash2 = builder.add_stage('Trash')

    data_source >> delay1 >> trash1
    data_source >= delay2
    delay2 >> trash2

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
    sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')

    stage1 = snapshot[delay1.instance_name]
    assert 1 == len(stage1.output)

    stage2 = snapshot[delay2.instance_name]
    assert 1 == len(stage2.output)

    assert 'abc' == stage1.output[0].field['data']
    assert 'error' == stage2.output[0].field['data']

    history = sdc_executor.get_pipeline_history(pipeline)
    assert expected_delay <= history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('mean') <= expected_delay + delta
