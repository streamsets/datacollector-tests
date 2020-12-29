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


@pytest.mark.parametrize('ignore_records', [False, True])
def test_delay_stage(sdc_builder, sdc_executor, ignore_records):
    """
    Tests if there is a delay when a batch contains records or is empty.

    The pipeline:
        Dev Raw Data Source >> Delay >> Wiretap

    In the both cases there should be a delay.
    """

    expected_record_count = 2
    expected_delay = 2

    input_data = '{"data": "abc"}{"data": "xyz"}'

    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                          stop_after_first_batch=True,
                                                                          raw_data=input_data)

    delay = builder.add_stage('Delay').set_attributes(delay_between_batches=2000, on_record_error='DISCARD')

    wiretap = builder.add_wiretap()

    if ignore_records:
        # Ignore all incoming records to this stage, this is to give to DelayProcessor an empty batch.
        delay.preconditions = ["${false}"]
        expected_record_count = 0

    data_source >> delay >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert expected_record_count == len(wiretap.output_records)
    if not ignore_records:
        assert 'abc' == wiretap.output_records[0].field['data']
        assert 'xyz' == wiretap.output_records[1].field['data']

    history = sdc_executor.get_pipeline_history(pipeline)
    pipeline_mean_delay = history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('mean')
    assert expected_delay <= pipeline_mean_delay <= expected_delay + delta


@sdc_min_version('3.19.0')
def test_skip_delay_on_empty_batch_is_ignored_when_batches_are_not_empty(sdc_builder, sdc_executor):
    """
    Tests if there is a delay when a record batch and an event batch contain records and skip_delay_on_empty_batch
    is true.

    The pipeline:
        Dev Raw Data Source >> Delay1 >> Wiretap
                            >= Delay2 >> Wiretap

    We expect that the pipeline finishes with a delay of >= ~3 secs.

    NOTE: The case when there is no delay if batches are empty and skip_delay_on_empty_batch is true is checked in
    stage/configuration/test_skip_delay_on_empty_batch.py
    """

    expected_delay = 3

    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                          stop_after_first_batch=True,
                                                                          raw_data='{"data": "abc"}',
                                                                          event_data='{"data": "error"}')

    delay1 = builder.add_stage('Delay').set_attributes(delay_between_batches=2000, skip_delay_on_empty_batch=True)

    delay2 = builder.add_stage('Delay').set_attributes(delay_between_batches=1000, skip_delay_on_empty_batch=True)

    records_wiretap = builder.add_wiretap()
    events_wiretap = builder.add_wiretap()

    data_source >> delay1 >> records_wiretap.destination
    data_source >= delay2
    delay2 >> events_wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert 1 == len(records_wiretap.output_records)
    assert 1 == len(events_wiretap.output_records)

    assert 'abc' == records_wiretap.output_records[0].field['data']
    assert 'error' == events_wiretap.output_records[0].field['data']

    history = sdc_executor.get_pipeline_history(pipeline)
    pipeline_mean_delay = history.latest.metrics.timer('pipeline.batchProcessing.timer')._data.get('mean')
    assert expected_delay <= pipeline_mean_delay <= expected_delay + delta