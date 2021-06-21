# Copyright 2021 StreamSets Inc.
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
import math

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import sdc_min_version


@stub
def test_batch_size(sdc_builder, sdc_executor):
    pass


@stub
def test_delay_between_batches(sdc_builder, sdc_executor):
    pass


@stub
def test_event_name(sdc_builder, sdc_executor):
    pass


@stub
def test_fields_to_generate(sdc_builder, sdc_executor):
    pass


@stub
def test_header_attributes(sdc_builder, sdc_executor):
    pass


@stub
def test_number_of_threads(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'root_field_type': 'LIST_MAP'}, {'root_field_type': 'MAP'}])
def test_root_field_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@sdc_min_version('4.1.0')
@pytest.mark.parametrize("stage_attributes", [
    ({"batch_size": 10, "records_to_be_generated": 12, "number_of_threads": 1}),
    ({"batch_size": 5, "records_to_be_generated": 15, "number_of_threads": 1}),
    ({"batch_size": 10, "records_to_be_generated": 5, "number_of_threads": 1}),
    ({"batch_size": 10, "records_to_be_generated": 147, "number_of_threads": 4}),
    ({"batch_size": 10, "records_to_be_generated": 330, "number_of_threads": 4}),
    ({"batch_size": 1000, "records_to_be_generated": 528, "number_of_threads": 4})
])
def test_number_of_records(sdc_builder, sdc_executor, stage_attributes):
    """Assert the Dev data generator is generating the proper number of records arranged properly in batches
    depending on the `records_to_be_generated` parameter """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Data Generator').set_attributes(**stage_attributes)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)

    # This pipeline is gonna finish as soon as all the required records are generated
    sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED')
    history = sdc_executor.get_pipeline_history(pipeline)

    expected_batch_count = math.ceil(stage_attributes['records_to_be_generated'] / stage_attributes['batch_size'])
    expected_number_of_records = stage_attributes['records_to_be_generated']
    assert history.latest.metrics.counter('pipeline.batchCount.counter').count == expected_batch_count
    assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == expected_number_of_records
