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

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'output_type': 'BYTE_ARRAY'}, {'output_type': 'STRING'}])
def test_output_type(sdc_builder, sdc_executor, stage_attributes):
    """Test Data Generator processor. The pipeline would look like:

        dev_raw_data_source >> data_generator >> wiretap
    """
    raw_data = '{ "contact": { "name": "Jane Smith", "age":30 } }'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    # Set output_type
    data_generator = pipeline_builder.add_stage('Data Generator', type='processor')
    data_generator.set_attributes(target_field='/', output_type=stage_attributes['output_type'], data_format='JSON')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> data_generator >> wiretap.destination

    try:
        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        assert len(wiretap.output_records) == 1
        items = [record.field for record in wiretap.output_records]

        if stage_attributes['output_type'] == 'BYTE_ARRAY':
            expected_data = [b'{"contact":{"name":"Jane Smith","age":30}}']
        elif stage_attributes['output_type'] == 'STRING':
            expected_data = ['{"contact":{"name":"Jane Smith","age":30}}']

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_target_field(sdc_builder, sdc_executor):
    pass

