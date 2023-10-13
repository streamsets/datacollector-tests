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
# limitations under the License

import pytest

from streamsets.testframework.decorators import stub


def test_field_to_parse(sdc_builder, sdc_executor):
    raw_data = '{"key": "value"}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Set output_type
    data_parser = pipeline_builder.add_stage('Data Parser', type='processor')
    data_parser.set_attributes(field_to_parse='/text',
                               target_field='/',
                               multiple_values_behavior='FIRST_ONLY',
                               data_format='JSON')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> data_parser >> wiretap.destination

    try:
        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        output_records = wiretap.output_records
        assert len(output_records) == 1
        expected_data = {"key":"value"}
        assert output_records[0].field == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'ALL_AS_LIST'},
                                              {'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes):
    raw_data = """{"id": 1} {"name": "Mac"}
                  {"city":["New York","San Francisco","Chicago"]}"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Set output_type
    data_parser = pipeline_builder.add_stage('Data Parser', type='processor')
    data_parser.set_attributes(field_to_parse='/text',
                               target_field='/',
                               multiple_values_behavior=stage_attributes['multiple_values_behavior'],
                               data_format='JSON')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> data_parser >> wiretap.destination

    try:
        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        output_records = wiretap.output_records
        if stage_attributes['multiple_values_behavior'] == 'ALL_AS_LIST':
            assert len(output_records) == 2
            assert output_records[0].field == [{"id":1},{"name":"Mac"}]
            assert output_records[1].field == [{"city":["New York","San Francisco","Chicago"]}]
        elif stage_attributes['multiple_values_behavior'] == 'FIRST_ONLY':
            assert len(output_records) == 2
            assert output_records[0].field == {"id":1}
            assert output_records[1].field == {"city":["New York","San Francisco","Chicago"]}
        elif stage_attributes['multiple_values_behavior'] == 'SPLIT_INTO_MULTIPLE_RECORDS':
            assert len(output_records) == 3
            assert output_records[0].field == {"id":1}
            assert output_records[1].field == {"name":"Mac"}
            assert output_records[2].field == {"city":["New York","San Francisco","Chicago"]}
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_target_field(sdc_builder, sdc_executor):
    pass

