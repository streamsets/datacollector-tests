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

JSONP_01_ERROR = "JSONP_01"

def test_field_to_parse(sdc_builder, sdc_executor):
    """
    By definition the JSON parser processor expects a serialized JSON object in a string field.
    We test here that JSON parser extracts the json object from the field specified in the field_to_parse field.

    The test pipeline is as follows:

    Dev Random Record Source >> JSON Generator >> JSON Parser >> Wiretap

    """

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Random Record Source')
    source.fields_to_generate = 'a,b,c'
    source.max_records_to_generate = 1

    generator = builder.add_stage('JSON Generator', type='processor')
    generator.field_to_serialize = '/'
    generator.target_field = '/json'

    parser = builder.add_stage('JSON Parser', type='processor')
    parser.field_to_parse = '/json'
    parser.target_field = '/json'

    wiretap = builder.add_wiretap()

    source >> generator >> parser >> wiretap.destination

    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)

    try:
        status = sdc_executor.start_pipeline(pipeline)
        status.wait_for_pipeline_output_records_count(2)

        assert len(wiretap.error_records) == 0
        assert len(wiretap.output_records) == 1

        obj = {
            'a': wiretap.output_records[0].field['a'].value,
            'b': wiretap.output_records[0].field['b'].value,
            'c': wiretap.output_records[0].field['c'].value
        }
        assert obj == wiretap.output_records[0].field['json']

    finally:
        sdc_executor.stop_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_control_characters': False}, {'ignore_control_characters': True}])
def test_ignore_control_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    raw_data = """
        [
          { "json": "{\\"a\\":\\"A\\"}" },
          { "json": null },
          { "json": "{" },
          { "json": "" }
        ]
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    json_parser = pipeline_builder.add_stage('JSON Parser', type='processor')
    json_parser.set_attributes(field_to_parse='/json',
                               target_field='/parsed',
                               ignore_control_characters=False,
                               on_record_error=stage_attributes['on_record_error'])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> json_parser >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        if stage_attributes['on_record_error'] == 'STOP_PIPELINE':
            with pytest.raises(Exception) as ex:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert JSONP_01_ERROR in str(ex.value)
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            output_records = wiretap.output_records
            error_records = wiretap.error_records
            assert len(output_records) == 2
            assert output_records[0].field['json'] == '{"a":"A"}'
            assert output_records[0].field['parsed'] == {'a': 'A'}
            assert output_records[1].field['json'] == ''
            assert 'parsed' not in output_records[1].field

            if stage_attributes['on_record_error'] == 'DISCARD':
                assert len(error_records) == 0
            elif stage_attributes['on_record_error'] == 'TO_ERROR':
                assert len(error_records) == 2
                assert error_records[0].field['json'].value is None
                assert error_records[1].field['json'] == '{'
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

