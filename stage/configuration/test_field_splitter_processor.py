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

import json

import pytest
from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'too_many_splits': 'TO_LIST'}])
def test_field_for_remaining_splits(sdc_builder, sdc_executor, stage_attributes):
    pass


def test_field_to_split(sdc_builder, sdc_executor):
    DATA = dict(name='Al Gore', birthplace='Washington, D.C.')
    EXPECTED_SPLIT_DATA = dict(fieldSplit1='Al', fieldSplit2='Gore', birthplace='Washington, D.C.')

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.stop_after_first_batch = True
    field_splitter = pipeline_builder.add_stage('Field Splitter').set_attributes(field_to_split='/name',
                                                                                 new_split_fields=['/fieldSplit1',
                                                                                                   '/fieldSplit2'],
                                                                                 separator='[ ]')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_splitter >> wiretap.destination
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        record = wiretap.output_records[0]
        assert record.field == EXPECTED_SPLIT_DATA
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
def test_new_split_fields(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('stage_attributes', [{'not_enough_splits': 'CONTINUE'}, {'not_enough_splits': 'TO_ERROR'}])
def test_not_enough_splits(sdc_builder, sdc_executor, stage_attributes):
    raw_data = """
        [ { "line": "a b"
          },
          { "line": "a b c"
          },
          { "line": "a"
          },
          { "line": ""
          },
          { "line": " "
          },
          { "line": null
          }
        ]
    """
    raw_list = json.loads(raw_data)
    separator = "\\s"
    split_fields = ['/a','/b']

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_splitter = pipeline_builder.add_stage('Field Splitter')
    field_splitter.set_attributes(field_to_split=f'/line', new_split_fields=split_fields,
                                  not_enough_splits=stage_attributes['not_enough_splits'], original_field='KEEP',
                                  separator=separator, too_many_splits='TO_LAST_FIELD')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_splitter >> wiretap.destination
    pipeline = pipeline_builder.build('Field Splitter test_not_enough_splits pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        #Verify and assert result
        output_records = wiretap.output_records
        error_records = wiretap.error_records
        assert output_records[0].field['line'] == 'a b'
        assert output_records[0].field['a'] == 'a'
        assert output_records[0].field['b'] == 'b'
        assert output_records[1].field['line'] == 'a b c'
        assert output_records[1].field['a'] == 'a'
        assert output_records[1].field['b'] == 'b c'

        if stage_attributes['not_enough_splits'] == 'CONTINUE':
            assert output_records[2].field['line'] == 'a'
            assert output_records[2].field['a'] =='a'
            assert output_records[2].field['b'].value is None
            assert output_records[3].field['line'] == ''
            assert output_records[3].field['a'] == ''
            assert output_records[3].field['b'].value is None
            assert output_records[4].field['line'] == ' '
            assert output_records[4].field['a'] == ''
            assert output_records[4].field['b'] == ''
            assert output_records[5].field['line'].value is None
            assert output_records[5].field['a'].value is None
            assert output_records[5].field['b'].value is None
            assert len(error_records) == 0
            assert len(output_records) == 6
        elif stage_attributes['not_enough_splits'] == 'TO_ERROR':
            assert len(error_records) == 3
            assert len(output_records) == 3
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'original_field': 'KEEP'}, {'original_field': 'REMOVE'}])
def test_original_field(sdc_builder, sdc_executor, stage_attributes):
    raw_data = """
        [ { "line": "a b"
          }
        ]
    """
    raw_list = json.loads(raw_data)
    separator = "\\s"
    split_fields = ['/a','/b']

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_splitter = pipeline_builder.add_stage('Field Splitter')
    field_splitter.set_attributes(field_to_split=f'/line', new_split_fields=split_fields,
                                  original_field=stage_attributes['original_field'],
                                  separator=separator, too_many_splits='TO_LAST_FIELD')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_splitter >> wiretap.destination
    pipeline = pipeline_builder.build('Field Splitter test_original_field pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        assert 'a' == output_records[0].field['a']
        assert 'b' == output_records[0].field['b']

        if stage_attributes['original_field'] == 'KEEP':
          assert output_records[0].field['line'] == 'a b'
        elif stage_attributes['original_field'] == 'REMOVE':
          assert 'line' not in output_records[0].field
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_separator(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'too_many_splits': 'TO_LAST_FIELD'}, {'too_many_splits': 'TO_LIST'}])
def test_too_many_splits(sdc_builder, sdc_executor, stage_attributes):
    pass

