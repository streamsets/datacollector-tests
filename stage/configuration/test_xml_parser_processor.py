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
def test_charset(sdc_builder, sdc_executor):
    pass


@stub
def test_delimiter_element(sdc_builder, sdc_executor):
    pass


@stub
def test_field_to_parse(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_control_characters': False}, {'ignore_control_characters': True}])
def test_ignore_control_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'include_field_xpaths': False}, {'include_field_xpaths': True}])
def test_include_field_xpaths(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'ALL_AS_LIST'},
                                              {'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes):
    """Test XML parser processor. XML in this test is simple.
       The pipeline would look like:

           dev_raw_data_source >> xml_parser >> wiretap
    """
    raw_data = """<?xml version="1.0" encoding="UTF-8"?>
                  <root>
                      <a>1</a>
                      <a>2</a>
                      <a>3</a>
                  </root>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    # Note that since the delimiter text '</dummy>' does not exist in input XML, the output of this stage is whole XML.
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    xml_parser = pipeline_builder.add_stage('XML Parser', type='processor')
    xml_parser.set_attributes(field_to_parse='/text', ignore_control_characters=True, target_field='/text',
                              multiple_values_behavior=stage_attributes['multiple_values_behavior'], delimiter_element='a')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_parser >> wiretap.destination
    pipeline = pipeline_builder.build('XML Parser test multiple values behaviour pipeline')

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data for verification.
        record = wiretap.output_records

        if stage_attributes['multiple_values_behavior']=='ALL_AS_LIST':
            assert len(record) == 1
            resultList = wiretap.output_records[0].field['text']
            assert len(resultList) == 3
            expectedList = [{'value': '1'}, {'value': '2'}, {'value': '3'}]
        elif stage_attributes['multiple_values_behavior']=='FIRST_ONLY':
            assert len(record) == 1
            resultList = wiretap.output_records[0].field['text']
            assert len(resultList) == 1
            expectedList = {'value': '1'}
        elif stage_attributes['multiple_values_behavior']=='SPLIT_INTO_MULTIPLE_RECORDS':
            assert len(record) == 3
            resultList=[]
            for item in record:
              resultList.append(item.field['text'])
            assert len(resultList) == 3
            expectedList = [{'value': '1'}, {'value': '2'}, {'value': '3'}]

        assert resultList == expectedList
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
def test_namespaces(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'output_field_attributes': False}, {'output_field_attributes': True}])
def test_output_field_attributes(sdc_builder, sdc_executor, stage_attributes):
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

