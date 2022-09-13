# Copyright 2022 StreamSets Inc.
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


#
# Text base file format parsing via Data Parser processor
#


@sdc_min_version('3.0.0.0')
def test_parse_delimited(sdc_builder, sdc_executor):
    """Validate parsing of delimited content via the Data Parser processor."""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'DELIMITED', '1,2,3')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('[0]') == '1'
    assert wiretap.output_records[0].get_field_data('[1]') == '2'
    assert wiretap.output_records[0].get_field_data('[2]') == '3'


@sdc_min_version('3.8.0')
def test_parse_multichar_delimited(sdc_builder, sdc_executor):
    """Validate parsing of delimited content via the Data Parser processor."""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'DELIMITED', 'abcd||efgh||ijkl',
                                             delimiter_format_type='MULTI_CHARACTER',
                                             multi_character_field_delimiter='||', header_line='NO_HEADER')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    outputs = wiretap.output_records
    assert len(outputs) == 1
    output_record = outputs[0]
    assert output_record.get_field_data('[0]') == 'abcd'
    assert output_record.get_field_data('[1]') == 'efgh'
    assert output_record.get_field_data('[2]') == 'ijkl'


# SDC-11869: Add ability to specify quote mode when generating CSV data
@sdc_min_version('3.10.0')
@pytest.mark.parametrize('quote_mode,expected', [
    ('ALL', '"a"|"b"|" c"\r\n'),
    ('MINIMAL', 'a|b|" c"\r\n'),
    ('NONE', 'a|b| c\r\n')
])
def test_delimited_quote_mode(sdc_builder, sdc_executor, quote_mode, expected):
    """Ensure that delimited quote mode works properly."""
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'DELIMITED'
    source.header_line = 'WITH_HEADER'
    source.raw_data = 'a,b,c\na,b, c'

    generator = builder.add_stage('Data Generator')
    generator.data_format = 'DELIMITED'
    generator.delimiter_format = 'CUSTOM'
    generator.quote_mode = quote_mode
    generator.output_type = 'STRING'
    # Due to STF-833
    generator.target_field = '/target'

    wiretap = builder.add_wiretap()

    source >> generator >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/target') == expected


@sdc_min_version('5.3.0')
def test_delimited_record_separator(sdc_builder, sdc_executor):
    """Ensure that Separator Record works properly."""
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'DELIMITED'
    source.header_line = 'NO_HEADER'
    source.raw_data = 'a,b,c\ne,f,g'

    temp_dir = sdc_executor.execute_shell(f'mktemp -d').stdout.rstrip()
    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.directory_template = temp_dir
    local_fs.data_format = 'DELIMITED'
    local_fs.delimiter_format = 'CUSTOM'
    local_fs.record_separator_string = "X42X"

    source >> local_fs

    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert 'a|b|cX42Xe|f|gX42X' == sdc_executor.execute_shell(f'cat {temp_dir}/*').stdout


@sdc_min_version('3.8.0')
def test_delimited_quoted_newline(sdc_builder, sdc_executor):
    """Ensure that delimited data with newlines between quotes are correctly parsed"""
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'DELIMITED'
    source.header_line = 'WITH_HEADER'
    source.delimiter_format_type = 'MULTI_CHARACTER'
    source.multi_character_line_delimiter = 'xxx'
    source.raw_data = 'a||b||cxxx"dexxxf"||g||h'

    wiretap = builder.add_wiretap()

    source >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    records = [record.field for record in wiretap.output_records]
    assert records == [{"a": "dexxxf", "b": "g", "c": "h"}]


def test_delimited_escape_multichar(sdc_builder, sdc_executor):
    """Ensure that delimited data with newlines between quotes are correctly parsed"""
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'DELIMITED'
    source.header_line = 'WITH_HEADER'
    source.delimiter_format_type = 'MULTI_CHARACTER'
    source.raw_data = 'a||b||c\n"de\\||f"||g||h'

    wiretap = builder.add_wiretap()

    source >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    records = [record.field for record in wiretap.output_records]
    assert records == [{"a": "de||f", "b": "g", "c": "h"}]


def create_text_pipeline(sdc_builder, data_format, content, **parser_configs):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'TEXT'
    origin.raw_data = content
    origin.stop_after_first_batch = True

    parser = builder.add_stage('Data Parser')

    parser.field_to_parse = '/text'
    parser.target_field = '/'
    parser.data_format = data_format

    if parser_configs:
        parser.set_attributes(**parser_configs)

    wiretap = builder.add_wiretap()

    origin >> parser >> wiretap.destination

    return builder.build('Parse {}'.format(data_format)), wiretap
