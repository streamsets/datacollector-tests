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

import logging
import pytest
import os
import tempfile

from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ALL_PARSERS = ['UNIVOCITY', 'LEGACY_PARSER']


# Simple CSV, three columns, two rows, header
SIMPLE_CSV = "A,B,C\n" \
             "1,2,3\n" \
             "10,20,30\n"


# Variant of simple where field separator is semicolon
FIELD_SEMICOLON_CSV = "A;B;C\n" \
             "1;2;3\n" \
             "10;20;30\n"


# CSV with 3 rows, two of 5 characters and middle one with 11 characters
MAX_LINE_CSV = "A,B,C\n" \
             "1,2,3\n" \
             "100,200,300\n" \
             "1,2,3\n"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_header_with_header(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, SIMPLE_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['C'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_header_ignore_without(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, SIMPLE_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'IGNORE_HEADER'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['0'] == "1"
    assert records[0].field['1'] == "2"
    assert records[0].field['2'] == "3"
    assert records[1].field['0'] == "10"
    assert records[1].field['1'] == "20"
    assert records[1].field['2'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_header_no_header(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, SIMPLE_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'NO_HEADER'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 3

    assert records[0].field['0'] == "A"
    assert records[0].field['1'] == "B"
    assert records[0].field['2'] == "C"
    assert records[1].field['0'] == "1"
    assert records[1].field['1'] == "2"
    assert records[1].field['2'] == "3"
    assert records[2].field['0'] == "10"
    assert records[2].field['1'] == "20"
    assert records[2].field['2'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_field_separator(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, FIELD_SEMICOLON_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'NO_HEADER'
    # For legacy parser
    origin.delimiter_format_type = 'CUSTOM'
    origin.delimiter_character = ';'
    # For univocity parser
    origin.field_separator = ";"

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 3

    assert records[0].field['0'] == "A"
    assert records[0].field['1'] == "B"
    assert records[0].field['2'] == "C"
    assert records[1].field['0'] == "1"
    assert records[1].field['1'] == "2"
    assert records[1].field['2'] == "3"
    assert records[2].field['0'] == "10"
    assert records[2].field['1'] == "20"
    assert records[2].field['2'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
@pytest.mark.parametrize('separator', [';', ';;']) # Univocity limits line separator to max 2 characters (speed optimization on their side)
def test_line_separator(sdc_builder, sdc_executor, csv_parser, separator):
    data = f"A,B,C{separator}" \
           f"1,2,3{separator}" \
           f"10,20,30{separator}"
    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    # For legacy parser
    origin.delimiter_format_type = 'MULTI_CHARACTER'
    origin.multi_character_line_delimiter = separator
    origin.multi_character_field_delimiter = ","
    # For univocity parser
    origin.line_separator = separator

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['C'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_comments(sdc_builder, sdc_executor, csv_parser):
    data = f"#Comment\n" \
           f"A,B,C\n" \
           f"#Comment\n" \
           f"1,2,3\n" \
           f"#Comment\n" \
           f"10,20,30\n" \
           f"#Comment\n"

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    # For legacy parser
    origin.delimiter_format_type = 'CUSTOM'
    origin.enable_comments = True
    origin.delimiter_character = ','
    origin.comment_marker = '#'
    # For univocity parser
    origin.allow_comments = True
    origin.comment_character = '#'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['C'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_empty_lines(sdc_builder, sdc_executor, csv_parser):
    data = f"A,B,C\n" \
           f"\n" \
           f"1,2,3\n" \
           f"\n" \
           f"10,20,30\n" \
           f"\n"

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['C'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_do_not_skip_empty_lines(sdc_builder, sdc_executor, csv_parser):
    data = f"A,B,C\n" \
           f"\n" \
           f"1,2,3\n" \
           f"\n" \
           f"10,20,30\n" \
           f"\n"

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    # For legacy parser
    origin.delimiter_format_type = 'CUSTOM'
    origin.delimiter_character = ','
    origin.ignore_empty_lines = False
    # For univocity parser
    origin.skip_empty_lines = False

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 5

    assert records[0].field['A'] == (None if csv_parser == 'UNIVOCITY' else '')

    assert records[1].field['A'] == "1"
    assert records[1].field['B'] == "2"
    assert records[1].field['C'] == "3"

    assert records[2].field['A'] == (None if csv_parser == 'UNIVOCITY' else '')

    assert records[3].field['A'] == "10"
    assert records[3].field['B'] == "20"
    assert records[3].field['C'] == "30"

    assert records[4].field['A'] == (None if csv_parser == 'UNIVOCITY' else '')


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
@pytest.mark.parametrize('quote', ['"', ';'])
def test_quote(sdc_builder, sdc_executor, csv_parser, quote):
    data = f'A,B,C\n' \
           f'{quote}1,Z{quote},2,3\n' \
           f'100,{quote}200,Z{quote},300\n' \
           f'1,2,{quote}3,Z{quote}\n'

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.delimiter_format_type = 'CUSTOM'
    origin.delimiter_character = ","
    origin.quote_character = quote

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 3

    assert records[0].field['A'] == "1,Z"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "100"
    assert records[1].field['B'] == "200,Z"
    assert records[1].field['C'] == "300"
    assert records[2].field['A'] == "1"
    assert records[2].field['B'] == "2"
    assert records[2].field['C'] == "3,Z"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ['UNIVOCITY'])
@pytest.mark.parametrize('quote', ['a', '"', ';'])
@pytest.mark.parametrize('escape', ['x', '+', '!'])
def test_quote_escape(sdc_builder, sdc_executor, csv_parser, quote, escape):
    data = f'A,B,C\n' \
           f'{quote}1,{escape}{quote}Z{quote},2,3\n' \
           f'100,{quote}200,{escape}{quote}Z{quote},300\n' \
           f'1,2,{quote}3,{escape}{quote}Z{quote}\n'

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.delimiter_format_type = 'CUSTOM'
    origin.delimiter_character = ","
    origin.quote_character = quote
    origin.escape_character = escape

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 3

    assert records[0].field['A'] == f"1,{quote}Z"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "100"
    assert records[1].field['B'] == f"200,{quote}Z"
    assert records[1].field['C'] == "300"
    assert records[2].field['A'] == "1"
    assert records[2].field['B'] == "2"
    assert records[2].field['C'] == f"3,{quote}Z"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_skip_lines_with_header(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, SIMPLE_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.lines_to_skip = 1

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 1

    assert records[0].field['1'] == "10"
    assert records[0].field['2'] == "20"
    assert records[0].field['3'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_skip_lines_ignore_header(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, SIMPLE_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'IGNORE_HEADER'
    origin.lines_to_skip = 1

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 1

    assert records[0].field['0'] == "10"
    assert records[0].field['1'] == "20"
    assert records[0].field['2'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_skip_lines_no_header(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, SIMPLE_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'NO_HEADER'
    origin.lines_to_skip = 1

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['0'] == "1"
    assert records[0].field['1'] == "2"
    assert records[0].field['2'] == "3"
    assert records[1].field['0'] == "10"
    assert records[1].field['1'] == "20"
    assert records[1].field['2'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ['LEGACY_PARSER'])
def test_max_record_length_in_chars(sdc_builder, sdc_executor, csv_parser):
    work_dir = _prepare_work_dir(sdc_executor, MAX_LINE_CSV)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.max_record_length_in_chars = 7

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "1"
    assert records[1].field['B'] == "2"
    assert records[1].field['C'] == "3"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_wider_header_then_data(sdc_builder, sdc_executor, csv_parser):
    data = f"A,B,C,D,E,F\n" \
           f"1,2,3\n" \
           f"10,20,30\n" \

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['C'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
def test_allow_extra_columns(sdc_builder, sdc_executor, csv_parser):
    data = f"A,B\n" \
           f"1,2,3\n" \
           f"10,20,30\n" \

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.allow_extra_columns = True

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['_extra_01'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['_extra_01'] == "30"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ALL_PARSERS)
@pytest.mark.parametrize('null', ['NULL'])
def test_parse_nulls(sdc_builder, sdc_executor, csv_parser, null):
    data = f"A,B,C\n" \
           f"1,{null},3\n" \
           f"10,20,{null}\n" \

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.parse_nulls = True
    origin.null_constant = null

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] is None
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['C'] is None


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ['UNIVOCITY'])
def test_max_columns(sdc_builder, sdc_executor, csv_parser):
    # Reaching number of columns is non-recoverable error with Univocity, we will be done with file after reaching such line
    data = f"A,B\n" \
           f"1,2\n" \
           f"10,20,30\n" \
           f"100,200\n"

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.max_columns = 2

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 1

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ['UNIVOCITY'])
def test_max_characters_per_column(sdc_builder, sdc_executor, csv_parser):
    # Reaching max number of characters is non-recoverable error with Univocity, we will be done with file after reaching such line
    data = f"A,B\n" \
           f"1,2\n" \
           f"10,2\n" \
           f"3,4\n"

    work_dir = _prepare_work_dir(sdc_executor, data)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'
    origin.max_characters_per_column = 1

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 1

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"


def _prepare_work_dir(sdc_executor, data):
    """Create work directory, insert test data, return the work directory."""
    work_dir = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir -p {work_dir}')
    sdc_executor.write_file(os.path.join(work_dir, 'input.csv'), data)
    return work_dir