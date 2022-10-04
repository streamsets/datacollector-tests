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
import os
import pytest
import tempfile

from streamsets.sdk.sdc_api import RunError
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

LINE_SEPARATOR = "\n"
FIELD_SEPARATOR = "||"
ESCAPE_CHAR = "\\"
QUOTE_CHAR = "\""
ALL = 'ALL'
MIN = 'MINIMAL'
NONE = 'NONE'

DEFAULT_TABLE = 'col1,col2,col3\na,b,c'
DEFAULT_TABLE_WITH_ESCAPES_AND_QUOTES = 'col1,col2,col3\na,b,c\n d,\e, "f"'

#
# Text base file format parsing via Data Parser processor
#


@sdc_min_version('3.0.0.0')
def test_parse_delimited(sdc_builder, sdc_executor):
    """Validate parsing of delimited content via the Data Parser processor."""
    pipeline, wiretap = _create_text_pipeline(sdc_builder, 'DELIMITED', '1,2,3')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('[0]') == '1'
    assert wiretap.output_records[0].get_field_data('[1]') == '2'
    assert wiretap.output_records[0].get_field_data('[2]') == '3'


@sdc_min_version('3.8.0')
def test_parse_multichar_delimited(sdc_builder, sdc_executor):
    """Validate parsing of delimited content via the Data Parser processor."""
    pipeline, wiretap = _create_text_pipeline(sdc_builder, 'DELIMITED', 'abcd||efgh||ijkl',
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
    ('ALL', '"a"|"b"|" c"\n'),
    ('MINIMAL', 'a|b|" c"\n'),
    ('NONE', 'a|b| c\n')
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


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('delimiter, delimiter_interpreted', [
    ("||", "||"),
    (";", ";"),
    ("${str:unescapeJava('\\\\n')}", '\n'),
    ("x${str:unescapeJava('\\\\t')}x", 'x\tx')
])
def test_delimited_multichar_generator_line_delimiter(sdc_builder, sdc_executor, delimiter, delimiter_interpreted):
    """
    Tests the multi-char delimiter option works as expected with different line delimiters.
    """
    _test_delimited_multichar_generator_parameters(
        sdc_builder,
        sdc_executor,
        delimiter,
        delimiter_interpreted
    )


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('field_delimiter, field_delimiter_interpretation', [
    ("||", "||"),
    (",", ","),
    ("${str:unescapeJava('\\\\t')}", '\t'),
    ("x${str:unescapeJava('\\\\t')}x", 'x\tx')
])
def test_delimited_multichar_generator_field_delimiter(
        sdc_builder,
        sdc_executor,
        field_delimiter,
        field_delimiter_interpretation
):
    """
    Tests the multi-char delimiter option works as expected with different field delimiters.
    """
    _test_delimited_multichar_generator_parameters(
        sdc_builder,
        sdc_executor,
        LINE_SEPARATOR,
        LINE_SEPARATOR,
        field_delimiter,
        field_delimiter_interpretation
    )


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('escape_char', ['\\', '%', QUOTE_CHAR, '&', '5'])
def test_delimited_multichar_generator_escape_char(
        sdc_builder,
        sdc_executor,
        escape_char
):
    """
    Tests the multi-char delimiter option works as expected with different escape chars.
    """
    _test_delimited_multichar_generator_parameters(
        sdc_builder,
        sdc_executor,
        LINE_SEPARATOR,
        LINE_SEPARATOR,
        FIELD_SEPARATOR,
        FIELD_SEPARATOR,
        True,
        escape_char
    )


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('quote_char', ['"', '%', ESCAPE_CHAR, '&', '5'])
def test_delimited_multichar_generator_quote_char(sdc_builder, sdc_executor, quote_char):
    """
    Tests the multi-char delimiter option works as expected with different quote chars.
    """
    _test_delimited_multichar_generator_parameters(
        sdc_builder,
        sdc_executor,
        LINE_SEPARATOR,
        LINE_SEPARATOR,
        FIELD_SEPARATOR,
        FIELD_SEPARATOR,
        True,
        ESCAPE_CHAR,
        quote_char
    )


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('quote_mode', ['ALL', 'MINIMAL', 'NONE'])
def test_delimited_multichar_generator_quote_mode(sdc_builder, sdc_executor, quote_mode):
    """
    Tests the multi-char delimiter option works as expected with different quote modes.
    """
    _test_delimited_multichar_generator_parameters(
        sdc_builder,
        sdc_executor,
        LINE_SEPARATOR,
        LINE_SEPARATOR,
        FIELD_SEPARATOR,
        FIELD_SEPARATOR,
        True,
        ESCAPE_CHAR,
        QUOTE_CHAR,
        quote_mode
    )


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('header_line', ['WITH_HEADER', 'IGNORE_HEADER', 'NO_HEADER'])
def test_delimited_multichar_generator_header_options(sdc_builder, sdc_executor, header_line):
    """
    Tests the multi-char delimiter option works as expected with different header options.
    """
    _test_delimited_multichar_generator_parameters(
        sdc_builder,
        sdc_executor,
        LINE_SEPARATOR,
        LINE_SEPARATOR,
        FIELD_SEPARATOR,
        FIELD_SEPARATOR,
        True,
        ESCAPE_CHAR,
        QUOTE_CHAR,
        'MINIMAL',
        header_line
    )


def _test_delimited_multichar_generator_parameters(
        sdc_builder,
        sdc_executor,
        delimiter=LINE_SEPARATOR,
        unescaped_delimiter=LINE_SEPARATOR,
        field_delimiter=FIELD_SEPARATOR,
        unescaped_field_delimiter=FIELD_SEPARATOR,
        add_complex_examples=False,
        escape_char=ESCAPE_CHAR,
        quote_char=QUOTE_CHAR,
        quote_mode='MINIMAL',
        header_line='WITH_HEADER'
):
    work_dir = None
    pipeline = None

    try:
        builder = sdc_builder.get_pipeline_builder()
        work_dir = _prepare_work_dir(sdc_executor)

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(
            data_format='DELIMITED',
            header_line='WITH_HEADER',
            raw_data=DEFAULT_TABLE_WITH_ESCAPES_AND_QUOTES if add_complex_examples else DEFAULT_TABLE,
            stop_after_first_batch=True
        )

        local_fs = builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(
            directory_template=work_dir,
            data_format='DELIMITED',
            header_line=header_line,
            delimiter_format='MULTI_CHARACTER',
            multi_character_line_delimiter=delimiter,
            multi_character_field_delimiter=field_delimiter,
            escape_character=escape_char,
            quote_character=quote_char,
            quote_mode=quote_mode
        )

        dev_raw_data_source >> local_fs

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        expected_output = _generate_default_expected_output(
            unescaped_delimiter,
            unescaped_field_delimiter,
            add_complex_examples,
            escape_char,
            quote_char,
            quote_mode,
            header_line
        )
        assert sdc_executor.execute_shell(f'cat {work_dir}/*').stdout == expected_output
    finally:
        if work_dir is not None:
            logger.info('Delete directory in %s...', work_dir)
            sdc_executor.execute_shell(f'rm -r {work_dir}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('line_delimiter, field_delimiter, escape_char, quote_char, error_message', [
        ("", FIELD_SEPARATOR, ESCAPE_CHAR, QUOTE_CHAR, 'VALIDATION_0007'),
        (LINE_SEPARATOR, "", ESCAPE_CHAR, QUOTE_CHAR, 'VALIDATION_0007'),
        (LINE_SEPARATOR, FIELD_SEPARATOR, "", QUOTE_CHAR, 'CREATION_012'),
        (LINE_SEPARATOR, FIELD_SEPARATOR, "##", QUOTE_CHAR, 'CREATION_012'),
        (LINE_SEPARATOR, FIELD_SEPARATOR, ESCAPE_CHAR, "", 'CREATION_012'),
        (LINE_SEPARATOR, FIELD_SEPARATOR, ESCAPE_CHAR, "##", 'CREATION_012')
])
def test_delimited_multichar_generator_with_invalid_parameters(
        sdc_builder,
        sdc_executor,
        line_delimiter,
        field_delimiter,
        escape_char,
        quote_char,
        error_message
):
    """
    Tests the proper error messages are sent when invalid parameters are used.
    """
    work_dir = None
    pipeline = None

    try:
        builder = sdc_builder.get_pipeline_builder()
        work_dir = _prepare_work_dir(sdc_executor)

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(
            data_format='DELIMITED',
            header_line='WITH_HEADER',
            raw_data=DEFAULT_TABLE,
            stop_after_first_batch=True
        )

        local_fs = builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(
            directory_template=work_dir,
            data_format='DELIMITED',
            header_line='WITH_HEADER',
            delimiter_format='MULTI_CHARACTER',
            multi_character_line_delimiter=line_delimiter,
            multi_character_field_delimiter=field_delimiter,
            escape_character=escape_char,
            quote_character=quote_char
        )

        dev_raw_data_source >> local_fs

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)

        try:
            sdc_executor.validate_pipeline(pipeline)
            assert False, 'The test should not reach here, a validation error should have been thrown'
        except Exception as error:
            assert error_message in error.issues
    finally:
        if work_dir is not None:
            logger.info('Delete directory in %s...', work_dir)
            sdc_executor.execute_shell(f'rm -r {work_dir}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('field_delimiter, escape_char, quote_char, error_message', [
        ("${str:unescapeJava('\\\\n')}", ESCAPE_CHAR, QUOTE_CHAR, 'DELIMITED_GENERATOR_02'),
        (",", ESCAPE_CHAR, ",", 'DELIMITED_GENERATOR_03'),
        ("&", "&", QUOTE_CHAR, 'DELIMITED_GENERATOR_04')
])
def test_delimited_multichar_generator_incorrect_parameters(
        sdc_builder,
        sdc_executor,
        field_delimiter,
        escape_char,
        quote_char,
        error_message
):
    """
    Tests the generator throws the proper errors when incorrect values are used.
    """
    work_dir = None
    pipeline = None

    try:
        builder = sdc_builder.get_pipeline_builder()
        work_dir = _prepare_work_dir(sdc_executor)

        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(
            data_format='DELIMITED',
            header_line='WITH_HEADER',
            raw_data=DEFAULT_TABLE,
            stop_after_first_batch=True
        )

        local_fs = builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(
            directory_template=work_dir,
            data_format='DELIMITED',
            header_line='WITH_HEADER',
            delimiter_format='MULTI_CHARACTER',
            multi_character_line_delimiter=LINE_SEPARATOR,
            multi_character_field_delimiter=field_delimiter,
            escape_character=escape_char,
            quote_character=quote_char
        )

        dev_raw_data_source >> local_fs

        pipeline = builder.build()
        sdc_executor.add_pipeline(pipeline)

        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert False, 'The test should not reach here, a RunError should have been thrown'
        except RunError as error:
            assert error_message in error.message
    finally:
        if work_dir is not None:
            logger.info('Delete directory in %s...', work_dir)
            sdc_executor.execute_shell(f'rm -r {work_dir}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


def _create_text_pipeline(sdc_builder, data_format, content, **parser_configs):
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


def _prepare_work_dir(sdc_executor):
    """Create work directory, insert test data, return the work directory."""
    work_dir = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir -p {work_dir}')
    return work_dir


def _generate_default_expected_output(
        line_delimiter=LINE_SEPARATOR,
        field_delimiter=FIELD_SEPARATOR,
        include_escape_and_quotes_row=False,
        escape_char=ESCAPE_CHAR,
        quote_char=QUOTE_CHAR,
        quote_mode='MINIMAL',
        header_line='WITH_HEADER'
):
    required_quote = '' if quote_mode == 'NONE' else quote_char
    non_required_quote = quote_char if quote_mode == 'ALL' else ''

    output = ""

    if header_line == 'WITH_HEADER':
        output = output + f"{non_required_quote}col1{non_required_quote}{field_delimiter}" \
                          f"{non_required_quote}col2{non_required_quote}{field_delimiter}" \
                          f"{non_required_quote}col3{non_required_quote}{line_delimiter}"

    output = output + f"{non_required_quote}a{non_required_quote}{field_delimiter}" \
                      f"{non_required_quote}b{non_required_quote}{field_delimiter}" \
                      f"{non_required_quote}c{non_required_quote}{line_delimiter}"

    if include_escape_and_quotes_row:
        output = output + f'{required_quote} d{required_quote}{field_delimiter}'

        if escape_char == ESCAPE_CHAR:
            output = output + f'{required_quote}{escape_char}\e{required_quote}{field_delimiter}'
        else:
            output = output + f'{non_required_quote}\e{field_delimiter}{non_required_quote}'

        if (quote_char == QUOTE_CHAR) and (quote_mode != 'NONE'):
            output = output + f'{required_quote} {escape_char}"f{escape_char}"{required_quote}{line_delimiter}'
        else:
            output = output + f'{required_quote} "f"{required_quote}{line_delimiter}'

    return output
