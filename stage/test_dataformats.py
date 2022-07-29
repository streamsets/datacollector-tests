# Copyright 2018 StreamSets Inc.
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
import io
import textwrap
from xlwt import Workbook

from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string
from stage.utils.utils_xml import get_xml_output_field

logger = logging.getLogger(__name__)

#
# Text base file format parsing via Data Parser processor
#


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


@sdc_min_version('3.0.0.0')
def test_parse_json(sdc_builder, sdc_executor):
    """Validate parsing of JSON content via the Data Parser processor."""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'JSON', '{"key" : "value"}')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/key') == 'value'


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('param,expected', [
    ("null", None),
    ("false", False),
    ("true", True),
    ("\"string\"", "string")
])
def test_parse_json_constants(sdc_builder, sdc_executor, param, expected):
    """Ensure that we properly read JSON constants outside of just 'objects'."""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'JSON', param)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/') == expected


@sdc_min_version('3.0.0.0')
def test_parse_log(sdc_builder, sdc_executor):
    """Validate parsing of log content via the Data Parser processor."""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'LOG', '127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/request') == '/apache_pb.gif'
    assert wiretap.output_records[0].get_field_data('/clientip') == '127.0.0.1'


def test_parse_syslog(sdc_builder, sdc_executor):
    """Validate parsing of syslog content via the Data Parser processor."""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'SYSLOG', "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/")

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/severity') == 2
    assert wiretap.output_records[0].get_field_data('/host') == 'mymachine'


@sdc_min_version('3.0.0.0')
def test_parse_xml(sdc_builder, sdc_executor):
    """Validate parsing of xml content via the Data Parser processor."""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'XML', "<root><key>value</key></root>")

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    key_field = get_xml_output_field(pipeline[0], wiretap.output_records[0].field, 'root')
    assert key_field['key'][0]['value'] == 'value'


@sdc_min_version('3.14.0')
def test_parse_xml_preserve_root_element(sdc_builder, sdc_executor):
    """Validate parsing of xml content via the Data Parser processor.
    Since 3.14.0 there is a new property 'preserve root element', set to True by default"""
    pipeline, wiretap = create_text_pipeline(sdc_builder, 'XML', "<root><key>value</key></root>", preserve_root_element=True)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/root/key[0]/value') == 'value'


def test_excel_with_empty_columns(sdc_builder, sdc_executor):
    """Test if some records had empty value for a column, it don't ignore the column and keep the same schema and
    write it in avro file. Test empty values in the first, medium and last column and get it as a null.

    directory >> schema_generator >> local_fs
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    files_directory = os.path.join('/tmp', get_random_string())
    directory_out = os.path.join('/tmp', get_random_string())
    file_name = f'{get_random_string()}.xls'
    file_path = os.path.join(files_directory, file_name)
    schema_name = 'test_schema'
    num_records = 4

    try:
        sdc_executor.execute_shell(f'mkdir {files_directory}')
        file_writer(sdc_executor, file_path, generate_excel_file().getvalue())

        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(excel_header_option='WITH_HEADER',
                                 data_format='EXCEL',
                                 files_directory=files_directory,
                                 file_name_pattern='*.xls')

        schema_generator = pipeline_builder.add_stage('Schema Generator')
        schema_generator.set_attributes(schema_name=schema_name,
                                        namespace=schema_name,
                                        nullable_fields=True)

        local_fs = pipeline_builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(data_format='AVRO', avro_schema_location='HEADER', directory_template=directory_out)

        directory >> schema_generator >> local_fs

        pipeline_write = pipeline_builder.build('Read Excel files')
        sdc_executor.add_pipeline(pipeline_write)

        sdc_executor.start_pipeline(pipeline_write).wait_for_pipeline_output_records_count(num_records)
        sdc_executor.stop_pipeline(pipeline_write)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        origin = pipeline_builder.add_stage('Directory')
        origin.set_attributes(data_format='AVRO',
                              files_directory=directory_out,
                              file_name_pattern='*')

        wiretap = pipeline_builder.add_wiretap()

        origin >> wiretap.destination

        pipeline_read = pipeline_builder.build('Wiretap Pipeline')
        sdc_executor.add_pipeline(pipeline_read)

        sdc_executor.start_pipeline(pipeline_read).wait_for_pipeline_output_records_count(num_records)
        sdc_executor.stop_pipeline(pipeline_read)

        assert len(wiretap.output_records) == num_records
        for x in range(num_records):
            name_field = 'column' + str(x)
            assert wiretap.output_records[x].field[name_field].value == ''
            # We check if it works for 2 empty columns at end of the row
            if x == 3:
                name_field = 'column' + str(x-1)
                assert wiretap.output_records[x].field[name_field].value == ''
    finally:
        logger.info('Delete directory in %s and %s...', files_directory, directory_out)
        sdc_executor.execute_shell(f'rm -r {files_directory}')
        sdc_executor.execute_shell(f'rm -r {directory_out}')


@sdc_min_version('5.1.0')
def test_excel_with_duplicated_headers(sdc_builder, sdc_executor):
    """
    Tests that the Excel parser throws a Stage Error if the "With Header Line" option is set and there is at least a
    couple of columns that have the same header. After throwing the Stage Error, the processing shall continue: the
    following rows are read and the values in the repeated columns are overwritten with the latest values.
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    try:
        # Create the Directory
        files_directory = os.path.join('/tmp', get_random_string())
        file_name = f'{get_random_string()}.xls'
        file_path = os.path.join(files_directory, file_name)

        # Create the Excel file with a repeated column name
        sdc_executor.execute_shell(f'mkdir {files_directory}')
        file_excel = io.BytesIO()
        workbook = Workbook(encoding='utf-8')
        sheet = workbook.add_sheet('sheet1')

        sheet.write(0, 0, 'Col1')
        sheet.write(0, 1, 'Col2')
        sheet.write(0, 2, 'Col1')

        sheet.write(1, 0, '1')
        sheet.write(1, 1, '2')
        sheet.write(1, 2, '3')

        workbook.save(file_excel)
        file_writer(sdc_executor, file_path, file_excel.getvalue())

        # Create a pipeline Directory -> Wiretap
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(
            excel_header_option='WITH_HEADER',
            data_format='EXCEL',
            files_directory=files_directory,
            file_name_pattern='*.xls'
        )

        wiretap = pipeline_builder.add_wiretap()
        directory >> wiretap.destination

        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        # Check the record has been read
        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['Col2'] == '2'
        assert records[0].field['Col1'] == '3'

        # Check the stage error has been thrown
        stage_errors = sdc_executor.get_stage_errors(pipeline, directory)
        assert len(stage_errors) == 1
        assert stage_errors[0].error_code == 'EXCEL_PARSER_07'

        sdc_executor.stop_pipeline(pipeline)

    finally:
        if files_directory is not None:
            logger.info('Delete directory in %s...', files_directory)
            sdc_executor.execute_shell(f'rm -r {files_directory}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


HEADERS_DATA = [
    ('text', 'abcd', 'abcd'),
    ('number', 1234, '1234'),
    ('currency', '1024,00€', '\"1024,00â\x82¬\"'),
    ('short_date', '14/03/93', '93\"'),
    ('long_date', 'Friday, 14 June 2002', '\"Friday, 14 June 2002\"'),
    ('time', '00:00:00', '\"00:00:00\"'),
    ('percentage', '1024,97%', '\"1024,97%\"'),
    ('scientific', '1024E+06', '\"1024E+06\"')
]
@sdc_min_version('5.1.0')
@pytest.mark.parametrize('header_data_type, header_data, expected_header', HEADERS_DATA, ids=[i[0] for i in HEADERS_DATA])
def test_excel_dataformats_headers(sdc_builder, sdc_executor, header_data_type, header_data, expected_header):
    """
    Tests that the Excel parser can read different dataformats in the headers.

    The pipeline would look like:
        directory >> wiretap.destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    try:
        # Create the Directory
        files_directory = os.path.join('/tmp', get_random_string())
        file_name = f'{get_random_string()}.xls'
        file_path = os.path.join(files_directory, file_name)

        # Create the Excel file with a repeated column name
        sdc_executor.execute_shell(f'mkdir {files_directory}')
        file_excel = io.BytesIO()
        workbook = Workbook(encoding='utf-8')
        sheet = workbook.add_sheet('sheet1')

        sheet.write(0, 0, header_data)
        sheet.write(1, 0, 'Hello World')

        workbook.save(file_excel)
        file_writer(sdc_executor, file_path, file_excel.getvalue())

        # Create a pipeline Directory -> Wiretap
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(
            excel_header_option='WITH_HEADER',
            data_format='EXCEL',
            files_directory=files_directory,
            file_name_pattern='*.xls'
        )

        wiretap = pipeline_builder.add_wiretap()
        directory >> wiretap.destination

        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        # Check the record has been read
        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field.get(expected_header) == 'Hello World'

        sdc_executor.stop_pipeline(pipeline)

    finally:
        if files_directory is not None:
            logger.info('Delete directory in %s...', files_directory)
            sdc_executor.execute_shell(f'rm -r {files_directory}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


def file_writer(sdc_executor, file_path, file_contents):
    encoding = 'utf8'
    FILE_WRITER_SCRIPT_BINARY = """
        with open('{filepath}', 'wb') as f:
            f.write({file_contents})
    """

    builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='noop', stop_after_first_batch=True)
    jython_evaluator = builder.add_stage('Jython Evaluator')

    file_writer_script = FILE_WRITER_SCRIPT_BINARY
    jython_evaluator.script = textwrap.dedent(file_writer_script).format(filepath=str(file_path),
                                                                         file_contents=file_contents,
                                                                         encoding=encoding)
    trash = builder.add_stage('Trash')
    dev_raw_data_source >> jython_evaluator >> trash
    pipeline = builder.build('File writer pipeline')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(pipeline)


def generate_excel_file():
    """Builds excel file in memory, later bind this data to BINARY file.
    """
    file_excel = io.BytesIO()  # create a file-like object
    # Create the Excel file
    workbook = Workbook(encoding='utf-8')
    sheet = workbook.add_sheet('sheet1')

    sheet.write(0, 0, 'column0')
    sheet.write(0, 1, 'column1')
    sheet.write(0, 2, 'column2')
    sheet.write(0, 3, 'column3')

    sheet.write(1, 0, None)
    sheet.write(1, 1, 'yes')
    sheet.write(1, 2, 'yes')
    sheet.write(1, 3, 'yes')

    sheet.write(2, 0, 'no')
    sheet.write(2, 1, None)
    sheet.write(2, 2, 'no')
    sheet.write(2, 3, 'no')

    sheet.write(3, 0, 'hello')
    sheet.write(3, 1, 'hello')
    sheet.write(3, 2, None)
    sheet.write(3, 3, 'hello')

    sheet.write(4, 0, 'world')
    sheet.write(4, 1, 'world')
    sheet.write(4, 2, None)
    sheet.write(4, 3, None)

    workbook.save(file_excel)
    return file_excel
