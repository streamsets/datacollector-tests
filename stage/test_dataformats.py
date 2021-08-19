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
from streamsets.sdk.utils import Version
from decimal import Decimal
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


# SDC-11018: Re-scale data when writing Decimal into Avro
@sdc_min_version('3.2.0.0') # Data Generator
def test_avro_decimal_incorrect_scale(sdc_builder, sdc_executor):
    """Make sure that we auto-rescale decimal as needed when writing to Avro.

       raw data source >> type converter >> generator >> parser >> trash
    """
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = """{"a": "1.10"}
                         {"a": null}"""
    source.stop_after_first_batch = True

    type_converter = builder.add_stage('Field Type Converter')
    type_converter.conversion_method = 'BY_FIELD'
    type_converter.field_type_converter_configs = [{
        "fields" : [ "/a" ],
        "targetType" : "DECIMAL",
        "scale" : 2,
        "decimalScaleRoundingStrategy" : "ROUND_HALF_EVEN"
      }]

    generator = builder.add_stage('Data Generator')
    generator.data_format = 'AVRO'
    generator.avro_schema_location = 'INLINE'
    generator.avro_schema = """{
      "type" : "record",
      "name" : "TestDecimal",
      "fields" : [ {
        "name" : "a",
        "type" : [ "null", {
          "type" : "bytes",
          "logicalType" : "decimal",
          "precision" : 7,
          "scale" : 5
        } ],
        "default" : null
      }]
    }"""

    parser = builder.add_stage('Data Parser')
    parser.field_to_parse = '/'
    parser.target_field = '/'
    parser.data_format = 'AVRO'
    parser.avro_schema_location = 'SOURCE'

    wiretap = builder.add_wiretap()

    source >> type_converter >> generator >> parser >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 2
    assert wiretap.output_records[0].get_field_data('/a') == Decimal('1.10000')
    assert wiretap.output_records[1].get_field_data('/a') == None


# SDC-11022: Do not use avro union index when writing avro data
@sdc_min_version('3.2.0.0') # Data Generator
def test_avro_decimal_union_index_on_write(sdc_builder, sdc_executor):
    """Make sure that avro union index is not used when writing data out to Avro file format.

       raw data source >> expression >> generator >> parser >> trash
    """
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = '{"a": "b"}'
    source.stop_after_first_batch = True

    # Use clearly non-existing typeIndex
    expression = builder.add_stage('Expression Evaluator')
    expression.header_attribute_expressions = [
        {'attributeToSet': 'avro.union.typeIndex./a', 'headerAttributeExpression': '666'}
    ]

    generator = builder.add_stage('Data Generator')
    generator.data_format = 'AVRO'
    generator.avro_schema_location = 'INLINE'
    generator.avro_schema = """{
      "type" : "record",
      "name" : "TestDecimal",
      "fields" : [ {
        "name" : "a",
        "type" : [ "null", "int", "string"],
        "default" : null
      }]
    }"""

    parser = builder.add_stage('Data Parser')
    parser.field_to_parse = '/'
    parser.target_field = '/'
    parser.data_format = 'AVRO'
    parser.avro_schema_location = 'SOURCE'

    wiretap = builder.add_wiretap()

    source >> expression >> generator >> parser >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].get_field_data('/a') == 'b'


# SDC-11557: Publish field attributes for typed nulls when reading Avro
@sdc_min_version('3.9.0')
def test_avro_decimal_field_attributes_for_typed_null(sdc_builder, sdc_executor):
    """Make sure that we persist decimal's field attributes for typed nul """
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    source.raw_data = '{"decimal": "12.01"}{"decimal":null}'

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/decimal'],
        'targetType': 'DECIMAL'
    }]

    generator = builder.add_stage('Data Generator')
    generator.data_format = 'AVRO'
    generator.avro_schema_location = 'INLINE'
    generator.avro_schema = """{
      "type" : "record",
      "name" : "TestDecimal",
      "fields" : [ {
        "name" : "decimal",
        "type" : ["null", {"name": "name", "type": "bytes", "logicalType": "decimal", "precision":4, "scale":2}],
        "default" : null
      }]
    }"""

    parser = builder.add_stage('Data Parser')
    parser.field_to_parse = '/'
    parser.target_field = '/'
    parser.data_format = 'AVRO'
    parser.avro_schema_location = 'SOURCE'

    wiretap = builder.add_wiretap()

    source >> converter >> generator >> parser >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 2
    assert wiretap.output_records[0].get_field_data('/decimal') == Decimal("12.01")
    assert wiretap.output_records[1].get_field_data('/decimal') == None

    assert wiretap.output_records[0].get_field_data('/decimal').attributes['precision'] == '4'
    assert wiretap.output_records[1].get_field_data('/decimal').attributes['precision'] == '4'

    assert wiretap.output_records[0].get_field_data('/decimal').attributes['scale'] == '2'
    assert wiretap.output_records[1].get_field_data('/decimal').attributes['scale'] == '2'


# COLLECTOR-164 Avro complex schema UNION types not working
# COLLECTOR-204: Avro complex schema UNION types working for first schema match only
@sdc_min_version('4.2.0')
@pytest.mark.parametrize('union_type', [1, 2])
def test_avro_complex_union(sdc_builder, sdc_executor, union_type):
    """Ensure this type of schema works for both union types"""
    union_type_1_data = """
        {
            "directmessageId": "b7615b58-ad6e-47b9-8702-50d3d5fb4331",
            "timestamp": "2020-07-27T21:16:58Z",
            "domaindata": {
                "domaindata_1": {
                    "npiid": "1619964335",
                    "patient": {
                        "identifier": "<identifier>",
                        "name": {
                            "family": "Bing",
                            "given": "Chandler Muriel"
                        },
                        "birthDate": "1969-04-01"
                    },
                    "directmessagemetadata": {
                        "fromaddress": "webmail@address.net",
                        "message": "<<A new message has been received>>"
                    }
                }
            }
        }"""

    union_type_2_data = """
        {
          "directmessageId": "b7615b58-ad6e-47b9-8702-50d3d5fb4332",
          "timestamp": "2020-07-27T21:16:59Z",
          "domaindata": {
            "domaindata_2": {
              "npiid": "1619964336"
            }
          }
        }"""

    schema = """
        {
            "type": "record",
            "name": "directmessage",
            "fields": [{
                "name": "directmessageId",
                "type": "string"
            }, {
                "name": "timestamp",
                "type": "string"
            }, {
                "name": "domaindata",
                "type": [{
                    "type": "record",
                    "name": "domaindata_1",
                    "fields": [{
                        "name": "npiid",
                        "type": "string"
                    }, {
                        "name": "patient",
                        "type": {
                            "type": "record",
                            "name": "patient",
                            "fields": [{
                                "name": "identifier",
                                "type": "string"
                            }, {
                                "name": "name",
                                "type": {
                                    "type": "record",
                                    "name": "name",
                                    "fields": [{
                                        "name": "given",
                                        "type": "string"
                                    }, {
                                        "name": "family",
                                        "type": "string"
                                    }]
                                }
                            }, {
                                "name": "birthDate",
                                "type": "string"
                            }]
                        }
                    }, {
                        "name": "directmessagemetadata",
                        "type": {
                            "type": "record",
                            "name": "directmessagemetadata",
                            "fields": [{
                                "name": "fromaddress",
                                "type": "string"
                            }, {
                                "name": "message",
                                "type": "string"
                            }]
                        }
                    }]
                }, {
                    "type": "record",
                    "name": "domaindata_2",
                    "fields": [{
                        "name": "npiid",
                        "type": "string"
                    }]
                }]
            }]
        }"""

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.stop_after_first_batch = True
    source.data_format = 'JSON'
    # Check union type union_type
    if union_type == 1:
        source.raw_data = union_type_1_data
    elif union_type == 2:
        source.raw_data = union_type_2_data

    generator = builder.add_stage('Data Generator')
    generator.data_format = 'AVRO'
    generator.avro_schema_location = 'INLINE'
    generator.avro_schema = schema

    parser = builder.add_stage('Data Parser')
    parser.field_to_parse = '/'
    parser.target_field = '/'
    parser.data_format = 'AVRO'
    parser.avro_schema_location = 'SOURCE'

    wiretap = builder.add_wiretap()

    source >> generator >> parser >> wiretap.destination
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    output = wiretap.output_records[0]

    if union_type == 1:
        assert output.get_field_data('/directmessageId') == 'b7615b58-ad6e-47b9-8702-50d3d5fb4331'
        assert output.get_field_data('/timestamp') == '2020-07-27T21:16:58Z'
        assert output.get_field_data('/domaindata/npiid') == '1619964335'
        assert output.get_field_data('/domaindata/patient/identifier') == '<identifier>'
        assert output.get_field_data('/domaindata/patient/name/given') == 'Chandler Muriel'
        assert output.get_field_data('/domaindata/patient/name/family') == 'Bing'
        assert output.get_field_data('/domaindata/patient/birthDate') == '1969-04-01'
        assert output.get_field_data('/domaindata/directmessagemetadata/fromaddress') == 'webmail@address.net'
        assert output.get_field_data('/domaindata/directmessagemetadata/message') == \
               '<<A new message has been received>>'

    elif union_type == 2:
        assert output.get_field_data('/directmessageId') == 'b7615b58-ad6e-47b9-8702-50d3d5fb4332'
        assert output.get_field_data('/timestamp') == '2020-07-27T21:16:59Z'
        assert output.get_field_data('/domaindata/npiid') == '1619964336'


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
