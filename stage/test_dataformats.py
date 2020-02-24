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
from streamsets.sdk.utils import Version
from decimal import Decimal

from streamsets.testframework.markers import sdc_min_version
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

    parser = builder.add_stage('Data Parser')

    parser.field_to_parse = '/text'
    parser.target_field = '/'
    parser.data_format = data_format

    if parser_configs:
        parser.set_attributes(**parser_configs)

    trash = builder.add_stage('Trash')

    origin >> parser >> trash

    return builder.build('Parse {}'.format(data_format))


@sdc_min_version('3.0.0.0')
def test_parse_json(sdc_builder, sdc_executor):
    """Validate parsing of JSON content via the Data Parser processor."""
    pipeline = create_text_pipeline(sdc_builder, 'JSON', '{"key" : "value"}')

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    assert len(snapshot['DataParser_01'].output) == 1
    assert snapshot['DataParser_01'].output[0].get_field_data('/key') == 'value'


@sdc_min_version('3.0.0.0')
def test_parse_delimited(sdc_builder, sdc_executor):
    """Validate parsing of delimited content via the Data Parser processor."""
    pipeline = create_text_pipeline(sdc_builder, 'DELIMITED', '1,2,3')

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    assert len(snapshot['DataParser_01'].output) == 1
    assert snapshot['DataParser_01'].output[0].get_field_data('[0]') == '1'
    assert snapshot['DataParser_01'].output[0].get_field_data('[1]') == '2'
    assert snapshot['DataParser_01'].output[0].get_field_data('[2]') == '3'


@sdc_min_version('3.8.0')
def test_parse_multichar_delimited(sdc_builder, sdc_executor):
    """Validate parsing of delimited content via the Data Parser processor."""
    pipeline = create_text_pipeline(sdc_builder, 'DELIMITED', 'abcd||efgh||ijkl',
                                    delimiter_format_type='MULTI_CHARACTER',
                                    multi_character_field_delimiter='||', header_line='NO_HEADER')

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    outputs = snapshot['DataParser_01'].output
    assert len(outputs) == 1
    output_record = outputs[0]
    assert output_record.get_field_data('[0]') == 'abcd'
    assert output_record.get_field_data('[1]') == 'efgh'
    assert output_record.get_field_data('[2]') == 'ijkl'


@sdc_min_version('3.0.0.0')
def test_parse_log(sdc_builder, sdc_executor):
    """Validate parsing of log content via the Data Parser processor."""
    pipeline = create_text_pipeline(sdc_builder, 'LOG', '127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326')

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    assert len(snapshot['DataParser_01'].output) == 1
    assert snapshot['DataParser_01'].output[0].get_field_data('/request') == '/apache_pb.gif'
    assert snapshot['DataParser_01'].output[0].get_field_data('/clientip') == '127.0.0.1'


def test_parse_syslog(sdc_builder, sdc_executor):
    """Validate parsing of syslog content via the Data Parser processor."""
    pipeline = create_text_pipeline(sdc_builder, 'SYSLOG', "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/")

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    assert len(snapshot['DataParser_01'].output) == 1
    assert snapshot['DataParser_01'].output[0].get_field_data('/severity') == 2
    assert snapshot['DataParser_01'].output[0].get_field_data('/host') == 'mymachine'


@sdc_min_version('3.0.0.0')
def test_parse_xml(sdc_builder, sdc_executor):
    """Validate parsing of xml content via the Data Parser processor."""
    pipeline = create_text_pipeline(sdc_builder, 'XML', "<root><key>value</key></root>")

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    sdc_executor.stop_pipeline(pipeline)

    assert len(snapshot['DataParser_01'].output) == 1
    key_field = get_xml_output_field(pipeline[0], snapshot['DataParser_01'].output[0].field, 'root')
    assert key_field['key'][0]['value'] == 'value'


@sdc_min_version('3.14.0')
def test_parse_xml_preserve_root_element(sdc_builder, sdc_executor):
    """Validate parsing of xml content via the Data Parser processor.
    Since 3.14.0 there is a new property 'preserve root element', set to True by default"""
    pipeline = create_text_pipeline(sdc_builder, 'XML', "<root><key>value</key></root>", preserve_root_element=True)

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    assert len(snapshot['DataParser_01'].output) == 1
    assert snapshot['DataParser_01'].output[0].get_field_data('/root/key[0]/value') == 'value'


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

    trash = builder.add_stage('Trash')

    source >> type_converter >> generator >> parser >> trash
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    assert len(snapshot[parser].output) == 2
    assert snapshot[parser].output[0].get_field_data('/a') == Decimal('1.10000')
    assert snapshot[parser].output[1].get_field_data('/a') == None


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

    trash = builder.add_stage('Trash')

    source >> expression >> generator >> parser >> trash
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    assert len(snapshot[parser].output) == 1
    assert snapshot[parser].output[0].get_field_data('/a') == 'b'


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

    trash = builder.add_stage('Trash')

    source >> converter >> generator >> parser >> trash
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    assert len(snapshot[parser].output) == 2
    assert snapshot[parser].output[0].get_field_data('/decimal') == Decimal("12.01")
    assert snapshot[parser].output[1].get_field_data('/decimal') == None

    assert snapshot[parser].output[0].get_field_data('/decimal').attributes['precision'] == '4'
    assert snapshot[parser].output[1].get_field_data('/decimal').attributes['precision'] == '4'

    assert snapshot[parser].output[0].get_field_data('/decimal').attributes['scale'] == '2'
    assert snapshot[parser].output[1].get_field_data('/decimal').attributes['scale'] == '2'


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

    trash = builder.add_stage('Trash')

    source >> generator >> trash
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    assert len(snapshot[generator].output) == 1
    assert snapshot[generator].output[0].get_field_data('/target') == expected
