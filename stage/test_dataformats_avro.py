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
from decimal import Decimal

from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

#
# Text base file format parsing via Data Parser processor
#


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
        "fields": ["/a"],
        "targetType": "DECIMAL",
        "scale": 2,
        "decimalScaleRoundingStrategy": "ROUND_HALF_EVEN"
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
