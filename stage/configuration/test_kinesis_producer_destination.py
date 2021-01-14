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
def test_access_key_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_compression_codec': 'BZIP2', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'DEFLATE', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'NULL', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'SNAPPY', 'data_format': 'AVRO'}])
def test_avro_compression_codec(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE', 'data_format': 'AVRO'}])
def test_avro_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'INLINE', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
def test_avro_schema_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY'}])
def test_binary_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_charset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'checksum_algorithm': 'MD5',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'MURMUR3_128',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'MURMUR3_32',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'SHA1',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'SHA256',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True},
                                              {'checksum_algorithm': 'SHA512',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True}])
def test_checksum_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'data_format': 'BINARY'},
                                              {'data_format': 'DELIMITED'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'PROTOBUF'},
                                              {'data_format': 'SDC_JSON'},
                                              {'data_format': 'TEXT'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
def test_delimiter_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'EXCEL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'MULTI_CHARACTER'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'MYSQL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'POSTGRES_CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'POSTGRES_TEXT'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'RFC4180'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'TDF'}])
def test_delimiter_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'region': 'OTHER'}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
def test_escape_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'file_exists': 'OVERWRITE'},
                                              {'data_format': 'WHOLE_FILE', 'file_exists': 'TO_ERROR'}])
def test_file_exists(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_file_name_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'include_checksum_in_events': False},
                                              {'data_format': 'WHOLE_FILE', 'include_checksum_in_events': True}])
def test_include_checksum_in_events(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'include_schema': False},
                                              {'data_format': 'AVRO', 'include_schema': True}])
def test_include_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': False,
                                               'on_missing_field': 'IGNORE'},
                                              {'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': True,
                                               'on_missing_field': 'IGNORE'}])
def test_insert_record_separator_if_no_text(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'}])
def test_json_content(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_kinesis_producer_configuration(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_message_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'replace_new_line_characters': True}])
def test_new_line_character_replacement(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'on_missing_field': 'ERROR'},
                                              {'data_format': 'TEXT', 'on_missing_field': 'IGNORE'}])
def test_on_missing_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'partitioning_strategy': 'EXPRESSION'}])
def test_partition_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'partitioning_strategy': 'EXPRESSION'},
                                              {'partitioning_strategy': 'RANDOM'}])
def test_partitioning_strategy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'preserve_record_order': False}, {'preserve_record_order': True}])
def test_preserve_record_order(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'pretty_format': False},
                                              {'data_format': 'XML', 'pretty_format': True}])
def test_pretty_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
def test_quote_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'quote_mode': 'ALL'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'quote_mode': 'MINIMAL'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'quote_mode': 'NONE'}])
def test_quote_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
def test_record_separator(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'region': 'AP_NORTHEAST_1'},
                                              {'region': 'AP_NORTHEAST_2'},
                                              {'region': 'AP_NORTHEAST_3'},
                                              {'region': 'AP_SOUTHEAST_1'},
                                              {'region': 'AP_SOUTHEAST_2'},
                                              {'region': 'AP_SOUTH_1'},
                                              {'region': 'CA_CENTRAL_1'},
                                              {'region': 'CN_NORTHWEST_1'},
                                              {'region': 'CN_NORTH_1'},
                                              {'region': 'EU_CENTRAL_1'},
                                              {'region': 'EU_WEST_1'},
                                              {'region': 'EU_WEST_2'},
                                              {'region': 'EU_WEST_3'},
                                              {'region': 'OTHER'},
                                              {'region': 'SA_EAST_1'},
                                              {'region': 'US_EAST_1'},
                                              {'region': 'US_EAST_2'},
                                              {'region': 'US_GOV_WEST_1'},
                                              {'region': 'US_WEST_1'},
                                              {'region': 'US_WEST_2'}])
def test_region(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': False},
                                              {'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': False},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True}])
def test_register_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'replace_new_line_characters': False},
                                              {'data_format': 'DELIMITED', 'replace_new_line_characters': True}])
def test_replace_new_line_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'response_type': 'DESTINATION_RESPONSE',
                                               'send_response_to_origin': True},
                                              {'response_type': 'SUCCESS_RECORDS', 'send_response_to_origin': True}])
def test_response_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'}])
def test_schema_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
def test_schema_registry_urls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'},
                                              {'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True}])
def test_schema_subject(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_secret_access_key(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'send_response_to_origin': False}, {'send_response_to_origin': True}])
def test_send_response_to_origin(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_stream_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
def test_text_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': False},
                                              {'data_format': 'XML', 'validate_schema': True}])
def test_validate_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': True}])
def test_xml_schema(sdc_builder, sdc_executor, stage_attributes):
    pass

