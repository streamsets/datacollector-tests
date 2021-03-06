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

from streamsets.testframework.decorators import stub
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.markers import category, cluster, credentialstore, sdc_min_version
from streamsets.testframework.utils import Version, get_random_string

logger = logging.getLogger(__name__)

ENCODED_KEYTAB_CONTENTS = 'encoded_keytab_contents'
CREDENTIAL_FUNCTION = 'credential_function'
CREDENTIAL_FUNCTION_WITH_GROUP = 'credential_function_with_group'


@pytest.fixture(autouse=True)
def kafka_check(cluster):
    if isinstance(cluster, ClouderaManagerCluster) and not hasattr(cluster, 'kafka'):
        pytest.skip('Kafka tests require Kafka to be installed on the cluster')


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': False,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
def test_allow_extra_columns(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
def test_auth_file(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'auto_offset_reset': 'EARLIEST'},
                                              {'auto_offset_reset': 'LATEST'},
                                              {'auto_offset_reset': 'NONE'},
                                              {'auto_offset_reset': 'TIMESTAMP'}])
def test_auto_offset_reset(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'auto_offset_reset': 'TIMESTAMP'}])
def test_auto_offset_reset_timestamp_in_ms(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE', 'data_format': 'AVRO'}])
def test_avro_schema(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'SOURCE', 'data_format': 'AVRO'}])
def test_avro_schema_location(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
def test_basic_auth_user_info(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
def test_broker_uri(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_buffer_size_in_bytes(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_charset(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True}])
def test_comment_marker(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'compression_format': 'ARCHIVE', 'data_format': 'BINARY'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'BINARY'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'BINARY'},
                                              {'compression_format': 'NONE', 'data_format': 'BINARY'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'DELIMITED'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'DELIMITED'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'DELIMITED'},
                                              {'compression_format': 'NONE', 'data_format': 'DELIMITED'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'JSON'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'JSON'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'JSON'},
                                              {'compression_format': 'NONE', 'data_format': 'JSON'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'LOG'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'LOG'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'LOG'},
                                              {'compression_format': 'NONE', 'data_format': 'LOG'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'PROTOBUF'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'PROTOBUF'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'PROTOBUF'},
                                              {'compression_format': 'NONE', 'data_format': 'PROTOBUF'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'SDC_JSON'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'SDC_JSON'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'SDC_JSON'},
                                              {'compression_format': 'NONE', 'data_format': 'SDC_JSON'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'TEXT'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'TEXT'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'TEXT'},
                                              {'compression_format': 'NONE', 'data_format': 'TEXT'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'XML'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'XML'},
                                              {'compression_format': 'COMPRESSED_FILE', 'data_format': 'XML'},
                                              {'compression_format': 'NONE', 'data_format': 'XML'}])
def test_compression_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_configuration_properties(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
def test_consumer_group(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'convert_hi_res_time_and_interval': False,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'},
                                              {'convert_hi_res_time_and_interval': True,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'}])
def test_convert_hi_res_time_and_interval(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'use_custom_delimiter': True}])
def test_custom_delimiter(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': True}])
def test_custom_log4j_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'APACHE_CUSTOM_LOG_FORMAT'}])
def test_custom_log_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'data_format': 'BINARY'},
                                              {'data_format': 'DATAGRAM'},
                                              {'data_format': 'DELIMITED'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'LOG'},
                                              {'data_format': 'PROTOBUF'},
                                              {'data_format': 'SDC_JSON'},
                                              {'data_format': 'TEXT'},
                                              {'data_format': 'XML'}])
def test_data_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'RAW_DATA'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'SYSLOG'}])
def test_datagram_packet_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF', 'delimited_messages': False},
                                              {'data_format': 'PROTOBUF', 'delimited_messages': True}])
def test_delimited_messages(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'}])
def test_delimiter_character(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
def test_delimiter_element(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'EXCEL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MYSQL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'POSTGRES_CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'POSTGRES_TEXT'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'RFC4180'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'TDF'}])
def test_delimiter_format_type(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': False},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True}])
def test_enable_comments(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_escape_character(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'excel_header_option': 'IGNORE_HEADER'},
                                              {'data_format': 'EXCEL', 'excel_header_option': 'NO_HEADER'},
                                              {'data_format': 'EXCEL', 'excel_header_option': 'WITH_HEADER'}])
def test_excel_header_option(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': False},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': True}])
def test_exclude_interval(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
def test_extra_column_prefix(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'REGEX'}])
def test_field_path_to_regex_group_mapping(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'compression_format': 'ARCHIVE', 'data_format': 'BINARY'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'DELIMITED'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'JSON'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'LOG'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'PROTOBUF'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'SDC_JSON'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'TEXT'},
                                              {'compression_format': 'ARCHIVE', 'data_format': 'XML'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'BINARY'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'DELIMITED'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'JSON'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'LOG'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'PROTOBUF'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'SDC_JSON'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'TEXT'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE', 'data_format': 'XML'}])
def test_file_name_pattern_within_compressed_directory(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern_definition(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'ignore_control_characters': False},
                                              {'data_format': 'DATAGRAM', 'ignore_control_characters': True},
                                              {'data_format': 'DELIMITED', 'ignore_control_characters': False},
                                              {'data_format': 'DELIMITED', 'ignore_control_characters': True},
                                              {'data_format': 'JSON', 'ignore_control_characters': False},
                                              {'data_format': 'JSON', 'ignore_control_characters': True},
                                              {'data_format': 'LOG', 'ignore_control_characters': False},
                                              {'data_format': 'LOG', 'ignore_control_characters': True},
                                              {'data_format': 'TEXT', 'ignore_control_characters': False},
                                              {'data_format': 'TEXT', 'ignore_control_characters': True},
                                              {'data_format': 'XML', 'ignore_control_characters': False},
                                              {'data_format': 'XML', 'ignore_control_characters': True}])
def test_ignore_control_characters(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': False},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': True}])
def test_ignore_empty_lines(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'read_all_sheets': False}])
def test_import_sheets(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'include_custom_delimiter': False,
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': True,
                                               'use_custom_delimiter': True}])
def test_include_custom_delimiter(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'include_field_xpaths': False},
                                              {'data_format': 'XML', 'include_field_xpaths': True}])
def test_include_field_xpaths(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'include_timestamps': False}, {'include_timestamps': True}])
def test_include_timestamps(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'}])
def test_json_content(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'key_capture_mode': 'RECORD_FIELD'},
                                              {'key_capture_mode': 'RECORD_HEADER_AND_FIELD'}])
def test_key_capture_field(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'key_capture_mode': 'RECORD_HEADER'},
                                              {'key_capture_mode': 'RECORD_HEADER_AND_FIELD'}])
def test_key_capture_header_attribute(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'key_capture_mode': 'NONE'},
                                              {'key_capture_mode': 'RECORD_FIELD'},
                                              {'key_capture_mode': 'RECORD_HEADER'},
                                              {'key_capture_mode': 'RECORD_HEADER_AND_FIELD'}])
def test_key_capture_mode(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'key_deserializer': 'CONFLUENT'},
                                              {'data_format': 'AVRO', 'key_deserializer': 'STRING'}])
def test_key_deserializer(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@category('basic')
@cluster('cdh')
@credentialstore
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('keytab_format', [ENCODED_KEYTAB_CONTENTS, CREDENTIAL_FUNCTION,
                                           CREDENTIAL_FUNCTION_WITH_GROUP])
@pytest.mark.parametrize('stage_attributes', [{'provide_keytab': True}])
def test_keytab(sdc_builder, sdc_executor, cluster, stage_attributes,  keytab_format):
    test_principal(sdc_builder, sdc_executor, cluster, stage_attributes, keytab_format=keytab_format)


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED'}])
def test_lines_to_skip(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'APACHE_CUSTOM_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'APACHE_ERROR_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'CEF'},
                                              {'data_format': 'LOG', 'log_format': 'COMBINED_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'COMMON_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'GROK'},
                                              {'data_format': 'LOG', 'log_format': 'LEEF'},
                                              {'data_format': 'LOG', 'log_format': 'LOG4J'},
                                              {'data_format': 'LOG', 'log_format': 'REGEX'}])
def test_log_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'AUTO'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_max_batch_size_in_records(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY'}])
def test_max_data_size_in_bytes(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG'}])
def test_max_line_length(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON'}])
def test_max_object_length_in_chars(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
def test_max_record_length_in_chars(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
def test_max_templates_in_cache(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_message_type(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_multi_character_field_delimiter(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_multi_character_line_delimiter(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
def test_namespaces(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': True}])
def test_null_constant(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
def test_number_of_threads(sdc_builder, sdc_executor, cluster):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'LOG4J', 'on_parse_error': 'ERROR'},
                                              {'data_format': 'LOG', 'log_format': 'LOG4J', 'on_parse_error': 'IGNORE'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE'}])
def test_on_parse_error(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'output_field_attributes': False},
                                              {'data_format': 'XML', 'output_field_attributes': True}])
def test_output_field_attributes(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': False},
                                              {'data_format': 'DELIMITED', 'parse_nulls': True}])
def test_parse_nulls(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'preserve_root_element': False},
                                              {'data_format': 'XML', 'preserve_root_element': True}])
def test_preserve_root_element(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@category('basic')
@cluster('cdh')
@credentialstore
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('stage_attributes', [{'provide_keytab': True}])
def test_principal(sdc_builder, sdc_executor, cluster, stage_attributes,
                   keytab_format=ENCODED_KEYTAB_CONTENTS):
    if not cluster.kafka.is_kerberized:
        pytest.skip('Test runs only if Kafka is kerberized')
    cloudera_streamsets = getattr(cluster, 'streamsets')
    if keytab_format in [CREDENTIAL_FUNCTION, CREDENTIAL_FUNCTION_WITH_GROUP]:
        if not cloudera_streamsets.credential_stores:
            pytest.skip('Test with credential function runs only if credential store was enabled')

    if keytab_format in [CREDENTIAL_FUNCTION_WITH_GROUP]:
        azure_keyvault = cloudera_streamsets.credential_stores.get('azure')
        if not azure_keyvault or not azure_keyvault.enforce_entry_group:
            pytest.skip('Test with credential function with enforce group runs only'
                        ' if enforceEntryGroup was set to True')

    encoded_keytabs_for_stages = getattr(cluster.kafka, 'encoded_keytabs_for_stages', None)
    keytab_for_stage = (encoded_keytabs_for_stages.get('Kafka Multitopic Consumer')
                        if encoded_keytabs_for_stages else None)
    if not keytab_for_stage:
        pytest.skip('Test runs only if --stage-keytab argument is provided for `Kafka Multitopic Consumer` stage')

    if keytab_format == ENCODED_KEYTAB_CONTENTS:
        keytab_value = keytab_for_stage.base64_encoded_keytab_contents
    elif keytab_format in [CREDENTIAL_FUNCTION, CREDENTIAL_FUNCTION_WITH_GROUP]:
        keytab_value = keytab_for_stage.credential_function_for_keytab

    MESSAGE = 'Hello World from SDC & DPM!'
    EXPECTED = {'text': 'Hello World from SDC & DPM!'}

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()

    topic_name = get_random_string()
    kafka_multitopic_consumer = builder.add_stage('Kafka Multitopic Consumer',
                                                  library=cluster.kafka.standalone_stage_lib)
    if Version(sdc_builder.version) < Version('3.19'):
        stage_attributes.update({'keytab': keytab_value,
                                 'principal': keytab_for_stage.principal})
    else:
        if 'provide_keytab' in stage_attributes:
            stage_attributes['provide_keytab_at_runtime'] = stage_attributes.pop('provide_keytab')
        stage_attributes.update({'runtime_keytab': keytab_value,
                                 'runtime_principal': keytab_for_stage.principal})
    kafka_multitopic_consumer.set_attributes(auto_offset_reset='EARLIEST',
                                             batch_wait_time_in_ms=2000,
                                             data_format='TEXT',
                                             topic_list=[topic_name],
                                             **stage_attributes)

    wiretap = builder.add_wiretap()
    kafka_multitopic_consumer >> wiretap.destination
    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Publish messages to Kafka and verify using wiretap if the same messages are received.
        producer = cluster.kafka.producer()
        producer.send(topic_name, MESSAGE.encode())

        # Start Pipeline.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # Verify wiretap data.
        records = [record.field for record in wiretap.output_records]
        assert [EXPECTED] == records
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'produce_single_record': False}, {'produce_single_record': True}])
def test_produce_single_record(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@category('basic')
@cluster('cdh')
@credentialstore
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('stage_attributes', [{'provide_keytab': False}, {'provide_keytab': True}])
def test_provide_keytab(sdc_builder, sdc_executor, cluster, stage_attributes):
    if stage_attributes['provide_keytab']:
        test_principal(sdc_builder, sdc_executor, cluster, stage_attributes)
    else:
        test_topic_list(sdc_builder, sdc_executor, cluster)


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_quote_character(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_rate_per_second(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'read_all_sheets': False},
                                              {'data_format': 'EXCEL', 'read_all_sheets': True}])
def test_read_all_sheets(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY'}])
def test_record_generation_mode(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'REGEX'}])
def test_regular_expression(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'retain_original_line': False},
                                              {'data_format': 'LOG', 'retain_original_line': True}])
def test_retain_original_line(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'root_field_type': 'LIST'},
                                              {'data_format': 'DELIMITED', 'root_field_type': 'LIST_MAP'}])
def test_root_field_type(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'}])
def test_schema_id(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
def test_schema_registry_urls(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_schema_subject(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': False},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': True}])
def test_skip_cells_with_no_header(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'skip_union_indexes': False},
                                              {'data_format': 'AVRO', 'skip_union_indexes': True}])
def test_skip_union_indexes(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
def test_template_cache_timeout_in_ms(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@category('basic')
@cluster('cdh', 'kafka')
@sdc_min_version('3.0.0.0')
def test_topic_list(sdc_builder, sdc_executor, cluster):
    MESSAGE = 'Hello World from SDC & DPM!'
    EXPECTED = {'text': 'Hello World from SDC & DPM!'}

    # Build the Kafka consumer pipeline with Standalone mode.
    builder = sdc_builder.get_pipeline_builder()

    topic_name = get_random_string()
    kafka_multitopic_consumer = builder.add_stage('Kafka Multitopic Consumer',
                                                  library=cluster.kafka.standalone_stage_lib)

    if Version(sdc_builder.version) < Version('3.7.0'):
        kafka_multitopic_consumer.set_attributes(batch_wait_time_in_ms=2000,
                                                 data_format='TEXT',
                                                 topic_list=[topic_name])
        kafka_multitopic_consumer.configuration_properties = [{ 'key':'auto.offset.reset', 'value': 'earliest'}]
    else:
        kafka_multitopic_consumer.set_attributes(auto_offset_reset='EARLIEST',
                                                 batch_wait_time_in_ms=2000,
                                                 data_format='TEXT',
                                                 topic_list=[topic_name])

    wiretap = builder.add_wiretap()
    kafka_multitopic_consumer >> wiretap.destination
    pipeline = builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Publish messages to Kafka and verify using wiretap if the same messages are received.
        producer = cluster.kafka.producer()
        producer.send(topic_name, MESSAGE.encode())

        # Start Pipeline.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # Verify wiretap data.
        records = [record.field for record in wiretap.output_records]
        assert [EXPECTED] == records
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE'}])
def test_trim_stack_trace_to_length(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
def test_typesdb_file_path(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'use_custom_delimiter': False},
                                              {'data_format': 'TEXT', 'use_custom_delimiter': True}])
def test_use_custom_delimiter(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': False},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': True}])
def test_use_custom_log_format(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'value_deserializer': 'CONFLUENT'},
                                              {'data_format': 'AVRO', 'value_deserializer': 'DEFAULT'}])
def test_value_deserializer(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'verify_checksum': False},
                                              {'data_format': 'WHOLE_FILE', 'verify_checksum': True}])
def test_verify_checksum(sdc_builder, sdc_executor, cluster, stage_attributes):
    pass
