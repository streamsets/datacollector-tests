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

from streamsets.sdk.exceptions import StartError
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import Version


KEYSTORE_FILE_PATH = 'resources/tls/keystore.jks'
KEYSTORE_TYPE = 'JKS'
KEYSTORE_PASSWORD = 'password'


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': False,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
@sdc_min_version('3.4.0')
def test_allow_extra_columns(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@sdc_min_version('3.4.0')
def test_application_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'application_id_in_url': False}, {'application_id_in_url': True}])
@sdc_min_version('3.4.0')
def test_application_id_in_url(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
@sdc_min_version('3.4.0')
def test_auth_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_compression_codec': 'BZIP2', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'DEFLATE', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'NULL', 'data_format': 'AVRO'},
                                              {'avro_compression_codec': 'SNAPPY', 'data_format': 'AVRO'}])
@sdc_min_version('3.4.0')
def test_avro_compression_codec(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'avro_schema_location': 'INLINE', 'data_format': 'AVRO'}])
@sdc_min_version('3.4.0')
def test_avro_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'INLINE', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
@sdc_min_version('3.4.0')
def test_avro_schema_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY'}])
@sdc_min_version('3.4.0')
def test_binary_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
@sdc_min_version('3.4.0')
def test_buffer_size_in_bytes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@sdc_min_version('3.4.0')
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
@sdc_min_version('3.4.0')
def test_checksum_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
@sdc_min_version('3.4.0')
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True}])
@sdc_min_version('3.4.0')
def test_comment_marker(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
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
@sdc_min_version('3.4.0')
def test_compression_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'convert_hi_res_time_and_interval': False,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'},
                                              {'convert_hi_res_time_and_interval': True,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'}])
@sdc_min_version('3.4.0')
def test_convert_hi_res_time_and_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'use_custom_delimiter': True}])
@sdc_min_version('3.4.0')
def test_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': True}])
@sdc_min_version('3.4.0')
def test_custom_log4j_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'APACHE_CUSTOM_LOG_FORMAT'}])
@sdc_min_version('3.4.0')
def test_custom_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'data_format': 'BINARY'},
                                              {'data_format': 'DATAGRAM'},
                                              {'data_format': 'DELIMITED'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'LOG'},
                                              {'data_format': 'PROTOBUF'},
                                              {'data_format': 'SDC_JSON'},
                                              {'data_format': 'TEXT'},
                                              {'data_format': 'XML'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'XML'}])
@sdc_min_version('3.4.0')
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'RAW_DATA'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'SYSLOG'}])
@sdc_min_version('3.4.0')
def test_datagram_packet_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF', 'delimited_messages': False},
                                              {'data_format': 'PROTOBUF', 'delimited_messages': True}])
@sdc_min_version('3.4.0')
def test_delimited_messages(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
@sdc_min_version('3.4.0')
def test_delimiter_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
@sdc_min_version('3.4.0')
def test_delimiter_element(sdc_builder, sdc_executor, stage_attributes):
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
@sdc_min_version('3.4.0')
def test_delimiter_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'EXCEL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MYSQL'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'POSTGRES_CSV'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'POSTGRES_TEXT'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'RFC4180'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'TDF'}])
@sdc_min_version('3.4.0')
def test_delimiter_format_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': False},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True}])
@sdc_min_version('3.4.0')
def test_enable_comments(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
@sdc_min_version('3.4.0')
def test_escape_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'excel_header_option': 'IGNORE_HEADER'},
                                              {'data_format': 'EXCEL', 'excel_header_option': 'NO_HEADER'},
                                              {'data_format': 'EXCEL', 'excel_header_option': 'WITH_HEADER'}])
@sdc_min_version('3.4.0')
def test_excel_header_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': False},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': True}])
@sdc_min_version('3.4.0')
def test_exclude_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
@sdc_min_version('3.4.0')
def test_extra_column_prefix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'REGEX'}])
@sdc_min_version('3.4.0')
def test_field_path_to_regex_group_mapping(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'file_exists': 'OVERWRITE'},
                                              {'data_format': 'WHOLE_FILE', 'file_exists': 'TO_ERROR'}])
@sdc_min_version('3.4.0')
def test_file_exists(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
@sdc_min_version('3.4.0')
def test_file_name_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
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
@sdc_min_version('3.4.0')
def test_file_name_pattern_within_compressed_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
@sdc_min_version('3.4.0')
def test_grok_pattern(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
@sdc_min_version('3.4.0')
def test_grok_pattern_definition(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
@sdc_min_version('3.4.0')
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@sdc_min_version('3.4.0')
def test_http_listening_port(sdc_builder, sdc_executor):
    pass


@stub
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
@sdc_min_version('3.4.0')
def test_ignore_control_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': False},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': True}])
@sdc_min_version('3.4.0')
def test_ignore_empty_lines(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'read_all_sheets': False}])
@sdc_min_version('3.4.0')
def test_import_sheets(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'include_checksum_in_events': False},
                                              {'data_format': 'WHOLE_FILE', 'include_checksum_in_events': True}])
@sdc_min_version('3.4.0')
def test_include_checksum_in_events(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'include_custom_delimiter': False,
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': True,
                                               'use_custom_delimiter': True}])
@sdc_min_version('3.4.0')
def test_include_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'include_field_xpaths': False},
                                              {'data_format': 'XML', 'include_field_xpaths': True}])
@sdc_min_version('3.4.0')
def test_include_field_xpaths(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'include_schema': False},
                                              {'data_format': 'AVRO', 'include_schema': True}])
@sdc_min_version('3.4.0')
def test_include_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': False,
                                               'on_missing_field': 'IGNORE'},
                                              {'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': True,
                                               'on_missing_field': 'IGNORE'}])
@sdc_min_version('3.4.0')
def test_insert_record_separator_if_no_text(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'}])
@sdc_min_version('3.4.0')
def test_json_content(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'use_tls': True, 'keystore_file': KEYSTORE_FILE_PATH},
                                              {'use_tls': True, 'keystore_file': 'wrong/path/file.jks'}])
@sdc_min_version('3.4.0')
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    """Test "KeyStore path" config parameter. It is tested with two values, one pointing to a real KeyStore file
    and the other to an unexisting file. We check a TLS_01 error is raised for the unexisting file and that
    the pipeline successfully transitions to RUNNING state if the file exists.

    Pipeline:
      rest_srv >> trash

    """
    builder = sdc_builder.get_pipeline_builder()
    rest_srv = builder.add_stage('REST Service')

    if Version('3.16.0') <= Version(sdc_builder.version) < Version('3.17.0'):
        list_of_application_ids = [{"appId": 'admin'}]
        rest_srv.set_attributes(list_of_application_ids=list_of_application_ids)
    elif Version(sdc_builder.version) >= Version('3.17.0'):
        list_of_application_ids = [{"credential": 'admin'}]
        rest_srv.set_attributes(list_of_application_ids=list_of_application_ids)
    else:
        app_id = 'admin'
        rest_srv.set_attributes(application_id=app_id)

    rest_srv.set_attributes(keystore_type=KEYSTORE_TYPE,
                            keystore_password=KEYSTORE_PASSWORD,
                            **stage_attributes)

    trash = builder.add_stage('Trash')
    rest_srv >> trash

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    if stage_attributes['keystore_file'] == KEYSTORE_FILE_PATH:
        # Expecting SDC loads the KeyStore and successfully starts to run the pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_status(status='RUNNING')
        sdc_executor.stop_pipeline(pipeline)
    else:
        # Expecting a StartError from SDC due to unexisting KeyStore file (TLS_01 error).
        with pytest.raises(StartError) as e:
            sdc_executor.start_pipeline(pipeline).wait_for_status(status='RUNNING')
        assert e.value.message.startswith('TLS_01')


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
@sdc_min_version('3.4.0')
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
@sdc_min_version('3.4.0')
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_tls': True},
                                              {'keystore_type': 'PKCS12', 'use_tls': True}])
@sdc_min_version('3.4.0')
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED'}])
@sdc_min_version('3.4.0')
def test_lines_to_skip(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'APACHE_CUSTOM_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'APACHE_ERROR_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'CEF'},
                                              {'data_format': 'LOG', 'log_format': 'COMBINED_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'COMMON_LOG_FORMAT'},
                                              {'data_format': 'LOG', 'log_format': 'GROK'},
                                              {'data_format': 'LOG', 'log_format': 'LEEF'},
                                              {'data_format': 'LOG', 'log_format': 'LOG4J'},
                                              {'data_format': 'LOG', 'log_format': 'REGEX'}])
@sdc_min_version('3.4.0')
def test_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'lookup_schema_by': 'AUTO'},
                                              {'data_format': 'AVRO', 'lookup_schema_by': 'ID'},
                                              {'data_format': 'AVRO', 'lookup_schema_by': 'SUBJECT'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
@sdc_min_version('3.4.0')
def test_lookup_schema_by(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@sdc_min_version('3.4.0')
def test_max_concurrent_requests(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY'}])
@sdc_min_version('3.4.0')
def test_max_data_size_in_bytes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}, {'data_format': 'LOG'}])
@sdc_min_version('3.4.0')
def test_max_line_length(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON'}])
@sdc_min_version('3.4.0')
def test_max_object_length_in_chars(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED'}, {'data_format': 'XML'}])
@sdc_min_version('3.4.0')
def test_max_record_length_in_chars(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@sdc_min_version('3.4.0')
def test_max_request_size_in_mb(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
@sdc_min_version('3.4.0')
def test_max_templates_in_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}, {'data_format': 'PROTOBUF'}])
@sdc_min_version('3.4.0')
def test_message_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
@sdc_min_version('3.4.0')
def test_multi_character_field_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
@sdc_min_version('3.4.0')
def test_multi_character_line_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
@sdc_min_version('3.4.0')
def test_namespaces(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'replace_new_line_characters': True}])
@sdc_min_version('3.4.0')
def test_new_line_character_replacement(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': True}])
@sdc_min_version('3.4.0')
def test_null_constant(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'on_missing_field': 'ERROR'},
                                              {'data_format': 'TEXT', 'on_missing_field': 'IGNORE'}])
@sdc_min_version('3.4.0')
def test_on_missing_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'LOG4J', 'on_parse_error': 'ERROR'},
                                              {'data_format': 'LOG', 'log_format': 'LOG4J', 'on_parse_error': 'IGNORE'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE'}])
@sdc_min_version('3.4.0')
def test_on_parse_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
@sdc_min_version('3.4.0')
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'output_field_attributes': False},
                                              {'data_format': 'XML', 'output_field_attributes': True}])
@sdc_min_version('3.4.0')
def test_output_field_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': False},
                                              {'data_format': 'DELIMITED', 'parse_nulls': True}])
@sdc_min_version('3.4.0')
def test_parse_nulls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'pretty_format': False},
                                              {'data_format': 'XML', 'pretty_format': True}])
@sdc_min_version('3.4.0')
def test_pretty_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}, {'data_format': 'PROTOBUF'}])
@sdc_min_version('3.4.0')
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'},
                                              {'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
@sdc_min_version('3.4.0')
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
@sdc_min_version('3.4.0')
def test_quote_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
@sdc_min_version('3.4.0')
def test_rate_per_second(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'read_all_sheets': False},
                                              {'data_format': 'EXCEL', 'read_all_sheets': True}])
@sdc_min_version('3.4.0')
def test_read_all_sheets(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW', 'record_generation_mode': 'INTERPRETED_ONLY'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED'},
                                              {'data_format': 'NETFLOW', 'record_generation_mode': 'RAW_ONLY'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY'}])
@sdc_min_version('3.4.0')
def test_record_generation_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
@sdc_min_version('3.4.0')
def test_record_separator(sdc_builder, sdc_executor, stage_attributes):
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
@sdc_min_version('3.4.0')
def test_register_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'REGEX'}])
@sdc_min_version('3.4.0')
def test_regular_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'replace_new_line_characters': False},
                                              {'data_format': 'DELIMITED', 'replace_new_line_characters': True}])
@sdc_min_version('3.4.0')
def test_replace_new_line_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'require_client_authentication': False, 'use_tls': True},
                                              {'require_client_authentication': True, 'use_tls': True}])
@sdc_min_version('3.4.0')
def test_require_client_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'retain_original_line': False},
                                              {'data_format': 'LOG', 'retain_original_line': True}])
@sdc_min_version('3.4.0')
def test_retain_original_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'root_field_type': 'LIST'},
                                              {'data_format': 'DELIMITED', 'root_field_type': 'LIST_MAP'}])
@sdc_min_version('3.4.0')
def test_root_field_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'}])
@sdc_min_version('3.4.0')
def test_schema_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
@sdc_min_version('3.4.0')
def test_schema_registry_urls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'lookup_schema_by': 'SUBJECT'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'},
                                              {'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'register_schema': True}])
@sdc_min_version('3.4.0')
def test_schema_subject(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'send_raw_response': False}, {'send_raw_response': True}])
@sdc_min_version('3.4.0')
def test_send_raw_response(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': False},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': True}])
@sdc_min_version('3.4.0')
def test_skip_cells_with_no_header(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'skip_union_indexes': False},
                                              {'data_format': 'AVRO', 'skip_union_indexes': True}])
@sdc_min_version('3.4.0')
def test_skip_union_indexes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
@sdc_min_version('3.4.0')
def test_template_cache_timeout_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
@sdc_min_version('3.4.0')
def test_text_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
@sdc_min_version('3.4.0')
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE'}])
@sdc_min_version('3.4.0')
def test_trim_stack_trace_to_length(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
@sdc_min_version('3.4.0')
def test_truststore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
@sdc_min_version('3.4.0')
def test_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
@sdc_min_version('3.4.0')
def test_truststore_trust_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'truststore_type': 'JKS', 'use_tls': True},
                                              {'truststore_type': 'PKCS12', 'use_tls': True}])
@sdc_min_version('3.4.0')
def test_truststore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
@sdc_min_version('3.4.0')
def test_typesdb_file_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'use_custom_delimiter': False},
                                              {'data_format': 'TEXT', 'use_custom_delimiter': True}])
@sdc_min_version('3.4.0')
def test_use_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': False},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': True}])
@sdc_min_version('3.4.0')
def test_use_custom_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True},
                                              {'use_default_cipher_suites': True, 'use_tls': True}])
@sdc_min_version('3.4.0')
def test_use_default_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True},
                                              {'use_default_protocols': True, 'use_tls': True}])
@sdc_min_version('3.4.0')
def test_use_default_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
@sdc_min_version('3.4.0')
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': False},
                                              {'data_format': 'XML', 'validate_schema': True}])
@sdc_min_version('3.4.0')
def test_validate_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': True}])
@sdc_min_version('3.4.0')
def test_xml_schema(sdc_builder, sdc_executor, stage_attributes):
    pass

