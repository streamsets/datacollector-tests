import json

import pytest
from pretenders.common.constants import FOREVER
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import http
from streamsets.testframework.utils import get_random_string
import string


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC', 'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST', 'use_oauth_2': True},
                                              {'authentication_type': 'NONE', 'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL', 'use_oauth_2': True}])
def test_additional_key_value_pairs_in_token_request_body(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': False,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
def test_allow_extra_columns(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
def test_auth_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC'},
                                              {'authentication_type': 'DIGEST'},
                                              {'authentication_type': 'NONE'},
                                              {'authentication_type': 'OAUTH'},
                                              {'authentication_type': 'UNIVERSAL'}])
def test_authentication_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE', 'data_format': 'AVRO'}])
def test_avro_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'},
                                              {'avro_schema_location': 'SOURCE', 'data_format': 'AVRO'}])
def test_avro_schema_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_buffer_size_in_bytes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_charset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'NONE',
                                               'credentials_grant_type': 'CLIENT_CREDENTIALS',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True}])
def test_client_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'NONE',
                                               'credentials_grant_type': 'CLIENT_CREDENTIALS',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True}])
def test_client_secret(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True}])
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
def test_compression_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connect_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'OAUTH'}])
def test_consumer_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'OAUTH'}])
def test_consumer_secret(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'convert_hi_res_time_and_interval': False,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'},
                                              {'convert_hi_res_time_and_interval': True,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'}])
def test_convert_hi_res_time_and_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'CLIENT_CREDENTIALS',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'CLIENT_CREDENTIALS',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'CLIENT_CREDENTIALS',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'CLIENT_CREDENTIALS',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True}])
def test_credentials_grant_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'use_custom_delimiter': True}])
def test_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': True}])
def test_custom_log4j_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'APACHE_CUSTOM_LOG_FORMAT'}])
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
                                              {'data_format': 'XML'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'RAW_DATA'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'SYSLOG'}])
def test_datagram_packet_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_method': 'DELETE'},
                                              {'http_method': 'EXPRESSION'},
                                              {'http_method': 'PATCH'},
                                              {'http_method': 'POST'},
                                              {'http_method': 'PUT'}])
def test_default_request_content_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF', 'delimited_messages': False},
                                              {'data_format': 'PROTOBUF', 'delimited_messages': True}])
def test_delimited_messages(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'}])
def test_delimiter_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
def test_delimiter_element(sdc_builder, sdc_executor, stage_attributes):
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
def test_delimiter_format_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': False},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True}])
def test_enable_comments(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': False}, {'enable_request_logging': True}])
def test_enable_request_logging(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_escape_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'excel_header_option': 'IGNORE_HEADER'},
                                              {'data_format': 'EXCEL', 'excel_header_option': 'NO_HEADER'},
                                              {'data_format': 'EXCEL', 'excel_header_option': 'WITH_HEADER'}])
def test_excel_header_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': False},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': True}])
def test_exclude_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
def test_extra_column_prefix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'REGEX'}])
def test_field_path_to_regex_group_mapping(sdc_builder, sdc_executor, stage_attributes):
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
def test_file_name_pattern_within_compressed_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern_definition(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'header_output_location': 'HEADER'}])
def test_header_attribute_prefix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'header_output_location': 'FIELD'}])
def test_header_output_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'header_output_location': 'FIELD'},
                                              {'header_output_location': 'HEADER'},
                                              {'header_output_location': 'NONE'}])
def test_header_output_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_headers(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_compression': 'GZIP'},
                                              {'http_compression': 'NONE'},
                                              {'http_compression': 'SNAPPY'}])
def test_http_compression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_method': 'DELETE'},
                                              {'http_method': 'EXPRESSION'},
                                              {'http_method': 'GET'},
                                              {'http_method': 'HEAD'},
                                              {'http_method': 'PATCH'},
                                              {'http_method': 'POST'},
                                              {'http_method': 'PUT'}])
def test_http_method(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_method': 'EXPRESSION'}])
def test_http_method_expression(sdc_builder, sdc_executor, stage_attributes):
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
def test_ignore_control_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': False},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': True}])
def test_ignore_empty_lines(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'read_all_sheets': False}])
def test_import_sheets(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'include_custom_delimiter': False,
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': True,
                                               'use_custom_delimiter': True}])
def test_include_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'include_field_xpaths': False},
                                              {'data_format': 'XML', 'include_field_xpaths': True}])
def test_include_field_xpaths(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'use_oauth_2': True}])
def test_jwt_claims(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'NONE',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'NONE',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'NONE',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'NONE',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True}])
def test_jwt_signing_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'HS512',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS256',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS384',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'JWT',
                                               'jwt_signing_algorithm': 'RS512',
                                               'use_oauth_2': True}])
def test_jwt_signing_key_in_base64_encoded(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_tls': True},
                                              {'keystore_type': 'PKCS12', 'use_tls': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED'}])
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
def test_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True, 'log_level': 'ALL'},
                                              {'enable_request_logging': True, 'log_level': 'CONFIG'},
                                              {'enable_request_logging': True, 'log_level': 'FINE'},
                                              {'enable_request_logging': True, 'log_level': 'FINER'},
                                              {'enable_request_logging': True, 'log_level': 'FINEST'},
                                              {'enable_request_logging': True, 'log_level': 'INFO'},
                                              {'enable_request_logging': True, 'log_level': 'OFF'},
                                              {'enable_request_logging': True, 'log_level': 'SEVERE'},
                                              {'enable_request_logging': True, 'log_level': 'WARNING'}])
def test_log_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'AUTO'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY'}])
def test_max_data_size_in_bytes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True}])
def test_max_entity_size(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}, {'data_format': 'LOG'}])
def test_max_line_length(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON'}])
def test_max_object_length_in_chars(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED'}, {'data_format': 'XML'}])
def test_max_record_length_in_chars(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
def test_max_templates_in_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_maximum_parallel_requests(sdc_builder, sdc_executor):
    pass


@stub
def test_maximum_request_time_in_sec(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_message_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_multi_character_field_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_multi_character_line_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'ALL_AS_LIST'},
                                              {'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes, http_client):
    """Test HTTP Lookup Processor for various HTTP methods. We do so by
    sending a request to a pre-defined HTTP server endpoint
    (testPostJsonEndpoint) and getting expected data. The pipeline looks like:

        dev_raw_data_source >> http_client_processor >> trash
    """
    multiple_values_behavior = stage_attributes['multiple_values_behavior']
    records = [
        {
            'city': 'San Francisco',
            'latitude': '37.7576948',
            'longitude': '-122.4726194'
        },
        {
            'city': 'Barcelona',
            'latitude': '41.3851',
            'longitude': '2.1734'
        },
        {
            'city': 'Chicago',
            'latitude': '41.8781',
            'longitude': '87.6298'
        }
    ]
    data = '\n'.join([json.dumps(record) for record in records])
    mock_path = get_random_string(string.ascii_letters, 10)
    http_mock = http_client.mock()

    http_mock.when(
        rule=f'GET /{mock_path}'
    ).reply(
        body=data,
        status=200,
        times=FOREVER
    )
    mock_uri = f'{http_mock.pretend_url}/{mock_path}'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data='{}')
    http_client_processor = builder.add_stage('HTTP Client', type='processor')
    # for POST/PATCH, we post 'raw_data' and expect 'expected_dict' as response data
    http_client_processor.set_attributes(
        resource_url=mock_uri,
        output_field='/',
        multiple_values_behavior=multiple_values_behavior,
        http_method='GET',
        data_format='JSON'
    )
    trash = builder.add_stage('Trash')

    dev_raw_data_source >> http_client_processor >> trash
    pipeline = builder.build(
        title=f'HTTP Lookup Processor Multiple Values Behavior {multiple_values_behavior} pipeline')
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        snapshot_records = snapshot[http_client_processor.instance_name].output
        if multiple_values_behavior == 'ALL_AS_LIST':
            assert len(snapshot_records) == 1
            assert list(snapshot_records[0].field.values())[0] == records
        elif multiple_values_behavior == 'FIRST_ONLY':
            assert len(snapshot_records) == 1
            assert list(snapshot_records[0].field.values())[0] == records[0]
        else:
            assert len(snapshot_records) == 3
            assert [list(record.field.values())[0] for record in snapshot_records] == records

    finally:
        http_mock.delete_mock()


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
def test_namespaces(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': True}])
def test_null_constant(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'LOG4J', 'on_parse_error': 'ERROR'},
                                              {'data_format': 'LOG', 'log_format': 'LOG4J', 'on_parse_error': 'IGNORE'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE'}])
def test_on_parse_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_output_field(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'output_field_attributes': False},
                                              {'data_format': 'XML', 'output_field_attributes': True}])
def test_output_field_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': False},
                                              {'data_format': 'DELIMITED', 'parse_nulls': True}])
def test_parse_nulls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC'},
                                              {'authentication_type': 'DIGEST'},
                                              {'authentication_type': 'UNIVERSAL'},
                                              {'use_proxy': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_uri(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_quote_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_rate_limit_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_rate_per_second(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'read_all_sheets': False},
                                              {'data_format': 'EXCEL', 'read_all_sheets': True}])
def test_read_all_sheets(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_read_timeout(sdc_builder, sdc_executor):
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
def test_record_generation_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'REGEX'}])
def test_regular_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'http_method': 'DELETE'},
                                              {'http_method': 'EXPRESSION'},
                                              {'http_method': 'PATCH'},
                                              {'http_method': 'POST'},
                                              {'http_method': 'PUT'}])
def test_request_data(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'request_transfer_encoding': 'BUFFERED'},
                                              {'request_transfer_encoding': 'CHUNKED'},
                                              {'authentication_type': 'BASIC',
                                               'request_transfer_encoding': 'BUFFERED',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC',
                                               'request_transfer_encoding': 'CHUNKED',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'request_transfer_encoding': 'BUFFERED',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'request_transfer_encoding': 'CHUNKED',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'request_transfer_encoding': 'BUFFERED',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'request_transfer_encoding': 'CHUNKED',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'request_transfer_encoding': 'BUFFERED',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'request_transfer_encoding': 'CHUNKED',
                                               'use_oauth_2': True}])
def test_request_transfer_encoding(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@http
def test_resource_url(sdc_builder, sdc_executor, http_client):
    DATA = dict(latitude='37.7576948', longitude='-122.4726194')
    OUTPUT_FIELD = 'result'
    mock_path = get_random_string()

    try:
        http_mock = http_client.mock()
        http_mock.when(f'GET /{mock_path}').reply(json.dumps(DATA), times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                               raw_data='ok')
        http_client = pipeline_builder.add_stage('HTTP Client',
                                                 type='processor').set_attributes(resource_url=mock_uri,
                                                                                  output_field=f'/{OUTPUT_FIELD}')
        trash = pipeline_builder.add_stage('Trash')

        dev_raw_data_source >> http_client >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[http_client].output[0]
        assert record.field[OUTPUT_FIELD] == DATA
    finally:
        http_mock.delete_mock()
        sdc_executor.stop_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'retain_original_line': False},
                                              {'data_format': 'LOG', 'retain_original_line': True}])
def test_retain_original_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'root_field_type': 'LIST'},
                                              {'data_format': 'DELIMITED', 'root_field_type': 'LIST_MAP'}])
def test_root_field_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'}])
def test_schema_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY', 'data_format': 'AVRO'}])
def test_schema_registry_urls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_schema_subject(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': False},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': True}])
def test_skip_cells_with_no_header(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'skip_union_indexes': False},
                                              {'data_format': 'AVRO', 'skip_union_indexes': True}])
def test_skip_union_indexes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
def test_template_cache_timeout_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'OAUTH'}])
def test_token(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'OAUTH'}])
def test_token_secret(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC', 'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST', 'use_oauth_2': True},
                                              {'authentication_type': 'NONE', 'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL', 'use_oauth_2': True}])
def test_token_url(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE'}])
def test_trim_stack_trace_to_length(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_trust_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'truststore_type': 'JKS', 'use_tls': True},
                                              {'truststore_type': 'PKCS12', 'use_tls': True}])
def test_truststore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
def test_typesdb_file_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'use_custom_delimiter': False},
                                              {'data_format': 'TEXT', 'use_custom_delimiter': True}])
def test_use_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': False},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'use_custom_log_format': True}])
def test_use_custom_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True},
                                              {'use_default_cipher_suites': True, 'use_tls': True}])
def test_use_default_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True},
                                              {'use_default_protocols': True, 'use_tls': True}])
def test_use_default_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC', 'use_oauth_2': False},
                                              {'authentication_type': 'BASIC', 'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST', 'use_oauth_2': False},
                                              {'authentication_type': 'DIGEST', 'use_oauth_2': True},
                                              {'authentication_type': 'NONE', 'use_oauth_2': False},
                                              {'authentication_type': 'NONE', 'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL', 'use_oauth_2': False},
                                              {'authentication_type': 'UNIVERSAL', 'use_oauth_2': True}])
def test_use_oauth_2(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'NONE',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL',
                                               'credentials_grant_type': 'RESOURCE_OWNER',
                                               'use_oauth_2': True},
                                              {'authentication_type': 'BASIC'},
                                              {'authentication_type': 'DIGEST'},
                                              {'authentication_type': 'UNIVERSAL'},
                                              {'use_proxy': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True, 'verbosity': 'HEADERS_ONLY'},
                                              {'enable_request_logging': True, 'verbosity': 'PAYLOAD_ANY'},
                                              {'enable_request_logging': True, 'verbosity': 'PAYLOAD_TEXT'}])
def test_verbosity(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'verify_checksum': False},
                                              {'data_format': 'WHOLE_FILE', 'verify_checksum': True}])
def test_verify_checksum(sdc_builder, sdc_executor, stage_attributes):
    pass

