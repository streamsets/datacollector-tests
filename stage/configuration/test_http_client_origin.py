import json

import pytest
from pretenders.common.constants import FOREVER
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import http
from streamsets.testframework.utils import get_random_string


@stub
@pytest.mark.parametrize('stage_attributes', [{'action_for_timeout': 'RETRY_EXPONENTIAL_BACKOFF'},
                                              {'action_for_timeout': 'RETRY_IMMEDIATELY'},
                                              {'action_for_timeout': 'RETRY_LINEAR_BACKOFF'},
                                              {'action_for_timeout': 'STAGE_ERROR'}])
def test_action_for_timeout(sdc_builder, sdc_executor, stage_attributes):
    pass


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
@pytest.mark.parametrize('stage_attributes', [{'action_for_timeout': 'RETRY_EXPONENTIAL_BACKOFF'},
                                              {'action_for_timeout': 'RETRY_LINEAR_BACKOFF'}])
def test_base_backoff_interval_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
def test_body_time_zone(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
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
                                              {'http_method': 'GET'},
                                              {'http_method': 'HEAD'},
                                              {'http_method': 'POST'},
                                              {'http_method': 'PUT'}])
def test_http_method(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'pagination_mode': 'BY_OFFSET'}, {'pagination_mode': 'BY_PAGE'}])
def test_initial_page_or_offset(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'}])
def test_json_content(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'keep_all_fields': False, 'pagination_mode': 'BY_OFFSET'},
                                              {'keep_all_fields': True, 'pagination_mode': 'BY_OFFSET'},
                                              {'keep_all_fields': False, 'pagination_mode': 'BY_PAGE'},
                                              {'keep_all_fields': True, 'pagination_mode': 'BY_PAGE'},
                                              {'keep_all_fields': False, 'pagination_mode': 'LINK_FIELD'},
                                              {'keep_all_fields': True, 'pagination_mode': 'LINK_FIELD'},
                                              {'keep_all_fields': False, 'pagination_mode': 'LINK_HEADER'},
                                              {'keep_all_fields': True, 'pagination_mode': 'LINK_HEADER'}])
def test_keep_all_fields(sdc_builder, sdc_executor, stage_attributes):
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
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'action_for_timeout': 'RETRY_EXPONENTIAL_BACKOFF'},
                                              {'action_for_timeout': 'RETRY_IMMEDIATELY'},
                                              {'action_for_timeout': 'RETRY_LINEAR_BACKOFF'}])
def test_max_retries(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
def test_max_templates_in_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_message_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'BATCH'}, {'mode': 'POLLING'}, {'mode': 'STREAMING'}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_multi_character_field_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_multi_character_line_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML'}])
def test_namespaces(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'pagination_mode': 'LINK_FIELD'}])
def test_next_page_link_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'pagination_mode': 'LINK_FIELD'}])
def test_next_page_link_prefix(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'output_field_attributes': False},
                                              {'data_format': 'XML', 'output_field_attributes': True}])
def test_output_field_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'pagination_mode': 'BY_OFFSET'},
                                              {'pagination_mode': 'BY_PAGE'},
                                              {'pagination_mode': 'LINK_FIELD'},
                                              {'pagination_mode': 'LINK_HEADER'},
                                              {'pagination_mode': 'NONE'}])
def test_pagination_mode(sdc_builder, sdc_executor, stage_attributes):
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
def test_per_status_actions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'POLLING'}])
def test_polling_interval_in_ms(sdc_builder, sdc_executor, stage_attributes):
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
                                              {'http_method': 'POST'},
                                              {'http_method': 'PUT'}])
def test_request_body(sdc_builder, sdc_executor, stage_attributes):
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


@http
def test_resource_url(sdc_builder, sdc_executor, http_client):
    DATA = dict(first=1)
    mock_path = get_random_string()

    try:
        http_mock = http_client.mock()
        http_mock.when(f'GET /{mock_path}').reply(json.dumps(DATA), times=FOREVER)
        mock_uri = f'{http_mock.pretend_url}/{mock_path}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        http_client_source = pipeline_builder.add_stage('HTTP Client', type='origin')
        http_client_source.resource_url = mock_uri

        trash = pipeline_builder.add_stage('Trash')

        http_client_source >> trash

        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[http_client_source].output[0]
        assert record.field == DATA
    finally:
        http_mock.delete_mock()
        sdc_executor.stop_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'pagination_mode': 'BY_OFFSET'},
                                              {'pagination_mode': 'BY_PAGE'},
                                              {'pagination_mode': 'LINK_FIELD'},
                                              {'pagination_mode': 'LINK_HEADER'}])
def test_result_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'skip_union_indexes': False},
                                              {'data_format': 'AVRO', 'skip_union_indexes': True}])
def test_skip_union_indexes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'pagination_mode': 'LINK_FIELD'}])
def test_stop_condition(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'pagination_mode': 'BY_OFFSET'},
                                              {'pagination_mode': 'BY_PAGE'},
                                              {'pagination_mode': 'LINK_FIELD'},
                                              {'pagination_mode': 'LINK_HEADER'}])
def test_wait_time_between_pages_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass

