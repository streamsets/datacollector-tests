import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_type': 'BASIC', 'use_oauth_2': True},
                                              {'authentication_type': 'DIGEST', 'use_oauth_2': True},
                                              {'authentication_type': 'NONE', 'use_oauth_2': True},
                                              {'authentication_type': 'UNIVERSAL', 'use_oauth_2': True}])
def test_additional_key_value_pairs_in_token_request_body(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': False}, {'enable_request_logging': True}])
def test_enable_request_logging(sdc_builder, sdc_executor, stage_attributes):
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
                                               'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True}])
def test_max_entity_size(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'one_request_per_batch': False}, {'one_request_per_batch': True}])
def test_one_request_per_batch(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'pretty_format': False},
                                              {'data_format': 'XML', 'pretty_format': True}])
def test_pretty_format(sdc_builder, sdc_executor, stage_attributes):
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
def test_rate_limit_in_requests_per_sec(sdc_builder, sdc_executor):
    pass


@stub
def test_read_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
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
def test_register_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'replace_new_line_characters': False},
                                              {'data_format': 'DELIMITED', 'replace_new_line_characters': True}])
def test_replace_new_line_characters(sdc_builder, sdc_executor, stage_attributes):
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


@stub
def test_resource_url(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'send_response_to_origin': False}, {'send_response_to_origin': True}])
def test_send_response_to_origin(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
def test_text_field_path(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': False},
                                              {'data_format': 'XML', 'validate_schema': True}])
def test_validate_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_request_logging': True, 'verbosity': 'HEADERS_ONLY'},
                                              {'enable_request_logging': True, 'verbosity': 'PAYLOAD_ANY'},
                                              {'enable_request_logging': True, 'verbosity': 'PAYLOAD_TEXT'}])
def test_verbosity(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'validate_schema': True}])
def test_xml_schema(sdc_builder, sdc_executor, stage_attributes):
    pass

