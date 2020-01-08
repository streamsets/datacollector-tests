from collections import OrderedDict
import json
import logging
import os

import pytest
from streamsets.sdk.sdc_api import StartError
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import ftp, sdc_min_version, sftp
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


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
@pytest.mark.parametrize('stage_attributes', [{'archive_on_error': True},
                                              {'data_format': 'AVRO', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'BINARY', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'DATAGRAM', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'DELIMITED', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'EXCEL', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'FLOWFILE', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'JSON', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'LOG', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'NETFLOW', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'PROTOBUF', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'SDC_JSON', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'SYSLOG', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'TEXT', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'XML', 'file_post_processing': 'ARCHIVE'}])
def test_archive_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'archive_on_error': False}, {'archive_on_error': True}])
def test_archive_on_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
def test_auth_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'NONE'},
                                              {'authentication': 'PASSWORD'},
                                              {'authentication': 'PRIVATE_KEY'}])
def test_authentication(sdc_builder, sdc_executor, stage_attributes):
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
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_buffer_size_in_bytes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_charset(sdc_builder, sdc_executor):
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
def test_connection_timeout(sdc_builder, sdc_executor):
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
@ftp
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'data_format': 'DELIMITED'},
                                              {'data_format': 'EXCEL'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'LOG'},
                                              {'data_format': 'PROTOBUF'},
                                              {'data_format': 'SDC_JSON'},
                                              {'data_format': 'TEXT'},
                                              {'data_format': 'WHOLE_FILE'},
                                              {'data_format': 'XML'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes, ftp):
    DATA = [{'Alex': 'Developer'}, {'Xavi': 'Developer'}]
    ftp_file_name = get_random_string()
    try:
        ftp.put_string(ftp_file_name, json.dumps(DATA))

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sftp_ftp_ftps_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client',
                                                          type='origin').set_attributes(file_name_pattern=ftp_file_name,
                                                                                        **stage_attributes)
        trash = pipeline_builder.add_stage('Trash')

        sftp_ftp_ftps_client >> trash
        pipeline = pipeline_builder.build().configure_for_environment(ftp)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]
        if sftp_ftp_ftps_client.data_format == 'JSON':
            assert records == DATA if sftp_ftp_ftps_client.json_content == 'ARRAY_OBJECTS' else [DATA]

    finally:
        client = ftp.client
        client.delete(ftp_file_name)
        client.quit()
        sdc_executor.stop_pipeline(pipeline)


@stub
def test_data_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'RAW_DATA'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'SYSLOG'}])
def test_datagram_packet_format(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'disable_read_ahead_stream': False}, {'disable_read_ahead_stream': True}])
def test_disable_read_ahead_stream(sdc_builder, sdc_executor, stage_attributes):
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


@ftp
@sdc_min_version('3.9.0')
def test_file_name_pattern(sdc_builder, sdc_executor, ftp):
    """:py:func:`stage.configuration.test_sftp_ftp_ftps_client_origin.test_data_format` sets the file name pattern
    to the exact file it has created, so we simply use its JSON case as a simple test.
    """
    test_data_format(sdc_builder, sdc_executor, stage_attributes=dict(data_format='JSON'), ftp=ftp)


@sdc_min_version('3.8.0')
@sftp
@pytest.mark.parametrize('stage_attributes', [{'file_name_pattern_mode': 'GLOB'},
                                              {'file_name_pattern_mode': 'REGEX'}])
def test_file_name_pattern_mode(sdc_builder, sdc_executor, stage_attributes, sftp, keep_data):
    """Test for the File Name Pattern Mode configuration (could be glob or regex)."""
    DATA = {'name': 'Patrick Kane'}
    file_name = f'{get_random_string()}.txt'
    sftp.put_string(os.path.join(sftp.path, file_name), json.dumps(DATA))
    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        file_name_pattern = '*.txt' if stage_attributes['file_name_pattern_mode'] == 'GLOB' else r'[A-Za-z]+\.txt'
        sftp_ftp_ftps_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
        sftp_ftp_ftps_client.set_attributes(file_name_pattern=file_name_pattern, **stage_attributes)
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        sftp_ftp_ftps_client >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(sftp)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]
        assert records == [DATA]
    finally:
        if not keep_data:
            transport, client = sftp.client
            client.remove(os.path.join(sftp.path, file_name))
            client.close()
            transport.close()


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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'AVRO', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'AVRO', 'file_post_processing': 'NONE'},
                                              {'data_format': 'BINARY', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'BINARY', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'BINARY', 'file_post_processing': 'NONE'},
                                              {'data_format': 'DATAGRAM', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'DATAGRAM', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'DATAGRAM', 'file_post_processing': 'NONE'},
                                              {'data_format': 'DELIMITED', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'DELIMITED', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'DELIMITED', 'file_post_processing': 'NONE'},
                                              {'data_format': 'EXCEL', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'EXCEL', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'EXCEL', 'file_post_processing': 'NONE'},
                                              {'data_format': 'FLOWFILE', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'FLOWFILE', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'FLOWFILE', 'file_post_processing': 'NONE'},
                                              {'data_format': 'JSON', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'JSON', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'JSON', 'file_post_processing': 'NONE'},
                                              {'data_format': 'LOG', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'LOG', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'LOG', 'file_post_processing': 'NONE'},
                                              {'data_format': 'NETFLOW', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'NETFLOW', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'NETFLOW', 'file_post_processing': 'NONE'},
                                              {'data_format': 'PROTOBUF', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'PROTOBUF', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'PROTOBUF', 'file_post_processing': 'NONE'},
                                              {'data_format': 'SDC_JSON', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'SDC_JSON', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'SDC_JSON', 'file_post_processing': 'NONE'},
                                              {'data_format': 'SYSLOG', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'SYSLOG', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'SYSLOG', 'file_post_processing': 'NONE'},
                                              {'data_format': 'TEXT', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'TEXT', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'TEXT', 'file_post_processing': 'NONE'},
                                              {'data_format': 'XML', 'file_post_processing': 'ARCHIVE'},
                                              {'data_format': 'XML', 'file_post_processing': 'DELETE'},
                                              {'data_format': 'XML', 'file_post_processing': 'NONE'}])
def test_file_post_processing(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_first_file_to_process(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_client_certificate_keystore_type': 'JKS',
                                               'use_client_certificate_for_ftps': True},
                                              {'ftps_client_certificate_keystore_type': 'PKCS12',
                                               'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_data_channel_protection_level': 'CLEAR'},
                                              {'ftps_data_channel_protection_level': 'PRIVATE'}])
def test_ftps_data_channel_protection_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_mode': 'EXPLICIT'}, {'ftps_mode': 'IMPLICIT'}])
def test_ftps_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE'}])
def test_ftps_truststore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE'}])
def test_ftps_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'ALLOW_ALL'},
                                              {'ftps_truststore_provider': 'FILE'},
                                              {'ftps_truststore_provider': 'JVM_DEFAULT'}])
def test_ftps_truststore_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE', 'ftps_truststore_type': 'JKS'},
                                              {'ftps_truststore_provider': 'FILE', 'ftps_truststore_type': 'PKCS12'}])
def test_ftps_truststore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern(sdc_builder, sdc_executor, stage_attributes):
    pass


@sftp
@pytest.mark.parametrize('grok_pattern_definition', ['MYCUSTOMLOG %{COMMONAPACHELOG} %{QS:referrer}',
                                                     r'MYCUSTOMLOG \[%{HTTPDATE:timestamp}\]'])
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern_definition(sdc_builder, sdc_executor, stage_attributes, grok_pattern_definition, sftp, keep_data):
    """Test for different grok_pattern_definitions."""
    file_name = get_random_string()
    FILE_CONTENT = ('127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache.gif HTTP/1.0" 200 2326 '
                    '"http://www.example.com/strt.html"')
    if grok_pattern_definition == 'MYCUSTOMLOG %{COMMONAPACHELOG} %{QS:referrer}':
        EXPECTED_OUTPUT = {'request': '/apache.gif', 'auth': 'frank', 'ident': '-', 'verb': 'GET',
                           'referrer': '"http://www.example.com/strt.html"', 'response': '200', 'bytes': '2326',
                           'clientip': '127.0.0.1', 'httpversion': '1.0', 'rawrequest': None,
                           'timestamp': '10/Oct/2000:13:55:36 -0700'}
    else:
        EXPECTED_OUTPUT = {'timestamp': '10/Oct/2000:13:55:36 -0700'}

    try:
        sftp.put_string(os.path.join(sftp.path, file_name), FILE_CONTENT)
        builder = sdc_builder.get_pipeline_builder()
        sftp_ftp_ftps_client = builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
        sftp_ftp_ftps_client.set_attributes(file_name_pattern=file_name,
                                            grok_pattern='%{MYCUSTOMLOG}',
                                            grok_pattern_definition=grok_pattern_definition,
                                            **stage_attributes)
        trash = builder.add_stage('Trash')
        pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
        sftp_ftp_ftps_client >> [trash, pipeline_finisher]
        pipeline = builder.build().configure_for_environment(sftp)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]
        assert records == [EXPECTED_OUTPUT]

    finally:
        transport, client = sftp.client
        try:
            if not keep_data:
                client.remove(os.path.join(sftp.path, file_name))
        finally:
            client.close()
            transport.close()


@sftp
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes, sftp, keep_data):
    """Test if Header Line configuration is respected.

    Cases:
    1. IGNORE_HEADER: Should discard header of delimited file. Records should be list-maps with integer keys.
    2. NO_HEADER:  Should interpret header line as records.
    3. WITH_HEADER - Should produce records with header fields as field names.
    """
    file_name = get_random_string()

    DATA = [['c1', 'c2', 'c3'], ['f11', 'f12', 'f13'], ['f21', 'f22', 'f23']]
    EXPECTED_IGNORE_HEADER_OUTPUT = [OrderedDict([('0', 'f11'), ('1', 'f12'), ('2', 'f13')]),
                                     OrderedDict([('0', 'f21'), ('1', 'f22'), ('2', 'f23')])]
    EXPECTED_NO_HEADER_OUTPUT = [OrderedDict([('0', 'c1'), ('1', 'c2'), ('2', 'c3')]),
                                 OrderedDict([('0', 'f11'), ('1', 'f12'), ('2', 'f13')]),
                                 OrderedDict([('0', 'f21'), ('1', 'f22'), ('2', 'f23')])]
    EXPECTED_WITH_HEADER_OUTPUT = [OrderedDict([('c1', 'f11'), ('c2', 'f12'), ('c3', 'f13')]),
                                   OrderedDict([('c1', 'f21'), ('c2', 'f22'), ('c3', 'f23')])]
    file_content = '\n'.join([','.join(line) for line in DATA])

    try:
        sftp.put_string(os.path.join(sftp.path, file_name), file_content)

        builder = sdc_builder.get_pipeline_builder()
        sftp_ftp_ftps_client = builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
        sftp_ftp_ftps_client.set_attributes(file_name_pattern=file_name, **stage_attributes)
        trash = builder.add_stage('Trash')
        pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
        sftp_ftp_ftps_client >> [trash, pipeline_finisher]
        pipeline = builder.build().configure_for_environment(sftp)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]
        if stage_attributes['header_line'] == 'IGNORE_HEADER':
            assert records == EXPECTED_IGNORE_HEADER_OUTPUT
        elif stage_attributes['header_line'] == 'NO_HEADER':
            assert records == EXPECTED_NO_HEADER_OUTPUT
        elif stage_attributes['header_line'] == 'WITH_HEADER':
            assert records == EXPECTED_WITH_HEADER_OUTPUT
    finally:
        if not keep_data:
            transport, client = sftp.client
            client.remove(os.path.join(sftp.path, file_name))
            client.close()
            transport.close()


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


@sftp
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'include_custom_delimiter': False,
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': True,
                                               'use_custom_delimiter': True}])
def test_include_custom_delimiter(sdc_builder, sdc_executor, stage_attributes, sftp, keep_data):
    """Test for whether the Include Custom Delimiter configuration is respected."""
    file_name = get_random_string()
    CUSTOM_DELIMITER = '@'
    FILE_CONTENTS = f'f1{CUSTOM_DELIMITER}f2{CUSTOM_DELIMITER}f3'
    EXPECTED_OUTPUT = ([{'text': f'f1{CUSTOM_DELIMITER}'}, {'text': f'f2{CUSTOM_DELIMITER}'}, {'text': 'f3'}]
                       if stage_attributes['include_custom_delimiter']
                       else [{'text': 'f1'}, {'text': 'f2'}, {'text': 'f3'}])


    try:
        sftp.put_string(os.path.join(sftp.path, file_name), FILE_CONTENTS)
        pipeline_builder = sdc_builder.get_pipeline_builder()
        sftp_ftp_ftps_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
        sftp_ftp_ftps_client.set_attributes(file_name_pattern=file_name,
                                            custom_delimiter=CUSTOM_DELIMITER,
                                            **stage_attributes)
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        sftp_ftp_ftps_client >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(sftp)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]
        assert records == EXPECTED_OUTPUT
    finally:
        if not keep_data:
            transport, client = sftp.client
            client.remove(os.path.join(sftp.path, file_name))
            client.close()
            transport.close()


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'include_field_xpaths': False},
                                              {'data_format': 'XML', 'include_field_xpaths': True}])
def test_include_field_xpaths(sdc_builder, sdc_executor, stage_attributes):
    pass


@ftp
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'}])
def test_json_content(sdc_builder, sdc_executor, stage_attributes, ftp):
    """:py:func:`stage.configuration.test_sftp_ftp_ftps_client_origin.test_data_format` has been written to handle
    ``json_content`` being set to either of its allowed values, which we take advantage of here.
    """
    test_data_format(sdc_builder, sdc_executor, stage_attributes, ftp)


@stub
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': True}])
def test_known_hosts_file(sdc_builder, sdc_executor, stage_attributes):
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


@sdc_min_version('3.9.0')
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'},
                                              {'data_format': 'LOG'}])
@pytest.mark.parametrize('max_line_length', [155, 82])
@sftp
def test_max_line_length(sdc_builder, sdc_executor, stage_attributes, sftp, max_line_length, keep_data):
    """Check how SFTP/FTP/FTPS origin reads line in text and log file with Max Line Length set.

    Case 1 Max Line Length > length of record -> Should read complete record
    Case 2 Max Line Length < length of record -> Should truncate the record to Max Line Length value.
    """
    file_name = get_random_string()
    CONTENT = ('2019-04-30 08:23:53 AM [INFO] [streamsets.sdk.sdc_api] Pipeline Filewriterpipeline5340a2b5-b792-'
               '45f7-ac44-cf3d6df1dc29 reached status EDITED (took 0.00 s).')
    sftp.put_string(os.path.join(sftp.path, file_name), CONTENT)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_ftps_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
    sftp_ftp_ftps_client.set_attributes(file_name_pattern=file_name,
                                        max_line_length=max_line_length,
                                        **stage_attributes)
    if stage_attributes['data_format'] == 'LOG':
        REGULAR_EXPRESSION = r'(\S+) (\S+) (\S+) (\S+) (\S+) (.*)'
        LOG_FORMAT = 'REGEX'
        LOG_FIELD_MAPPING = [{'fieldPath': '/date', 'group': 1},
                             {'fieldPath': '/time', 'group': 2},
                             {'fieldPath': '/timehalf', 'group': 3},
                             {'fieldPath': '/info', 'group': 4},
                             {'fieldPath': '/file', 'group': 5},
                             {'fieldPath': '/message', 'group': 6}]
        sftp_ftp_ftps_client.set_attributes(field_path_to_regex_group_mapping=LOG_FIELD_MAPPING,
                                            log_format=LOG_FORMAT,
                                            regular_expression=REGULAR_EXPRESSION)
    trash = pipeline_builder.add_stage('Trash')
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    sftp_ftp_ftps_client >> [trash, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(sftp)
    sdc_executor.add_pipeline(pipeline)
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]
        if stage_attributes['data_format'] == 'TEXT':
            texts = [record['text'] for record in records]
            assert texts == [CONTENT[:max_line_length]]
        else:
            messages = [record['/message'] for record in records]
            assert messages == [CONTENT[55:max_line_length]]

    finally:
        if not keep_data:
            transport, client = sftp.client
            client.remove(os.path.join(sftp.path, file_name))
            client.close()
            transport.close()


@sdc_min_version('3.9.0')
@sftp
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON'}])
def test_max_object_length_in_chars(sdc_builder, sdc_executor, stage_attributes, sftp, keep_data):
    """Check if SFTP/FTP/FTPS origin honors "Max Object Length (chars)" configuration.

    Expected behavior has records after Max Object Length is reached sent to error.
    """
    DATA = [{'name': 'Amit Kumar', 'age': 24, 'car': 'lll company', 'address': ''},
            {'name': 'Nitish Kumar', 'age': 30, 'car': 'hhh company',
             'address': 'FLAT NO 555 xyz society opposite to abc school near ddd chowk wakad Pune - 411057'},
            {'name': 'Rahul HiFi', 'age': 28, 'car': 'rrr company', 'address': 'ttt'}]
    EXPECTED_OUTPUT = [{'name': 'Amit Kumar', 'age': 24, 'car': 'lll company', 'address': ''}]
    file_name = f'{get_random_string()}.json'
    file_content = ''.join([json.dumps(record) for record in DATA])
    sftp.put_string(os.path.join(sftp.path, file_name), file_content)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_ftps_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
    sftp_ftp_ftps_client.set_attributes(file_name_pattern=file_name, max_object_length_in_chars=100, **stage_attributes)
    trash = pipeline_builder.add_stage('Trash')
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    sftp_ftp_ftps_client >> [trash, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(sftp)

    sdc_executor.add_pipeline(pipeline)
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]

        assert records == EXPECTED_OUTPUT
    finally:
        if not keep_data:
            transport, client = sftp.client
            client.remove(os.path.join(sftp.path, file_name))
            client.close()
            transport.close()


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
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'path_relative_to_user_home_directory': False},
                                              {'path_relative_to_user_home_directory': True},
                                              {'data_format': 'AVRO',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'AVRO',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'BINARY',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'BINARY',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'DATAGRAM',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'DATAGRAM',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'DELIMITED',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'DELIMITED',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'EXCEL',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'EXCEL',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'FLOWFILE',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'FLOWFILE',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'JSON',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'JSON',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'LOG',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'LOG',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'NETFLOW',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'NETFLOW',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'PROTOBUF',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'PROTOBUF',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'SDC_JSON',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'SDC_JSON',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'SYSLOG',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'SYSLOG',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'TEXT',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'TEXT',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True},
                                              {'data_format': 'XML',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': False},
                                              {'data_format': 'XML',
                                               'file_post_processing': 'ARCHIVE',
                                               'path_relative_to_user_home_directory': True}])
def test_path_relative_to_user_home_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'PLAIN_TEXT'}])
def test_private_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'FILE'}])
def test_private_key_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY'}])
def test_private_key_passphrase(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'FILE'},
                                              {'authentication': 'PRIVATE_KEY', 'private_key_provider': 'PLAIN_TEXT'}])
def test_private_key_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'process_subdirectories': False}, {'process_subdirectories': True}])
def test_process_subdirectories(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF'}])
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format_type': 'CUSTOM'},
                                              {'data_format': 'DELIMITED', 'delimiter_format_type': 'MULTI_CHARACTER'}])
def test_quote_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@sftp
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_rate_per_second(sdc_builder, sdc_executor, stage_attributes, sftp, shell_executor, keep_data):
    """Test if SFTP/FTP/FTPS origin honors "Rate Per Second" configuration.

    Pipeline will be run four times with configuration set to different values
    and the expected inverse proportionality of the value and the pipeline run time checked.
    """
    try:
        DATA = 'a' * 10 * 1024 * 1024  # 10 MB file.
        file_name = get_random_string()
        sftp.put_string(os.path.join(sftp.path, file_name), DATA)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sftp_ftp_ftps_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client',
                                                          type='origin').set_attributes(file_name_pattern=file_name,
                                                                                        **stage_attributes)
        # Using Whole File data format at the origin requires a destination that supports it, as well. Local FS
        # is an easy one to use.
        local_fs = pipeline_builder.add_stage('Local FS').set_attributes(data_format='WHOLE_FILE',
                                                                         directory_template='/tmp',
                                                                         file_name_expression=file_name,
                                                                         file_type='WHOLE_FILE',
                                                                         files_prefix='')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        sftp_ftp_ftps_client >> [local_fs, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(sftp)

        pipeline_run_times = []
        for rate_per_second in ['${10 * MB}', '${5 * MB}', '${1 * MB}', '${500 * KB}']:
            sftp_ftp_ftps_client.rate_per_second = rate_per_second
            sdc_executor.add_pipeline(pipeline)
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            history = sdc_executor.get_pipeline_history(pipeline)
            pipeline_finishing_timestamp = next(entry['timeStamp']
                                                for entry in history.entries
                                                if entry['status'] == 'FINISHING')
            pipeline_running_timestamp = next(entry['timeStamp']
                                              for entry in history.entries
                                              if entry['status'] == 'RUNNING')
            pipeline_run_time = pipeline_finishing_timestamp - pipeline_running_timestamp
            logger.info('Pipeline with rate per second of %s ran for %s s', rate_per_second, pipeline_run_time)
            pipeline_run_times.append(pipeline_run_time)
            sdc_executor.remove_pipeline(pipeline)
            shell_executor(f"rm {os.path.join('/tmp', file_name)}")

        # The rate_per_second we iterate over should result in monotonically increasing run times.
        assert pipeline_run_times[0] < pipeline_run_times[1] < pipeline_run_times[2] < pipeline_run_times[3]

    finally:
        if not keep_data:
            transport, client = sftp.client
            client.remove(os.path.join(sftp.path, file_name))
            client.close()
            transport.close()


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL', 'read_all_sheets': False},
                                              {'data_format': 'EXCEL', 'read_all_sheets': True}])
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
def test_record_generation_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'REGEX'}])
def test_regular_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@sftp
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('resource_url_is_correct', [True, False])
def test_resource_url(sdc_builder, sdc_executor, sftp, resource_url_is_correct, keep_data):
    """Check if the SFTP/FTP/FTPS Client origin honors the Resource URL configuration.

    A positive and negative test case is included to ensure an Exception is raised when the URL
    is malformed.
    """
    DATA = {'name': 'Edward'}
    file_name = get_random_string()
    sftp.put_string(os.path.join(sftp.path, file_name), json.dumps(DATA))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_ftps_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
    sftp_ftp_ftps_client.file_name_pattern = file_name
    trash = pipeline_builder.add_stage('Trash')
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    sftp_ftp_ftps_client >> [trash, pipeline_finisher]
    pipeline = pipeline_builder.build().configure_for_environment(sftp)
    if not resource_url_is_correct:
        sftp_ftp_ftps_client.resource_url = 'somecrazyurlthatwillnotwork'
    transport, client = sftp.client
    try:
        sdc_executor.add_pipeline(pipeline)
        if resource_url_is_correct:
            snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
            records = [record.field for record in snapshot[sftp_ftp_ftps_client].output]
            assert records == [DATA]
        else:
            with pytest.raises(StartError):
                sdc_executor.start_pipeline(pipeline)
    finally:
        if not keep_data:
            client.remove(os.path.join(sftp.path, file_name))
            client.close()
            transport.close()


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
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': False}, {'strict_host_checking': True}])
def test_strict_host_checking(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW'},
                                              {'data_format': 'DATAGRAM', 'datagram_packet_format': 'NETFLOW'}])
def test_template_cache_timeout_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE'}])
def test_trim_stack_trace_to_length(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
def test_typesdb_file_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': False},
                                              {'use_client_certificate_for_ftps': True}])
def test_use_client_certificate_for_ftps(sdc_builder, sdc_executor, stage_attributes):
    pass


@sdc_min_version('3.9.0')
@sftp
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'use_custom_delimiter': False},
                                              {'data_format': 'TEXT', 'use_custom_delimiter': True}])
def test_use_custom_delimiter(sdc_builder, sdc_executor, stage_attributes, sftp, keep_data):
    """Test for SFTP/FTP/FTPS origin can read text file with use custom delimiter parameter as true or false.
    use custom parameter | expected outcome
    -------------------------------------------------------------------------------------------------------------------
    True                 | Records will be created based on the custom delimiters.
    False                | By default, the text data format creates records based on line breaks."""
    CONTENT = 'Python is an interpreted, high-level, general-purpose programming language;Created by Guido van Rossum'
    EXPECTED_OUTPUT = ['Python is an interpreted, high-level, general-purpose programming language',
                       'Created by Guido van Rossum']
    sftp_file_name = get_random_string()
    sftp.put_string(os.path.join(sftp.path, sftp_file_name), CONTENT)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = pipeline_builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
    sftp_ftp_client.set_attributes(file_name_pattern=sftp_file_name, custom_delimiter=';', **stage_attributes)
    trash = pipeline_builder.add_stage('Trash')
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    sftp_ftp_client >> [trash, pipeline_finisher]
    sftp_ftp_client_pipeline = pipeline_builder.build().configure_for_environment(sftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(sftp_ftp_client_pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[sftp_ftp_client.instance_name].output]
        if stage_attributes['use_custom_delimiter']:
            assert records == [{'text': EXPECTED_OUTPUT[0]}, {'text': EXPECTED_OUTPUT[1]}]
        else:
            assert records == [{'text': CONTENT}]
    finally:
        if not keep_data:
            transport, client = sftp.client
            client.remove(os.path.join(sftp.path, sftp_file_name))
            client.close()
            transport.close()


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
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}, {'authentication': 'PRIVATE_KEY'}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass
