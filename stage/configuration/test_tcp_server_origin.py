import pytest

from streamsets.testframework.decorators import stub


@stub
def test_ack_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': False,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'allow_extra_columns': False,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'allow_extra_columns': False,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_allow_extra_columns(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_auth_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_avro_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'SOURCE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'SOURCE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'avro_schema_location': 'SOURCE',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_avro_schema_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_batch_completed_ack_message(sdc_builder, sdc_executor):
    pass


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_bind_address(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'WHOLE_FILE', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'WHOLE_FILE', 'tcp_mode': 'FLUME_AVRO_IPC'}])
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_comment_marker(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_compression_format(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'convert_hi_res_time_and_interval': False,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'convert_hi_res_time_and_interval': True,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'convert_hi_res_time_and_interval': False,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'convert_hi_res_time_and_interval': True,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'convert_hi_res_time_and_interval': False,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'convert_hi_res_time_and_interval': True,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_convert_hi_res_time_and_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'use_custom_delimiter': True}])
def test_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_log_format': True},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_log_format': True},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'use_custom_log_format': True}])
def test_custom_log4j_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'APACHE_CUSTOM_LOG_FORMAT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'APACHE_CUSTOM_LOG_FORMAT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'APACHE_CUSTOM_LOG_FORMAT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_custom_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'BINARY', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'JSON', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'PROTOBUF', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'SDC_JSON', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'TEXT', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'AVRO', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'BINARY', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'JSON', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'PROTOBUF', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'SDC_JSON', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'TEXT', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'AVRO', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'BINARY', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'JSON', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'PROTOBUF', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'SDC_JSON', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'TEXT', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'XML', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'RAW_DATA',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'SYSLOG',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'RAW_DATA',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'SYSLOG',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'RAW_DATA',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'SYSLOG',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_datagram_packet_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF',
                                               'delimited_messages': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'PROTOBUF',
                                               'delimited_messages': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'PROTOBUF',
                                               'delimited_messages': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'PROTOBUF',
                                               'delimited_messages': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'PROTOBUF',
                                               'delimited_messages': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'PROTOBUF',
                                               'delimited_messages': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_delimited_messages(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_delimiter_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_delimiter_element(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CSV',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'EXCEL',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MYSQL',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'POSTGRES_CSV',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'POSTGRES_TEXT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'RFC4180',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'TDF',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CSV',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'EXCEL',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MYSQL',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'POSTGRES_CSV',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'POSTGRES_TEXT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'RFC4180',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'TDF',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CSV',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'EXCEL',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MYSQL',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'POSTGRES_CSV',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'POSTGRES_TEXT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'RFC4180',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'TDF',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_delimiter_format_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'enable_comments': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_enable_comments(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_native_transports_in_epoll': False},
                                              {'enable_native_transports_in_epoll': True}])
def test_enable_native_transports_in_epoll(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_escape_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL',
                                               'excel_header_option': 'IGNORE_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'NO_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'IGNORE_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'NO_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'IGNORE_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'NO_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_excel_header_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'exclude_interval': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_exclude_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_extra_column_prefix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_field_path_to_regex_group_mapping(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'compression_format': 'ARCHIVE',
                                               'data_format': 'BINARY',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'BINARY',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'BINARY',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'DELIMITED',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'DELIMITED',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'DELIMITED',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'JSON',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'JSON',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'JSON',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'LOG',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'LOG',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'LOG',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'PROTOBUF',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'PROTOBUF',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'PROTOBUF',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'SDC_JSON',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'SDC_JSON',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'SDC_JSON',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'TEXT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'TEXT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'TEXT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'XML',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'XML',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'ARCHIVE',
                                               'data_format': 'XML',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'BINARY',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'BINARY',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'BINARY',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'DELIMITED',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'DELIMITED',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'DELIMITED',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'JSON',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'JSON',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'JSON',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'LOG',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'LOG',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'LOG',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'PROTOBUF',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'PROTOBUF',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'PROTOBUF',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'SDC_JSON',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'SDC_JSON',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'SDC_JSON',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'TEXT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'TEXT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'TEXT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'XML',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'XML',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'compression_format': 'COMPRESSED_ARCHIVE',
                                               'data_format': 'XML',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_file_name_pattern_within_compressed_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_grok_pattern(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_grok_pattern_definition(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'header_line': 'IGNORE_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'NO_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'IGNORE_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'NO_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'IGNORE_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'NO_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'JSON',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'JSON',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'JSON',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'JSON',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'JSON',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'JSON',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'TEXT',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'TEXT',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'TEXT',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'TEXT',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'TEXT',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'TEXT',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'XML',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML',
                                               'ignore_control_characters': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'XML',
                                               'ignore_control_characters': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_ignore_control_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'ignore_empty_lines': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_ignore_empty_lines(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL',
                                               'read_all_sheets': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'read_all_sheets': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'read_all_sheets': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_import_sheets(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'include_custom_delimiter': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': False,
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': True,
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'include_custom_delimiter': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'use_custom_delimiter': True}])
def test_include_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML',
                                               'include_field_xpaths': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML',
                                               'include_field_xpaths': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML',
                                               'include_field_xpaths': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML',
                                               'include_field_xpaths': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML',
                                               'include_field_xpaths': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'XML',
                                               'include_field_xpaths': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_include_field_xpaths(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON',
                                               'json_content': 'ARRAY_OBJECTS',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'JSON',
                                               'json_content': 'MULTIPLE_OBJECTS',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'JSON',
                                               'json_content': 'ARRAY_OBJECTS',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'JSON',
                                               'json_content': 'MULTIPLE_OBJECTS',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'JSON',
                                               'json_content': 'ARRAY_OBJECTS',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'JSON',
                                               'json_content': 'MULTIPLE_OBJECTS',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_json_content(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_lines_to_skip(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'APACHE_CUSTOM_LOG_FORMAT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'APACHE_ERROR_LOG_FORMAT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'CEF',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'COMBINED_LOG_FORMAT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'COMMON_LOG_FORMAT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LEEF',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'APACHE_CUSTOM_LOG_FORMAT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'APACHE_ERROR_LOG_FORMAT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'CEF',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'COMBINED_LOG_FORMAT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'COMMON_LOG_FORMAT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LEEF',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'APACHE_CUSTOM_LOG_FORMAT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'APACHE_ERROR_LOG_FORMAT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG', 'log_format': 'CEF', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'COMBINED_LOG_FORMAT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'COMMON_LOG_FORMAT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'GROK',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LEEF',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_log_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'AUTO',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'AUTO',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'AUTO',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_batch_size_in_messages(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'BINARY', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'BINARY', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_max_data_size_in_bytes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'TEXT', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'TEXT', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_max_line_length(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_message_size_in_bytes(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'JSON', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'JSON', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_max_object_length_in_chars(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'XML', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_max_record_length_in_chars(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'NETFLOW', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'NETFLOW', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'tcp_mode': 'NETFLOW'}])
def test_max_templates_in_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'PROTOBUF', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'PROTOBUF', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_message_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_multi_character_field_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_multi_character_line_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_namespaces(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'syslog_message_transfer_framing_mode': 'NON_TRANSPARENT_FRAMING',
                                               'tcp_mode': 'SYSLOG'}])
def test_non_transparent_framing_separator(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'parse_nulls': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'parse_nulls': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'parse_nulls': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_null_constant(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_number_of_receiver_threads(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'ERROR',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'IGNORE',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'ERROR',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'IGNORE',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'ERROR',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'IGNORE',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_on_parse_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML',
                                               'output_field_attributes': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML',
                                               'output_field_attributes': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'XML',
                                               'output_field_attributes': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML',
                                               'output_field_attributes': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'XML',
                                               'output_field_attributes': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'XML',
                                               'output_field_attributes': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_output_field_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'parse_nulls': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'parse_nulls': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'parse_nulls': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'parse_nulls': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'parse_nulls': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'parse_nulls': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_parse_nulls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_port(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'PROTOBUF', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'PROTOBUF', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'CUSTOM',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format_type': 'MULTI_CHARACTER',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_quote_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'WHOLE_FILE', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'WHOLE_FILE', 'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_rate_per_second(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL',
                                               'read_all_sheets': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'read_all_sheets': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'read_all_sheets': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'read_all_sheets': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'read_all_sheets': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'EXCEL',
                                               'read_all_sheets': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_read_all_sheets(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_read_timeout_in_seconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'INTERPRETED_ONLY',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_AND_INTERPRETED',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'record_generation_mode': 'RAW_ONLY',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'record_generation_mode': 'INTERPRETED_ONLY', 'tcp_mode': 'NETFLOW'},
                                              {'record_generation_mode': 'RAW_AND_INTERPRETED', 'tcp_mode': 'NETFLOW'},
                                              {'record_generation_mode': 'RAW_ONLY', 'tcp_mode': 'NETFLOW'}])
def test_record_generation_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_record_processed_ack_message(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'tcp_mode': 'DELIMITED_RECORDS'}])
def test_record_separator(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'REGEX',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_regular_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'retain_original_line': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'retain_original_line': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'retain_original_line': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'retain_original_line': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'retain_original_line': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'LOG',
                                               'retain_original_line': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_retain_original_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'root_field_type': 'LIST',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'root_field_type': 'LIST_MAP',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DELIMITED',
                                               'root_field_type': 'LIST',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'root_field_type': 'LIST_MAP',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DELIMITED',
                                               'root_field_type': 'LIST',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DELIMITED',
                                               'root_field_type': 'LIST_MAP',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_root_field_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_schema_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_schema_registry_urls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_schema_subject(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'EXCEL',
                                               'excel_header_option': 'WITH_HEADER',
                                               'skip_cells_with_no_header': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_skip_cells_with_no_header(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO',
                                               'skip_union_indexes': False,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'AVRO',
                                               'skip_union_indexes': True,
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'AVRO',
                                               'skip_union_indexes': False,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'AVRO',
                                               'skip_union_indexes': True,
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'AVRO',
                                               'skip_union_indexes': False,
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'AVRO',
                                               'skip_union_indexes': True,
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_skip_union_indexes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'syslog_message_transfer_framing_mode': 'NON_TRANSPARENT_FRAMING',
                                               'tcp_mode': 'SYSLOG'},
                                              {'syslog_message_transfer_framing_mode': 'OCTET_COUNTING',
                                               'tcp_mode': 'SYSLOG'}])
def test_syslog_message_transfer_framing_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'tcp_mode': 'NETFLOW'},
                                              {'tcp_mode': 'SYSLOG'}])
def test_tcp_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'NETFLOW', 'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'NETFLOW', 'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'NETFLOW', 'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'NETFLOW',
                                               'tcp_mode': 'FLUME_AVRO_IPC'},
                                              {'tcp_mode': 'NETFLOW'}])
def test_template_cache_timeout_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'on_parse_error': 'INCLUDE_AS_STACK_TRACE',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_trim_stack_trace_to_length(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'DELIMITED_RECORDS'},
                                              {'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD',
                                               'tcp_mode': 'FLUME_AVRO_IPC'}])
def test_typesdb_file_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_delimiter': False},
                                              {'data_format': 'TEXT',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_delimiter': False},
                                              {'data_format': 'TEXT',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_delimiter': True},
                                              {'data_format': 'TEXT',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'use_custom_delimiter': False},
                                              {'data_format': 'TEXT',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'use_custom_delimiter': True}])
def test_use_custom_delimiter(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_log_format': False},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'use_custom_log_format': True},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_log_format': False},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'use_custom_log_format': True},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'use_custom_log_format': False},
                                              {'data_format': 'LOG',
                                               'log_format': 'LOG4J',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
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
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'verify_checksum': False},
                                              {'data_format': 'WHOLE_FILE',
                                               'tcp_mode': 'CHARACTER_BASED_LENGTH_FIELD',
                                               'verify_checksum': True},
                                              {'data_format': 'WHOLE_FILE',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'verify_checksum': False},
                                              {'data_format': 'WHOLE_FILE',
                                               'tcp_mode': 'DELIMITED_RECORDS',
                                               'verify_checksum': True},
                                              {'data_format': 'WHOLE_FILE',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'verify_checksum': False},
                                              {'data_format': 'WHOLE_FILE',
                                               'tcp_mode': 'FLUME_AVRO_IPC',
                                               'verify_checksum': True}])
def test_verify_checksum(sdc_builder, sdc_executor, stage_attributes):
    pass

