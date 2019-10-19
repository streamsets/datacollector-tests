import pytest

from streamsets.testframework.decorators import stub


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('header_line', ['WITH_HEADER'])
@pytest.mark.parametrize('allow_extra_columns', [False, True])
@stub
def test_allow_extra_columns(sdc_builder, sdc_executor, data_format, header_line, allow_extra_columns):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@stub
def test_auth_file(sdc_builder, sdc_executor, data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('auto_offset_reset', ['EARLIEST', 'LATEST', 'NONE', 'TIMESTAMP'])
@stub
def test_auto_offset_reset(sdc_builder, sdc_executor, auto_offset_reset):
    pass


@pytest.mark.parametrize('auto_offset_reset', ['TIMESTAMP'])
@stub
def test_auto_offset_reset_timestamp_in_ms(sdc_builder, sdc_executor, auto_offset_reset):
    pass


@pytest.mark.parametrize('avro_schema_location', ['INLINE'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@stub
def test_avro_schema(sdc_builder, sdc_executor, avro_schema_location, data_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('avro_schema_location', ['INLINE', 'REGISTRY', 'SOURCE'])
@stub
def test_avro_schema_location(sdc_builder, sdc_executor, data_format, avro_schema_location):
    pass


@stub
def test_batch_wait_time_in_ms(sdc_builder, sdc_executor):
    pass


@stub
def test_broker_uri(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@stub
def test_buffer_size_in_bytes(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM', 'DELIMITED', 'JSON', 'LOG', 'TEXT', 'XML'])
@stub
def test_charset(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('enable_comments', [True])
@stub
def test_comment_marker(sdc_builder, sdc_executor, data_format, delimiter_format_type, enable_comments):
    pass


@pytest.mark.parametrize('data_format', ['BINARY', 'DELIMITED', 'JSON', 'LOG', 'PROTOBUF', 'SDC_JSON', 'TEXT', 'XML'])
@pytest.mark.parametrize('compression_format', ['ARCHIVE', 'COMPRESSED_ARCHIVE', 'COMPRESSED_FILE', 'NONE'])
@stub
def test_compression_format(sdc_builder, sdc_executor, data_format, compression_format):
    pass


@stub
def test_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
def test_consumer_group(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@pytest.mark.parametrize('convert_hi_res_time_and_interval', [False, True])
@stub
def test_convert_hi_res_time_and_interval(sdc_builder, sdc_executor, data_format, datagram_packet_format, convert_hi_res_time_and_interval):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.parametrize('use_custom_delimiter', [True])
@stub
def test_custom_delimiter(sdc_builder, sdc_executor, data_format, use_custom_delimiter):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.parametrize('use_custom_log_format', [True])
@stub
def test_custom_log4j_format(sdc_builder, sdc_executor, data_format, log_format, use_custom_log_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['APACHE_CUSTOM_LOG_FORMAT'])
@stub
def test_custom_log_format(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO', 'BINARY', 'DATAGRAM', 'DELIMITED', 'JSON', 'LOG', 'PROTOBUF', 'SDC_JSON', 'TEXT', 'XML'])
@stub
def test_data_format(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD', 'NETFLOW', 'RAW_DATA', 'SYSLOG'])
@stub
def test_datagram_packet_format(sdc_builder, sdc_executor, data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@pytest.mark.parametrize('delimited_messages', [False, True])
@stub
def test_delimited_messages(sdc_builder, sdc_executor, data_format, delimited_messages):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@stub
def test_delimiter_character(sdc_builder, sdc_executor, data_format, delimiter_format_type):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@stub
def test_delimiter_element(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CSV', 'CUSTOM', 'EXCEL', 'MULTI_CHARACTER', 'MYSQL', 'POSTGRES_CSV', 'POSTGRES_TEXT', 'RFC4180', 'TDF'])
@stub
def test_delimiter_format_type(sdc_builder, sdc_executor, data_format, delimiter_format_type):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('enable_comments', [False, True])
@stub
def test_enable_comments(sdc_builder, sdc_executor, data_format, delimiter_format_type, enable_comments):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM', 'MULTI_CHARACTER'])
@stub
def test_escape_character(sdc_builder, sdc_executor, data_format, delimiter_format_type):
    pass


@pytest.mark.parametrize('data_format', ['EXCEL'])
@pytest.mark.parametrize('excel_header_option', ['IGNORE_HEADER', 'NO_HEADER', 'WITH_HEADER'])
@stub
def test_excel_header_option(sdc_builder, sdc_executor, data_format, excel_header_option):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@pytest.mark.parametrize('exclude_interval', [False, True])
@stub
def test_exclude_interval(sdc_builder, sdc_executor, data_format, datagram_packet_format, exclude_interval):
    pass


@pytest.mark.parametrize('allow_extra_columns', [True])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('header_line', ['WITH_HEADER'])
@stub
def test_extra_column_prefix(sdc_builder, sdc_executor, allow_extra_columns, data_format, header_line):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['REGEX'])
@stub
def test_field_path_to_regex_group_mapping(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('compression_format', ['ARCHIVE', 'COMPRESSED_ARCHIVE'])
@pytest.mark.parametrize('data_format', ['BINARY', 'DELIMITED', 'JSON', 'LOG', 'PROTOBUF', 'SDC_JSON', 'TEXT', 'XML'])
@stub
def test_file_name_pattern_within_compressed_directory(sdc_builder, sdc_executor, compression_format, data_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['GROK'])
@stub
def test_grok_pattern(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['GROK'])
@stub
def test_grok_pattern_definition(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('header_line', ['IGNORE_HEADER', 'NO_HEADER', 'WITH_HEADER'])
@stub
def test_header_line(sdc_builder, sdc_executor, data_format, header_line):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM', 'DELIMITED', 'JSON', 'LOG', 'TEXT', 'XML'])
@pytest.mark.parametrize('ignore_control_characters', [False, True])
@stub
def test_ignore_control_characters(sdc_builder, sdc_executor, data_format, ignore_control_characters):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('ignore_empty_lines', [False, True])
@stub
def test_ignore_empty_lines(sdc_builder, sdc_executor, data_format, delimiter_format_type, ignore_empty_lines):
    pass


@pytest.mark.parametrize('data_format', ['EXCEL'])
@pytest.mark.parametrize('read_all_sheets', [False])
@stub
def test_import_sheets(sdc_builder, sdc_executor, data_format, read_all_sheets):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.parametrize('use_custom_delimiter', [True])
@pytest.mark.parametrize('include_custom_delimiter', [False, True])
@stub
def test_include_custom_delimiter(sdc_builder, sdc_executor, data_format, use_custom_delimiter, include_custom_delimiter):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('include_field_xpaths', [False, True])
@stub
def test_include_field_xpaths(sdc_builder, sdc_executor, data_format, include_field_xpaths):
    pass


@pytest.mark.parametrize('data_format', ['JSON'])
@pytest.mark.parametrize('json_content', ['ARRAY_OBJECTS', 'MULTIPLE_OBJECTS'])
@stub
def test_json_content(sdc_builder, sdc_executor, data_format, json_content):
    pass


@pytest.mark.parametrize('key_capture_mode', ['RECORD_FIELD', 'RECORD_HEADER_AND_FIELD'])
@stub
def test_key_capture_field(sdc_builder, sdc_executor, key_capture_mode):
    pass


@pytest.mark.parametrize('key_capture_mode', ['RECORD_HEADER', 'RECORD_HEADER_AND_FIELD'])
@stub
def test_key_capture_header_attribute(sdc_builder, sdc_executor, key_capture_mode):
    pass


@pytest.mark.parametrize('key_capture_mode', ['NONE', 'RECORD_FIELD', 'RECORD_HEADER', 'RECORD_HEADER_AND_FIELD'])
@stub
def test_key_capture_mode(sdc_builder, sdc_executor, key_capture_mode):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('key_deserializer', ['CONFLUENT', 'STRING'])
@stub
def test_key_deserializer(sdc_builder, sdc_executor, data_format, key_deserializer):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@stub
def test_lines_to_skip(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['APACHE_CUSTOM_LOG_FORMAT', 'APACHE_ERROR_LOG_FORMAT', 'CEF', 'COMBINED_LOG_FORMAT', 'COMMON_LOG_FORMAT', 'GROK', 'LEEF', 'LOG4J', 'REGEX'])
@stub
def test_log_format(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['AUTO', 'ID', 'SUBJECT'])
@stub
def test_lookup_schema_by(sdc_builder, sdc_executor, avro_schema_location, data_format, lookup_schema_by):
    pass


@stub
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['BINARY'])
@stub
def test_max_data_size_in_bytes(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG', 'TEXT'])
@stub
def test_max_line_length(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['JSON'])
@stub
def test_max_object_length_in_chars(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED', 'XML'])
@stub
def test_max_record_length_in_chars(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['NETFLOW'])
@stub
def test_max_templates_in_cache_for_data_format_netflow(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['NETFLOW'])
@stub
def test_max_templates_in_cache(sdc_builder, sdc_executor, data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@stub
def test_message_type(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['MULTI_CHARACTER'])
@stub
def test_multi_character_field_delimiter(sdc_builder, sdc_executor, data_format, delimiter_format_type):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['MULTI_CHARACTER'])
@stub
def test_multi_character_line_delimiter(sdc_builder, sdc_executor, data_format, delimiter_format_type):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@stub
def test_namespaces(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('parse_nulls', [True])
@stub
def test_null_constant(sdc_builder, sdc_executor, data_format, parse_nulls):
    pass


@stub
def test_number_of_threads(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.parametrize('on_parse_error', ['ERROR', 'IGNORE', 'INCLUDE_AS_STACK_TRACE'])
@stub
def test_on_parse_error(sdc_builder, sdc_executor, data_format, log_format, on_parse_error):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@stub
def test_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('output_field_attributes', [False, True])
@stub
def test_output_field_attributes(sdc_builder, sdc_executor, data_format, output_field_attributes):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('parse_nulls', [False, True])
@stub
def test_parse_nulls(sdc_builder, sdc_executor, data_format, parse_nulls):
    pass


@pytest.mark.parametrize('produce_single_record', [False, True])
@stub
def test_produce_single_record(sdc_builder, sdc_executor, produce_single_record):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@stub
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM', 'MULTI_CHARACTER'])
@stub
def test_quote_character(sdc_builder, sdc_executor, data_format, delimiter_format_type):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@stub
def test_rate_per_second(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['EXCEL'])
@pytest.mark.parametrize('read_all_sheets', [False, True])
@stub
def test_read_all_sheets(sdc_builder, sdc_executor, data_format, read_all_sheets):
    pass


@pytest.mark.parametrize('data_format', ['NETFLOW'])
@pytest.mark.parametrize('record_generation_mode', ['INTERPRETED_ONLY', 'RAW_AND_INTERPRETED', 'RAW_ONLY'])
@stub
def test_record_generation_mode_for_data_format_netflow(sdc_builder, sdc_executor, data_format, record_generation_mode):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['NETFLOW'])
@pytest.mark.parametrize('record_generation_mode', ['INTERPRETED_ONLY', 'RAW_AND_INTERPRETED', 'RAW_ONLY'])
@stub
def test_record_generation_mode(sdc_builder, sdc_executor, data_format, datagram_packet_format, record_generation_mode):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['REGEX'])
@stub
def test_regular_expression(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('retain_original_line', [False, True])
@stub
def test_retain_original_line(sdc_builder, sdc_executor, data_format, retain_original_line):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('root_field_type', ['LIST', 'LIST_MAP'])
@stub
def test_root_field_type(sdc_builder, sdc_executor, data_format, root_field_type):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['ID'])
@stub
def test_schema_id(sdc_builder, sdc_executor, avro_schema_location, data_format, lookup_schema_by):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@stub
def test_schema_registry_urls(sdc_builder, sdc_executor, avro_schema_location, data_format):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['SUBJECT'])
@stub
def test_schema_subject(sdc_builder, sdc_executor, avro_schema_location, data_format, lookup_schema_by):
    pass


@pytest.mark.parametrize('data_format', ['EXCEL'])
@pytest.mark.parametrize('excel_header_option', ['WITH_HEADER'])
@pytest.mark.parametrize('skip_cells_with_no_header', [False, True])
@stub
def test_skip_cells_with_no_header(sdc_builder, sdc_executor, data_format, excel_header_option, skip_cells_with_no_header):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('skip_union_indexes', [False, True])
@stub
def test_skip_union_indexes(sdc_builder, sdc_executor, data_format, skip_union_indexes):
    pass


@pytest.mark.parametrize('data_format', ['NETFLOW'])
@stub
def test_template_cache_timeout_in_ms_for_data_format_netflow(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['NETFLOW'])
@stub
def test_template_cache_timeout_in_ms(sdc_builder, sdc_executor, data_format, datagram_packet_format):
    pass


@stub
def test_topic_list(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.parametrize('on_parse_error', ['INCLUDE_AS_STACK_TRACE'])
@stub
def test_trim_stack_trace_to_length(sdc_builder, sdc_executor, data_format, log_format, on_parse_error):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@stub
def test_typesdb_file_path(sdc_builder, sdc_executor, data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.parametrize('use_custom_delimiter', [False, True])
@stub
def test_use_custom_delimiter(sdc_builder, sdc_executor, data_format, use_custom_delimiter):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.parametrize('use_custom_log_format', [False, True])
@stub
def test_use_custom_log_format(sdc_builder, sdc_executor, data_format, log_format, use_custom_log_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('value_deserializer', ['CONFLUENT', 'DEFAULT'])
@stub
def test_value_deserializer(sdc_builder, sdc_executor, data_format, value_deserializer):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@pytest.mark.parametrize('verify_checksum', [False, True])
@stub
def test_verify_checksum(sdc_builder, sdc_executor, data_format, verify_checksum):
    pass
