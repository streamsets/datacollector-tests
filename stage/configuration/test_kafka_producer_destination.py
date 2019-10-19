import pytest

from streamsets.testframework.decorators import stub


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('avro_compression_codec', ['BZIP2', 'DEFLATE', 'NULL', 'SNAPPY'])
@stub
def test_avro_compression_codec(sdc_builder, sdc_executor, data_format, avro_compression_codec):
    pass


@pytest.mark.parametrize('avro_schema_location', ['INLINE'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@stub
def test_avro_schema(sdc_builder, sdc_executor, avro_schema_location, data_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('avro_schema_location', ['HEADER', 'INLINE', 'REGISTRY'])
@stub
def test_avro_schema_location(sdc_builder, sdc_executor, data_format, avro_schema_location):
    pass


@pytest.mark.parametrize('data_format', ['BINARY'])
@stub
def test_binary_field_path(sdc_builder, sdc_executor, data_format):
    pass


@stub
def test_broker_uri(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED', 'JSON', 'TEXT'])
@stub
def test_charset(sdc_builder, sdc_executor, data_format, charset):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@pytest.mark.parametrize('include_checksum_in_events', [True])
@pytest.mark.parametrize('checksum_algorithm', ['MD5', 'MURMUR3_128', 'MURMUR3_32', 'SHA1', 'SHA256', 'SHA512'])
@stub
def test_checksum_algorithm(sdc_builder, sdc_executor, data_format, include_checksum_in_events, checksum_algorithm):
    pass


@pytest.mark.parametrize('data_format', ['AVRO', 'BINARY', 'DELIMITED', 'JSON', 'PROTOBUF', 'SDC_JSON', 'TEXT', 'XML'])
@stub
def test_data_format(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format', ['CUSTOM'])
@stub
def test_delimiter_character(sdc_builder, sdc_executor, data_format, delimiter_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format', ['CSV', 'CUSTOM', 'EXCEL', 'MULTI_CHARACTER', 'MYSQL', 'POSTGRES_CSV', 'POSTGRES_TEXT', 'RFC4180', 'TDF'])
@stub
def test_delimiter_format(sdc_builder, sdc_executor, data_format, delimiter_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format', ['CUSTOM'])
@stub
def test_escape_character(sdc_builder, sdc_executor, data_format, delimiter_format):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@pytest.mark.parametrize('file_exists', ['OVERWRITE', 'TO_ERROR'])
@stub
def test_file_exists(sdc_builder, sdc_executor, data_format, file_exists):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@stub
def test_file_name_expression(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('header_line', ['IGNORE_HEADER', 'NO_HEADER', 'WITH_HEADER'])
@stub
def test_header_line(sdc_builder, sdc_executor, data_format, header_line):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@pytest.mark.parametrize('include_checksum_in_events', [False, True])
@stub
def test_include_checksum_in_events(sdc_builder, sdc_executor, data_format, include_checksum_in_events):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('include_schema', [False, True])
@stub
def test_include_schema(sdc_builder, sdc_executor, data_format, include_schema):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.parametrize('on_missing_field', ['IGNORE'])
@pytest.mark.parametrize('insert_record_separator_if_no_text', [False, True])
@stub
def test_insert_record_separator_if_no_text(sdc_builder, sdc_executor, data_format, on_missing_field, insert_record_separator_if_no_text):
    pass


@pytest.mark.parametrize('data_format', ['JSON'])
@pytest.mark.parametrize('json_content', ['ARRAY_OBJECTS', 'MULTIPLE_OBJECTS'])
@stub
def test_json_content(sdc_builder, sdc_executor, data_format, json_content):
    pass


@stub
def test_kafka_configuration(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('message_key_format', ['AVRO', 'STRING'])
@stub
def test_kafka_message_key(sdc_builder, sdc_executor, message_key_format):
    pass


@pytest.mark.parametrize('message_key_format', ['AVRO'])
@pytest.mark.parametrize('key_serializer', ['CONFLUENT', 'STRING'])
@stub
def test_key_serializer(sdc_builder, sdc_executor, message_key_format, key_serializer):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['ID', 'SUBJECT'])
@stub
def test_lookup_schema_by(sdc_builder, sdc_executor, avro_schema_location, data_format, lookup_schema_by):
    pass


@pytest.mark.parametrize('message_key_format', ['AVRO', 'STRING'])
@stub
def test_message_key_format(sdc_builder, sdc_executor, message_key_format):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@stub
def test_message_type(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('replace_new_line_characters', [True])
@stub
def test_new_line_character_replacement(sdc_builder, sdc_executor, data_format, replace_new_line_characters):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.parametrize('on_missing_field', ['ERROR', 'IGNORE'])
@stub
def test_on_missing_field(sdc_builder, sdc_executor, data_format, on_missing_field):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@stub
def test_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.parametrize('one_message_per_batch', [False, True])
@stub
def test_one_message_per_batch(sdc_builder, sdc_executor, one_message_per_batch):
    pass


@pytest.mark.parametrize('partition_strategy', ['DEFAULT', 'EXPRESSION'])
@stub
def test_partition_expression(sdc_builder, sdc_executor, partition_strategy):
    pass


@pytest.mark.parametrize('partition_strategy', ['DEFAULT', 'EXPRESSION', 'RANDOM', 'ROUND_ROBIN'])
@stub
def test_partition_strategy(sdc_builder, sdc_executor, partition_strategy):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('pretty_format', [False, True])
@stub
def test_pretty_format(sdc_builder, sdc_executor, data_format, pretty_format):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@stub
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format', ['CUSTOM'])
@stub
def test_quote_character(sdc_builder, sdc_executor, data_format, delimiter_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format', ['CUSTOM'])
@pytest.mark.parametrize('quote_mode', ['ALL', 'MINIMAL', 'NONE'])
@stub
def test_quote_mode(sdc_builder, sdc_executor, data_format, delimiter_format, quote_mode):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@stub
def test_record_separator(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('avro_schema_location', ['HEADER', 'INLINE'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('register_schema', [False, True])
@stub
def test_register_schema(sdc_builder, sdc_executor, avro_schema_location, data_format, register_schema):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('replace_new_line_characters', [False, True])
@stub
def test_replace_new_line_characters(sdc_builder, sdc_executor, data_format, replace_new_line_characters):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('send_response_to_origin', [True])
@pytest.mark.parametrize('response_type', ['DESTINATION_RESPONSE', 'SUCCESS_RECORDS'])
@stub
def test_response_type(sdc_builder, sdc_executor, send_response_to_origin, response_type):
    pass


@pytest.mark.parametrize('runtime_topic_resolution', [False, True])
@stub
def test_runtime_topic_resolution(sdc_builder, sdc_executor, runtime_topic_resolution):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['ID'])
@stub
def test_schema_id(sdc_builder, sdc_executor, avro_schema_location, data_format, lookup_schema_by):
    pass


@pytest.mark.parametrize('avro_schema_location', ['HEADER', 'INLINE'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('register_schema', [True])
@stub
def test_schema_registry_urls(sdc_builder, sdc_executor, avro_schema_location, data_format, register_schema):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@stub
def test_schema_registry_urls_when_schema_in_registry(sdc_builder, sdc_executor, avro_schema_location, data_format):
    pass


@pytest.mark.parametrize('avro_schema_location', ['REGISTRY'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['SUBJECT'])
@stub
def test_schema_subject_when_schema_in_registry(sdc_builder, sdc_executor, avro_schema_location, data_format, lookup_schema_by):
    pass


@pytest.mark.parametrize('avro_schema_location', ['HEADER', 'INLINE'])
@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('register_schema', [True])
@stub
def test_schema_subject(sdc_builder, sdc_executor, avro_schema_location, data_format, register_schema):
    pass


@pytest.mark.parametrize('send_response_to_origin', [False, True])
@stub
def test_send_response_to_origin(sdc_builder, sdc_executor, send_response_to_origin):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@stub
def test_text_field_path(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('runtime_topic_resolution', [False])
@stub
def test_topic(sdc_builder, sdc_executor, runtime_topic_resolution):
    pass


@pytest.mark.parametrize('runtime_topic_resolution', [True])
@stub
def test_topic_expression(sdc_builder, sdc_executor, runtime_topic_resolution):
    pass


@pytest.mark.parametrize('runtime_topic_resolution', [True])
@stub
def test_topic_white_list(sdc_builder, sdc_executor, runtime_topic_resolution):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('validate_schema', [False, True])
@stub
def test_validate_schema(sdc_builder, sdc_executor, data_format, validate_schema):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('value_serializer', ['CONFLUENT', 'DEFAULT'])
@stub
def test_value_serializer(sdc_builder, sdc_executor, data_format, value_serializer):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('validate_schema', [True])
@stub
def test_xml_schema(sdc_builder, sdc_executor, data_format, validate_schema):
    pass
