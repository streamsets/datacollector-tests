import pytest

from streamsets.testframework.decorators import stub


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
@pytest.mark.parametrize('stage_attributes', [{'compression_codec': 'BZIP2', 'file_type': 'SEQUENCE_FILE'},
                                              {'compression_codec': 'GZIP', 'file_type': 'SEQUENCE_FILE'},
                                              {'compression_codec': 'LZ4', 'file_type': 'SEQUENCE_FILE'},
                                              {'compression_codec': 'NONE', 'file_type': 'SEQUENCE_FILE'},
                                              {'compression_codec': 'OTHER', 'file_type': 'SEQUENCE_FILE'},
                                              {'compression_codec': 'SNAPPY', 'file_type': 'SEQUENCE_FILE'},
                                              {'compression_codec': 'BZIP2', 'file_type': 'TEXT'},
                                              {'compression_codec': 'GZIP', 'file_type': 'TEXT'},
                                              {'compression_codec': 'LZ4', 'file_type': 'TEXT'},
                                              {'compression_codec': 'NONE', 'file_type': 'TEXT'},
                                              {'compression_codec': 'OTHER', 'file_type': 'TEXT'},
                                              {'compression_codec': 'SNAPPY', 'file_type': 'TEXT'}])
def test_compression_codec(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'compression_codec': 'OTHER', 'file_type': 'SEQUENCE_FILE'},
                                              {'compression_codec': 'OTHER', 'file_type': 'TEXT'}])
def test_compression_codec_class(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'},
                                              {'data_format': 'BINARY'},
                                              {'data_format': 'DELIMITED'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'PROTOBUF'},
                                              {'data_format': 'SDC_JSON'},
                                              {'data_format': 'TEXT'},
                                              {'data_format': 'WHOLE_FILE'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_data_time_zone(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'directory_in_header': False}, {'directory_in_header': True}])
def test_directory_in_header(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'directory_in_header': False}])
def test_directory_template(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'file_type': 'SEQUENCE_FILE'},
                                              {'file_type': 'TEXT'},
                                              {'file_type': 'WHOLE_FILE'}])
def test_file_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_files_prefix(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'file_type': 'SEQUENCE_FILE'}, {'file_type': 'TEXT'}])
def test_files_suffix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'file_type': 'SEQUENCE_FILE'}, {'file_type': 'TEXT'}])
def test_idle_timeout(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'include_checksum_in_events': False},
                                              {'data_format': 'WHOLE_FILE', 'include_checksum_in_events': True}])
def test_include_checksum_in_events(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'late_record_handling': 'SEND_TO_LATE_RECORDS_FILE'}])
def test_late_record_directory_template(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'late_record_handling': 'SEND_TO_ERROR'},
                                              {'late_record_handling': 'SEND_TO_LATE_RECORDS_FILE'}])
def test_late_record_handling(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_late_record_time_limit_in_secs(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'file_type': 'SEQUENCE_FILE'}, {'file_type': 'TEXT'}])
def test_max_file_size_in_mb(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'file_type': 'SEQUENCE_FILE'}, {'file_type': 'TEXT'}])
def test_max_records_in_file(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_permissions_expression(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'use_roll_attribute': True}])
def test_roll_attribute_name(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'file_type': 'SEQUENCE_FILE'}])
def test_sequence_file_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'skip_file_recovery': False}, {'skip_file_recovery': True}])
def test_skip_file_recovery(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
def test_text_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_time_basis(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_roll_attribute': False}, {'use_roll_attribute': True}])
def test_use_roll_attribute(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'validate_permissions': False}, {'validate_permissions': True}])
def test_validate_permissions(sdc_builder, sdc_executor, stage_attributes):
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

