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

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_compression_codec': 'BZIP2',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'},
                                              {'avro_compression_codec': 'DEFLATE',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'},
                                              {'avro_compression_codec': 'NULL',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'},
                                              {'avro_compression_codec': 'SNAPPY',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'}])
def test_avro_compression_codec(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'}])
def test_avro_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'}])
def test_avro_schema_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'BINARY', 'mode': 'PUBLISH'}])
def test_binary_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'PUBLISH'}])
def test_channel(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_charset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'checksum_algorithm': 'MD5',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True,
                                               'mode': 'PUBLISH'},
                                              {'checksum_algorithm': 'MURMUR3_128',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True,
                                               'mode': 'PUBLISH'},
                                              {'checksum_algorithm': 'MURMUR3_32',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True,
                                               'mode': 'PUBLISH'},
                                              {'checksum_algorithm': 'SHA1',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True,
                                               'mode': 'PUBLISH'},
                                              {'checksum_algorithm': 'SHA256',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True,
                                               'mode': 'PUBLISH'},
                                              {'checksum_algorithm': 'SHA512',
                                               'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True,
                                               'mode': 'PUBLISH'}])
def test_checksum_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_timeout_in_sec(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'mode': 'PUBLISH'},
                                              {'data_format': 'BINARY', 'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED', 'mode': 'PUBLISH'},
                                              {'data_format': 'JSON', 'mode': 'PUBLISH'},
                                              {'data_format': 'PROTOBUF', 'mode': 'PUBLISH'},
                                              {'data_format': 'SDC_JSON', 'mode': 'PUBLISH'},
                                              {'data_format': 'TEXT', 'mode': 'PUBLISH'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'mode': 'PUBLISH'}])
def test_delimiter_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format': 'CSV',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'EXCEL',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'MULTI_CHARACTER',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'MYSQL',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'POSTGRES_CSV',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'POSTGRES_TEXT',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'RFC4180',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'TDF',
                                               'mode': 'PUBLISH'}])
def test_delimiter_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'mode': 'PUBLISH'}])
def test_escape_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'BATCH'}])
def test_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE',
                                               'file_exists': 'OVERWRITE',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'WHOLE_FILE',
                                               'file_exists': 'TO_ERROR',
                                               'mode': 'PUBLISH'}])
def test_file_exists(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'mode': 'PUBLISH'}])
def test_file_name_expression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'header_line': 'IGNORE_HEADER',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'NO_HEADER',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER',
                                               'mode': 'PUBLISH'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': False,
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'WHOLE_FILE',
                                               'include_checksum_in_events': True,
                                               'mode': 'PUBLISH'}])
def test_include_checksum_in_events(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO', 'include_schema': False, 'mode': 'PUBLISH'},
                                              {'data_format': 'AVRO', 'include_schema': True, 'mode': 'PUBLISH'}])
def test_include_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': False,
                                               'mode': 'PUBLISH',
                                               'on_missing_field': 'IGNORE'},
                                              {'data_format': 'TEXT',
                                               'insert_record_separator_if_no_text': True,
                                               'mode': 'PUBLISH',
                                               'on_missing_field': 'IGNORE'}])
def test_insert_record_separator_if_no_text(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON',
                                               'json_content': 'ARRAY_OBJECTS',
                                               'mode': 'PUBLISH'},
                                              {'data_format': 'JSON',
                                               'json_content': 'MULTIPLE_OBJECTS',
                                               'mode': 'PUBLISH'}])
def test_json_content(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'mode': 'PUBLISH'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'mode': 'PUBLISH'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_batch_wait_time(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF', 'mode': 'PUBLISH'}])
def test_message_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'BATCH'}, {'mode': 'PUBLISH'}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'mode': 'PUBLISH',
                                               'replace_new_line_characters': True}])
def test_new_line_character_replacement(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'mode': 'PUBLISH', 'on_missing_field': 'ERROR'},
                                              {'data_format': 'TEXT', 'mode': 'PUBLISH', 'on_missing_field': 'IGNORE'}])
def test_on_missing_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'mode': 'PUBLISH', 'pretty_format': False},
                                              {'data_format': 'XML', 'mode': 'PUBLISH', 'pretty_format': True}])
def test_pretty_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'PROTOBUF', 'mode': 'PUBLISH'}])
def test_protobuf_descriptor_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'mode': 'PUBLISH'}])
def test_quote_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'mode': 'PUBLISH',
                                               'quote_mode': 'ALL'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'mode': 'PUBLISH',
                                               'quote_mode': 'MINIMAL'},
                                              {'data_format': 'DELIMITED',
                                               'delimiter_format': 'CUSTOM',
                                               'mode': 'PUBLISH',
                                               'quote_mode': 'NONE'}])
def test_quote_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'mode': 'PUBLISH'}])
def test_record_separator(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': False},
                                              {'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': False},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': True}])
def test_register_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED',
                                               'mode': 'PUBLISH',
                                               'replace_new_line_characters': False},
                                              {'data_format': 'DELIMITED',
                                               'mode': 'PUBLISH',
                                               'replace_new_line_characters': True}])
def test_replace_new_line_characters(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_retry_attempts(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID',
                                               'mode': 'PUBLISH'}])
def test_schema_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': True},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH'}])
def test_schema_registry_urls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT',
                                               'mode': 'PUBLISH'},
                                              {'avro_schema_location': 'HEADER',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': True},
                                              {'avro_schema_location': 'INLINE',
                                               'data_format': 'AVRO',
                                               'mode': 'PUBLISH',
                                               'register_schema': True}])
def test_schema_subject(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT', 'mode': 'PUBLISH'}])
def test_text_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_uri(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'mode': 'PUBLISH', 'validate_schema': False},
                                              {'data_format': 'XML', 'mode': 'PUBLISH', 'validate_schema': True}])
def test_validate_schema(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'XML', 'mode': 'PUBLISH', 'validate_schema': True}])
def test_xml_schema(sdc_builder, sdc_executor, stage_attributes):
    pass

