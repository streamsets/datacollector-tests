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
import logging

from string import ascii_letters, ascii_lowercase
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.decorators import stub

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
@pytest.mark.parametrize('stage_attributes', [{'archiving_option': 'COPY_TO_BUCKET',
                                               'error_handling_option': 'ARCHIVE'},
                                              {'archiving_option': 'COPY_TO_PREFIX',
                                               'error_handling_option': 'ARCHIVE'},
                                              {'archiving_option': 'MOVE_TO_BUCKET',
                                               'error_handling_option': 'ARCHIVE'},
                                              {'archiving_option': 'MOVE_TO_PREFIX',
                                               'error_handling_option': 'ARCHIVE'}])
def test_archiving_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DATAGRAM', 'datagram_packet_format': 'COLLECTD'}])
def test_auth_file(sdc_builder, sdc_executor, stage_attributes):
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
def test_bucket(sdc_builder, sdc_executor):
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
def test_common_prefix(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'convert_hi_res_time_and_interval': False,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'},
                                              {'convert_hi_res_time_and_interval': True,
                                               'data_format': 'DATAGRAM',
                                               'datagram_packet_format': 'COLLECTD'}])
def test_convert_hi_res_time_and_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@gcp
def test_credentials_file_content_in_json(sdc_builder, sdc_executor, gcp):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')

    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    credentials_content_json = sdc_builder.read_file(gcp.path)
    google_cloud_storage.set_attributes(credentials_provider='JSON',
                                        credentials_file_content_in_json=credentials_content_json)

    sdc_executor.add_pipeline(pipeline)

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        data = [get_random_string(ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10, timeout_sec=120)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        if pipeline:
            sdc_executor.stop_pipeline(pipeline, force=True)

        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_credentials_file_path_in_json(sdc_builder, sdc_executor, gcp):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    storage_client = gcp.storage_client

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')

    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    credentials_file_path = gcp.path
    google_cloud_storage.set_attributes(credentials_provider='JSON_PROVIDER',
                                        credentials_file_path_in_json=credentials_file_path)

    sdc_executor.add_pipeline(pipeline)

    created_bucket = gcp.retry_429(storage_client.create_bucket)(bucket_name)
    try:
        data = [get_random_string(ascii_letters, 100) for _ in range(10)]
        blob = created_bucket.blob('gcs-test/a/b/c/d/e/sdc-test.txt')
        blob.upload_from_string('\n'.join(data))

        logger.info('Starting GCS Origin pipeline and wait until the information is read ...')
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10, timeout_sec=120)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        if pipeline:
            sdc_executor.stop_pipeline(pipeline, force=True)

        logger.info('Deleting bucket %s ...', created_bucket.name)
        gcp.retry_429(created_bucket.delete)(force=True)


@gcp
def test_credentials_file_path_in_json_not_found(sdc_builder, sdc_executor, gcp):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')

    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    credentials_file_path = '/tmp/hola.txt'
    google_cloud_storage.set_attributes(credentials_provider='JSON_PROVIDER',
                                        credentials_file_path_in_json=credentials_file_path)

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.stop_pipeline(pipeline, force=True)
        assert False, 'Should not reach here.'

    except Exception as error:
        assert 'GOOGLE_01' in error.message



@gcp
def test_credentials_file_path_with_invalid_json(sdc_builder, sdc_executor, gcp):
    pipeline_builder = sdc_builder.get_pipeline_builder()

    bucket_name = get_random_string(ascii_lowercase, 10)

    google_cloud_storage = pipeline_builder.add_stage('Google Cloud Storage', type='origin')

    google_cloud_storage.set_attributes(bucket=bucket_name,
                                        common_prefix='gcs-test',
                                        prefix_pattern='**/*.txt',
                                        data_format='TEXT')

    wiretap = pipeline_builder.add_wiretap()

    google_cloud_storage >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    credentials_file_path = '/tmp/credentials.json'
    sdc_executor.execute_shell(f'touch {credentials_file_path}')
    google_cloud_storage.set_attributes(credentials_provider='JSON_PROVIDER',
                                        credentials_file_path_in_json=credentials_file_path)

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.stop_pipeline(pipeline, force=True)
        assert False, 'Should not reach here.'

    except Exception as error:
        assert 'GOOGLE_02' in error.message

    finally:
        sdc_executor.execute_shell(f'rm -rf {credentials_file_path}')



@stub
@pytest.mark.parametrize('stage_attributes', [{'credentials_provider': 'DEFAULT_PROVIDER'},
                                              {'credentials_provider': 'JSON'},
                                              {'credentials_provider': 'JSON_PROVIDER'}])
def test_credentials_provider(sdc_builder, sdc_executor, stage_attributes):
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
                                              {'data_format': 'DELIMITED'},
                                              {'data_format': 'EXCEL'},
                                              {'data_format': 'JSON'},
                                              {'data_format': 'LOG'},
                                              {'data_format': 'PROTOBUF'},
                                              {'data_format': 'SDC_JSON'},
                                              {'data_format': 'TEXT'},
                                              {'data_format': 'WHOLE_FILE'}])
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
@pytest.mark.parametrize('stage_attributes', [{'archiving_option': 'COPY_TO_BUCKET',
                                               'error_handling_option': 'ARCHIVE'},
                                              {'archiving_option': 'MOVE_TO_BUCKET',
                                               'error_handling_option': 'ARCHIVE'}])
def test_error_bucket(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'error_handling_option': 'ARCHIVE'},
                                              {'error_handling_option': 'DELETE'},
                                              {'error_handling_option': 'NONE'}])
def test_error_handling_option(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'error_handling_option': 'ARCHIVE'}])
def test_error_prefix(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'JSON', 'json_content': 'ARRAY_OBJECTS'},
                                              {'data_format': 'JSON', 'json_content': 'MULTIPLE_OBJECTS'}])
def test_json_content(sdc_builder, sdc_executor, stage_attributes):
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
def test_max_result_queue_size(sdc_builder, sdc_executor):
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
def test_prefix_pattern(sdc_builder, sdc_executor):
    pass


@stub
def test_project_id(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'verify_checksum': False},
                                              {'data_format': 'WHOLE_FILE', 'verify_checksum': True}])
def test_verify_checksum(sdc_builder, sdc_executor, stage_attributes):
    pass

