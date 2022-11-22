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
import os
import pytest
import string
import tempfile

from streamsets.testframework.decorators import stub

from streamsets.testframework.markers import sftp, ftp, sdc_min_version
from streamsets.testframework.utils import get_random_string


@sdc_min_version("3.22.0")
@sftp
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'NONE'},
                                              {'authentication': 'PASSWORD'},
                                              {'authentication': 'PRIVATE_KEY'}])
def test_authentication(sdc_builder, sdc_executor, sftp, stage_attributes):
    """Test SFTP and FTP/FTPS destination. We first create a local file using shell and use
    that file for SFTP/FTP/FTPS destination stage to see if it gets successfully uploaded.
    The pipelines look like:
        directory >> sftp_ftp_client
    """
    # Our origin SFTP/FTP/FTPS file name
    sftp_ftp_file_name = get_random_string(string.ascii_letters, 10)
    local_tmp_directory = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_file_name = f'sdc-{get_random_string(string.ascii_letters, 5)}'
    raw_text_data = 'Hello World!'

    sdc_executor.execute_shell(f'mkdir {local_tmp_directory}/')
    sdc_executor.execute_shell(f'echo {raw_text_data} >> {local_tmp_directory}/{local_file_name}')

    # Build source file pipeline logic
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'WHOLE_FILE'
    directory.file_name_pattern = 'sdc*'
    directory.files_directory = local_tmp_directory

    sftp_ftp_client = builder.add_stage(name='com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget')
    sftp_ftp_client.file_name_expression = sftp_ftp_file_name

    directory >> sftp_ftp_client

    sftp_ftp_client.authentication = stage_attributes['authentication']

    sftp_ftp_client_pipeline = builder.build('SFTP Destination Pipeline - Authentication').configure_for_environment(
        sftp)

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    # Start SFTP/FTP/FTPS upload (destination) file pipeline and assert pipeline has processed expected number of files
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)
    history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)

    try:
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0

        assert sftp.get_string(os.path.join(sftp.path, sftp_ftp_file_name)).strip() == raw_text_data

        # Delete the test SFTP origin file we created
        transport, client = sftp.client
        client.remove(os.path.join(sftp.path, sftp_ftp_file_name))
    finally:
        client.close()
        transport.close()


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
@pytest.mark.parametrize('stage_attributes', [{'checksum_algorithm': 'MD5', 'data_format': 'WHOLE_FILE'},
                                              {'checksum_algorithm': 'MURMUR3_128', 'data_format': 'WHOLE_FILE'},
                                              {'checksum_algorithm': 'MURMUR3_32', 'data_format': 'WHOLE_FILE'},
                                              {'checksum_algorithm': 'SHA1', 'data_format': 'WHOLE_FILE'},
                                              {'checksum_algorithm': 'SHA256', 'data_format': 'WHOLE_FILE'},
                                              {'checksum_algorithm': 'SHA512', 'data_format': 'WHOLE_FILE'}])
def test_checksum_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_path': False}, {'create_path': True}])
def test_create_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_data_timeout(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'delimiter_format': 'CUSTOM'}])
def test_escape_character(sdc_builder, sdc_executor, stage_attributes):
    pass


@ftp
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE', 'file_exists': 'OVERWRITE'},
                                              {'data_format': 'WHOLE_FILE', 'file_exists': 'TO_ERROR'}])
def test_file_exists(sdc_builder, sdc_executor, ftp, stage_attributes):
    """Test FTP/FTPS destination. We first create a file in the SFTP/FTP/FTPS server. Then create a local file with the
        same name but different content using shell and upload that file with SFTP/FTP/FTPS destination stage to see the
        response when the file already exists.
        The pipelines look like:
            directory >> sftp_ftp_client
    """
    # Our destination SFTP/FTP/FTPS file name
    sftp_ftp_file_name = get_random_string(string.ascii_letters, 10)
    # Local temporary directory where we will create a source file to be uploaded to SFTP/FTP/FTPS server
    local_tmp_directory = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_file_name = f'sdc-{get_random_string(string.ascii_letters, 5)}'
    raw_data = {'source_text': 'Hello World!',
                'replaced_text': 'Hi There!'}

    # Create source file in FTP server
    ftp.client.cwd('/')
    ftp.put_string(sftp_ftp_file_name, raw_data['source_text'])

    # Create replacing file in a subdirectory
    sdc_executor.execute_shell(f'mkdir {local_tmp_directory}/; ' +
                               f'echo {raw_data["replaced_text"]} >> {local_tmp_directory}/{local_file_name}')

    # Build source file pipeline logic
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format=stage_attributes['data_format'],
                             file_name_pattern='sdc*',
                             files_directory=local_tmp_directory)

    # Build SFTP/FTP/FTPS destination logic
    sftp_ftp_client = builder.add_stage(name='com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget')
    sftp_ftp_client.set_attributes(protocol='FTPS',
                                   file_name_expression=sftp_ftp_file_name,
                                   data_format=stage_attributes['data_format'],
                                   file_exists=stage_attributes['file_exists'])

    directory >> sftp_ftp_client

    sftp_ftp_client_pipeline = builder.build().configure_for_environment(ftp)

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    try:
        # Start SFTP/FTP/FTPS upload (destination) file pipeline and assert pipeline has processed expected number of files
        sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_batch_count(1, timeout_sec=60)
        sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)
        history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)

        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1

        if stage_attributes['file_exists'] == 'OVERWRITE':
            assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1
            assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
            # Read FTP destination file and compare our source data to assert
            assert ftp.get_string(os.path.join(ftp.path, sftp_ftp_file_name)).strip() == raw_data['replaced_text']

        elif stage_attributes['file_exists'] == 'TO_ERROR':
            assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 0
            assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 1
            # Read FTP destination file and compare our source data to assert
            assert ftp.get_string(os.path.join(ftp.path, sftp_ftp_file_name)).strip() == raw_data['source_text']

    finally:
        # Delete the test FTP destination file we created
        client = ftp.client
        client.delete(sftp_ftp_file_name)
        client.quit()
        sdc_executor.execute_shell(f'rm -R {local_tmp_directory}')


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_file_name_expression(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'header_line': 'IGNORE_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'NO_HEADER'},
                                              {'data_format': 'DELIMITED', 'header_line': 'WITH_HEADER'}])
def test_header_line(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': True}])
def test_known_hosts_file(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'path_relative_to_user_home_directory': False},
                                              {'path_relative_to_user_home_directory': True}])
def test_path_relative_to_user_home_directory(sdc_builder, sdc_executor, stage_attributes):
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
def test_resource_url(sdc_builder, sdc_executor):
    pass


@sdc_min_version('3.22.0')
@sftp
def test_sftp_protocol(sdc_builder, sdc_executor, sftp):
    """Test SFTP destination. We first create a local file using shell and use that file for SFTP/FTP/FTPS destination
    stage to see if it gets successfully uploaded.
    The pipelines look like:
        directory >> sftp_ftp_client
    """

    # Our destination SFTP/FTP/FTPS file name
    sftp_ftp_file_name = get_random_string(string.ascii_letters, 10)
    # Local temporary directory where we will create a source file to be uploaded to SFTP/FTP/FTPS server
    local_tmp_directory = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_file_name = f'sdc-{get_random_string(string.ascii_letters, 5)}'
    raw_data = 'Hello World!'

    sdc_executor.execute_shell(f'mkdir {local_tmp_directory}/')
    sdc_executor.execute_shell(f'echo {raw_data} >> {local_tmp_directory}/{local_file_name}')

    # Build source file pipeline logic
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'WHOLE_FILE'
    directory.file_name_pattern = 'sdc*'
    directory.files_directory = local_tmp_directory

    sftp_ftp_client = builder.add_stage(name='com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget')
    sftp_ftp_client.file_name_expression = sftp_ftp_file_name

    directory >> sftp_ftp_client

    sftp_ftp_client.protocol = 'SFTP'

    sftp_ftp_client_pipeline = builder.build('SFTP Destination Pipeline - Protocol').configure_for_environment(sftp)

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    # Start SFTP/FTP/FTPS upload (destination) file pipeline and assert pipeline has processed expected number of files
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)
    history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)

    try:
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0

        # Read SFTP destination file and compare our source data to assert
        assert sftp.get_string(os.path.join(sftp.path, sftp_ftp_file_name)).strip() == raw_data

        # Delete the test SFTP origin file we created
        transport, client = sftp.client
        client.remove(os.path.join(sftp.path, sftp_ftp_file_name))

    finally:
        client.close()
        transport.close()
        sdc_executor.execute_shell(f'rm -R {local_tmp_directory}')


@sdc_min_version('3.22.0')
@ftp
@pytest.mark.parametrize('stage_attributes', [{'protocol': 'FTP'},
                                              {'protocol': 'FTPS'}
                                              ])
def test_ftp_protocol(sdc_builder, sdc_executor, ftp, stage_attributes):
    """Test FTP/FTPS destination. We first create a local file using shell and use that file for SFTP/FTP/FTPS
    destination stage to see if it gets successfully uploaded.
    The pipelines look like:
        directory >> sftp_ftp_client
    """
    if ('FTPS' in ftp.ftp_type) and stage_attributes['protocol'] == 'FTP':
        pytest.skip('FTP protocol only runs with ftp-type FTP')
    elif ftp.ftp_type == 'FTP' and stage_attributes['protocol'] == 'FTPS':
        pytest.skip('FTPS protocol only runs with ftp-type FTPS')

    # Our destination SFTP/FTP/FTPS file name
    sftp_ftp_file_name = get_random_string(string.ascii_letters, 10)
    # Local temporary directory where we will create a source file to be uploaded to SFTP/FTP/FTPS server
    local_tmp_directory = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_file_name = f'sdc-{get_random_string(string.ascii_letters, 5)}'
    raw_data = 'Hello World!'

    sdc_executor.execute_shell(f'mkdir {local_tmp_directory}/')
    sdc_executor.execute_shell(f'echo {raw_data} >> {local_tmp_directory}/{local_file_name}')

    # Build source file pipeline logic
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'WHOLE_FILE'
    directory.file_name_pattern = 'sdc*'
    directory.files_directory = local_tmp_directory

    sftp_ftp_client = builder.add_stage(name='com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget')
    sftp_ftp_client.file_name_expression = sftp_ftp_file_name

    directory >> sftp_ftp_client

    sftp_ftp_client.protocol = stage_attributes['protocol']

    if stage_attributes['protocol'] == 'FTP':
        sftp_ftp_client_pipeline = builder.build('FTP Destination Pipeline - Protocol').configure_for_environment(ftp)
    else:
        sftp_ftp_client_pipeline = builder.build('FTPS Destination Pipeline - Protocol').configure_for_environment(ftp)

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    # Start SFTP/FTP/FTPS upload (destination) file pipeline and assert pipeline has processed expected number of files
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)
    history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)

    try:
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0

        # Read FTP destination file and compare our source data to assert
        assert ftp.get_string(os.path.join(ftp.path, sftp_ftp_file_name)).strip() == raw_data

    finally:
        # Delete the test FTP destination file we created
        client = ftp.client
        client.delete(sftp_ftp_file_name)
        client.quit()
        sdc_executor.execute_shell(f'rm -R {local_tmp_directory}')


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
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': False}, {'strict_host_checking': True}])
def test_strict_host_checking(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'TEXT'}])
def test_text_field_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': False},
                                              {'use_client_certificate_for_ftps': True}])
def test_use_client_certificate_for_ftps(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}, {'authentication': 'PRIVATE_KEY'}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
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

