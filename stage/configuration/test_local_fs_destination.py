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
import string
import logging
import json
from datetime import datetime, timedelta
from math import ceil, inf

from streamsets.testframework.decorators import stub
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


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


@sdc_min_version('4.0.0')
def test_directory_template(sdc_builder, sdc_executor):
    """Test Directory Template. Two pipelines are started, each one generating a _tmp_sdc file. Their paths are similar
    but use different parameters. While they are running, we check that starting the second pipeline did not rename the
    _tmp_sdc file of the first pipeline.
    The pipelines look like:
        dev_data_generator >> local_fs
    """

    # Create first pipeline
    tmp_directory_1 = '/tmp/out/${directory1}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(directory_template=tmp_directory_1,
                            data_format='JSON')

    dev_data_generator >> local_fs

    pipeline1 = pipeline_builder.build().configure_for_environment()
    pipeline1.add_parameters(directory1='test1')
    sdc_executor.add_pipeline(pipeline1)

    # Create second pipeline
    tmp_directory_2 = '/tmp/out/${directory2}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(directory_template=tmp_directory_2,
                            data_format='JSON')

    dev_data_generator >> local_fs

    pipeline2 = pipeline_builder.build().configure_for_environment()
    pipeline2.add_parameters(directory2='test2')
    sdc_executor.add_pipeline(pipeline2)

    try:
        sdc_executor.start_pipeline(pipeline1).wait_for_pipeline_batch_count(5)
        sdc_executor.start_pipeline(pipeline2).wait_for_pipeline_batch_count(5)

        final_tmp_directory_1 = '/tmp/out/test1'
        final_tmp_directory_2 = '/tmp/out/test2'

        num_tmp_files_1 = int(sdc_executor.execute_shell(f'ls {final_tmp_directory_1} | grep _tmp_ | wc -l').stdout)
        num_tmp_files_2 = int(sdc_executor.execute_shell(f'ls {final_tmp_directory_2} | grep _tmp_ | wc -l').stdout)

        assert num_tmp_files_1 == 1
        assert num_tmp_files_2 == 1
    finally:
        sdc_executor.stop_pipeline(pipeline1)
        sdc_executor.stop_pipeline(pipeline2)
        logger.info('Deleting files created by Local FS in %s ...', tmp_directory_1)
        sdc_executor.execute_shell(f'rm -R {tmp_directory_1} ')
        logger.info('Deleting files created by Local FS in %s ...', tmp_directory_2)
        sdc_executor.execute_shell(f'rm -R {tmp_directory_2} ')


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


@pytest.mark.parametrize('idle_timeout, expected_num_files', [('${1 * SECONDS}', 5), ('${3 * SECONDS}', 1)])
def test_idle_timeout(sdc_builder, sdc_executor, idle_timeout, expected_num_files):
    """Test Idle Timeout. The pipeline test how many files are created when we write 5 batches if the Idle Timeout
    are lower and higher than the the delay between batches.
    Pipeline looks like:
        dev_data_generator >> local_fs
    """
    tmp_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=100,
                                      delay_between_batches=2000,
                                      fields_to_generate=[{'field': 'a', 'type': 'STRING'},
                                                          {'field': 'b', 'type': 'STRING'},
                                                          {'field': 'c', 'type': 'STRING'}])

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='DELIMITED',
                            directory_template=tmp_directory,
                            idle_timeout=idle_timeout)

    dev_data_generator >> local_fs

    pipeline = pipeline_builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(5)
        sdc_executor.stop_pipeline(pipeline)

        num_created_files = int(sdc_executor.execute_shell(f'ls {tmp_directory} | wc -l').stdout)

        assert num_created_files == expected_num_files
    finally:
        logger.info('Deleting files created by Local FS in %s ...', tmp_directory)
        sdc_executor.execute_shell(f'rm -R {tmp_directory} ')


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


@pytest.mark.parametrize('late_records_time, expected_late_records', [
    ('${1 * SECONDS}', True),
    ('${2 * HOURS}', False)
])
def test_late_record_time_limit_in_secs(sdc_builder, sdc_executor, late_records_time, expected_late_records):
    """Test Late Records Time Limit (secs). We had 2 records and only second record had a 30 seconds delay. The
    pipeline test how many files are created with different Late Records Time Limit.
    Pipeline looks like:
        dev_data_generator >> field_type_converter >> delay >> local_fs
    """
    tmp_directory_prefix = '/tmp/out'
    tmp_directory = '/${YYYY()}-${MM()}-${DD()}-${hh()}-${mm()}'
    tmp_late_directory = '/tmp/late/{}'.format(get_random_string(string.ascii_letters, 10))
    timestamp = datetime.now() - timedelta(hours=1)
    raw_data = [{'id': 1, 'timestamp': str(timestamp)},
                {'id': 2, 'timestamp': str(timestamp)}]

    field_type_converter_configs = [
        {
            'fields': ['/timestamp'],
            'targetType': 'DATE',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS'
        }]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in raw_data),
                                       stop_after_first_batch=False)

    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(conversion_method='BY_FIELD',
                                               field_type_converter_configs=field_type_converter_configs)

    delay = pipeline_builder.add_stage('Delay')
    delay.set_attributes(delay_between_batches=10000)   # 30 seconds

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='JSON',
                            directory_template=tmp_directory_prefix + tmp_directory,
                            time_basis='${record:value("/timestamp")}',
                            late_record_time_limit_in_secs=late_records_time,
                            late_record_handling='SEND_TO_LATE_RECORDS_FILE',
                            late_record_directory_template=tmp_late_directory)

    dev_raw_data_source >> field_type_converter_fields >> delay >> local_fs

    pipeline = pipeline_builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(5)
        sdc_executor.stop_pipeline(pipeline)

        num_created_files = int(sdc_executor.execute_shell(f'ls {tmp_late_directory} | wc -l').stdout)
        if expected_late_records:
            assert num_created_files > 0
        else:
            assert num_created_files == 0

    finally:
        logger.info('Deleting files created by Local FS in %s and %s ...', tmp_directory_prefix, tmp_late_directory)
        sdc_executor.execute_shell(f'rm -R {tmp_directory_prefix} ')
        sdc_executor.execute_shell(f'rm -R {tmp_late_directory} ')


@stub
@pytest.mark.parametrize('stage_attributes', [{'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'ID'},
                                              {'avro_schema_location': 'REGISTRY',
                                               'data_format': 'AVRO',
                                               'lookup_schema_by': 'SUBJECT'}])
def test_lookup_schema_by(sdc_builder, sdc_executor, stage_attributes):
    pass

@pytest.mark.parametrize('max_file_size', [0, 1])
def test_max_file_size_in_mb(sdc_builder, sdc_executor, max_file_size):
    """Test Max File Size in MB. The pipeline writes many records to files, none of which must exceed
    the maximum file size. The Local FS stage does sometimes exceed the maximum file size by some bytes
    due to its design, but that error margin must always be strictly less than 1MB. This is to say,
    the actual file size < max_file_size + 1.

    The pipeline looks like the following:
            dev_data_generator >> local_fs
    """
    tmp_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=10000,
                                      delay_between_batches=100,
                                      root_field_type='LIST_MAP',
                                      fields_to_generate=[{'field': 'a', 'type': 'STRING'},
                                                          {'field': 'b', 'type': 'STRING'},
                                                          {'field': 'c', 'type': 'STRING'}])

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='DELIMITED',
                            directory_template=tmp_directory,
                            max_file_size_in_mb=max_file_size)

    dev_data_generator >> local_fs

    pipeline = pipeline_builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)
        sdc_executor.stop_pipeline(pipeline)

        if max_file_size == 0:
            max_file_size_bytes = inf
        else:
            # The stage is not very precise with file sizes, so we'll give it some arbitrary
            # error margin, let's say 1MB.
            max_file_size_bytes = (max_file_size + 1) * 1024 * 1024

        # Get the size in bytes of each file. Each line has the file size in bytes and the file name.
        # Exclude the last line, which contains the total number of bytes.
        file_sizes = sdc_executor.execute_shell(f"wc -c {tmp_directory}/sdc-*").stdout.split('\n')[:-1]
        # Extract the first element (the file size) as an integer.
        file_sizes = [int(line.split()[0]) for line in file_sizes if line]
        file_sizes = file_sizes[:-1]

        # Assert no file is greater than max_file_size
        for file_size in file_sizes:
            assert file_size < max_file_size_bytes

    finally:
        logger.info('Deleting files created by Local FS in %s ...', tmp_directory)
        sdc_executor.execute_shell(f'rm -R {tmp_directory}')


@pytest.mark.parametrize('max_records_in_file', [10, 100])
def test_max_records_in_file(sdc_builder, sdc_executor, max_records_in_file):
    """Test Max Records in File. The pipeline test how many files are created when we write 100 records if
    the Max Records in File are lower and higher than the number of records.
     Pipeline looks like:
         dev_data_generator >> local_fs
     """
    tmp_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=100,
                                      delay_between_batches=1000,
                                      root_field_type='LIST_MAP',
                                      fields_to_generate=[{'field': 'a', 'type': 'STRING'},
                                                          {'field': 'b', 'type': 'STRING'},
                                                          {'field': 'c', 'type': 'STRING'}])

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='DELIMITED',
                            directory_template=tmp_directory,
                            max_records_in_file=max_records_in_file)

    dev_data_generator >> local_fs

    pipeline = pipeline_builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
    sdc_executor.stop_pipeline(pipeline)
    history = sdc_executor.get_pipeline_history(pipeline)
    output_record_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    try:
        num_created_files = int(sdc_executor.execute_shell(f'ls {tmp_directory} | wc -l').stdout)
        expected_num_files = ceil(output_record_count / max_records_in_file)
        assert num_created_files == expected_num_files

    finally:
        logger.info('Deleting files created by Local FS in %s ...', tmp_directory)
        sdc_executor.execute_shell(f'rm -R {tmp_directory} ')


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
