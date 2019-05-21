#
# -*- stage: com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource -*-
#
# -*- start test template -*-
#     pipeline_builder = sdc_builder.get_pipeline_builder()
#     directory = pipeline_builder.add_stage('Directory')
#     directory.set_attributes({stage_attributes})
#     trash = pipeline_builder.add_stage('Trash')
#     directory >> trash
#
#     pipeline = pipeline_builder.build()
#     sdc_executor.add_pipeline(pipeline)
# -*- end test template -*-
#
import json
import logging
import math
import os
from collections import OrderedDict

import pytest
from streamsets.sdk.sdc_api import StartError
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__file__)


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('header_line', ['WITH_HEADER'])
@pytest.mark.parametrize('extra_columns_present', [False, True])
@pytest.mark.parametrize('allow_extra_columns', [False, True])
def test_directory_origin_configuration_allow_extra_columns(sdc_builder, sdc_executor,
                                                            shell_executor, file_writer,
                                                            data_format, header_line,
                                                            extra_columns_present, allow_extra_columns):
    """Test for Allow Extra Columns configuration covering the following scenarios:

    Extra Columns Present | Allow Extra Columns | Expected outcome
    --------------------------------------------------------------
    True                  | True                | File processed
    True                  | False               | Records to error
    False                 | True                | File processed
    False                 | False               | File processed
    """
    FILES_DIRECTORY = '/tmp'
    HEADER = ['columnA', 'columnB', 'columnC']
    EXTRA_COLUMNS_PRESENT_DATA = [dict(columnA='1', columnB='2', columnC='3'),
                                  dict(columnA='4', columnB='5', columnC='6', _extra_01='7'),
                                  dict(columnA='8', columnB='9', columnC='10', _extra_01='11', _extra_02='12')]
    EXTRA_COLUMNS_NOT_PRESENT_DATA = [dict(columnA='1', columnB='2', columnC='3'),
                                      dict(columnA='4', columnB='5', columnC='6'),
                                      dict(columnA='7', columnB='8', columnC='9')]
    file_name = f'{get_random_string()}.txt'
    file_path = os.path.join(FILES_DIRECTORY, file_name)

    try:
        data = EXTRA_COLUMNS_PRESENT_DATA if extra_columns_present else EXTRA_COLUMNS_NOT_PRESENT_DATA
        file_contents = '\n'.join([','.join(HEADER)] + [','.join(record.values()) for record in data])
        file_writer(file_path, file_contents)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(allow_extra_columns=allow_extra_columns,
                                 data_format=data_format,
                                 header_line=header_line,
                                 files_directory=FILES_DIRECTORY,
                                 file_name_pattern=file_name)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[directory].output]
        error_records = [error_record.field['columns'] for error_record in snapshot[directory].error_records]
        sdc_executor.stop_pipeline(pipeline)
        if extra_columns_present and not allow_extra_columns:
            # The first record should show up in the snapshot, but the rest should go to error.
            assert records == data[0:1]
            assert error_records == [list(record.values()) for record in data[1:]]
        else:
            assert records == data
    finally:
        shell_executor(f'rm file_path')
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('create_directory_first', [True, False])
@pytest.mark.parametrize('allow_late_directory', [False, True])
def test_directory_origin_configuration_allow_late_directory(sdc_builder, sdc_executor,
                                                             shell_executor, file_writer,
                                                             create_directory_first, allow_late_directory):
    """Test for Allow Late Directory configuration covering the following scenarios:

    Create Directory Early | Allow Late Directory | Expected outcome
    ----------------------------------------------------------------
    True                   | True                 | File processed
    True                   | False                | File processed
    False                  | True                 | File processed
    False                  | False                | Error
    """

    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'keepingjoe.txt'
    FILE_CONTENTS = 'Sam is the Kid'

    def create_directory_and_file(files_directory, file_name, file_contents):
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, file_name), file_contents)

    try:
        if create_directory_first:
            create_directory_and_file(files_directory, FILE_NAME, FILE_CONTENTS)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(allow_late_directory=allow_late_directory,
                                 data_format='TEXT',
                                 files_directory=files_directory,
                                 file_name_pattern=FILE_NAME)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        if not allow_late_directory and not create_directory_first:
            with pytest.raises(StartError):
                sdc_executor.start_pipeline(pipeline)
        else:
            snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
            if not create_directory_first:
                assert not snapshot[directory].output
                create_directory_and_file(files_directory, FILE_NAME, FILE_CONTENTS)
                snapshot = sdc_executor.capture_snapshot(pipeline).snapshot

            record = snapshot[directory].output[0]
            assert record.field['text'] == FILE_CONTENTS
    finally:
        shell_executor(f'rm -r {files_directory}')
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('file_post_processing', ['ARCHIVE'])
def test_directory_origin_configuration_archive_directory(sdc_builder, sdc_executor,
                                                          shell_executor, file_writer,
                                                          file_post_processing):
    """Verify that the Archive Directory configuration is used when archiving files as part of post-processing."""
    files_directory = os.path.join('/tmp', get_random_string())
    archive_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'keepingjoe.txt'
    FILE_CONTENTS = "Machi's not the Kid"

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENTS)

        logger.debug('Creating archive directory %s ...', archive_directory)
        shell_executor(f'mkdir {archive_directory}')

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(archive_directory=archive_directory,
                                 data_format='TEXT',
                                 files_directory=files_directory,
                                 file_name_pattern=FILE_NAME,
                                 file_post_processing=file_post_processing)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[directory].output[0]
        assert record.field['text'] == FILE_CONTENTS
        sdc_executor.stop_pipeline(pipeline)
        # Confirm that the file has been archived by taking another snapshot and asserting to its emptiness.
        sdc_executor.reset_origin(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        assert not snapshot[directory].output

        logger.info('Verifying that file %s was moved as part of post-processing ...', FILE_NAME)
        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format='TEXT',
                                 files_directory=archive_directory,
                                 file_name_pattern=FILE_NAME)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[directory].output[0]
        assert record.field['text'] == FILE_CONTENTS

    finally:
        shell_executor(f'rm -r {files_directory} {archive_directory}')
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('file_post_processing', ['ARCHIVE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_archive_retention_time_in_mins(sdc_builder, sdc_executor, file_post_processing):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_auth_file(sdc_builder, sdc_executor, data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_avro_schema(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('batch_size_in_recs', [2, 3, 4])
def test_directory_origin_configuration_batch_size_in_recs(sdc_builder, sdc_executor, shell_executor,
                                                           file_writer, batch_size_in_recs):
    """Verify batch size in records (batch_size_in_recs) configuration for various values
    which limits maximum number of records to pass through pipeline at time.
    e.g. For 2 files with each containing 3 records. Verify with batch_size_in_recs = 2
    (less than records per file), 3 (equal to number of records per file),
    4 (greater than number of records per file).
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME_1 = 'streamsets_temp1.txt'
    FILE_NAME_2 = 'streamsets_temp2.txt'
    FILE_CONTENTS_1 = DirectoryOriginCommon.get_text_file_content('1')
    FILE_CONTENTS_2 = DirectoryOriginCommon.get_text_file_content('2')
    number_of_batches = math.ceil(3 / batch_size_in_recs) + math.ceil(3 / batch_size_in_recs)

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME_1), FILE_CONTENTS_1)
        file_writer(os.path.join(files_directory, FILE_NAME_2), FILE_CONTENTS_2)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format='TEXT',
                                 files_directory=files_directory,
                                 file_name_pattern='*.txt',
                                 batch_size_in_recs=batch_size_in_recs)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=number_of_batches).snapshot
        sdc_executor.stop_pipeline(pipeline)

        raw_data = '{}\n{}'.format(FILE_CONTENTS_1, FILE_CONTENTS_2)
        temp = []
        for snapshot_batch in snapshot.snapshot_batches:
            for value in snapshot_batch[directory.instance_name].output_lanes.values():
                assert batch_size_in_recs >= len(value)
                for record in value:
                    temp.append(str(record.field['text']))
        stage_output = '\n'.join(temp)
        assert raw_data == stage_output
    finally:
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_batch_wait_time_in_secs(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_buffer_limit_in_kb(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_buffer_size_in_bytes(sdc_builder, sdc_executor, data_format):
    pass


# TODO: Add test cases for other data formats.
@pytest.mark.parametrize('data_format', ['TEXT', # 'DATAGRAM', 'DELIMITED', 'JSON', 'LOG', 'XML'
                                        ])
@pytest.mark.parametrize('charset_correctly_set', [True, False])
def test_directory_origin_configuration_charset(sdc_builder, sdc_executor, file_writer,
                                                data_format, charset_correctly_set):
    """Instead of iterating over every possible charset (which would be a huge waste of time to write
    and run), let's just take some random set of characters from the Big5 character set and ensure that,
    if those characters are written to disk and the Directory charset is set correctly, that they're
    correctly parsed and they aren't if it isn't.
    """
    FILES_DIRECTORY = '/tmp'
    FILE_CONTENTS = '駿 鮮 鮫 鮪 鮭 鴻 鴿 麋 黏 點 黜 黝 黛 鼾 齋 叢'
    file_name = f'{get_random_string()}.txt'
    ENCODING = 'Big5'

    file_writer(os.path.join(FILES_DIRECTORY, file_name), FILE_CONTENTS, ENCODING)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory')
    directory.set_attributes(charset=ENCODING if charset_correctly_set else 'US-ASCII',
                             data_format=data_format,
                             files_directory=FILES_DIRECTORY,
                             file_name_pattern=file_name)
    trash = pipeline_builder.add_stage('Trash')
    directory >> trash
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    record = snapshot[directory].output[0]
    sdc_executor.stop_pipeline(pipeline)

    if charset_correctly_set:
        assert record.field['text'] == FILE_CONTENTS
    else:
        assert record.field['text'] != FILE_CONTENTS


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('enable_comments', [True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_comment_marker(sdc_builder, sdc_executor,
                                                       delimiter_format_type, data_format, enable_comments):
    pass


@pytest.mark.parametrize('data_format', ['BINARY', 'DELIMITED', 'JSON', 'LOG', 'PROTOBUF', 'SDC_JSON', 'TEXT', 'XML'])
@pytest.mark.parametrize('compression_format', ['ARCHIVE', 'COMPRESSED_ARCHIVE', 'COMPRESSED_FILE', 'NONE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_compression_format(sdc_builder, sdc_executor, data_format, compression_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@pytest.mark.parametrize('convert_hi_res_time_and_interval', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_convert_hi_res_time_and_interval(sdc_builder, sdc_executor,
                                                                         data_format, datagram_packet_format,
                                                                         convert_hi_res_time_and_interval):
    pass


@pytest.mark.parametrize('use_custom_delimiter', [True])
@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_custom_delimiter(sdc_builder, sdc_executor, use_custom_delimiter, data_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('use_custom_log_format', [True])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_custom_log4j_format(sdc_builder, sdc_executor,
                                                            data_format, use_custom_log_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['APACHE_CUSTOM_LOG_FORMAT'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_custom_log_format(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO', 'DELIMITED', 'EXCEL', 'JSON', 'LOG',
                                         'PROTOBUF', 'SDC_JSON', 'TEXT', 'WHOLE_FILE', 'XML'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_data_format(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD', 'NETFLOW', 'RAW_DATA', 'SYSLOG'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_datagram_packet_format(sdc_builder, sdc_executor,
                                                               data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@pytest.mark.parametrize('delimited_messages', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_delimited_messages(sdc_builder, sdc_executor, data_format, delimited_messages):
    pass


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_delimiter_character(sdc_builder, sdc_executor,
                                                            delimiter_format_type, data_format):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_delimiter_element(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('delimiter_format_type', ['CSV', 'CUSTOM', 'POSTGRES_CSV', 'TDF', 'RFC4180', 'EXCEL',
                                                   'POSTGRES_TEXT', 'MYSQL'])
@pytest.mark.parametrize('root_field_type', ['LIST_MAP'])
@pytest.mark.parametrize('header_line', ['WITH_HEADER'])
def test_directory_origin_configuration_delimiter_format_type(sdc_builder, sdc_executor, data_format,
                                                              delimiter_format_type, delimited_file_writer,
                                                              shell_executor, root_field_type, header_line):
    """Test for Directory origin can read delimited file with different delimiter format type.
    Here we will be creating delimited files in different formats for testing. e.g. POSTGRES_CSV, TDF, RFC4180, etc.,
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'delimited_file.csv'
    FILE_CONTENTS = [['field1', 'field2', 'field3'], ['Field11', 'Field12', 'fält13'], ['стол', 'Field22', 'Field23']]
    delimiter_character_map = {'CUSTOM': '^'}
    delimiter_character = '^' if delimiter_format_type == 'CUSTOM' else None

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        delimited_file_writer(os.path.join(files_directory, FILE_NAME),
                              FILE_CONTENTS, delimiter_format_type, delimiter_character)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern='delimited_*',
                                 file_name_pattern_mode='GLOB',
                                 delimiter_format_type=delimiter_format_type,
                                 delimiter_character=delimiter_character,
                                 root_field_type=root_field_type,
                                 header_line=header_line)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        sdc_executor.stop_pipeline(pipeline)
        output_records = snapshot[directory.instance_name].output
        new_line_field = 'Field12\nSTR' if delimiter_format_type == 'EXCEL' else 'Field12'

        assert 2 == len(output_records)
        assert output_records[0].field == OrderedDict(
            [('field1', 'Field11'), ('field2', new_line_field), ('field3', 'fält13')])
        assert output_records[1].field == OrderedDict(
            [('field1', 'стол'), ('field2', 'Field22'), ('field3', 'Field23')])
    finally:
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('enable_comments', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_enable_comments(sdc_builder, sdc_executor,
                                                        delimiter_format_type, data_format, enable_comments):
    pass


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_error_directory(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_escape_character(sdc_builder, sdc_executor, delimiter_format_type, data_format):
    pass


@pytest.mark.parametrize('data_format', ['EXCEL'])
@pytest.mark.parametrize('excel_header_option', ['IGNORE_HEADER', 'NO_HEADER', 'WITH_HEADER'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_excel_header_option(sdc_builder, sdc_executor,
                                                            data_format, excel_header_option):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@pytest.mark.parametrize('exclude_interval', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_exclude_interval(sdc_builder, sdc_executor,
                                                         data_format, datagram_packet_format, exclude_interval):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('allow_extra_columns', [True])
@pytest.mark.parametrize('header_line', ['WITH_HEADER'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_extra_column_prefix(sdc_builder, sdc_executor,
                                                            data_format, allow_extra_columns, header_line):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['REGEX'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_field_path_to_regex_group_mapping(sdc_builder, sdc_executor,
                                                                          data_format, log_format):
    pass


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_file_name_pattern(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('file_name_pattern_mode', ['GLOB', 'REGEX'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_file_name_pattern_mode(sdc_builder, sdc_executor, file_name_pattern_mode):
    pass


@pytest.mark.parametrize('data_format', ['BINARY', 'DELIMITED', 'JSON', 'LOG', 'PROTOBUF', 'SDC_JSON', 'TEXT', 'XML'])
@pytest.mark.parametrize('compression_format', ['ARCHIVE', 'COMPRESSED_ARCHIVE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_file_name_pattern_within_compressed_directory(sdc_builder, sdc_executor,
                                                                                      data_format, compression_format):
    pass


@pytest.mark.parametrize('file_post_processing', ['ARCHIVE', 'DELETE', 'NONE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_file_post_processing(sdc_builder, sdc_executor, file_post_processing):
    pass


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_files_directory(sdc_builder, sdc_executor):
    pass


def test_directory_origin_configuration_first_file_to_process(sdc_builder, sdc_executor,
                                                              file_writer, shell_executor):
    files_directory = os.path.join('/tmp', get_random_string())
    FIRST_FILE_NAME = 'b.txt'
    FIRST_FILE_CONTENTS = 'This is file b'
    EARLIER_FILE_NAME = 'a.txt'
    EARLIER_FILE_CONTENTS = 'This is file a'

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FIRST_FILE_NAME), FIRST_FILE_CONTENTS)
        file_writer(os.path.join(files_directory, EARLIER_FILE_NAME), EARLIER_FILE_CONTENTS)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format='TEXT',
                                 files_directory=files_directory,
                                 file_name_pattern='*.txt',
                                 first_file_to_process=FIRST_FILE_NAME)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=2).snapshot
        records = [record.field
                   for batch in snapshot.snapshot_batches
                   for record in batch.stage_outputs[directory.instance_name].output]
        assert len(records) == 1
        assert records[0] == {'text': FIRST_FILE_CONTENTS}
        sdc_executor.stop_pipeline(pipeline)
    finally:
        shell_executor(f'rm -r {files_directory}')
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['GROK'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_grok_pattern(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['GROK'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_grok_pattern_definition(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('header_line', ['IGNORE_HEADER', 'NO_HEADER', 'WITH_HEADER'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_header_line(sdc_builder, sdc_executor, data_format, header_line):
    pass


@pytest.mark.parametrize('ignore_control_characters', [True, False])
def test_directory_origin_configuration_ignore_control_characters_text(sdc_builder, sdc_executor, ignore_control_characters,
                                                                  shell_executor, file_writer):
    """Check if directory origin honours ignore_control_characters parameter.
    When set to true it should ignore all control characters.
    When False it should maintain these characters.
    """
    file_name = 'ignore_ctrl_chars.txt'
    file_content = 'File \0 with \a control characters with normal \v string to \f check the ignore control characters parameter.'
    try:
        files_directory = DirectoryOriginCommon.create_file_directory(file_name, file_content, shell_executor,
                                                                      file_writer)

        attributes = DirectoryOriginCommon.get_control_characters_attributes('TEXT', files_directory,
                                                                             ignore_control_characters)
        directory, pipeline = DirectoryOriginCommon.get_directory_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[directory].output[0]
        if ignore_control_characters:
            assert record.field['text'] == 'File  with  control characters with normal  string to  check the ignore control characters parameter.'
        else:
            assert record.field['text'] == 'File \x00 with \a control characters with normal \v string to \f check the ignore control characters parameter.'
    finally:
        shell_executor(f'rm -r {files_directory}')
        sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('ignore_control_characters', [True, False])
def test_directory_origin_configuration_ignore_control_characters_delimited(sdc_builder, sdc_executor,
                                                                       ignore_control_characters, shell_executor,
                                                                        delimited_file_writer):
    files_directory = os.path.join('/tmp', get_random_string())
    file_name = 'ignore_ctrl_chars.csv'
    file_content = [['field1', 'field2', 'field3'], ['Field\0 11', 'Field\v12', 'Fie\fld\a13']]

    try:
        files_directory = DirectoryOriginCommon.create_file_directory(file_name, file_content, shell_executor,
                                                                      delimited_file_writer, 'CSV')

        attributes = DirectoryOriginCommon.get_control_characters_attributes('DELIMITED', files_directory,
                                                                             ignore_control_characters)
        attributes['header_line'] = 'WITH_HEADER'
        directory, pipeline = DirectoryOriginCommon.get_directory_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        output_records = snapshot[directory.instance_name].output

        assert 1 == len(output_records)
        if ignore_control_characters:
            assert output_records[0].field == OrderedDict([('field1', 'Field 11'), ('field2', 'Field12'),
                                                           ('field3', 'Field13')])
        else:
            assert output_records[0].field == OrderedDict([('field1', 'Field\x00 11'), ('field2', 'Field\v12'),
                                                           ('field3', 'Fie\fld\a13')])
    finally:
        shell_executor(f'rm -r {files_directory}')
        sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('ignore_control_characters', [True, False])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_ignore_control_characters_json(sdc_builder, sdc_executor,
                                                                       ignore_control_characters, shell_executor,
                                                                       file_writer):
    pass


@pytest.mark.parametrize('ignore_control_characters', [True, False])
def test_directory_origin_configuration_ignore_control_characters_log(sdc_builder, sdc_executor,
                                                                      ignore_control_characters, shell_executor,
                                                                      file_writer):
    file_name = 'ignore_ctrl_chars.log'
    file_content = '200 [main] DEBUG org.StreamSets.Log4j unknown - Th\fis is sam\aple l\0og message\v'
    try:
        files_directory = DirectoryOriginCommon.create_file_directory(file_name, file_content, shell_executor,
                                                                      file_writer)

        attributes = DirectoryOriginCommon.get_control_characters_attributes('DELIMITED', files_directory,
                                                                             ignore_control_characters)
        attributes.update({'data_format': 'LOG', 'log_format': 'LOG4J'})
        directory, pipeline = DirectoryOriginCommon.get_directory_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[directory].output[0]
        if ignore_control_characters:
            assert record.field['message'] == 'This is sample log message'
        else:
            assert record.field['message'] == 'Th\fis is sam\aple l\x00og message\v'
    finally:
        shell_executor(f'rm -r {files_directory}')
        sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('ignore_control_characters', [True, False])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_ignore_control_characters_xml(sdc_builder, sdc_executor,
                                                                       ignore_control_characters, shell_executor,
                                                                       file_writer):
    pass


@pytest.mark.parametrize('ignore_control_characters', [True, False])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_ignore_control_characters_datagram(sdc_builder, sdc_executor,
                                                                       ignore_control_characters, shell_executor,
                                                                       file_writer):
    pass


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('ignore_empty_lines', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_ignore_empty_lines(sdc_builder, sdc_executor,
                                                           delimiter_format_type, data_format, ignore_empty_lines):
    pass


@pytest.mark.parametrize('use_custom_delimiter', [True])
@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.parametrize('include_custom_delimiter', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_include_custom_delimiter(sdc_builder, sdc_executor,
                                                                 use_custom_delimiter, data_format,
                                                                 include_custom_delimiter):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('include_field_xpaths', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_include_field_xpaths(sdc_builder, sdc_executor,
                                                             data_format, include_field_xpaths):
    pass


@pytest.mark.parametrize('data_format', ['JSON'])
@pytest.mark.parametrize('json_content', ['ARRAY_OBJECTS', 'MULTIPLE_OBJECTS'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_json_content(sdc_builder, sdc_executor, data_format, json_content):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_lines_to_skip(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['APACHE_CUSTOM_LOG_FORMAT', 'APACHE_ERROR_LOG_FORMAT', 'CEF',
                                        'COMBINED_LOG_FORMAT', 'COMMON_LOG_FORMAT', 'GROK', 'LEEF', 'LOG4J', 'REGEX'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_log_format(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['AUTO', 'ID', 'SUBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_lookup_schema_by(sdc_builder, sdc_executor, data_format, lookup_schema_by):
    pass


@pytest.mark.parametrize('data_format', ['BINARY'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_data_size_in_bytes(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_files_soft_limit(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_line_length(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_line_length(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['JSON'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_object_length_in_chars(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED', 'XML'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_record_length_in_chars(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['NETFLOW'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_templates_in_cache(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['NETFLOW'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_templates_in_cache(sdc_builder, sdc_executor,
                                                               data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_message_type(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_namespaces(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('parse_nulls', [True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_null_constant(sdc_builder, sdc_executor, data_format, parse_nulls):
    pass


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_number_of_threads(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.parametrize('on_parse_error', ['ERROR', 'IGNORE', 'INCLUDE_AS_STACK_TRACE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_on_parse_error(sdc_builder, sdc_executor,
                                                       data_format, log_format, on_parse_error):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('output_field_attributes', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_output_field_attributes(sdc_builder, sdc_executor,
                                                                data_format, output_field_attributes):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('parse_nulls', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_parse_nulls(sdc_builder, sdc_executor, data_format, parse_nulls):
    pass


@pytest.mark.parametrize('read_order', ['TIMESTAMP'])
@pytest.mark.parametrize('process_subdirectories', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_process_subdirectories(sdc_builder, sdc_executor,
                                                               read_order, process_subdirectories):
    pass


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_protobuf_descriptor_file(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_quote_character(sdc_builder, sdc_executor, delimiter_format_type, data_format):
    pass


@pytest.mark.parametrize('data_format', ['WHOLE_FILE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_rate_per_second(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('read_order', ['LEXICOGRAPHICAL', 'TIMESTAMP'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_read_order(sdc_builder, sdc_executor, read_order):
    pass


@pytest.mark.parametrize('data_format', ['NETFLOW'])
@pytest.mark.parametrize('record_generation_mode', ['INTERPRETED_ONLY', 'RAW_AND_INTERPRETED', 'RAW_ONLY'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_record_generation_mode(sdc_builder, sdc_executor,
                                                               data_format, record_generation_mode):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['NETFLOW'])
@pytest.mark.parametrize('record_generation_mode', ['INTERPRETED_ONLY', 'RAW_AND_INTERPRETED', 'RAW_ONLY'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_record_generation_mode(sdc_builder, sdc_executor,
                                                               data_format, datagram_packet_format,
                                                               record_generation_mode):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['REGEX'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_regular_expression(sdc_builder, sdc_executor, data_format, log_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('retain_original_line', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_retain_original_line(sdc_builder, sdc_executor,
                                                             data_format, retain_original_line):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('root_field_type', ['LIST', 'LIST_MAP'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_root_field_type(sdc_builder, sdc_executor, data_format, root_field_type):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['ID'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_schema_id(sdc_builder, sdc_executor, data_format, lookup_schema_by):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_schema_registry_urls(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['AVRO'])
@pytest.mark.parametrize('lookup_schema_by', ['SUBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_schema_subject(sdc_builder, sdc_executor, data_format, lookup_schema_by):
    pass


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_spooling_period_in_secs(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('data_format', ['NETFLOW'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_template_cache_timeout_in_ms(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['NETFLOW'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_template_cache_timeout_in_ms(sdc_builder, sdc_executor,
                                                                     data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.parametrize('on_parse_error', ['INCLUDE_AS_STACK_TRACE'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_trim_stack_trace_to_length(sdc_builder, sdc_executor,
                                                                   data_format, log_format, on_parse_error):
    pass


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_typesdb_file_path(sdc_builder, sdc_executor,
                                                          data_format, datagram_packet_format):
    pass


@pytest.mark.parametrize('data_format', ['TEXT'])
@pytest.mark.parametrize('use_custom_delimiter', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_use_custom_delimiter(sdc_builder, sdc_executor,
                                                             data_format, use_custom_delimiter):
    pass


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['LOG4J'])
@pytest.mark.parametrize('use_custom_log_format', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_use_custom_log_format(sdc_builder, sdc_executor,
                                                              data_format, log_format, use_custom_log_format):
    pass


# Class with common functionalities
class DirectoryOriginCommon(object):

    def __init__(self):
        pass

    @staticmethod
    def get_directory_trash_pipeline(sdc_builder, attributes):
        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(**attributes)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()
        return directory, pipeline

    @staticmethod
    def create_file_directory(file_name, file_content, shell_executor, file_writer, delimiter_format_type=None,
                              delimiter_character=None):
        files_directory = os.path.join('/tmp', get_random_string())
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_path = os.path.join(files_directory, file_name)
        if delimiter_format_type:
            file_writer(file_path, file_content, delimiter_format_type, delimiter_character)
        else:
            file_writer(file_path, file_content)
        return files_directory

    @staticmethod
    def get_log_field_mapping():
        return [{"fieldPath": "/date", "group": 1},
                {"fieldPath": "/time", "group": 2},
                {"fieldPath": "/timehalf", "group": 3},
                {"fieldPath": "/info", "group": 4},
                {"fieldPath": "/file", "group": 5},
                {"fieldPath": "/message", "group": 6}]

    @staticmethod
    def get_text_file_content(file_number, lines_needed=3):
        return '\n'.join(['This is line{}{}'.format(str(file_number), i) for i in range(1, (lines_needed + 1))])

    @staticmethod
    def verify_delimited_output(output_records, data, header=None):
        if not header:
            header = [str(i) for i in range(0, 3)]
        assert 2 == len(output_records)
        assert output_records[0].field == OrderedDict(zip(header, data[0]))
        assert output_records[1].field == OrderedDict(zip(header, data[1]))

    @staticmethod
    def execute_and_verify(sdc_executor, directory, pipeline, raw_data, snapshot_content, no_of_batches):
        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=no_of_batches).snapshot
        processed_data = snapshot_content(snapshot, directory)
        assert raw_data == processed_data

    @staticmethod
    def get_control_characters_attributes(data_format, files_directory, ignore_control_characters):
        return  {'data_format': data_format,
                 'file_name_pattern': 'ignore_ctrl_*',
                 'file_name_pattern_mode': 'GLOB',
                 'files_directory': files_directory,
                 'ignore_control_characters': ignore_control_characters}

