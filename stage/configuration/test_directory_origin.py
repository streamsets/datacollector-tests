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
import string
import tempfile
from collections import OrderedDict

import pytest
from streamsets.sdk.sdc_api import StartError
from streamsets.testframework.utils import get_random_string
from xml.etree import ElementTree

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
    FILE_CONTENTS_1 = get_text_file_content('1')
    FILE_CONTENTS_2 = get_text_file_content('2')
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


@pytest.mark.parametrize('data_format', ['TEXT', 'DELIMITED', 'JSON', 'SDC_JSON', 'XML', 'LOG'])
@pytest.mark.parametrize('compression_format', ['COMPRESSED_FILE'])
@pytest.mark.parametrize('compression_codec', ['GZIP', 'BZIP2'])
def test_directory_origin_configuration_compression_format(sdc_builder, sdc_executor, data_format, compression_format,
                                                           compressed_file_writer, compression_codec, shell_executor):
    """Verify direcotry origin can read data from compressed files.
        Pattern is inside the compressed file.
        e.g. compression_format_test.txt is compressed as compression_format_test.txt.gz then
        Using TEXT data format to check if data can be read from compressed txt files.
        Similarly generating compressed file for all data formats and testing it.
        Note : 1) PROTOBUF -> Bug filed SDC-11530. So protobuf related issues are resolved
        2) BINARY -> Not supported by Directory origin. Confirmed by developers.
        3) 'ARCHIVE', 'COMPRESSED_ARCHIVE' these two 'compression_format' are completed in
        'file_name_pattern_within_compressed_directory' test case.
        4) 'None' no 'compression_format' is completed in the 'data_format' test case.
    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    actual_file_content = get_data_format_content(data_format)
    try:
        file_content = actual_file_content
        if data_format == 'DELIMITED':
            file_content = '\n'.join([','.join(record) for record in file_content])
        elif data_format == 'SDC_JSON':
            file_content = ''.join(json.dumps(record) for record in file_content)

        # Writes compressed file to local FS.
        compressed_file_writer(tmp_directory, data_format, compression_format, file_content,
                               compression_codec=compression_codec, files_prefix='sdc-${sdc:id()}')
        # Reading from compressed file.
        attributes = {'data_format': data_format,
                      'file_name_pattern': 'sdc*',
                      'file_name_pattern_mode': 'GLOB',
                      'files_directory': tmp_directory,
                      'compression_format': compression_format,
                      'read_order': 'TIMESTAMP',
                      'header_line': 'WITH_HEADER',
                      'log_format': 'LOG4J'}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        execute_pipeline_and_verify_output(sdc_executor, directory, pipeline, data_format,
                                           actual_file_content, actual_file_content)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {tmp_directory}')


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
@pytest.mark.parametrize('delimiter_character', [' ', '^'])
@pytest.mark.parametrize('root_field_type', ['LIST_MAP'])
@pytest.mark.parametrize('header_line', ['WITH_HEADER'])
def test_directory_origin_configuration_delimiter_character(sdc_builder, sdc_executor,
                                                            delimiter_format_type, data_format,
                                                            delimiter_character, shell_executor, delimited_file_writer,
                                                            root_field_type, header_line):
    """Test for Directory origin can read delimited file with custom delimiter character format type.
    Here we will be creating delimited files with different delimiter character for testing. e.g. [' ', '^']
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'custom_delimited_file.csv'
    FILE_CONTENTS = [['header1', 'header2', 'header3'], ['Field11', 'Field12', 'fält13'],['стол', 'Field22', 'Field23']]
    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        delimited_file_writer(os.path.join(files_directory, FILE_NAME),
                              FILE_CONTENTS, delimiter_format_type, delimiter_character)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern='custom_delimited_*',
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
        output_records = snapshot[directory.instance_name].output
        assert 2 == len(output_records)
        assert (output_records[0].field == OrderedDict(
            [('header1', 'Field11'), ('header2', 'Field12'), ('header3', 'fält13')]))
        assert (output_records[1].field == OrderedDict(
            [('header1', 'стол'), ('header2', 'Field22'), ('header3', 'Field23')]))
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['XML'])
def test_directory_origin_configuration_delimiter_element(sdc_builder, sdc_executor, shell_executor, file_writer,
                                                          data_format):
    """Test for Directory origin can read delimited file with different delimiter format type.
    Here we will be creating XML delimited files for testing.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'xml_delimited_file.xml'
    FILE_CONTENTS = """<?xml version="1.0" encoding="UTF-8"?>
                      <root>
                          <msg>
                              <time>8/12/2016 6:01:00</time>
                              <request>GET /index.html 200</request>
                          </msg>
                          <msg>
                              <time>8/12/2016 6:03:43</time>
                              <request>GET /images/sponsored.gif 304</request>
                          </msg>
                      </root>"""

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENTS)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern='xml_delimited_file*',
                                 file_name_pattern_mode='GLOB',
                                 delimeter_element='msg')
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        output_records = snapshot[directory.instance_name].output
        item_list = output_records[0].field['msg']
        rows_from_snapshot = [{item['time'][0]['value'].value: item['request'][0]['value'].value}
                              for item in item_list]
        # Parse input xml data to verify results from snapshot.
        root = ElementTree.fromstring(FILE_CONTENTS)
        expected_data = [{msg.find('time').text: msg.find('request').text}
                         for msg in root.iter('msg')]
        assert rows_from_snapshot == expected_data
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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


@pytest.mark.parametrize('data_format', ['DATAGRAM', 'DELIMITED', 'JSON', 'LOG', 'TEXT', 'XML'])
@pytest.mark.parametrize('ignore_control_characters', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_ignore_control_characters(sdc_builder, sdc_executor,
                                                                  data_format, ignore_control_characters):
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


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('max_record_length_in_chars', [238, 240, 260])
def test_directory_origin_configuration_max_record_length_in_chars_xml(sdc_builder, sdc_executor, shell_executor,
                                                                       file_writer, data_format,
                                                                       max_record_length_in_chars):
    """Case 1:   Record length > max_record_length | Expected outcome --> Record to error
    Case 2:   Record length = max_record_length | Expected outcome --> Record processed
    Case 3:   Record length < max_record_length | Expected outcome --> Record processed
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'xml_max_record_length_in_chars_file.xml'
    FILE_CONTENTS = """<?xml version="1.0" encoding="UTF-8"?>
                          <root>
                              <msg>
                                  <time>8/12/2016 6:01:00</time>
                                  <request>GET /index.html 200</request>
                              </msg>
                          </root>"""

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENTS)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern='xml_max_record_length_in_chars_file*',
                                 file_name_pattern_mode='GLOB',
                                 delimeter_element='msg',
                                 max_record_length_in_chars=max_record_length_in_chars)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        output_records = snapshot[directory.instance_name].output

        if max_record_length_in_chars < 240:
            assert not snapshot[directory].output
        else:
            item_list = output_records[0].field['msg']
            rows_from_snapshot = [{item['time'][0]['value'].value: item['request'][0]['value'].value}
                                  for item in item_list]
            # Parse input xml data to verify results from snapshot.
            root = ElementTree.fromstring(FILE_CONTENTS)
            expected_data = [{msg.find('time').text: msg.find('request').text}
                             for msg in root.iter('msg')]
            assert rows_from_snapshot == expected_data
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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
def test_directory_origin_configuration_namespaces(sdc_builder, sdc_executor, shell_executor, file_writer, data_format):
    """Test for Directory origin can read XML files with namespaces.
    Here we will be creating XML file with namespaces and parsing the XML document using namespace prefix.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'xml_namespaces_file.xml'
    FILE_CONTENTS = """<?xml version="1.0" encoding="UTF-8"?>
                      <root>
                          <a:data xmlns:a="http://www.companyA.com">
                              <msg>
                                  <time>8/12/2016 6:01:00</time>
                                  <request>GET /index.html 200</request>
                              </msg>
                              </a:data>
                          <c:data xmlns:c="http://www.companyC.com">
                              <sale>
                                  <item>Shoes</item>
                                  <item>Magic wand</item>
                                  <item>Tires</item>
                              </sale>
                          </c:data>
                          <a:data xmlns:a="http://www.companyA.com">
                              <msg>
                                  <time>8/12/2016 6:03:43</time>
                                  <request>GET /images/sponsored.gif 304</request>
                              </msg>
                          </a:data>
                      </root>"""
    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENTS)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern='xml_namespaces_file*',
                                 file_name_pattern_mode='GLOB',
                                 delimiter_element='/root/a:data/msg',
                                 namespaces=[{'key': 'a', 'value': 'http://www.companyA.com'}])
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        item_list = [output.field for output in snapshot[directory.instance_name].output]
        rows_from_snapshot = [{item['time'][0]['value'].value: item['request'][0]['value'].value}
                              for item in item_list]
        # Parse input xml data to verify results from snapshot using xpath for search.
        root = ElementTree.fromstring(FILE_CONTENTS)
        expected_data = [{msg.find('time').text: msg.find('request').text}
                         for msg in root.findall('.//msg')]
        assert rows_from_snapshot == expected_data
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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
def test_directory_origin_configuration_output_field_attributes(sdc_builder, sdc_executor, shell_executor, file_writer,
                                                                data_format, output_field_attributes):
    """Test for Directory origin can read XML file with Output field attributes parameter as true or false.
    Here we will be creating XML file with namespaces .

    Output field attributes |Expected outcome
    --------------------------------------------------------------
    True                  |Includes XML attributes and namespace declarations in the record as field attributes.
    False                 |XML attributes and namespace declarations will not be included as field attributes.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'xml_output_field_attributes_file.xml'
    FILE_CONTENTS = """<?xml version="1.0" encoding="UTF-8"?>
                            <bookstore xmlns:prc="http://books.com/price">
                                <b:book xmlns:b="http://books.com/book">
                                    <title lang="en">Harry Potter</title>
                                    <prc:price>29.99</prc:price>
                                </b:book>
                                <b:book xmlns:b="http://books.com/book">
                                    <title lang="en_us">Learning XML</title>
                                    <prc:price>39.95</prc:price>
                                </b:book>
                            </bookstore>"""
    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENTS)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern='xml_output_field_attributes_file*',
                                 file_name_pattern_mode='GLOB',
                                 delimiter_element='/*[1]/*',
                                 output_field_attributes=output_field_attributes)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        item_list = [output.field for output in snapshot[directory.instance_name].output]
        rows_from_snapshot = [{item['title'][0]['value'].value: item['prc:price'][0]['value'].value}
                              for item in item_list]
        # Parse input xml data to verify results from snapshot using xpath for search.
        root = ElementTree.fromstring(FILE_CONTENTS)
        expected_data = [{msg.find('title').text: msg.find('{http://books.com/price}price').text}
                         for msg in root.findall('{http://books.com/book}book')]
        assert rows_from_snapshot == expected_data
        output_records = snapshot[directory.instance_name].output

        if output_field_attributes:
            # Test for Field Attributes. Currently using _data property since attributes are not accessible with the
            # field property.
            field_header = output_records[0]._data['value']['attributes']
            assert field_header['xmlns:b'] == 'http://books.com/book'
        else:
            assert 'attributes' not in output_records[0]._data['value']
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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
@pytest.mark.parametrize('regular_expression',
                         ['(\S+) (\S+) (\S+) (\S+) (\S+) (.*)', '(\S+)(\W+)(\S+)\[(\W+)\](\S+)(\W+)'])
def test_directory_origin_configuration_regular_expression(sdc_builder, sdc_executor, data_format,
                                                           log_format, shell_executor, file_writer, regular_expression):
    """Check if the regular expression configuration works. Here we consider logs from DC as our test data.
    There are two interations of this test case. One with valid regex which should parse logs.
    Another with invalid regex which should produce null or no result.
    """
    file_name = 'custom_log_data.log'
    file_content = '''2019-04-30 08:23:53 AM [INFO] [streamsets.sdk.sdc_api] Pipeline Filewriterpipeline5340a2b5-b792-45f7-ac44-cf3d6df1dc29 reached status EDITED (took 0.00 s).
    2019-04-30 08:23:57 AM [INFO] [streamsets.sdk.sdc] Starting pipeline Filewriterpipeline5340a2b5-b792-45f7-ac44-cf3d6df1dc29 ...'''

    field_path_to_regex_group_mapping = LOG_FIELD_MAPPING

    try:
        files_directory = create_file_and_directory(file_name, file_content, shell_executor, file_writer)

        attributes = {'data_format': data_format,
                      'log_format': log_format,
                      'files_directory': files_directory,
                      'file_name_pattern_mode': 'GLOB',
                      'file_name_pattern': '*.log',
                      'field_path_to_regex_group_mapping': field_path_to_regex_group_mapping,
                      'regular_expression': regular_expression}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        if regular_expression == '(\S+)(\W+)(\S+)\[(\W+)\](\S+)(\W+)':
            output_records = execute_pipeline(sdc_executor, directory, pipeline)
            assert not output_records
        else:
            execute_and_verify_log_regex_output(sdc_executor, directory, pipeline)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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
@pytest.mark.parametrize('trim_stack_trace_to_length', [2])
def test_directory_origin_configuration_trim_stack_trace_to_length(sdc_builder, sdc_executor,
                                                                   data_format, log_format, on_parse_error,
                                                                   trim_stack_trace_to_length, shell_executor,
                                                                   file_writer):
    """The stack trace will be trimmed to the specified number of lines.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    file_name = f'{get_random_string()}.txt'
    file_path = os.path.join(files_directory, file_name)
    input_content =['1 [main] ERROR test.pack.Log4J  - failed!',
                    'Exception in thread "main" java.lang.NullPointerException',
                    'at com.example.myproject.Book.getTitle(Book.java:16)',
                    'at com.example.myproject.Author.getBookTitles(Author.java:25)']
    file_contents = '\n'.join(input_content)
    try:
        shell_executor(f'mkdir {files_directory}')
        file_writer(file_path, file_contents)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(log_format=log_format,
                                 on_parse_error=on_parse_error,
                                 data_format=data_format,
                                 trim_stack_trace_to_length=trim_stack_trace_to_length,
                                 files_directory=files_directory,
                                 file_name_pattern=file_name)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        output_records = snapshot[directory.instance_name].output

        expected_output = '{}\n{}'.format('failed!',
                                          '\n'.join(input_content[1:trim_stack_trace_to_length+1]))
        assert output_records[0].field['message'] == expected_output

    finally:
        shell_executor(f'rm -r {files_directory}')
        sdc_executor.stop_pipeline(pipeline)


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


# Util functions

LOG_FIELD_MAPPING = [{'fieldPath': '/date', 'group': 1},
                     {'fieldPath': '/time', 'group': 2},
                     {'fieldPath': '/timehalf', 'group': 3},
                     {'fieldPath': '/info', 'group': 4},
                     {'fieldPath': '/file', 'group': 5},
                     {'fieldPath': '/message', 'group': 6}]
JSON_DATA = [{"name": "Manish Zope", "age": 35, "car": "lll company", "address": ""},
             {"name": "Sachin Tope", "age": 30, "car": "hhh company",
              "address": "FLAT NO 555 xyz society opposite to abc school near ddd chowk wakad Pune - 411057"},
             {"name": "Sagar HiFi", "age": 28, "car": "rrr company", "address": "ttt"}]
AVRO_RECORDS = [
    {
        "name": "sdc1",
        "age": 3,
        "emails": ["sdc1@streamsets.com", "sdc@company.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc2",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc3",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc4",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc5",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    }]
AVRO_SCHEMA = {
    "type": "record",
    "name": "Employee",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "emails", "type": {"type": "array", "items": "string"}},
        {"name": "boss", "type": ["Employee", "null"]}
    ]
}


def get_directory_to_trash_pipeline(sdc_builder, attributes):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory')
    directory.set_attributes(**attributes)
    trash = pipeline_builder.add_stage('Trash')
    directory >> trash
    pipeline = pipeline_builder.build()
    return directory, pipeline


def create_file_and_directory(file_name, file_content, shell_executor, file_writer, delimiter_format_type=None,
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


def get_text_file_content(file_number, lines_needed=3):
    return '\n'.join(['This is line{}{}'.format(str(file_number), i) for i in range(1, (lines_needed + 1))])


def verify_delimited_output(output_records, data, header=None):
    if not header:
        header = [str(i) for i in range(0, 3)]
    assert 2 == len(output_records)
    assert output_records[0].field == OrderedDict(zip(header, data[0]))
    assert output_records[1].field == OrderedDict(zip(header, data[1]))


def get_control_characters_attributes(data_format, files_directory, ignore_control_characters):
    return {'data_format': data_format,
            'file_name_pattern': 'ignore_ctrl_*',
            'file_name_pattern_mode': 'GLOB',
            'files_directory': files_directory,
            'ignore_control_characters': ignore_control_characters}


def snapshot_content(snapshot, directory):
    """This is common function can be used at in many TCs to get snapshot content."""
    processed_data = []
    for snapshot_batch in snapshot.snapshot_batches:
        for value in snapshot_batch[directory.instance_name].output_lanes.values():
            for record in value:
                processed_data.append(str(record.field['text']))
    return processed_data


def execute_and_verify_log_regex_output(sdc_executor, directory, pipeline):
    output_records = execute_pipeline(sdc_executor, directory, pipeline)
    assert (output_records[0].field == {'/time': '08:23:53', '/date': '2019-04-30', '/timehalf': 'AM',
                                        '/info': '[INFO]',
                                        '/message': 'Pipeline Filewriterpipeline5340a2b5-b792-45f7-ac44-cf3d6df1dc29 reached status EDITED (took 0.00 s).',
                                        '/file': '[streamsets.sdk.sdc_api]'})
    assert (output_records[1].field == {'/time': '08:23:57', '/date': '2019-04-30', '/timehalf': 'AM',
                                        '/info': '[INFO]',
                                        '/message': 'Starting pipeline Filewriterpipeline5340a2b5-b792-45f7-ac44-cf3d6df1dc29 ...',
                                        '/file': '[streamsets.sdk.sdc]'})


def write_multiple_files(sdc_builder, sdc_executor, tmp_directory, file_suffix):
    # 1st pipeline which writes one record per file with interval 0.1 seconds
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    batch_size = 100
    dev_data_generator.set_attributes(batch_size=batch_size,
                                      delay_between_batches=10)

    dev_data_generator.fields_to_generate = [{'field': 'text', 'precision': 10, 'scale': 2, 'type': 'STRING'}]

    max_records_in_file = 2
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=os.path.join(tmp_directory),
                            files_prefix='{suffix}'.format(suffix=file_suffix),
                            files_suffix='txt',
                            max_records_in_file=max_records_in_file)

    dev_data_generator >> local_fs

    number_of_batches = 10
    # run the 1st pipeline to create the directory and starting files
    files_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(files_pipeline)
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(number_of_batches)
    sdc_executor.stop_pipeline(files_pipeline)


def get_data_format_content(data_format):
    # Todo - Add data for Binary and protobuf format. sdc json need to generated with pipeline
    json_data = [{'col11': 'value11', 'col12': 'value12', 'col13': 'value13', 'col14': 'value14'}]
    data_format_content = {'TEXT': get_text_file_content(1, 1),
                           'DELIMITED': [['field1', 'field2', 'field3'], ['Field11', 'Field12', 'Field13']],
                           'JSON': json.dumps(json_data),
                           'LOG': '200 [main] DEBUG org.StreamSets.Log4j unknown - This is sample log message',
                           'XML': """<?xml version="1.0" encoding="UTF-8"?>
                                       <root>
                                         <msg>
                                             <request>GET /index.html 200</request>
                                             <metainfo>Index page:More info about content</metainfo>
                                         </msg>
                                       </root>""",
                           'SDC_JSON': json_data}
    return data_format_content[data_format]


def execute_pipeline_and_verify_output(sdc_executor, directory, pipeline, data_format, file_content,
                                       json_data=None):
    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    output_records = snapshot[directory.instance_name].output

    if data_format == 'TEXT':
        processed_data = snapshot_content(snapshot, directory)
        assert file_content == '\n'.join(processed_data)
    elif data_format == 'DELIMITED':
        assert output_records[0].field == OrderedDict(zip(file_content[0], file_content[1]))
    elif data_format == 'JSON':
        assert output_records[0].field == [{'col11': 'value11', 'col12': 'value12', 'col13': 'value13',
                                            'col14': 'value14'}]
    elif data_format == 'LOG':
        assert output_records[0].field == {'severity': 'DEBUG', 'relativetime': '200', 'thread': 'main',
                                           'category': 'org.StreamSets.Log4j', 'ndc': 'unknown',
                                           'message': 'This is sample log message'}
    elif data_format == 'XML':
        msg_field = output_records[0].field['msg']
        assert msg_field[0]['metainfo'][0]['value'] == 'Index page:More info about content'
        assert msg_field[0]['request'][0]['value'] == 'GET /index.html 200'
    elif data_format == 'SDC_JSON':
        assert output_records[0].field == json_data[0]


def execute_pipeline(sdc_executor, directory, pipeline):
    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    output_records = snapshot[directory].output
    return output_records
