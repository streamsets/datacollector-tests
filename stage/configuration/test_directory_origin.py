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
import time
from collections import OrderedDict

import pytest
from streamsets.sdk.utils import Version
from streamsets.sdk.sdc_api import StartError
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string
from stage.utils.utils_xml import get_xml_output_field
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
    if batch_size_in_recs == 3:
        number_of_batches = 3
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
                                                           compressed_file_writer, compression_codec, shell_executor,
                                                           keep_data):
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
        compressed_file_writer(tmp_directory, data_format, file_content, compression_codec=compression_codec,
                               files_prefix='sdc-${sdc:id()}')
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
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


@pytest.mark.parametrize('data_format', ['SDC_JSON'])
# 'AVRO', 'DELIMITED', 'EXCEL', 'JSON', 'LOG', 'PROTOBUF',  'TEXT', 'WHOLE_FILE', 'XML'
def test_directory_origin_configuration_data_format(sdc_builder, sdc_executor, data_format,
                                                    shell_executor, compressed_file_writer):
    """Test if Directory Origin can read data with different data format.
    We will be testing only SDC_JSON data formats now. Other data formats are covered in other TCs.
    Following is mapping of data format to respective TC.
    AVRO - test_directory_origin_configuration_avro_schema
    DELIMITED - test_directory_origin_configuration_delimiter_format_type
    EXCEL - test_directory_origin_configuration_excel_header_option
    JSON - test_directory_origin_configuration_json_content
    LOG - test_directory_origin_configuration_log_format
    PROTOBUF - test_directory_origin_configuration_protobuf_descriptor_file
    TEXT - test_directory_origin_configuration_process_subdirectories. Other TCs which validates different configs.
    WHOLE_FILE - test_directory_origin_configuration_buffer_size_in_bytes
    XML - test_directory_origin_configuration_delimiter_element
    """
    files_directory = os.path.join('/tmp', get_random_string())
    json_data = [{"field1": "abc", "field2": "def", "field3": "ghi"},
                 {"field1": "jkl", "field2": "mno", "field3": "pqr"}]
    file_content = ''.join(json.dumps(record) for record in json_data)

    try:
        compressed_file_writer(files_directory, data_format, file_content, 'NONE')

        attributes = {'data_format': data_format,
                      'file_name_pattern': '*.json',
                      'file_name_pattern_mode': 'GLOB',
                      'files_directory': files_directory,
                      'json_content': 'MULTIPLE_OBJECTS'}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        output_records = snapshot[directory].output
        assert 2 == len(output_records)
        assert output_records[0].field == json_data[0]
        assert output_records[1].field == json_data[1]
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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
    EXPECTED_OUTPUT = {'time': [{'value': '8/12/2016 6:01:00'}], 'request': [{'value': 'GET /index.html 200'}]}
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
                                 delimiter_element='msg')
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        output_data = snapshot[directory.instance_name].output[0].field
        output_records = get_xml_output_field(directory, output_data, 'msg')
        assert output_records == EXPECTED_OUTPUT

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
@pytest.mark.parametrize('comment_marker', ['#'])
def test_directory_origin_configuration_enable_comments(sdc_builder, sdc_executor,
                                                        shell_executor, file_writer, delimiter_format_type, data_format,
                                                        enable_comments, comment_marker):
    """Test for Directory origin can read delimited files with comments.
    Here we will be creating delimited files with comments and verify if DC can skip comments or read comments as texts
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'delimited_file.csv'
    csv_content = [['Field11', 'Field12', 'Field13'], ['{comment_marker} This is comment'],
                   ['Field21', 'Field22', 'Field23']]
    FILE_CONTENTS = '\n'.join([','.join(t) for t in csv_content]).format(comment_marker=comment_marker)
    try:
        files_directory = create_file_and_directory(FILE_NAME, FILE_CONTENTS, shell_executor,
                                                                      file_writer)
        attributes = {'data_format': data_format,
                      'files_directory': files_directory,
                      'file_name_pattern': '*.csv',
                      'file_name_pattern_mode': 'GLOB',
                      'delimiter_format_type': delimiter_format_type,
                      'enable_comments': enable_comments,
                      'comment_marker': comment_marker,
                      'delimiter_character': ','}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        output_records = snapshot[directory.instance_name].output

        if enable_comments:
            assert 2 == len(output_records)
            assert output_records[0].field == OrderedDict([('0', 'Field11'), ('1', 'Field12'), ('2', 'Field13')])
            assert output_records[1].field == OrderedDict([('0', 'Field21'), ('1', 'Field22'), ('2', 'Field23')])
        else:
            assert 3 == len(output_records)
            assert output_records[0].field == OrderedDict([('0', 'Field11'), ('1', 'Field12'), ('2', 'Field13')])
            assert output_records[1].field == OrderedDict([('0', '{} This is comment'.format(comment_marker))])
            assert output_records[2].field == OrderedDict([('0', 'Field21'), ('1', 'Field22'), ('2', 'Field23')])
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_error_directory(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_escape_character(sdc_builder, sdc_executor, delimiter_format_type, data_format):
    pass


@sdc_min_version('3.4.0')
@pytest.mark.parametrize('data_format', ['EXCEL'])
@pytest.mark.parametrize('excel_header_option', ['IGNORE_HEADER', 'NO_HEADER', 'WITH_HEADER'])
def test_directory_origin_configuration_excel_header_option(sdc_builder, sdc_executor,
                                                            data_format, excel_header_option, shell_executor,
                                                            file_writer):
    """Indicates whether files include a header row and whether to ignore the header row.
    A header row must be the first row of a file.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    file_name = f'{get_random_string()}.xls'
    file_path = os.path.join(files_directory, file_name)
    try:
        shell_executor(f'mkdir {files_directory}')
        file_writer(file_path, generate_excel_file().getvalue(), 'utf8', 'BINARY')

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(excel_header_option=excel_header_option,
                                 data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern='*.xls')
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        output_records = snapshot[directory.instance_name].output

        if excel_header_option == 'IGNORE_HEADER':
            assert output_records[0].field == OrderedDict([('0', 'Field11'), ('1', 'ఫీల్డ్12'), ('2', 'fält13')])
        elif excel_header_option == 'NO_HEADER':
            assert output_records[0].field == OrderedDict([('0', 'column1'), ('1', 'column2'), ('2', 'column3')])
        else:
            assert output_records[0].field == OrderedDict([('column1', 'Field11'), ('column2', 'ఫీల్డ్12'),
                                                           ('column3', 'fält13')])
    finally:
        shell_executor(f'rm -r {files_directory}')
        sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('data_format', ['DATAGRAM'])
@pytest.mark.parametrize('datagram_packet_format', ['COLLECTD'])
@pytest.mark.parametrize('exclude_interval', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_exclude_interval(sdc_builder, sdc_executor,
                                                         data_format, datagram_packet_format, exclude_interval):
    pass


@pytest.mark.parametrize('extra_column_prefix', ['', '_extra_'])
@pytest.mark.parametrize('stage_attributes', [{'allow_extra_columns': True,
                                               'data_format': 'DELIMITED',
                                               'header_line': 'WITH_HEADER'}])
def test_extra_column_prefix(sdc_builder, sdc_executor, stage_attributes, keep_data,
                             extra_column_prefix, shell_executor, file_writer):
    """Test the Extra Column Prefix configuration when extra columns are seen in records.

    In this test case, 3 comma-delimited lines are processed. The first, a header line, defines 3 fields.
    The next has 5 fields and the last has 3 fields. The extra column prefix should be seen in the additional
    lines in line two and nowhere else.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'file.csv'
    DATA = [['c1', 'c2', 'c3'], ['f11', 'f12', 'f13', 'f14'],
            ['f21', 'f22', 'f23']]
    EXPECTED_OUTPUT = [{'c1': 'f11', 'c2': 'f12', 'c3': 'f13', f'{extra_column_prefix}01': 'f14'},
                       {'c1': 'f21', 'c2': 'f22', 'c3': 'f23'}]

    file_content = '\n'.join(','.join(line) for line in DATA)
    try:
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), '\n'.join(','.join(line) for line in DATA))

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory').set_attributes(extra_column_prefix=extra_column_prefix,
                                                                           files_directory=files_directory,
                                                                           file_name_pattern=FILE_NAME,
                                                                           **stage_attributes)
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        directory >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[directory].output]

        assert records == EXPECTED_OUTPUT
    finally:
        if not keep_data:
            shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['LOG'])
@pytest.mark.parametrize('log_format', ['REGEX'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_field_path_to_regex_group_mapping(sdc_builder, sdc_executor,
                                                                          data_format, log_format):
    pass


@pytest.mark.parametrize('file_name_pattern', ['pattern_check_processing_1.txt', '*.txt', 'pattern_*', '*_check_*'])
def test_directory_origin_configuration_file_name_pattern(sdc_builder, sdc_executor, shell_executor,
                                                          file_writer, file_name_pattern):
    """Check Directory origin can read files with different patterns.
    Here we have two files pattern_check_processing_1.txt & pattern_check_processing_2.txt.
    Patterns '*.txt', 'pattern_*', '*_check_*' -> Should match both files and directory origin should
    read complete data from both files.
    Pattern 'pattern_check_processing_1.txt' -> Should match first file and directory origin should
    read complete data from first file.
    """
    files_name = ['pattern_check_processing_1.txt', 'pattern_check_processing_2.txt']
    files_content = [get_text_file_content(1, 1),
                     get_text_file_content(2, 1)]

    try:
        files_directory = create_file_and_directory(files_name[0], files_content[0], shell_executor, file_writer)
        file_writer(os.path.join(files_directory, files_name[1]), files_content[1])

        attributes = {'data_format': 'TEXT',
                      'file_name_pattern': file_name_pattern,
                      'file_name_pattern_mode': 'GLOB',
                      'files_directory': files_directory}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=2).snapshot
        sdc_executor.stop_pipeline(pipeline)

        raw_data = '\n'.join(files_content)
        processed_data = '\n'.join(snapshot_content(snapshot, directory))
        if file_name_pattern == 'pattern_check_processing_1.txt':
            assert files_content[0] == processed_data
        else:
            assert raw_data == processed_data
    finally:
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('file_name_pattern_mode', ['GLOB', 'REGEX'])
def test_directory_origin_configuration_file_name_pattern_mode(sdc_builder, sdc_executor, shell_executor,
                                                               file_writer, file_name_pattern_mode):
    """Check how DC process different file pattern mode. Here we will be creating 2 files:
    ``pattern_check_processing_1.txt`` and ``pattern_check_processing_2.txt``.
    with regex we match only 1st file and with glob both files.
    """
    files_name = ['pattern_check_processing_1.txt', 'pattern_check_processing_2.txt']
    files_content = [get_text_file_content(1, 1),
                     get_text_file_content(2, 1)]
    file_name_pattern = '*.txt' if file_name_pattern_mode == 'GLOB' else r'^p(.*)([0-9]{1})(\.txt)'
    number_of_batches = 2 if file_name_pattern_mode == 'GLOB' else 1

    try:
        files_directory = create_file_and_directory(files_name[0], files_content[0], shell_executor, file_writer)
        file_writer(os.path.join(files_directory, files_name[1]), files_content[1])

        attributes = {'data_format':'TEXT',
                      'files_directory':files_directory,
                      'file_name_pattern_mode':file_name_pattern_mode,
                      'file_name_pattern':file_name_pattern}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=number_of_batches).snapshot

        raw_data = '\n'.join(files_content)
        processed_data = '\n'.join(snapshot_content(snapshot, directory))
        if file_name_pattern_mode == 'GLOB':
            assert raw_data == processed_data
        else:
            assert files_content[0] == processed_data
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['TEXT', 'DELIMITED', 'JSON', 'LOG', 'SDC_JSON', 'XML'])
@pytest.mark.parametrize('compression_format', ['ARCHIVE', 'COMPRESSED_ARCHIVE'])
def test_directory_origin_configuration_file_name_pattern_within_compressed_directory(sdc_builder, sdc_executor,
                                                                                      data_format, compression_format,
                                                                                      shell_executor, file_writer,
                                                                                      delimited_file_writer,
                                                                                      compressed_file_writer):
    """Verify direcotry origin can read data from compressed files with GLOB pattern.
    Pattern is inside the compressed file.
    e.g. compression_format_test.txt is compressed as compression_format_test.txt.zip then
    file_name_pattern_within_compressed_directory = '.txt'
    Here we are using TEXT data format to check if data can be read from compressed txt files.
    Similarly we will generate compressed file for all data formats and test it.
    Note : 1) PROTOBUF -> Bug filed SDC-11530. So protobuf related issues are resolved
    2) BINARY -> Not supported by Directory origin. Confirmed by developers.
    """
    ext_map = {'BINARY': 'bin', 'TEXT': 'txt', 'DELIMITED': 'csv', 'JSON': 'json', 'LOG': 'log', 'PROTOBUF': 'proto',
               'SDC_JSON': 'json', 'XML': 'xml'}
    file_name = 'compression_format_test.%s' %(ext_map[data_format])
    compressed_file_name = file_name
    file_content = get_data_format_content(data_format)

    try:
        json_data = None
        if data_format == 'DELIMITED':
            files_directory = create_file_and_directory(file_name, file_content, shell_executor,
                                                        delimited_file_writer, 'CSV')
        elif data_format == 'SDC_JSON':
            files_directory = os.path.join('/tmp', get_random_string())
            file_name = file_name.replace('.json', '*.json')
            json_data = file_content  # Used in assertion
            file_content = ''.join(json.dumps(record) for record in file_content)
            compressed_file_writer(files_directory, data_format, file_content, 'NONE', 'compression_format_test')
        else:
            files_directory = create_file_and_directory(file_name, file_content, shell_executor, file_writer)

        if compression_format == 'ARCHIVE':
            shell_executor(f'cd {files_directory} '
                           f'&& tar -cvf {compressed_file_name}.tar {file_name} '
                           f'&& rm {file_name}')
            file_name_pattern = '*.tar'
        else:
            shell_executor(f'cd {files_directory} '
                           f'&& tar -czvf {compressed_file_name}.tar.gz {file_name} '
                           f'&& rm {file_name}')
            file_name_pattern = '*.gz'

        attributes = {'data_format': data_format,
                      'files_directory': files_directory,
                      'file_name_pattern_within_compressed_directory': 'compression_*',
                      'file_name_pattern': file_name_pattern,
                      'file_name_pattern_mode': 'GLOB',
                      'compression_format': compression_format,
                      'header_line': 'WITH_HEADER',
                      'log_format': 'LOG4J',
                      'json_content': 'MULTIPLE_OBJECTS'}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        execute_pipeline_and_verify_output(sdc_executor, directory, pipeline, data_format, file_content,
                                           json_data)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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


@pytest.mark.parametrize('grok_pattern_is_correct', [True, False])
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern(sdc_builder, sdc_executor, stage_attributes, grok_pattern_is_correct,
                      file_writer, shell_executor, keep_data):
    """Test for the grok pattern configuration that is used to parse the log line.

    For the positive case, we rely upon the field's default value of '%{COMMONAPACHELOG}' and then
    ensure that changing this has the intended effect of breaking things for the negative case.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'b.txt'
    DATA = '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache.gif HTTP/1.0" 200 232'
    EXPECTED_OUTPUT = {'request': '/apache.gif', 'auth': 'frank', 'ident': '-', 'response': '200', 'bytes':
                       '232', 'clientip': '127.0.0.1', 'verb': 'GET', 'httpversion': '1.0', 'rawrequest': None,
                       'timestamp': '10/Oct/2000:13:55:36 -0700'}
    try:
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), DATA)
        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory').set_attributes(files_directory=files_directory,
                                                                           file_name_pattern='*.txt',
                                                                           **stage_attributes)
        if not grok_pattern_is_correct:
            # We use '%{HOST}', a valid grok pattern, as this will ensure that we don't trigger exceptions
            # but simply break matching.
            directory.grok_pattern = '%{HOST}'
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        directory >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[directory].output]
        error_records = [record.field for record in snapshot[directory].error_records]
        if grok_pattern_is_correct:
            assert records == [EXPECTED_OUTPUT]
        else:
            # If grok pattern doesn't match the input, no records should go through and there should be stage errors
            # raised.
            assert not records and sdc_executor.get_stage_errors(pipeline, directory)
    finally:
        if not keep_data:
            shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('grok_pattern_definition', ['MY_CUSTOM_LOG_1', 'MY_CUSTOM_LOG_2'])
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'log_format': 'GROK'}])
def test_grok_pattern_definition(sdc_builder, sdc_executor, stage_attributes, shell_executor, file_writer,
                                 grok_pattern_definition, keep_data):
    """Test whether Directory origin can parse logs using a couple of different grok pattern definitions."""
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'b.txt'
    FILE_CONTENT = ('127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache.gif HTTP/1.0" 200 2326 '
                    '"http://www.example.com/strt.html"')

    if grok_pattern_definition == 'MY_CUSTOM_LOG_1':
        GROK_PATTERN_DEFINITION = 'MYCUSTOMLOG %{COMMONAPACHELOG} %{QS:referrer}'
        EXPECTED_OUTPUT = {'request': '/apache.gif', 'auth': 'frank', 'ident': '-', 'verb': 'GET',
                           'referrer': '"http://www.example.com/strt.html"', 'response': '200', 'bytes': '2326',
                           'clientip': '127.0.0.1', 'httpversion': '1.0', 'rawrequest': None,
                           'timestamp': '10/Oct/2000:13:55:36 -0700'}
    else:
        GROK_PATTERN_DEFINITION = r'MYCUSTOMLOG \[%{HTTPDATE:timestamp}\]'
        EXPECTED_OUTPUT = {'timestamp': '10/Oct/2000:13:55:36 -0700'}

    try:
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENT)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(files_directory=files_directory,
                                 file_name_pattern='*.txt',
                                 grok_pattern='%{MYCUSTOMLOG}',
                                 grok_pattern_definition=GROK_PATTERN_DEFINITION,
                                 **stage_attributes)
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        directory >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[directory].output]
        assert records == [EXPECTED_OUTPUT]
    finally:
        if not keep_data:
            shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('header_line', ['IGNORE_HEADER', 'NO_HEADER', 'WITH_HEADER'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_header_line(sdc_builder, sdc_executor, data_format, header_line):
    pass


@pytest.mark.parametrize('ignore_control_characters', [True, False])
def test_directory_origin_configuration_ignore_control_characters_text(sdc_builder, sdc_executor,
                                                                       ignore_control_characters, shell_executor,
                                                                       file_writer):
    """Check if directory origin honours ignore_control_characters parameter.
    When set to true it should ignore all control characters.
    When False it should maintain these characters.
    """
    file_name = 'ignore_ctrl_chars.txt'
    file_content = 'File \0 with \a control characters with normal \v string to \f check the ignore control characters parameter.'
    try:
        files_directory = create_file_and_directory(file_name, file_content, shell_executor, file_writer)

        attributes = get_control_characters_attributes('TEXT', files_directory, ignore_control_characters)
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

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
    """Check if directory origin honours ignore_control_characters parameter.
    When set to true it should ignore all control characters.
    When False it should maintain these characters.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    file_name = 'ignore_ctrl_chars.csv'
    file_content = [['field1', 'field2', 'field3'], ['Field\0 11', 'Field\v12', 'Fie\fld\a13']]

    try:
        files_directory = create_file_and_directory(file_name, file_content, shell_executor, delimited_file_writer,
                                                    'CSV')

        attributes = get_control_characters_attributes('DELIMITED', files_directory, ignore_control_characters)
        attributes['header_line'] = 'WITH_HEADER'
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

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
        #shell_executor(f'rm -r {files_directory}')
        sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('stage_attributes', [{'ignore_control_characters': True},
                                              {'ignore_control_characters': False}])
def test_directory_origin_configuration_ignore_control_characters_json(sdc_builder, sdc_executor,
                                                                       stage_attributes, keep_data,
                                                                       shell_executor, file_writer):
    """Test for the Ignore Control Characters configuration when parsing JSON."""
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'a.json'
    # We have to give a string of JSON data instead of using something like json.dumps on a dictionary because
    # the json library would always escape the very control characters we're trying to test (ECMA-404).
    DATA = '{"line": "This was some text with control char\vacters."}'
    EXPECTED_OUTPUT = {'line': 'This was some text with control characters.'}

    try:
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), DATA)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory').set_attributes(data_format='JSON',
                                                                           files_directory=files_directory,
                                                                           file_name_pattern='*.json',
                                                                           batch_size_in_recs=10,
                                                                           **stage_attributes)
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        directory >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[directory].output]
        if stage_attributes['ignore_control_characters']:
            assert records == [EXPECTED_OUTPUT]
        else:
            # If we don't ignore control characters, SPOOLDIR_01 is raised because the file cannot be parsed by the
            # Directory origin and no records are output.
            assert sdc_executor.get_stage_errors(pipeline, directory)[0].error_code == 'SPOOLDIR_01' and not records
    finally:
        if not keep_data:
            shell_executor(f'rm -r {files_directory}')
        # When Ignore Control Characters == False, a stage error means no batches are output, so the Pipeline Finisher
        # isn't triggered, meaning we need to stop the pipeline ourselves.
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@pytest.mark.parametrize('ignore_control_characters', [True, False])
def test_directory_origin_configuration_ignore_control_characters_log(sdc_builder, sdc_executor,
                                                                      ignore_control_characters, shell_executor,
                                                                      file_writer):
    """Check if directory origin honours ignore_control_characters parameter.
    When set to true it should ignore all control characters.
    When False it should maintain these characters.
    """
    file_name = 'ignore_ctrl_chars.log'
    file_content = '200 [main] DEBUG org.StreamSets.Log4j unknown - Th\fis is sam\aple l\0og message\v'
    try:
        files_directory = create_file_and_directory(file_name, file_content, shell_executor, file_writer)

        attributes = get_control_characters_attributes('LOG', files_directory, ignore_control_characters)
        attributes.update({'log_format': 'LOG4J'})
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

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
    """Directory origin not able to read XML data with control characters.
    Filed bug :- https://issues.streamsets.com/browse/SDC-11604.
    """
    pass


@pytest.mark.parametrize('ignore_control_characters', [True, False])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_ignore_control_characters_datagram(sdc_builder, sdc_executor,
                                                                       ignore_control_characters, shell_executor,
                                                                       file_writer):
    """Directory origin does not support Datagram, NetFlow abd Binary data formats."""
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
@pytest.mark.parametrize('include_field_xpaths', [True, False])
def test_directory_origin_configuration_include_field_xpaths(sdc_builder, sdc_executor, shell_executor, file_writer,
                                                             data_format, include_field_xpaths):
    """Test for Directory origin can read XML file with include field xpath parameter as true or false.
    Here we will be creating XML file with namespaces .

    Include Field Xpaths |Expected outcome
    --------------------------------------------------------------
    True                  |Includes the XPath to XML attribute in field attributes and in xmlns record header attribute.
    False                 | XPath will not be included.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'xml_include_xpath_file.xml'
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
        files_directory = create_file_and_directory(FILE_NAME, FILE_CONTENTS, shell_executor,file_writer)
        attributes = {'data_format': data_format,
                      'files_directory': files_directory,
                      'file_name_pattern': 'xml_include_xpath_file*',
                      'file_name_pattern_mode': 'GLOB',
                      'delimiter_element': '/*[1]/*',
                      'include_field_xpaths': include_field_xpaths}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot

        item_list = [get_xml_output_field(directory, output.field, 'b:book')
                     for output in snapshot[directory.instance_name].output]
        rows_from_snapshot = [{item['title'][0]['value'].value: item['prc:price'][0]['value'].value}
                              for item in item_list]
        # Parse input xml data to verify results from snapshot using xpath for search.
        root = ElementTree.fromstring(FILE_CONTENTS)
        expected_data = [{msg.find('title').text: msg.find('{http://books.com/price}price').text}
                         for msg in root.findall('{http://books.com/book}book')]
        assert rows_from_snapshot == expected_data
        output_records = snapshot[directory.instance_name].output

        output_data = output_records[0]._data['value']['value']
        field_info = get_xml_output_field(directory, output_data, 'b:book', 'value')

        if include_field_xpaths:
            # Test for Record Headers
            record_header = [record.header.values for record in output_records]
            assert record_header[0]['xmlns:b'] == 'http://books.com/book'
            assert record_header[0]['xmlns:prc'] == 'http://books.com/price'
            # Test for Field Headers .Currently using _data property since api for field header is not there.
            assert (field_info['title']['value'][0]['value']['attr|lang']['attributes'][
                        'xpath'] == '/bookstore/b:book/title/@lang')
            assert (field_info['title']['value'][0]['value']['value']['attributes'][
                        'xpath'] == '/bookstore/b:book/title')
            assert (field_info['prc:price']['value'][0]['value']['value']['attributes']['xpath'] ==
                    '/bookstore/b:book/prc:price')
        else:
            # Test for Record Headers
            record_header = [record.header.values for record in output_records]
            assert 'xmlns:b' not in record_header[0]
            assert 'xmlns:prc' not in record_header[0]
            # Test for Field Headers .Currently using _data property since api for field header is not there.
            assert 'attributes' not in field_info['title']['value'][0]['value']['attr|lang']
            assert 'attributes' not in field_info['title']['value'][0]['value']['value']
            assert 'attributes' not in field_info['prc:price']['value'][0]['value']['value']
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['JSON'])
@pytest.mark.parametrize('json_content', ['ARRAY_OBJECTS', 'MULTIPLE_OBJECTS'])
def test_directory_origin_configuration_json_content(sdc_builder, sdc_executor, shell_executor, data_format,
                                                     file_writer, json_content):
    """Verify Directory origin configurations for JSON contents type (i.e. ARRAY_OBJECT and MULTIPLE_OBJECTS)."""
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = f'{get_random_string()}.json'
    records = [{f'Key{i}': f'Value{i}'} for i in range(3)]
    raw_records = (json.dumps(records)
                   if json_content == 'ARRAY_OBJECTS'
                   else ''.join(json.dumps(record) for record in records))
    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), raw_records)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern="*.json",
                                 file_name_pattern_mode='GLOB',
                                 json_content=json_content)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        output_records = snapshot[directory.instance_name].output

        assert records == [record.field for record in output_records]
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['DELIMITED'])
def test_directory_origin_configuration_lines_to_skip(sdc_builder, sdc_executor, data_format, shell_executor,
                                                      file_writer):
    """Verify if DC skips the delimited file with given value."""
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = 'delimited_file.csv'
    FILE_CONTENTS = """Field11,Field12,Field13
Field21,Field22,Field23
Field31,Field32,Field33
Field41,Field42,Field43
Field51,Field52,Field53"""
    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENTS)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(data_format=data_format,
                                 files_directory=files_directory,
                                 file_name_pattern="*.csv",
                                 file_name_pattern_mode='GLOB',
                                 delimiter_character=",",
                                 lines_to_skip=3)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        output_records = snapshot[directory.instance_name].output

        assert 2 == len(output_records)
        assert output_records[0].field == OrderedDict(zip([str(i) for i in range(0, 3)],
                                                      FILE_CONTENTS.split('\n')[3].split(',')))
        assert output_records[1].field == OrderedDict(zip([str(i) for i in range(0, 3)],
                                                      FILE_CONTENTS.split('\n')[4].split(',')))
    finally:
        shell_executor(f'rm -r {files_directory}')
        sdc_executor.stop_pipeline(pipeline)


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


@pytest.mark.parametrize('data_format', ['LOG', 'TEXT'])
@pytest.mark.parametrize('max_line_length', [155, 82])
def test_directory_origin_configuration_max_line_length(sdc_builder, sdc_executor, data_format,
                                                        shell_executor, file_writer, max_line_length):
    """Check how Directory origin read line in text and log file with Max Line Length set.
    Case 1 Max Line Length > length of record -> Should read complete record
    Case 2 Max Line Length < length of record -> Should truncate the record to Max Line Length value.
    """
    extension = 'txt' if data_format == 'TEXT' else 'log'
    file_name = 'sample_input_file.{extension}'.format(extension=extension)
    file_content = '2019-04-30 08:23:53 AM [INFO] [streamsets.sdk.sdc_api] Pipeline Filewriterpipeline5340a2b5-b792-45f7-ac44-cf3d6df1dc29 reached status EDITED (took 0.00 s).'
    field_path_to_regex_group_mapping = LOG_FIELD_MAPPING
    try:
        files_directory = create_file_and_directory(file_name, file_content, shell_executor, file_writer)

        attributes = {'data_format': data_format,
                      'file_name_pattern': 'sample_input_*',
                      'file_name_pattern_mode': 'GLOB',
                      'files_directory': files_directory,
                      'max_line_length': max_line_length,
                      'field_path_to_regex_group_mapping': field_path_to_regex_group_mapping,
                      'regular_expression': r'(\S+) (\S+) (\S+) (\S+) (\S+) (.*)',
                      'log_format': 'REGEX'}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[directory].output[0]

        if data_format == 'TEXT':
            assert record.field['text'] == file_content[:max_line_length]
        else:
            assert record.field['/message'] == file_content[55:max_line_length]
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['JSON'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_object_length_in_chars(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['DELIMITED', 'XML'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_max_record_length_in_chars(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('data_format', ['XML'])
@pytest.mark.parametrize('max_record_length_in_chars', [206, 208, 220])
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
    EXPECTED_OUTPUT = {'time': [{'value': '8/12/2016 6:01:00'}], 'request': [{'value': 'GET /index.html 200'}]}
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
                                 delimiter_element='msg',
                                 max_record_length_in_chars=max_record_length_in_chars)
        trash = pipeline_builder.add_stage('Trash')
        directory >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        output_records = snapshot[directory.instance_name].output

        if max_record_length_in_chars < 208:
            assert not snapshot[directory].output
        else:
            msg_field = get_xml_output_field(directory, output_records[0].field, 'msg')
            assert msg_field == EXPECTED_OUTPUT

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

        item_list = [get_xml_output_field(directory, output.field, 'msg')
                     for output in snapshot[directory.instance_name].output]

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


@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': True}])
def test_null_constant(sdc_builder, sdc_executor, stage_attributes, keep_data, shell_executor, delimited_file_writer):
    """Verify that the Null Constant configuration works as expected when Parse Nulls is enabled for delimited files."""
    test_parse_nulls(sdc_builder, sdc_executor, stage_attributes, keep_data,
                     shell_executor, delimited_file_writer, '\\N')


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

        item_list = [get_xml_output_field(directory, output.field, 'b:book')
                     for output in snapshot[directory.instance_name].output]
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
            output_data = output_records[0]._data['value']
            field_header = get_xml_output_field(directory, output_data, 'value', 'b:book')
            assert field_header['attributes']['xmlns:b'] == 'http://books.com/book'
        else:
            assert 'attributes' not in output_records[0]._data['value']
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('null_constant', ['TEST_DATA'])
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'DELIMITED', 'parse_nulls': False},
                                              {'data_format': 'DELIMITED', 'parse_nulls': True}])
def test_parse_nulls(sdc_builder, sdc_executor, stage_attributes, keep_data,
                     shell_executor, delimited_file_writer, null_constant):
    """Verify that Data Collector replaces the specified string constant with null values for delimited files."""
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_NAME = f'{get_random_string()}.csv'
    FILE_CONTENTS = [['Field 1', null_constant, 'Field 3', 'Field 4']]
    EXPECTED_OUTPUT = OrderedDict([('0', 'Field 1'),
                                   ('1', None if stage_attributes['parse_nulls'] else null_constant),
                                   ('2', 'Field 3'),
                                   ('3', 'Field 4')])

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        shell_executor(f'mkdir {files_directory}')
        delimited_file_writer(os.path.join(files_directory, FILE_NAME), FILE_CONTENTS, 'CSV', ',')

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory').set_attributes(files_directory=files_directory,
                                                                           file_name_pattern='*.csv',
                                                                           **stage_attributes)
        if stage_attributes['parse_nulls']:
            directory.null_constant = null_constant
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        directory >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        records = [record.field for record in snapshot[directory].output]

        assert records == [EXPECTED_OUTPUT]
    finally:
        if not keep_data:
            shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('read_order', ['TIMESTAMP'])
@pytest.mark.parametrize('process_subdirectories', [False, True])
def test_directory_origin_configuration_process_subdirectories(sdc_builder, sdc_executor, read_order,
                                                               process_subdirectories, shell_executor, file_writer):
    """Check if the process_subdirectories configuration works properly. Here we will create  two files one
    in root level (direcotry which we process) and one in nested directory.
    """
    files_name = ['pattern_check_processing_1.txt', 'pattern_check_processing_2.txt']
    files_content = [get_text_file_content(1, 1), get_text_file_content(2, 1)]
    no_of_batches = 2 if process_subdirectories else 1

    try:
        files_directory = create_file_and_directory(files_name[0], files_content[0], shell_executor, file_writer)
        inner_directory = os.path.join(files_directory, get_random_string())
        logger.debug('Creating nested direcotry within files directory %s ...', files_directory)
        shell_executor(f'mkdir -p {inner_directory}')
        file_writer(os.path.join(inner_directory, files_name[1]), files_content[1])

        attributes = {'data_format': 'TEXT',
                      'files_directory': files_directory,
                      'process_subdirectories': process_subdirectories,
                      'read_order': read_order,
                      'file_name_pattern_mode': 'GLOB',
                      'file_name_pattern': '*.txt'}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        raw_data = "\n".join(files_content) if process_subdirectories else files_content[0]
        execute_pipeline_and_verify_output(sdc_executor, directory, pipeline, 'TEXT', raw_data, None, no_of_batches)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('data_format', ['PROTOBUF'])
@pytest.mark.skip('Not yet implemented')
def test_directory_origin_configuration_protobuf_descriptor_file(sdc_builder, sdc_executor, data_format):
    pass


@pytest.mark.parametrize('delimiter_format_type', ['CUSTOM'])
@pytest.mark.parametrize('data_format', ['DELIMITED'])
@pytest.mark.parametrize('quote_character', ['\t', ';' , ' '])
@pytest.mark.parametrize('delimiter_character', ['^'])
def test_directory_origin_configuration_quote_character(sdc_builder, sdc_executor, delimiter_format_type, data_format,
                                                        quote_character, shell_executor, delimited_file_writer,
                                                        delimiter_character):
    """Verify if directory origin can read delimited data with custom quote character.
    This TC check for different escape characters. Input data fields have delimiter characters.
    Directory origin should read this data and produce field without escape character.
    e.g. ;|Field is value of field with "|" as delimiter character and ";" as quote character
    then output field should be "|Field".
    """
    file_name = 'custom_delimited_file.csv'
    f = lambda ip_string: ip_string.format(quote_character=quote_character, delimiter_character=delimiter_character)
    f1 = lambda ip_string: ip_string.replace(quote_character, "")
    data = [[f('{quote_character}Field11{delimiter_character}{quote_character}'), 'Field12',
             f('{quote_character},Field13{quote_character}')],
            [f('{quote_character}Field{delimiter_character}21{quote_character}'), 'Field22', 'Field23']]

    try:
        files_directory = create_file_and_directory(file_name, data, shell_executor, delimited_file_writer,
                                                    delimiter_format_type, delimiter_character)

        attributes = {'data_format': data_format,
                      'files_directory': files_directory,
                      'file_name_pattern': 'custom_delimited_*',
                      'file_name_pattern_mode': 'GLOB',
                      'delimiter_format_type': delimiter_format_type,
                      'delimiter_character': delimiter_character,
                      'quote_character': quote_character}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batch_size=3).snapshot
        output_records = snapshot[directory.instance_name].output

        expected_output = [map(f1, data[0]), map(f1, data[1])]
        verify_delimited_output(output_records, expected_output)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('stage_attributes', [{'data_format': 'WHOLE_FILE'}])
def test_rate_per_second(sdc_builder, sdc_executor, stage_attributes, shell_executor, file_writer, keep_data):
    """Test if Directory origin honors "Rate Per Second" configuration.

    Pipeline will be run four times with configuration set to different values
    and the expected proportionality of the value and the pipeline throughput checked.
    """
    FILE_NAME = 'file.txt'
    DATA = 'a' * 1024  # 1 KB file. This needs to be small as larger values make the Jython processor we use to
                       # write the file go crazy.

    files_directory = os.path.join('/tmp', get_random_string())
    logger.debug('Creating files directory %s ...', files_directory)
    shell_executor(f'mkdir {files_directory}')

    try:
        file_writer(os.path.join(files_directory, FILE_NAME), DATA)
        pipeline_builder = sdc_builder.get_pipeline_builder()
        # Buffer size needs to be set to a small value to go along with the small file we're aiming to read.
        directory = pipeline_builder.add_stage('Directory').set_attributes(buffer_size_in_bytes=10,
                                                                           files_directory=files_directory,
                                                                           file_name_pattern=FILE_NAME,
                                                                           **stage_attributes)
        # Using Whole File data format at the origin requires a destination that supports it, as well. Local FS
        # is an easy one to use.
        local_fs = pipeline_builder.add_stage('Local FS').set_attributes(data_format='WHOLE_FILE',
                                                                         directory_template='/tmp',
                                                                         file_name_expression=FILE_NAME,
                                                                         file_type='WHOLE_FILE',
                                                                         files_prefix='')
        directory >> local_fs
        pipeline = pipeline_builder.build()
        pipeline_throughputs = []
        for rate_per_second in ['1000', '500', '100', '50']:
            directory.rate_per_second = rate_per_second
            benchmark_data = sdc_executor.benchmark_pipeline(pipeline, record_count=1, runs=1)
            throughput = benchmark_data['throughput_mean']
            logger.info('Pipeline with rate per second of %s had mean throughput of %s records/s',
                        rate_per_second,
                        throughput)
            pipeline_throughputs.append(throughput)
            shell_executor(f"rm {os.path.join('/tmp', FILE_NAME)}")

        # The rate_per_second we iterate over should result in monotonically decreasing throughput values.
        assert pipeline_throughputs[0] > pipeline_throughputs[1] > pipeline_throughputs[2] > pipeline_throughputs[3]
    finally:
        if not keep_data:
            shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('read_order', ['LEXICOGRAPHICAL', 'TIMESTAMP'])
def test_directory_origin_configuration_read_order(sdc_builder, sdc_executor, shell_executor,
                                                   file_writer, read_order):
    """Check how Directory origin read files in order given. We will create two files b_read_order_check.txt
    and a_read_order_check.txt
    LEXICOGRAPHICAL -> a_read_order_check.txt should be read 1st followed b_read_order_check.txt
    TIMESTAMP -> b_read_order_check.txt should be read 1st followed a_read_order_check.txt
    """
    file_name_1= 'b_read_order_check.txt'
    file_content_1 = get_text_file_content(2, 1)
    file_name_2 = 'a_read_order_check.txt'
    file_content_2 = get_text_file_content(1, 1)

    try:
        files_directory = create_file_and_directory(file_name_1, file_content_1, shell_executor, file_writer)
        file_writer(os.path.join(files_directory, file_name_2), file_content_2)

        attributes = {'data_format': 'TEXT',
                      'files_directory': files_directory,
                      'file_name_pattern_mode': "GLOB",
                      'file_name_pattern': "*.txt",
                      'read_order': read_order}
        directory, pipeline = get_directory_to_trash_pipeline(sdc_builder, attributes)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=2).snapshot
        processed_data = snapshot_content(snapshot, directory)

        if read_order == "LEXICOGRAPHICAL":
            raw_data = '{}\n{}'.format(file_content_2, file_content_1)
        else:
            raw_data = '{}\n{}'.format(file_content_1, file_content_2)
        assert raw_data == '\n'.join(processed_data)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


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
                         [r'(\S+) (\S+) (\S+) (\S+) (\S+) (.*)', r'(\S+)(\W+)(\S+)\[(\W+)\](\S+)(\W+)'])
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

        if regular_expression == r'(\S+)(\W+)(\S+)\[(\W+)\](\S+)(\W+)':
            output_records = execute_pipeline(sdc_executor, directory, pipeline)
            assert not output_records
        else:
            execute_and_verify_log_regex_output(sdc_executor, directory, pipeline)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        shell_executor(f'rm -r {files_directory}')


@pytest.mark.parametrize('stage_attributes', [{'data_format': 'LOG', 'retain_original_line': False},
                                              {'data_format': 'LOG', 'retain_original_line': True}])

def test_retain_original_line(sdc_builder, sdc_executor, stage_attributes, file_writer):
    """Verifies that the configuration properly includes the original log line as a record field, when enabled."""
    FILE_NAME = 'custom_log_data.log'
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_CONTENT = "2019-04-30 08:23:59 AM [INFO] [streamsets.sdk.sdc] Waiting for status ['RUNNING', 'FINISHED'] ..."
    EXPECTED_OUTPUT = {'/time': '08:23:59', '/date': '2019-04-30', '/timehalf': 'AM', '/info': '[INFO]',
                       '/message': "Waiting for status ['RUNNING', 'FINISHED'] ...", '/file': '[streamsets.sdk.sdc]'}
    field_path_to_regex_group_mapping = LOG_FIELD_MAPPING

    try:
        logger.debug('Creating files directory %s ...', files_directory)
        sdc_executor.execute_shell(f'mkdir -p {files_directory}')
        file_path = os.path.join(files_directory, FILE_NAME)
        file_writer(file_path, FILE_CONTENT)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(field_path_to_regex_group_mapping=LOG_FIELD_MAPPING,
                                 file_name_pattern='*.log',
                                 files_directory=files_directory, log_format='REGEX',
                                 regular_expression=r'(\S+) (\S+) (\S+) (\S+) (\S+) (.*)',
                                 **stage_attributes)
        trash = pipeline_builder.add_stage('Trash')
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        directory >> [trash, pipeline_finisher]
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        output_records = [record.field for record in snapshot[directory].output]

        if stage_attributes['retain_original_line']:
            EXPECTED_OUTPUT['originalLine'] = FILE_CONTENT
        assert output_records == [EXPECTED_OUTPUT]
    finally:
        sdc_executor.execute_shell(f'rm -r {files_directory}')


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


def test_directory_no_read_permissions(sdc_builder, sdc_executor, shell_executor):
    FILES_DIRECTORY = '/tmp'

    random_str = get_random_string(string.ascii_letters, 10)
    file_path = os.path.join(FILES_DIRECTORY, random_str)

    shell_executor(f"""
        mkdir {file_path}
        chmod -R 000 {file_path}
    """)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory')
    directory.set_attributes(data_format='TEXT',
                             files_directory=file_path,
                             file_name_pattern='*')
    trash = pipeline_builder.add_stage('Trash')
    directory >> trash
    pipeline = pipeline_builder.build()

    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.validate_pipeline(pipeline)
        assert False, 'Should not reach here'
    except Exception as error:
        assert error.issues['issueCount'] == 1

        assert 'SPOOLDIR_38' in error.issues['stageIssues']['Directory_01'][0]['message']
        assert 'FILES' in error.issues['stageIssues']['Directory_01'][0]['configGroup']
    finally:
        shell_executor(f'rm -R {file_path}')


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
    # pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    directory >> [trash]
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
                                       json_data=None, no_of_batches=1):
    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=no_of_batches).snapshot
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
        xml_output_field = get_xml_output_field(directory, output_records[0].field, 'root')
        msg_field = xml_output_field['msg']

        assert msg_field[0]['metainfo'][0]['value'] == 'Index page:More info about content'
        assert msg_field[0]['request'][0]['value'] == 'GET /index.html 200'
    elif data_format == 'SDC_JSON':
        assert output_records[0].field == json_data[0]


def generate_excel_file():
    """Builds excel file in memory, later bind this data to BINARY file.
    """
    import io
    from xlwt import Workbook

    file_excel = io.BytesIO()  # create a file-like object
    # Create the Excel file
    workbook = Workbook(encoding='utf-8')
    sheet = workbook.add_sheet('sheet1')

    sheet.write(0, 0, 'column1')
    sheet.write(0, 1, 'column2')
    sheet.write(0, 2, 'column3')

    sheet.write(1, 0, 'Field11')
    sheet.write(1, 1, 'ఫీల్డ్12')
    sheet.write(1, 2, 'fält13')

    sheet.write(2, 0, 'поле21')
    sheet.write(2, 1, 'फील्ड22')
    sheet.write(2, 2, 'สนาม23')
    workbook.save(file_excel)
    return file_excel


def execute_pipeline(sdc_executor, directory, pipeline):
    sdc_executor.add_pipeline(pipeline)
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    output_records = snapshot[directory].output
    return output_records
