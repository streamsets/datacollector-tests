# Copyright 2020 StreamSets Inc.
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

import json
import logging
import os
import string
import tempfile

import avro
import avro.schema
import pytest
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from streamsets.testframework.markers import ftp, sdc_min_version
from streamsets.testframework.utils import get_random_string
from xlwt import Workbook

from stage.utils.utils_xml import get_xml_output_field

logger = logging.getLogger(__name__)

SCHEMA = {
    'namespace': 'example.avro',
    'type': 'record',
    'name': 'Employee',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'age', 'type': 'int'},
        {'name': 'emails', 'type': {'type': 'array', 'items': 'string'}},
        {'name': 'boss', 'type': ['Employee', 'null']}
    ]
}

# Protobuf file path relative to $SDC_RESOURCES.
PROTOBUF_FILE_PATH = 'resources/protobuf/addressbook.desc'
TMPOUT = '/tmp/out/'
TMP = '/tmp/'

# The name SFTP/FTP/FTPS Client can not be used to create the stage
FTP_ORIGIN_CLIENT_NAME = 'com_streamsets_pipeline_stage_origin_remote_RemoteDownloadDSource'
FTP_DEST_CLIENT_NAME = 'com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget'


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_text_delete_subdirectory(sdc_builder, sdc_executor, ftp):
    """FTP origin test. We first create a two files on FTP server
    in root directory and in /TMP directory.
    The FTP origin stage read it.
    We then assert the data using wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
    The pipeline delete the files after processing
    """

    ftp_file_name = f'a{get_random_string(string.ascii_letters, 10)}'
    ftp_dir_name = f'b{get_random_string(string.ascii_letters, 10)}'

    ftp_file_name_1 = f'{ftp_file_name}_1'
    ftp_file_name_2 = f'{ftp_file_name}_2'

    raw_text_data_1 = 'Hello World 1!'
    raw_text_data_2 = 'Hello World 2!'

    client = ftp.client

    client.cwd('/')
    ftp.put_string(ftp_file_name_1, raw_text_data_1)
    client.mkd(ftp_dir_name)
    ftp.put_string(f'{ftp_dir_name}/{ftp_file_name_2}', raw_text_data_2)

    builder = sdc_builder.get_pipeline_builder()

    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.set_attributes(file_name_pattern=f'{ftp_file_name}*', data_format='TEXT',
                                   process_subdirectories=True,
                                   file_post_processing="DELETE")

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline Text').configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(2)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)

    try:
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field['text'] == raw_text_data_1
        assert wiretap.output_records[1].field['text'] == raw_text_data_2

        # Assert the first file was deleted by the pipeline.
        client.cwd('/')
        file_list = client.nlst()
        assert ftp_file_name_1 not in file_list

        # Assert the second file was deleted in tmp folder by the pipeline.
        client.cwd(ftp_dir_name)
        file_list = client.nlst()
        assert ftp_file_name_2 not in file_list

    finally:
        # Delete the tmp folder.
        client.cwd('/')
        client.rmd(ftp_dir_name)
        client.quit()


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_xml(sdc_builder, sdc_executor, ftp):
    """Test FTP origin, message is in format XML. We first create a file on FTP server
    and have the FTP origin stage read it.
    We then assert the data from the wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
    """

    ftp_file_name = get_random_string(string.ascii_letters, 10)
    ftp_dir_name = get_random_string(string.ascii_letters, 10)
    raw_text_data = '<developers><developer>Alex</developer><developer>Xavi</developer></developers>'
    expected = [{'value': 'Alex'}, {'value': 'Xavi'}]

    client = ftp.client
    client.cwd('/')
    client.mkd(ftp_dir_name)
    ftp.put_string(f'{ftp_dir_name}/{ftp_file_name}', raw_text_data)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.set_attributes(file_name_pattern=ftp_file_name, process_subdirectories=True, data_format='XML')

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline XML').configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)
    try:
        assert len(wiretap.output_records) == 1
        output_data = wiretap.output_records[0].field
        assert f'/{ftp_dir_name}/{ftp_file_name}' == wiretap.output_records[0].header.values['file']
        assert ftp_file_name == wiretap.output_records[0].header.values['filename']

        developers_element = get_xml_output_field(sftp_ftp_client, output_data, 'developers')
        assert developers_element['developer'] == expected

    finally:
        # Delete the test FTP origin file we created
        client = ftp.client
        client.delete(f'/{ftp_dir_name}/{ftp_file_name}')
        client.rmd(f'/{ftp_dir_name}')
        client.quit()


@ftp
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('use_subdirectory', [False, True])
def test_ftp_origin_text(sdc_builder, sdc_executor, ftp, use_subdirectory):
    """Test FTP origin, message is in format Text. We first create a file on FTP server
    and have the FTP origin stage read it.

    We include the directory in the path URL instead of in the pattern

    We then assert the data from the wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
        sftp_ftp_client >= wiretap_events
    """
    directory = os.path.join(get_random_string(), get_random_string()) if use_subdirectory else get_random_string()
    ftp_file_name = get_random_string(string.ascii_letters, 10)
    ftp_file_path = os.path.join(directory, ftp_file_name)
    raw_text_data = '[{\'value\': \'Alex\'}, {\'value\': \'Xavi\'}]'
    expected = [{'value': 'Alex'}, {'value': 'Xavi'}]

    client = ftp.client
    client.cwd('/')
    for path in directory.split('/'):
        client.mkd(path)
        client.cwd(path)
    client.cwd('/')
    ftp.put_string(ftp_file_path, raw_text_data)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.set_attributes(file_name_pattern=ftp_file_name, process_subdirectories=True, data_format='TEXT')

    wiretap = builder.add_wiretap()
    wiretap_events = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client >= wiretap_events.destination

    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline XML').configure_for_environment(ftp)
    sftp_ftp_client.resource_url = f'{sftp_ftp_client.resource_url}/{directory}'

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)

    try:
        assert len(wiretap.output_records) == 1
        assert ftp_file_name == wiretap.output_records[0].header.values['filename']
        assert len(wiretap_events.output_records) == 3
        assert wiretap_events.output_records[0].field['filepath'] == f'/{ftp_file_path}'
        assert wiretap_events.output_records[1].field['filepath'] == f'/{ftp_file_path}'
        assert wiretap.output_records[0].field['text'] == str(expected)

    finally:
        # Delete the test FTP origin file we created
        client = ftp.client
        client.delete(f'/{directory}/{ftp_file_name}')
        client.rmd(f'/{directory}')
        client.quit()


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_avro(sdc_builder, sdc_executor, ftp):
    """Test FTP origin message is in format Avro. We first create a file on FTP server
    and have the FTP origin stage read it.
    We then assert the data using a wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
    """

    ftp_file_name = get_random_string(string.ascii_letters, 10)
    msg = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}
    # expected = "<Record (field=OrderedDict([('name', boss), ('age', 60), " \
    #           "('emails', [boss@company.com, boss2@company.com]), ('boss', None)]))>"
    expected = {'name': 'boss', 'age': 60, 'emails': ['boss@company.com', 'boss2@company.com'], 'boss': None}

    # Create a data file using DataFileWriter
    with open(f'{TMP}{ftp_file_name}', "wb") as data_file:
        writer = DataFileWriter(data_file, DatumWriter(), avro.schema.Parse(json.dumps(SCHEMA)))

        # Write data using DatumWriter
        writer.append(msg)
        writer.close()

    with open(f'{TMP}{ftp_file_name}', 'rb') as fp:
        ftp.client.storbinary('STOR %s' % os.path.basename(ftp_file_name), fp, 1024)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.set_attributes(file_name_pattern=ftp_file_name, data_format='AVRO', avro_schema_location='INLINE',
                                   avro_schema=json.dumps(SCHEMA))

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline AVRO').configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)
    try:
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field == expected

    finally:
        # Delete the test FTP origin file we created
        client = ftp.client
        client.delete(ftp_file_name)
        os.remove(f'{TMP}{ftp_file_name}')
        client.quit()


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_delimited_with_finisher(sdc_builder, sdc_executor, ftp):
    """Test FTP origin, message is in format Delimited. We first create a file on FTP server
    and have the FTP origin stage read it.
    We add a pipeline finisher and stop the pipeline when there is no more data.
    We put the file in ftp server.

    We then assert the data using wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
        Sftp_ftp_client>= Pipeline finisher
    """

    ftp_file_name = get_random_string(string.ascii_letters, 10)
    ftp_file_name_1 = f'{ftp_file_name}_1'
    ftp_file_name_2 = f'{ftp_file_name}_2'

    message_1 = 'Alex,Xavi'
    message_2 = 'Tucu,Martin'
    expected_1 = {str(i): name for i, name in enumerate(message_1.split(','))}
    expected_2 = {str(i): name for i, name in enumerate(message_2.split(','))}

    ftp.put_string(ftp_file_name_1, message_1)
    ftp.put_string(ftp_file_name_2, message_2)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.set_attributes(file_name_pattern=f'{ftp_file_name}*', data_format='DELIMITED')

    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(
        stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client >= pipeline_finished_executor

    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline CSV-Finisher').configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_finished()
    try:
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field == expected_1
        assert wiretap.output_records[1].field == expected_2

    finally:
        # Delete the test FTP origin files we created
        client = ftp.client
        client.delete(ftp_file_name_1)
        client.delete(ftp_file_name_2)
        client.quit()


@sdc_min_version('1.4.0.0')
@ftp
def test_ftp_origin_wholefile_with_finisher(sdc_builder, sdc_executor, ftp):
    """Test FTP origin message is in format Whole File. We first create two files on FTP server
    and have the FTP origin stage read them.
    We add a pipeline finisher to check when there is no more data.

    We then assert the data using a wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
        Sftp_ftp_client>= Pipeline finisher
    """

    ftp_file_name = get_random_string(string.ascii_letters, 10)

    ftp_file_name_1 = f'{ftp_file_name}_1'
    ftp_file_name_2 = f'{ftp_file_name}_2'

    message_1 = 'Useless Message 1'
    message_2 = 'Useless Message 2'

    client = ftp.client

    client.cwd('/')
    ftp.put_string(ftp_file_name_1, message_1)
    ftp.put_string(ftp_file_name_2, message_2)

    expected_1 = f'/{ftp_file_name_1}'
    expected_2 = f'/{ftp_file_name_2}'

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    origin.set_attributes(file_name_pattern=f'{ftp_file_name}*', data_format='WHOLE_FILE')

    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]

    origin >> wiretap.destination
    origin >= pipeline_finished_executor

    pipeline = builder.build().configure_for_environment(ftp)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    try:
        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].field['fileInfo']['file'] == expected_1
        assert wiretap.output_records[1].field['fileInfo']['file'] == expected_2
    finally:
        client.quit()


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_protobuf(sdc_builder, sdc_executor, ftp):
    """Test FTP origin message is in format Protobuf.
    The file is created used a first pipeline. dev_raw -> local_fs
    The file is moved used a second pipeline.  directory -> ftp
    The FTP origin stage read it.
    We then assert the records ingested using a wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap

    """

    ftp_file_name = get_random_string(string.ascii_letters, 10)

    message = '{"first_name": "Martin","last_name": "Balzamo"}'
    expected = json.loads(message)

    produce_lfs_messages_protobuf(ftp_file_name, sdc_builder, sdc_executor, message, ftp)
    move_directory_messages_protobuf_ftp(ftp_file_name, sdc_builder, sdc_executor, message, ftp)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.file_name_pattern = f'{ftp_file_name}*'
    sftp_ftp_client.set_attributes(data_format='PROTOBUF', message_type='Contact', file_name_pattern_mode='REGEX',
                                   protobuf_descriptor_file=PROTOBUF_FILE_PATH, delimited_messages=True,
                                   process_subdirectories=True)

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination

    pipeline = builder.build('FTP Origin Pipeline Protobuf').configure_for_environment(ftp)
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(pipeline)

    try:
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field == expected

    finally:
        # Delete the file.
        client = ftp.client
        client.cwd('/')
        client.delete(ftp_file_name)
        client.quit()


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_syslog(sdc_builder, sdc_executor, ftp):
    """Test FTP origin using syslog format.
    We first create a file on FTP server and have the FTP origin stage read it.
    We then assert the data using wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
    """
    message = ('+20150320 [15:53:31,161] DEBUG PipelineConfigurationValidator - Pipeline \'test:preview\' validation. '
               'valid=true, canPreview=true, issuesCount=0 - ')

    ftp_file_name = get_random_string(string.ascii_letters, 10)
    ftp.put_string(ftp_file_name, message)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.file_name_pattern = ftp_file_name
    sftp_ftp_client.set_attributes(data_format='LOG',
                                   log_format='LOG4J',
                                   retain_original_line=True,
                                   on_parse_error='INCLUDE_AS_STACK_TRACE')

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline SysLog').configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)

    try:
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['originalLine'] == message

    finally:
        # Delete the test FTP origin file we created
        client = ftp.client
        client.delete(ftp_file_name)
        client.quit()


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_excel(sdc_builder, sdc_executor, ftp):
    """Test FTP origin using excel format.
    We first create a file on FTP server and have the FTP origin stage read it.
    We then assert the data using a wiretap. The pipeline looks like:
        sftp_ftp_client >> wiretap
    """
    ftp_file_name = get_random_string(string.ascii_letters, 10)

    # Create the Excel file

    workbook = Workbook()
    sheet = workbook.add_sheet('0')

    colcount = 5
    rowcount = 10

    for col in range(colcount):
        for row in range(rowcount):
            sheet.write(row, col, 'TAB({row}, {col})'.format(row=row, col=col))

    workbook.save(ftp_file_name)

    with open(ftp_file_name, 'rb') as fp:
        ftp.client.storbinary('STOR %s' % os.path.basename(ftp_file_name), fp, 1024)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.set_attributes(file_name_pattern=ftp_file_name, data_format='EXCEL',
                                   excel_header_option="NO_HEADER")

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline Excel').configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    # Start the pipeline, wait for 10 records and stop it.
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(10)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)

    output_records = [record.field for record in wiretap.output_records]
    len_records = len(output_records)
    try:
        # Compare the results get from the output_records
        for row_res in range(len_records):
            for col_res in range(colcount):
                assert output_records[row_res][str(col_res)] == "TAB({row}, {col})".format(row=row_res, col=col_res)

    finally:
        # Delete the test FTP origin file we created
        client = ftp.client
        client.delete(ftp_file_name)
        client.quit()
        os.remove(ftp_file_name)


@ftp
@sdc_min_version('3.9.0')
def test_ftp_origin_SDC_Record(sdc_builder, sdc_executor, ftp):
    """Test FTP origin message is in format SDC_Record.
    The file is created used a first pipeline. dev_raw -> local_fs
    The file is moved used a second pipeline.  directory -> ftp
    The FTP origin stage read it.
    We then assert the data using a wiretap. The pipeline looks like:
        sftp_ftp_client >> trash

    """

    json_data = [{"field1": "abc", "field2": "def", "field3": "ghi"},
                 {"field1": "jkl", "field2": "mno", "field3": "pqr"}]

    raw_data = ''.join(json.dumps(record) for record in json_data)

    ftp_file_name = get_random_string(string.ascii_letters, 10)
    produce_lfs_messages_SDC_Record(ftp_file_name, sdc_builder, sdc_executor, raw_data, ftp)
    move_directory_messages_SDC_record_ftp(ftp_file_name, sdc_builder, sdc_executor, raw_data, ftp)

    builder = sdc_builder.get_pipeline_builder()
    sftp_ftp_client = builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    sftp_ftp_client.set_attributes(file_name_pattern=f'{ftp_file_name}*', data_format='SDC_JSON',
                                   file_post_processing="DELETE",
                                   file_name_pattern_mode='REGEX', process_subdirectories=True)

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> wiretap.destination
    sftp_ftp_client_pipeline = builder.build('FTP Origin Pipeline SDC Record').configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(2)
    sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)

    assert len(wiretap.output_records) == 2
    assert wiretap.output_records[0].field == json_data[0]
    assert wiretap.output_records[1].field == json_data[1]


@sdc_min_version('3.17.0')
@ftp
def test_ftp_origin_whole_file_with_no_read_permission(sdc_builder, sdc_executor, ftp):
    """This is a test for SDC-14867.  It creates a file with no read permissions and creates one more file
     with read permissions, when the pipeline runs we will start ingesting from the second file and first
     file is skipped and an error is reported. We also drop another file when the pipeline is running and
     see whether that is also picked up rightly.
    """
    prefix = get_random_string(string.ascii_letters, 5)

    ftp_file_name1 = f'{prefix}{get_random_string(string.ascii_letters, 10)}.txt'
    ftp_file_name2 = f'{prefix}{get_random_string(string.ascii_letters, 10)}.txt'
    ftp_file_name3 = f'{prefix}{get_random_string(string.ascii_letters, 10)}.txt'

    raw_text_data = get_random_string(string.printable, 30000000)

    ftp.put_string(ftp_file_name1, raw_text_data)
    ftp.chmod(ftp_file_name1, 000)

    ftp.put_string(ftp_file_name2, raw_text_data)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    sftp_ftp_client = builder.add_stage('SFTP/FTP/FTPS Client', type='origin')
    sftp_ftp_client.set_attributes(file_name_pattern=f'{prefix}*',
                                   data_format='WHOLE_FILE',
                                   batch_wait_time_in_ms=10000,
                                   max_batch_size_in_records=1)

    trash = builder.add_stage('Trash')

    wiretap = builder.add_wiretap()

    sftp_ftp_client >> [wiretap.destination, trash]
    sftp_to_trash_pipeline = builder.build().configure_for_environment(ftp)
    sdc_executor.add_pipeline(sftp_to_trash_pipeline)
    try:
        # Start the pipeline and wait for 1 record
        start_command = sdc_executor.start_pipeline(sftp_to_trash_pipeline)
        start_command.wait_for_pipeline_output_records_count(3)

        ftp.put_string(ftp_file_name3, raw_text_data)
        start_command.wait_for_pipeline_output_records_count(6)

        error_msgs = sdc_executor.get_stage_errors(sftp_to_trash_pipeline, sftp_ftp_client)

        # Verify the stage error message
        assert 'REMOTE_DOWNLOAD_10' in [e.error_code for e in error_msgs]

        actual_records = [record.field['fileInfo']['filename'] for record in wiretap.output_records]
        sdc_executor.stop_pipeline(sftp_to_trash_pipeline)
        wiretap.reset()

        assert [ftp_file_name2, ftp_file_name3] == actual_records
    finally:
        if sdc_executor.get_pipeline_status(sftp_to_trash_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(sftp_to_trash_pipeline)
        # Delete the test SFTP origin files we created
        ftp.chmod(ftp_file_name1, 700)
        for ftp_file_name in [ftp_file_name1, ftp_file_name2, ftp_file_name3]:
            logger.debug('Removing file at %s/%s on FTP server ...', ftp.path, ftp_file_name)
            ftp.rm(ftp_file_name)


def produce_lfs_messages_protobuf(ftp_file, sdc_builder, sdc_executor, message, ftp):
    # Build a dev_raw > local_fs  pipeline to create a protobuf file.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=message, stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')

    local_fs.set_attributes(file_type='TEXT', files_prefix=ftp_file, directory_template=TMPOUT,
                            data_format='PROTOBUF', message_type='Contact',
                            protobuf_descriptor_file=PROTOBUF_FILE_PATH)

    dev_raw_data_source >> local_fs

    pipeline = builder.build(
        title='FS PROTOBUF pipeline - Producer').configure_for_environment(ftp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()


def move_directory_messages_protobuf_ftp(ftp_file, sdc_builder, sdc_executor, message, ftp):
    # Build a directory->ftp  pipeline to move a protobuf file to ftp server.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern=f'{ftp_file}*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=TMPOUT,
                             process_subdirectories=False, read_order='TIMESTAMP')

    sftp_ftp_client = builder.add_stage(name=FTP_DEST_CLIENT_NAME)
    sftp_ftp_client.file_name_expression = ftp_file

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(
        stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    directory >> sftp_ftp_client
    directory >= pipeline_finished_executor

    pipeline = builder.build(
        title='FTP PROTOBUF pipeline - Producer').configure_for_environment(ftp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()


def produce_lfs_messages_SDC_Record(ftp_file, sdc_builder, sdc_executor, message, ftp):
    # Build a dev_raw > local_fs  pipeline to create a SDC_record file.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=message, stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')

    local_fs.set_attributes(file_type='TEXT', files_prefix=ftp_file, directory_template=TMPOUT,
                            data_format='SDC_JSON')

    dev_raw_data_source >> local_fs

    pipeline = builder.build(
        title='FS SDC Record pipeline - Producer').configure_for_environment(ftp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()


def move_directory_messages_SDC_record_ftp(ftp_file, sdc_builder, sdc_executor, message, ftp):
    # Build a directory->ftp  pipeline to move a SDC_record file to ftp server.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern=f'{ftp_file}*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=TMPOUT,
                             process_subdirectories=False, read_order='TIMESTAMP')

    sftp_ftp_client = builder.add_stage(name=FTP_DEST_CLIENT_NAME)
    sftp_ftp_client.file_name_expression = ftp_file

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(
        stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    directory >> sftp_ftp_client
    directory >= pipeline_finished_executor

    pipeline = builder.build(
        title='FTP SDC record pipeline - Producer').configure_for_environment(ftp)

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
