# Copyright 2019 StreamSets Inc.
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

"""A module to test various SDC stages of Azure Datalake Storage Gen 2."""

import json
import logging
import os
import io
import string
import datetime, time
from operator import itemgetter

import avro
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import pytest
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

SOURCE_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_datalake_gen2_DataLakeGen2DSource'
TARGET_STAGE_NAME = 'com_streamsets_pipeline_stage_destination_datalake_gen2_DataLakeGen2DTarget'

TMP = '/tmp/'
SCHEMA = {
    'namespace': 'example.avro',
    'type': 'record',
    'name': 'Employee',
    'fields': [
        {'name': 'name', 'type': 'string'}
    ]
}

@pytest.fixture(autouse=True)
def storage_type_check(azure):
    if azure.storage_type == 'Storage':
        pytest.skip('ADLS Gen2 tests require storage type to be of Gen2.')


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_destination(sdc_builder, sdc_executor, azure):
    """Test for Data Lake Store target stage. We do so by running a dev raw data source generator to Data Lake Store
    destination with its provided account FQDN and then reading Data Lake Store using STF client to assert data
    between the client to what has been ingested by the pipeline. We use a record deduplicator processor in
    between dev raw data source origin and Data Lake Store destination in order to determine exactly what has
    been ingested.
    The pipeline looks like:

    Data Lake Store Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> azure_data_lake_store_destination
                                                   >> to_error
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = get_random_string(string.ascii_letters, 10)
    raw_list = [dict(contact=dict(name='Jane Smith', phone=2124050000, zip_code=27023)),
                dict(contact=dict(name='San', phone=2120998998, zip_code=14305))]
    raw_data = json.dumps(raw_list)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    azure_data_lake_store_destination = builder.add_stage(name=TARGET_STAGE_NAME)
    azure_data_lake_store_destination.set_attributes(data_format='JSON',
                                                     directory_template=f'/{directory_name}',
                                                     files_prefix=files_prefix,
                                                     files_suffix=files_suffix)

    dev_raw_data_source >> record_deduplicator >> azure_data_lake_store_destination
    record_deduplicator >> to_error

    datalake_dest_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(datalake_dest_pipeline)
    dl_fs = azure.datalake.file_system
    try:
        # start pipeline and capture pipeline messages to assert
        logger.info('Azure Data Lake directory %s will be created with files prefix %s', directory_name, files_prefix)
        sdc_executor.start_pipeline(datalake_dest_pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(datalake_dest_pipeline)

        paths = dl_fs.ls(directory_name).response.json()['paths']
        dl_files = [item['name'] for item in paths] if paths else []

        # assert Data Lake files generated
        assert len(dl_files) == 1

        # assert file prefix and suffix
        dl_file_name = dl_files[0].split('/')[-1]
        assert dl_file_name.startswith(files_prefix) and dl_file_name.endswith(files_suffix)

        # Assert file content. File will have len(raw_list) JSON formatted records, delimited by newline (\n).
        dl_file_contents = dl_fs.cat(dl_files[0]).response.content.decode()
        result_list = [json.loads(line) for line in dl_file_contents.split('\n')]

        assert raw_list == result_list
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_destination_avro(sdc_builder, sdc_executor, azure):
    """Test for Data Lake Store target stage. We do so by running a dev raw data source generator to Data Lake Store
    destination with its provided account FQDN and then reading Data Lake Store using STF client to assert data
    between the client to what has been ingested by the pipeline. We use a record deduplicator processor in
    between dev raw data source origin and Data Lake Store destination in order to determine exactly what has
    been ingested.
    The pipeline looks like:

    Data Lake Store Destination pipeline:
        dev_raw_data_source >> record_deduplicator >> azure_data_lake_store_destination
                                                   >> to_error
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = get_random_string(string.ascii_letters, 10)
    avro_schema = {
        'namespace': 'example.avro',
        'type': 'record',
        'name': 'Employee',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'phone', 'type': 'int'},
            {'name': 'zip_code', 'type': 'int'}
        ]
    }
    raw_list = [dict(name='Jane Smith', phone=2124050000, zip_code=27023),
                dict(name='San', phone=2120998998, zip_code=14305)]
    raw_data = json.dumps(raw_list)

    # Build the pipeline
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    azure_data_lake_store_destination = builder.add_stage(name=TARGET_STAGE_NAME)
    azure_data_lake_store_destination.set_attributes(data_format='AVRO',
                                                     avro_schema_location='INLINE',
                                                     avro_schema=json.dumps(avro_schema),
                                                     directory_template=f'/{directory_name}',
                                                     files_prefix=files_prefix,
                                                     files_suffix=files_suffix)

    dev_raw_data_source >> record_deduplicator >> azure_data_lake_store_destination
    record_deduplicator >> to_error

    datalake_dest_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(datalake_dest_pipeline)
    dl_fs = azure.datalake.file_system
    try:
        # start pipeline and capture pipeline messages to assert
        sdc_executor.start_pipeline(datalake_dest_pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(datalake_dest_pipeline)

        paths = dl_fs.ls(directory_name).response.json()['paths']
        dl_files = [item['name'] for item in paths] if paths else []

        # assert Data Lake files generated
        assert len(dl_files) == 1

        # assert file prefix and suffix
        dl_file_name = dl_files[0].split('/')[-1]
        assert dl_file_name.startswith(files_prefix) and dl_file_name.endswith(files_suffix)

        # Assert Avro file content.
        dl_file_response = dl_fs.cat(dl_files[0]).response

        with io.BytesIO(dl_file_response.content) as avro_file:
            with DataFileReader(avro_file, DatumReader()) as reader:
                schema_from_file = json.loads(reader.meta['avro.schema'])
                employees = [employee for employee in reader]

        assert schema_from_file == avro_schema
        assert employees == raw_list
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_destination_max_records_events(sdc_builder, sdc_executor, azure):
    """Test for Data Lake Store target stage setting max number of records per file as 1.
       Azure_data_lake_store_destination produces events.

       The pipeline looks like:

        Data Lake Store Destination pipeline:
            dev_data_generator >> azure_data_lake_store_destination >= wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = 'json'

    raw_data = [dict(id=1, name='abc'), dict(id=2, name='def'),
                dict(id=3, name='ghi'), dict(id=4, name='jkl'),
                dict(id=5, name='mno'), dict(id=6, name='pqr'),
                dict(id=7, name='stu'), dict(id=8, name='vwx'),
                dict(id=9, name='y'), dict(id=10, name='z')]

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    azure_data_lake_store = pipeline_builder.add_stage(name=TARGET_STAGE_NAME)
    azure_data_lake_store.set_attributes(data_format='JSON',
                                         directory_template=f'/{directory_name}',
                                         files_prefix=files_prefix,
                                         files_suffix=files_suffix,
                                         max_records_in_file=1)
    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> azure_data_lake_store >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    dl_fs = azure.datalake.file_system

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        paths = dl_fs.ls(directory_name).response.json()['paths']
        dl_files = [item['name'] for item in paths] if paths else []
        assert len(dl_files) == len(raw_data)
        for dl_file in dl_files:
            dl_file_name = dl_file.split('/')[-1]
            assert dl_file_name.startswith(files_prefix) and dl_file_name.endswith(files_suffix)
        dl_file_contents = [json.loads(dl_fs.cat(dl_file).response.content.decode()) for dl_file in dl_files]

        assert sorted(dl_file_contents, key=itemgetter('id')) == sorted(raw_data, key=itemgetter('id'))

        assert len(wiretap.output_records) == 10
        for index in range(0, 9):
            assert wiretap.output_records[index].header['values']['sdc.event.type'] == 'file-closed'
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_origin(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake Store origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the ALDS Gen2 Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The pipeline looks like:

    azure_data_lake_store_origin >> wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(1, 10)]
    total_records = len(messages)

    try:
        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> wiretap.destination

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        # start pipeline and read file in ADLS
        sdc_executor.start_pipeline(datalake_origin_pipeline).wait_for_pipeline_output_records_count(total_records)
        sdc_executor.stop_pipeline(datalake_origin_pipeline)
        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_origin_with_avro(sdc_builder, sdc_executor, azure):
    """Ensure that the origin can properly read Avro document."""
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = get_random_string(string.ascii_letters, 10)
    file = f'{directory_name}/{file_name}.avro'
    data = {'name': 'Arvind P.'}
    total_records = len(data)

    try:
        # Create Avro file (with temporary location)
        with open(f'{TMP}{file_name}', "wb") as data_file:
            writer = DataFileWriter(data_file, DatumWriter(), avro.schema.Parse(json.dumps(SCHEMA)))

            # Write data using DatumWriter
            writer.append(data)
            writer.close()

        # And upload it to ADSL
        with open(f'{TMP}{file_name}', 'rb') as fp:
            dl_fs = azure.datalake.file_system
            dl_fs.mkdir(directory_name)
            dl_fs.touch(file)
            dl_fs.write(file, fp.read(), content_type='application/octet-stream')

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        origin.set_attributes(data_format='AVRO',
                              files_directory=f'/{directory_name}',
                              file_name_pattern='*')
        wiretap = builder.add_wiretap()
        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)

        # start pipeline and read file in ADLS
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_records)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['name'] == 'Arvind P.'
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_parse_timestamp_datalake_origin(sdc_builder, sdc_executor, azure):
    """ Test for time creation file in Data Lake Store origin stage. We do so by creating and modificating a file in
    Azure Data Lake Storage using the STF client, then reading last modification date from the file using the ALDS Gen2
    Origin Stage, to assert file's modification time ingested by the pipeline is the expected time from the moment the
    file was created. Assert the difference and discard the difference if they fluctuate in less than ten second.
    The pipeline looks like:

    azure_data_lake_store_origin >> wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 5)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(1, 2)]
    total_records = len(messages)

    try:
        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        time_modification_time = int(time.time())

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        wiretap = builder.add_wiretap()
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> wiretap.destination

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        sdc_executor.start_pipeline(datalake_origin_pipeline).wait_for_pipeline_output_records_count(total_records)
        sdc_executor.stop_pipeline(datalake_origin_pipeline)

        timestamp = wiretap.output_records[0].header.values["mtime"]
        time_modification_time_obtained = int(timestamp[:-3])

        assert abs(time_modification_time - time_modification_time_obtained) <= 10

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_origin_stop_go(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake Store origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the ALDS Gen2 Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The pipeline looks like:

    azure_data_lake_store_origin >> wiretap

    We stop the pipeline, insert more data and check the offset worked.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data-1.txt'
    messages = [f'message{i}' for i in range(1, 10)]
    total_records = len(messages)

    try:
        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> wiretap.destination

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        sdc_executor.start_pipeline(datalake_origin_pipeline).wait_for_pipeline_output_records_count(total_records)
        sdc_executor.stop_pipeline(datalake_origin_pipeline)
        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

        messages = [f'message{i}' for i in range(11, 20)]
        file_name = 'test-data-2.txt'

        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))

        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        # start pipeline and read file in ADLS
        wiretap.reset()
        sdc_executor.start_pipeline(datalake_origin_pipeline).wait_for_pipeline_output_records_count(total_records)
        sdc_executor.stop_pipeline(datalake_origin_pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_origin_events(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake Store origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the ALDS Gen2 Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The origin produce events. A pipeline finisher is connected to the origin. It stops the pipeline.
    We assert the events are the expected ones.
    The pipeline looks like:

    azure_data_lake_store_origin >> wiretap
    azure_data_lake_store_origin >= [finisher, wiretap]

    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(1, 10)]

    try:
        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        wiretap_events = builder.add_wiretap()
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')

        pipeline_finisher_executor = builder.add_stage('Pipeline Finisher Executor')
        pipeline_finisher_executor.set_attributes(
            stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

        azure_data_lake_store_origin >> wiretap.destination
        azure_data_lake_store_origin >= [pipeline_finisher_executor, wiretap_events.destination]

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        sdc_executor.start_pipeline(datalake_origin_pipeline).wait_for_finished()
        output_records = [record.field['text']
                          for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

        assert len(wiretap_events.output_records) == 2
        assert wiretap_events.output_records[0].header['values'][
                   'sdc.event.type'] == 'new-file'
        assert wiretap_events.output_records[1].header['values'][
                   'sdc.event.type'] == 'finished-file'

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_origin_resume_offset(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake Store origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the ALDS Gen2 Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file. We then create more data, restart the pipeline, and read from wiretap to
    ensure that the stage properly resumes from where the offset left off. The pipeline looks like:
    The pipeline looks like:

    azure_data_lake_store_origin >> wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    file2_name = 'test-data2.txt'
    messages = [f'message{i}' for i in range(1, 10)]
    messages2 = [f'message{i}' for i in range(11, 20)]
    total_records_1 = len(messages)
    total_records_2 = len(messages2)

    try:
        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> wiretap.destination

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        sdc_executor.start_pipeline(datalake_origin_pipeline).wait_for_pipeline_output_records_count(total_records_1)
        sdc_executor.stop_pipeline(datalake_origin_pipeline, wait=True)
        output_records = [record.field['text']
                          for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

        # Try adding the second file and resuming from the offset
        dl_fs.touch(f'{directory_name}/{file2_name}')
        dl_fs.write(f'{directory_name}/{file2_name}', '\n'.join(msg for msg in messages2))

        wiretap.reset()
        sdc_executor.start_pipeline(datalake_origin_pipeline).wait_for_pipeline_output_records_count(total_records_2)
        sdc_executor.stop_pipeline(datalake_origin_pipeline, wait=False)
        output_records = [record.field['text']
                          for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages2 == output_records
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('process_subdirs', [True, False])
@pytest.mark.parametrize('glob_pattern', ['', '*', '*/*', '*/*1', '*.txt'])
def test_datalake_origin_glob_expansion(sdc_builder, sdc_executor, azure, process_subdirs, glob_pattern):
    """Test glob expansion for the directory configuration in ADLS origin.

    When the directory path configured in ADLS is a glob pattern, it is expanded to a list of matched
    directories (note that file matches are discarded). For each matched directory, the origin processes files
    inside: only on the top level if process_subdirectories = False, or at any level if process_subdirectories
    = True. This test checks that the glob expansion is done properly, creating a directory tree in ADLS and
    trying different glob patterns to filter the directories to scan for files. We verify that the files
    processed matches those in the configuration, according to the test parameters 'process_subdirs' and
    'glob_pattern'.

    Pipeline:
      azure_data_lake_store_origin >> trash

    """
    fs = azure.datalake.file_system

    # Define the directory tree and the expected directories to be visited by ADLS origin. For the latter we
    # use a dict where each entry correspond to a different glob pattern to test.
    rootdir = '{}'.format(get_random_string(string.ascii_letters, 10))
    subdirs = ['.', 'a', 'a/a1', 'a/a2', 'a/a3', 'b', 'b/b1', 'b/b2', 'b/b3']
    if process_subdirs:
        expected_walk = {
            '': ['.', 'a', 'a/a1', 'a/a2', 'a/a3', 'b', 'b/b1', 'b/b2', 'b/b3'],
            '*': ['a', 'a/a1', 'a/a2', 'a/a3', 'b', 'b/b1', 'b/b2', 'b/b3'],
            '*/*': ['a/a1', 'a/a2', 'a/a3', 'b/b1', 'b/b2', 'b/b3'],
            '*/*1': ['a/a1', 'b/b1'],
            '*.txt': []  # Expands to a file, so no directory to walk through.
        }
    else:
        expected_walk = {
            '': ['.'],
            '*': ['a', 'b'],
            '*/*': ['a/a1', 'a/a2', 'a/a3', 'b/b1', 'b/b2', 'b/b3'],
            '*/*1': ['a/a1', 'b/b1'],
            '*.txt': []  # Expands to a file, so no directory to walk through.
        }
    expected_output = []
    for exp in expected_walk[glob_pattern]:
        filepath = os.path.join(rootdir, exp, '{}_file.txt'.format(exp.replace('/', '_')))
        expected_output += [filepath]

    # Create the directory tree and populate with files. The content of each file is just the filename.
    fs.mkdir(rootdir)
    for d in subdirs:
        if d != '.':
            fs.mkdir(os.path.join(rootdir, d))
        filename = '{}_file.txt'.format(d.replace('/', '_'))
        filepath = os.path.join(rootdir, d, filename)
        fs.touch(filepath)
        fs.write(filepath, filepath)

    if len(expected_walk[glob_pattern]) > 0:
        filename = 'dup.txt'

        for d in {expected_walk[glob_pattern][0], expected_walk[glob_pattern][-1]}:
            filepath = os.path.join(rootdir, d, filename)
            fs.touch(filepath)
            fs.write(filepath, filepath)
            expected_output += [filepath]

    pipeline = None

    try:
        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        adls_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        adls_origin.set_attributes(data_format='TEXT',
                                   files_directory=f'/{os.path.join(rootdir, glob_pattern)}',
                                   file_name_pattern='*',
                                   read_order='TIMESTAMP',
                                   process_subdirectories=process_subdirs)
        wiretap = builder.add_wiretap()
        adls_origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        # There may be up to 11 files to wait for, that may take some time, setting a long timeout.
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 2 * len(expected_output), timeout_sec=300)

        actual_output = [record.field['text'].value for record in wiretap.output_records]

        # Verify all the expected records were produced by the origin.
        assert sorted(actual_output) == sorted(expected_output)

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', rootdir)
        fs.rmdir(rootdir, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('action', ['ARCHIVE', 'DELETE'])
@pytest.mark.parametrize('process_subdirectories', [True, False])
def test_file_postprocessing(sdc_builder, sdc_executor, azure, action, process_subdirectories):
    """Test file post-processing functionality in ADLS Gen2 origin.

    The test creates a directory tree and populates it with files. Then it checks the files are ingested by
    the pipeline and post-processed accordingly (either removing the file from ADLS when 'DELETE' is
    configured or moving the files to the archive directory when 'ARCHIVE' is configured).

    Pipeline:  adls_origin >> trash

    """
    fs = azure.datalake.file_system

    # Variable `files` define the directory tree employed in the test. Keys are the directories and values are
    # the list of files contained for each directory.
    rootdir = 'stf_postprocessing_{}'.format(get_random_string(string.ascii_letters, 10))
    archive_dir = 'stf_postprocessing_archive_{}'.format(get_random_string(string.ascii_letters, 10))
    files = {'.': [get_random_string() for _ in range(3)],
             os.path.join('a1'): [get_random_string() for _ in range(3)],
             os.path.join('b1'): [get_random_string() for _ in range(3)],
             os.path.join('a1', 'a2'): [get_random_string() for _ in range(3)],
             os.path.join('b1', 'b2'): [get_random_string() for _ in range(3)]}
    num_files = sum([len(files[d]) for d in files]) if process_subdirectories else len(files['.'])

    # Create the directory tree according to `files`. The content of each file will be just the filename.
    # Also generate the directory where files will be archived.
    fs.mkdir(rootdir)
    fs.mkdir(archive_dir)
    for d in sorted([path for path in files.keys() if path != '.']):  # Excluding rootdir.
        fs.mkdir(os.path.join(rootdir, d))
    for (folder, filenames) in files.items():
        for f in filenames:
            _adls_create_file(fs, f, os.path.join(rootdir, folder, f))

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    adls_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
    adls_origin.set_attributes(data_format='TEXT',
                               files_directory=f'/{rootdir}',
                               file_name_pattern='*',
                               read_order='TIMESTAMP',
                               process_subdirectories=process_subdirectories,
                               file_post_processing=action,
                               archive_directory=f'/{archive_dir}')
    trash = builder.add_stage('Trash')
    adls_origin >> trash

    try:
        # Run the pipeline and wait until all the files were ingested.
        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(num_files)
        sdc_executor.stop_pipeline(pipeline)

        # Check all the files were correctly post-processed. First, verify that all the files were removed
        # from their original paths.
        root_paths = fs.ls(rootdir, recursive=process_subdirectories).response.json()['paths']
        root_files = [p['name'] for p in root_paths if 'isDirectory' not in p]
        assert not root_files

        # Second, verify the files where moved to the archive dir when 'ARCHIVE' is configured.
        if action == 'ARCHIVE':
            archived_paths = fs.ls(archive_dir, recursive=True).response.json()['paths']
            archived_files = [p['name'] for p in archived_paths if 'isDirectory' not in p]

            for folder, filenames in files.items():
                if folder == '.' or process_subdirectories:
                    for f in filenames:
                        expected_path = os.path.join(archive_dir, folder, f).replace('./', '')
                        assert expected_path in archived_files
                        assert fs.cat(expected_path).response.text == f

    finally:
        fs.rmdir(rootdir, recursive=True)
        fs.rmdir(archive_dir, recursive=True)


def _adls_create_file(adls_client, file_content, file_path):
    """Create a file in ADLS with the specified content.  If the file already exist, overrite content with
    `file_content`.

    """
    res1 = adls_client.touch(file_path)
    res2 = adls_client.write(file_path, file_content)
    if not (res1.response.ok and res2.response.ok):
        raise RuntimeError(f'Could not create file: {file_path}')


@azure('datalake')
@sdc_min_version("3.19.0")
def test_adls_gen2_file_event_filepath_when_whole_file_mode_disabled(sdc_builder, sdc_executor, azure):
    """
    We want to make sure that the filepath attribute of an event generated by the ADLS gen2 destination contains only
    a file path and doesn't contain a schema. A misbehaviour normally happened before when an old file was found
    during a pipeline start-up. This wasn't observed for events that were sent for files created during pipeline
    execution. The goal is to make the behaviour consistent, and since the name of the attribute is the filepath
    not URI we decided to remove the schema part from it.

    Pipeline:
              Dev Raw Data Source >> ADLS gen2 FS >= wiretap

    When the pipeline stops we assert the filepath attribute of the event generate by ADLS gen2 FS.
    """

    builder = sdc_builder.get_pipeline_builder()
    dl_fs = azure.datalake.file_system

    directory_name = get_random_string(string.ascii_letters)

    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/_tmp_sdc_0')
        dl_fs.write(f'{directory_name}/_tmp_sdc_0', 'message1\n')

        data_source = builder.add_stage('Dev Raw Data Source')
        data_source.stop_after_first_batch = True
        data_source.data_format = 'TEXT'
        data_source.raw_data = 'message2\n'

        azure_data_lake_store_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_datalake_gen2_DataLakeGen2DTarget')
        azure_data_lake_store_destination.set_attributes(data_format='TEXT',
                                                         directory_template=f'/{directory_name}',
                                                         files_prefix='sdc',
                                                         files_suffix='')

        wiretap = builder.add_wiretap()
        data_source >> azure_data_lake_store_destination >= wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        history = sdc_executor.get_pipeline_history(pipeline)
        pipeline_record_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        stage_record_count = len(wiretap.output_records)

        assert stage_record_count == 2
        assert pipeline_record_count == stage_record_count + 1
        for event_record in wiretap.output_records:
            assert event_record.get_field_data('/filepath').value.startswith(f'/{directory_name}/sdc_')

    finally:
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
def test_adls_gen2_file_event_filepath_when_whole_file_mode_enabled(sdc_builder, sdc_executor, azure):
    """
    We want to make sure that the /targetFileInfo/path and /sourceFileInfo/file attributes of the event generated by
    the ADLS gen2 FS destination in the WHOLE_FILE mode contains only a file path and doesn't contain schema.
    A misbehaviour was not observed before for the WHOLE_FILE mode. The goal is to make sure the behaviour
    is not broken by the fix.

    Pipeline:
              Directory >> ADLS gen2 FS >= Trash

    Before starting the pipeline we put a file to the source directory, this is the new file.
    We also put a file to the destination folder, this is the "old" file.
    When the pipeline stops we assert the /targetFileInfo/path and /sourceFileInfo/file attributes of the event
    generated by ADLS gen2 FS.
    """

    builder = sdc_builder.get_pipeline_builder()
    dl_fs = azure.datalake.file_system

    base_folder = f'/tmp/{get_random_string(string.ascii_letters)}'
    directory_name = get_random_string(string.ascii_letters)

    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/_tmp_sdc-output')
        dl_fs.write(f'{directory_name}/_tmp_sdc-output', 'message1\n')

        src = builder.add_stage('Directory')
        src.files_directory = f'{base_folder}/input'
        src.file_name_pattern = '*'
        src.data_format = 'WHOLE_FILE'
        src.batch_size_in_recs = 1
        src.batch_wait_time_in_secs = 1

        sdc_executor.execute_shell(f'mkdir -p {src.files_directory}')
        sdc_executor.write_file(f'{src.files_directory}/input.txt', 'message2\n')

        azure_data_lake_store_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_datalake_gen2_DataLakeGen2DTarget')
        azure_data_lake_store_destination.set_attributes(data_format='WHOLE_FILE',
                                                         file_type='WHOLE_FILE',
                                                         directory_template=f'/{directory_name}',
                                                         files_prefix='sdc',
                                                         file_name_expression='-output')

        wiretap = builder.add_wiretap()

        src >> azure_data_lake_store_destination >= wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 3)
        sdc_executor.stop_pipeline(pipeline, force=True)

        records = wiretap.output_records

        history = sdc_executor.get_pipeline_history(pipeline)
        pipeline_record_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        events_record_count = len(records)

        assert events_record_count == 1
        # 3 equals to 1 record + 1 event two times because of wiretap
        assert pipeline_record_count == 3
        for event_record in records:
            assert event_record.get_field_data('/targetFileInfo/path').value == f'/{directory_name}/sdc-output'
            assert event_record.get_field_data('/sourceFileInfo/file').value == f'{src.files_directory}/input.txt'

    finally:
        try:
            dl_fs.rmdir(directory_name, recursive=True)
        finally:
            sdc_executor.execute_shell(f'rm -fr {base_folder}')
