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
import string
import datetime, time
from operator import itemgetter

import pytest
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SOURCE_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_datalake_gen2_DataLakeGen2DSource'
TARGET_STAGE_NAME = 'com_streamsets_pipeline_stage_destination_datalake_gen2_DataLakeGen2DTarget'


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
def test_datalake_destination_max_records_events(sdc_builder, sdc_executor, azure):
    """Test for Data Lake Store target stage setting max number of records per file as 1.
       Azure_data_lake_store_destination produces events.

       The pipeline looks like:

        Data Lake Store Destination pipeline:
            dev_data_generator >> azure_data_lake_store_destination >= trash
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
    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> azure_data_lake_store >= trash

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    dl_fs = azure.datalake.file_system

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=1).wait_for_finished().snapshot

        paths = dl_fs.ls(directory_name).response.json()['paths']
        dl_files = [item['name'] for item in paths] if paths else []
        assert len(dl_files) == len(raw_data)
        for dl_file in dl_files:
            dl_file_name = dl_file.split('/')[-1]
            assert dl_file_name.startswith(files_prefix) and dl_file_name.endswith(files_suffix)
        dl_file_contents = [json.loads(dl_fs.cat(dl_file).response.content.decode()) for dl_file in dl_files]

        assert sorted(dl_file_contents, key=itemgetter('id')) == sorted(raw_data, key=itemgetter('id'))

        assert len(snapshot[azure_data_lake_store].event_records) == 10
        for index in range(0, 9):
            assert snapshot[azure_data_lake_store].event_records[index].header['values'][
                       'sdc.event.type'] == 'file-closed'
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

    azure_data_lake_store_origin >> trash
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
        trash = builder.add_stage('Trash')
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> trash

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        snapshot = sdc_executor.capture_snapshot(datalake_origin_pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(datalake_origin_pipeline)
        output_records = [record.field['text']
                          for record in snapshot[azure_data_lake_store_origin.instance_name].output]

        # assert Data Lake files generated
        assert messages == output_records
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_parse_timestamp_datalake_origin(sdc_builder, sdc_executor, azure):
    """ Test for time creation file in Data Lake Store origin stage. We do so by creating and modificating a file in
    Azure Data Lake Storage using the STF client, then reading last modification date from the file using the ALDS Gen2
    Origin Stage, to assert file's modification time ingested by the pipeline is the expected time from the moment the
    file was created. Assert the difference and discard the difference if they fluctuate in less than one second.
    The pipeline looks like:

    azure_data_lake_store_origin >> trash
    """
    directory_name = get_random_string(string.ascii_letters, 5)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(1, 2)]

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
        trash = builder.add_stage('Trash')
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> trash

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        snapshot = sdc_executor.capture_snapshot(datalake_origin_pipeline,
                                                 start_pipeline=True,
                                                 batches=1).snapshot
        sdc_executor.stop_pipeline(datalake_origin_pipeline)

        for record in snapshot[azure_data_lake_store_origin.instance_name].output:
            timestamp = record.header.values["mtime"]
        time_modification_time_obtained = int(timestamp[:-3])

        assert abs(time_modification_time - time_modification_time_obtained) <= 1


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

    azure_data_lake_store_origin >> trash

    We stop the pipeline, insert more data and check the offset worked.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data-1.txt'
    messages = [f'message{i}' for i in range(1, 10)]

    try:
        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        trash = builder.add_stage('Trash')
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> trash

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        snapshot = sdc_executor.capture_snapshot(datalake_origin_pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(datalake_origin_pipeline)
        output_records = [record.field['text']
                          for record in snapshot[azure_data_lake_store_origin.instance_name].output]

        # assert Data Lake files generated
        assert messages == output_records

        messages = [f'message{i}' for i in range(11, 20)]
        file_name = 'test-data-2.txt'

        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))

        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        # start pipeline and read file in ADLS
        snapshot = sdc_executor.capture_snapshot(datalake_origin_pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(datalake_origin_pipeline)
        output_records = [record.field['text']
                          for record in snapshot[azure_data_lake_store_origin.instance_name].output]

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

    azure_data_lake_store_origin >> trash
    azure_data_lake_store_origin >= finisher

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
        trash = builder.add_stage('Trash')
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')

        pipeline_finisher_executor = builder.add_stage('Pipeline Finisher Executor')
        pipeline_finisher_executor.set_attributes(
            stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

        azure_data_lake_store_origin >> trash
        azure_data_lake_store_origin >= pipeline_finisher_executor

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        snapshot = sdc_executor.capture_snapshot(datalake_origin_pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.get_pipeline_status(datalake_origin_pipeline).wait_for_status(status='FINISHED')
        output_records = [record.field['text']
                          for record in snapshot[azure_data_lake_store_origin.instance_name].output]

        # assert Data Lake files generated
        assert messages == output_records

        assert len(snapshot[azure_data_lake_store_origin].event_records) == 2
        assert snapshot[azure_data_lake_store_origin].event_records[0].header['values'][
                   'sdc.event.type'] == 'new-file'
        assert snapshot[azure_data_lake_store_origin].event_records[1].header['values'][
                   'sdc.event.type'] == 'finished-file'

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_origin_resume_offset(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake Store origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the ALDS Gen2 Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file. We then create more data, restart the pipeline, and take another snapshot to
    ensure that the stage properly resumes from where the offset left off. The pipeline looks like:
    The pipeline looks like:

    azure_data_lake_store_origin >> trash
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    file2_name = 'test-data2.txt'
    messages = [f'message{i}' for i in range(1, 10)]
    messages2 = [f'message{i}' for i in range(11, 20)]

    try:
        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        trash = builder.add_stage('Trash')
        azure_data_lake_store_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        azure_data_lake_store_origin.set_attributes(data_format='TEXT',
                                                    files_directory=f'/{directory_name}',
                                                    file_name_pattern='*')
        azure_data_lake_store_origin >> trash

        datalake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(datalake_origin_pipeline)

        # start pipeline and read file in ADLS
        snapshot = sdc_executor.capture_snapshot(datalake_origin_pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(datalake_origin_pipeline, wait=True)
        output_records = [record.field['text']
                          for record in snapshot[azure_data_lake_store_origin.instance_name].output]

        # assert Data Lake files generated
        assert messages == output_records

        # Try adding the second file and resuming from the offset
        dl_fs.touch(f'{directory_name}/{file2_name}')
        dl_fs.write(f'{directory_name}/{file2_name}', '\n'.join(msg for msg in messages2))

        snapshot = sdc_executor.capture_snapshot(datalake_origin_pipeline, start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(datalake_origin_pipeline, wait=False)
        output_records = [record.field['text']
                          for record in snapshot[azure_data_lake_store_origin.instance_name].output]

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
    expected_output = ['{}_file.txt'.format(exp.replace('/', '_'))
                       for exp in expected_walk[glob_pattern]]

    # Create the directory tree and populate with files. The content of each file is just the filename.
    fs.mkdir(rootdir)
    for d in subdirs:
        if d != '.':
            fs.mkdir(os.path.join(rootdir, d))
        filename = '{}_file.txt'.format(d.replace('/', '_'))
        filepath = os.path.join(rootdir, d, filename)
        fs.touch(filepath)
        fs.write(filepath, filename)

    try:
        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        adls_origin = builder.add_stage(name=SOURCE_STAGE_NAME)
        adls_origin.set_attributes(data_format='TEXT',
                                   files_directory=f'/{os.path.join(rootdir, glob_pattern)}',
                                   file_name_pattern='*',
                                   read_order='TIMESTAMP',
                                   process_subdirectories=process_subdirs)
        trash = builder.add_stage('Trash')
        adls_origin >> trash

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and read files in ADLS.
        num_batches = max(len(expected_walk[glob_pattern]), 1)
        snapshot = sdc_executor.capture_snapshot(pipeline,
                                                 start_pipeline=True,
                                                 batches=num_batches,
                                                 timeout_sec=120).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Verify all the expected records were produced by the origin.
        actual_output = [record.field['text'].value
                         for batch in snapshot
                         for record in batch[adls_origin.instance_name].output]
        assert sorted(actual_output) == sorted(expected_output)

    finally:
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
