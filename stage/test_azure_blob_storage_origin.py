# Copyright 2023 StreamSets Inc.
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
import time

import avro
import avro.schema
import pytest
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [azure('datalake'), sdc_min_version('5.5.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_origin_client_blob_BlobStorageDSource'

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
        pytest.skip('Blob Storage tests require storage type to be of Gen2.')


@pytest.mark.parametrize('read_order', ['LEXICOGRAPHICAL', 'TIMESTAMP'])
def test_blob_storage_origin(sdc_builder, sdc_executor, azure, read_order):
    """ Test for Blob Storage origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Blob Storage Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The pipeline looks like:

    azure_blob_storage_origin >> wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(1, 10)]
    total_records = len(messages)

    # Put files in the azure storage file system
    dl_fs = azure.datalake.file_system
    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_blob_storage_origin = builder.add_stage(name=STAGE_NAME)
        azure_blob_storage_origin.set_attributes(data_format='TEXT',
                                                 common_prefix=f'/{directory_name}',
                                                 read_order=read_order)
        azure_blob_storage_origin >> wiretap.destination

        blob_storage_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(blob_storage_origin_pipeline)

        sdc_executor.start_pipeline(blob_storage_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(blob_storage_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(blob_storage_origin_pipeline)
        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_blob_storage_origin_with_avro(sdc_builder, sdc_executor, azure):
    """Ensure that the origin can properly read Avro document."""
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = get_random_string(string.ascii_letters, 10)
    file = f'{directory_name}/{file_name}.avro'
    data = {'name': 'Joaquin Bo'}
    total_records = len(data)

    dl_fs = azure.datalake.file_system
    try:
        # Create Avro file (with temporary location)
        with open(f'{TMP}{file_name}', "wb") as data_file:
            writer = DataFileWriter(data_file, DatumWriter(), avro.schema.parse(json.dumps(SCHEMA)))

            # Write data using DatumWriter
            writer.append(data)
            writer.close()

        # And upload it to ADLS
        with open(f'{TMP}{file_name}', 'rb') as fp:
            dl_fs.mkdir(directory_name)
            dl_fs.touch(file)
            dl_fs.write(file, fp.read(), content_type='application/octet-stream')

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage(name=STAGE_NAME)
        origin.set_attributes(data_format='AVRO',
                              avro_schema_location='SOURCE',
                              common_prefix=f'/{directory_name}')
        wiretap = builder.add_wiretap()
        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['name'] == 'Joaquin Bo'
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_parse_timestamp_blob_storage_origin(sdc_builder, sdc_executor, azure):
    """ Test for time creation file in Blob Storage origin stage. We do so by creating and modificating a file in
    Azure Data Lake Storage using the STF client, then reading last modification date from the file using the Blob
    Storage Origin Stage, to assert file's modification time ingested by the pipeline is the expected time from the
    moment the file was created. Assert the difference and discard the difference if they fluctuate in less than ten
    second.
    The pipeline looks like:

    azure_blob_storage_origin >> wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 5)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(1, 2)]
    total_records = len(messages)

    # Put files in the azure storage file system
    dl_fs = azure.datalake.file_system
    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        time_modification_time = int(time.time())

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        azure_blob_storage_origin = builder.add_stage(name=STAGE_NAME)
        wiretap = builder.add_wiretap()
        azure_blob_storage_origin.set_attributes(data_format='TEXT',
                                                 common_prefix=f'/{directory_name}',
                                                 include_metadata=True)
        azure_blob_storage_origin >> wiretap.destination

        blob_storage_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(blob_storage_origin_pipeline)

        sdc_executor.start_pipeline(blob_storage_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(blob_storage_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(blob_storage_origin_pipeline)

        timestamp = wiretap.output_records[0].header.values["mtime"]
        time_modification_time_obtained = int(timestamp[:-3])

        assert abs(time_modification_time - time_modification_time_obtained) <= 10

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_blob_storage_origin_stop_go(sdc_builder, sdc_executor, azure):
    """ Test for Blob Storage origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Blob Storage Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The pipeline looks like:

    azure_blob_storage_origin >> wiretap

    We stop the pipeline, insert more data and check the offset worked.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data-1.txt'
    messages = [f'message{i}' for i in range(1, 10)]
    total_records = len(messages)

    # Put files in the azure storage file system
    dl_fs = azure.datalake.file_system
    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_blob_storage_origin = builder.add_stage(name=STAGE_NAME)
        azure_blob_storage_origin.set_attributes(data_format='TEXT',
                                                 common_prefix=f'/{directory_name}')
        azure_blob_storage_origin >> wiretap.destination

        blob_storage_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(blob_storage_origin_pipeline)

        sdc_executor.start_pipeline(blob_storage_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(blob_storage_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(blob_storage_origin_pipeline)
        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

        messages = [f'message{i}' for i in range(11, 20)]
        file_name = 'test-data-2.txt'

        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))

        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        wiretap.reset()
        sdc_executor.start_pipeline(blob_storage_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(blob_storage_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(blob_storage_origin_pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_blob_storage_origin_events(sdc_builder, sdc_executor, azure):
    """ Test for Blob Storage origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Blob Storage Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The origin produce events. A pipeline finisher is connected to the origin. It stops the pipeline.
    We assert the events are the expected ones.
    The pipeline looks like:

    azure_blob_storage_origin >> wiretap
    azure_blob_storage_origin >= [finisher, wiretap]

    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(1, 10)]

    # Put files in the azure storage file system
    dl_fs = azure.datalake.file_system
    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        wiretap_events = builder.add_wiretap()
        azure_blob_storage_origin = builder.add_stage(name=STAGE_NAME)
        azure_blob_storage_origin.set_attributes(data_format='TEXT',
                                                 common_prefix=f'/{directory_name}')

        pipeline_finisher_executor = builder.add_stage('Pipeline Finisher Executor')
        pipeline_finisher_executor.set_attributes(
            stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

        azure_blob_storage_origin >> wiretap.destination
        azure_blob_storage_origin >= [pipeline_finisher_executor, wiretap_events.destination]

        blob_storage_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(blob_storage_origin_pipeline)

        sdc_executor.start_pipeline(blob_storage_origin_pipeline).wait_for_finished()
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


def test_blob_storage_origin_resume_offset(sdc_builder, sdc_executor, azure):
    """ Test for Blob Storage origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Blob Storage Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file. We then create more data, restart the pipeline, and read from wiretap to
    ensure that the stage properly resumes from where the offset left off. The pipeline looks like:
    The pipeline looks like:

    azure_blob_storage_origin >> wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    file2_name = 'test-data2.txt'
    messages = [f'message{i}' for i in range(1, 10)]
    messages2 = [f'message{i}' for i in range(11, 20)]
    total_records_1 = len(messages)
    total_records_2 = len(messages2)

    # Put files in the azure storage file system
    dl_fs = azure.datalake.file_system
    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_blob_storage_origin = builder.add_stage(name=STAGE_NAME)
        azure_blob_storage_origin.set_attributes(data_format='TEXT',
                                                 common_prefix=f'/{directory_name}')
        azure_blob_storage_origin >> wiretap.destination

        blob_storage_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(blob_storage_origin_pipeline)

        sdc_executor.start_pipeline(blob_storage_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(blob_storage_origin_pipeline, 'input_record_count', total_records_1)
        sdc_executor.stop_pipeline(blob_storage_origin_pipeline, wait=True)
        output_records = [record.field['text']
                          for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

        # Try adding the second file and resuming from the offset
        dl_fs.touch(f'{directory_name}/{file2_name}')
        dl_fs.write(f'{directory_name}/{file2_name}', '\n'.join(msg for msg in messages2))

        wiretap.reset()
        sdc_executor.start_pipeline(blob_storage_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(blob_storage_origin_pipeline, 'input_record_count', total_records_2)
        sdc_executor.stop_pipeline(blob_storage_origin_pipeline, wait=False)
        output_records = [record.field['text']
                          for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages2 == output_records
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@pytest.mark.parametrize('prefix_pattern', ['**', '**/*.txt', '*', '*/*', '*/*1/*', 'a/*', 'a/**', 'b/b*/**', 'c'])
def test_blob_storage_origin_prefix_pattern(sdc_builder, sdc_executor, azure, prefix_pattern):
    """Test prefix pattern expressions for the Blob Storage origin.

    Prefix pattern (ANT pattern) takes care of the client when choosing which files to process, while common prefix is
    included in the query made to Blob Storage to choose which files to retrieve. Common prefix does not allow
    expressions (because the Azure API does not allow it), so by combining both prefixes we can have expressions
    without retrieving everything from Azure.

    Pipeline:
      azure_blob_storage_origin >> trash
    """
    fs = azure.datalake.file_system

    rootdir = '{}'.format(get_random_string(string.ascii_letters, 10))
    # we will create a file in each directory, each with the same name and some random content inside
    subdirs_data = {
        '.': get_random_string(string.ascii_letters, 10),
        'a': get_random_string(string.ascii_letters, 10),
        'a/a1': get_random_string(string.ascii_letters, 10),
        'a/a2': get_random_string(string.ascii_letters, 10),
        'a/a3': get_random_string(string.ascii_letters, 10),
        'b': get_random_string(string.ascii_letters, 10),
        'b/b1': get_random_string(string.ascii_letters, 10),
        'b/b2': get_random_string(string.ascii_letters, 10),
        'b/b3': get_random_string(string.ascii_letters, 10),
        'b/b4/b': get_random_string(string.ascii_letters, 10)
    }
    expected_walk = {
        '**': ['.', 'a', 'a/a1', 'a/a2', 'a/a3', 'b', 'b/b1', 'b/b2', 'b/b3', 'b/b4/b'],
        '**/*.txt': ['.', 'a', 'a/a1', 'a/a2', 'a/a3', 'b', 'b/b1', 'b/b2', 'b/b3', 'b/b4/b'],
        '*': ['.'],
        '*/*': ['a', 'b'],
        '*/*1/*': ['a/a1', 'b/b1'],
        'a/*': ['a'],
        'a/**': ['a', 'a/a1', 'a/a2', 'a/a3'],
        'b/b*/**': ['b/b1', 'b/b2', 'b/b3', 'b/b4/b'],
        'c': []
    }

    # Create the directory tree and populate with files. The content of each file is just the filename.
    # we create files with different names in the different folders (number of files in expected_walk)
    expected_output = []
    fs.mkdir(rootdir)
    for path, data in subdirs_data.items():
        if path != '.':
            fs.mkdir(os.path.join(rootdir, path))
        filepath = os.path.join(rootdir, path, 'file.txt')
        fs.touch(filepath)
        fs.write(filepath, data)
        if path in expected_walk[prefix_pattern]:
            expected_output += [data]

    try:
        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        azure_blob_storage_origin = builder.add_stage(name=STAGE_NAME)
        azure_blob_storage_origin.set_attributes(data_format='TEXT',
                                                 common_prefix=f'/{rootdir}',
                                                 prefix_pattern=prefix_pattern)
        wiretap = builder.add_wiretap()

        azure_blob_storage_origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(expected_output))
        sdc_executor.stop_pipeline(pipeline)

        actual_output = [record.field['text'].value for record in wiretap.output_records]

        # Verify all the expected records were produced by the origin.
        assert sorted(actual_output) == sorted(expected_output)
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', rootdir)
        fs.rmdir(rootdir, recursive=True)


@pytest.mark.parametrize('action', ['ARCHIVE', 'DELETE'])
def test_file_postprocessing(sdc_builder, sdc_executor, azure, action):
    """Test file post-processing functionality in Blob Storage Origin.

    The test creates a directory tree and populates it with files. Then it checks the files are ingested by
    the pipeline and post-processed accordingly (either removing the file from ADLS when 'DELETE' is
    configured or moving the files to the archive directory when 'ARCHIVE' is configured).

    Pipeline:  azure_blob_storage_origin >> trash

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
    num_files = sum([len(files[d]) for d in files])

    try:
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
        azure_blob_storage_origin = builder.add_stage(name=STAGE_NAME)
        azure_blob_storage_origin.set_attributes(data_format='TEXT',
                                                 common_prefix=f'/{rootdir}',
                                                 post_processing_option=action,
                                                 post_processing_prefix=f'/{archive_dir}',
                                                 error_handling_option='DELETE')
        trash = builder.add_stage('Trash')
        azure_blob_storage_origin >> trash

        # Run the pipeline and wait until all the files were ingested.
        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_files, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # Check all the files were correctly post-processed. First, verify that all the files were removed
        # from their original paths.
        root_paths = fs.ls(rootdir, recursive=True).response.json()['paths']
        root_files = [p['name'] for p in root_paths if 'isDirectory' not in p]
        assert not root_files

        # Second, verify the files where moved to the archive dir when 'ARCHIVE' is configured.
        if action == 'ARCHIVE':
            archived_paths = fs.ls(archive_dir, recursive=True).response.json()['paths']
            archived_files = [p['name'] for p in archived_paths if 'isDirectory' not in p]

            for folder, filenames in files.items():
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
