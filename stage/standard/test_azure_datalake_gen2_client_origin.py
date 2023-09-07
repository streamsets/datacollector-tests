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

import logging
import os
import string
import time

import pytest
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.category('standard'), azure('datalake'), sdc_min_version('5.5.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_origin_client_datalake_DataLakeStorageGen2DSource'

TMPOUT = '/tmp/out/'
PROTOBUF_FILE_PATH = 'resources/protobuf/addressbook.desc'


@pytest.fixture(autouse=True)
def storage_type_check(azure):
    if azure.storage_type == 'Storage':
        pytest.skip('ADLS Gen2 tests require storage type to be of Gen2.')


def test_data_types(sdc_builder, sdc_executor):
    pytest.skip("Data Lake stores objects without doing data type distinctions.")


# Reference:
# https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-shares--directories--files--and-metadata
AZURE_OBJECT_NAMES = [
    ('max_size', get_random_string(string.ascii_lowercase, 255)),
    ('min_size', get_random_string(string.ascii_lowercase, 1)),
    ('lowercase', get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20) + get_random_string(string.ascii_letters, 20)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "1234567890" + get_random_string(string.ascii_letters, 5)),
    ('special', get_random_string(string.ascii_letters, 5) + "!@·#$%&()='¡¿[]`^+{}´¨,;-_" + get_random_string(string.ascii_letters, 5))
]


def test_container_names(sdc_builder, sdc_executor):
    """
    Reference:
    https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata

    Problem here is that creating an Azure container is not easy as it is in other systems
    """
    pytest.skip("Cannot test Azure container names")


@pytest.mark.parametrize('test_name, object_name', AZURE_OBJECT_NAMES, ids=[i[0] for i in AZURE_OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, azure, test_name, object_name):
    """Test file names expressions for Data Lake origin.

    Pipeline:
        azure_data_lake_origin >> wiretap
    """
    fs = azure.datalake.file_system
    directory_name = get_random_string(string.ascii_letters, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='TEXT',
                                          common_path=f'/{directory_name}')

    if Version(sdc_builder.version) >= Version('5.7.0'):
        azure_data_lake_origin.file_processing_delay_in_ms = 1000

    wiretap = pipeline_builder.add_wiretap()

    azure_data_lake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        fs.mkdir(directory_name)
        fs.touch(f'{directory_name}/{test_name}/{object_name}')
        fs.write(f'{directory_name}/{test_name}/{object_name}', '\n'.join(msg for msg in data))
        time.sleep(5)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


def test_dataflow_events(sdc_builder, sdc_executor, azure):
    """
    Write 10 data files to Azure, read them using SDC, and check that we get:
        - 10 new-file
        - 10 finished-file
        - 1 no-more-data

    The pipeline looks like:
        azure_data_lake_origin >> trash
        azure_data_lake_origin >= wiretap
    """
    fs = azure.datalake.file_system
    directory_name = get_random_string(string.ascii_letters, 10)
    object_name = get_random_string(string.ascii_letters, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='TEXT',
                                          common_path=f'/{directory_name}')

    if Version(sdc_builder.version) >= Version('5.7.0'):
        azure_data_lake_origin.file_processing_delay_in_ms = 1000

    wiretap = pipeline_builder.add_wiretap()
    trash = pipeline_builder.add_stage("Trash")

    azure_data_lake_origin >> trash
    azure_data_lake_origin >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        fs.mkdir(directory_name)
        fs.touch(f'{directory_name}/{object_name}')
        fs.write(f'{directory_name}/{object_name}', '\n'.join(msg for msg in data))
        time.sleep(5)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        # One event should be created
        records = wiretap.output_records
        assert len(records) == 1 + 1 + 1
        assert records[0].header.values['sdc.event.type'] == 'new-file'
        assert records[1].header.values['sdc.event.type'] == 'finished-file'
        assert records[2].header.values['sdc.event.type'] == 'no-more-data'
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


def test_multiple_batches(sdc_builder, sdc_executor, azure):
    """
    Test that we can produce multiple batches and the pipeline processes all the data exactly once.

    The pipeline looks like:
        azure_data_lake_origin >> wiretap
    """
    fs = azure.datalake.file_system
    directory_name = get_random_string(string.ascii_letters, 10)
    object_name = get_random_string(string.ascii_letters, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    max_batch_size = 100
    number_of_batches = 10

    azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='TEXT',
                                          common_path=f'/{directory_name}',
                                          batch_wait_time_in_ms=20_000,
                                          max_batch_size_in_records=max_batch_size)

    if Version(sdc_builder.version) >= Version('5.7.0'):
        azure_data_lake_origin.file_processing_delay_in_ms = 1000


    wiretap = pipeline_builder.add_wiretap()

    azure_data_lake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        data = [get_random_string(string.ascii_letters, 100) for _ in range(max_batch_size * number_of_batches)]
        fs.mkdir(directory_name)
        fs.touch(f'{directory_name}/{object_name}')
        fs.write(f'{directory_name}/{object_name}', '\n'.join(msg for msg in data))
        time.sleep(5)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', max_batch_size * number_of_batches)
        sdc_executor.stop_pipeline(pipeline)

        rows_from_wiretap = [record.field['text'] for record in wiretap.output_records]

        assert len(rows_from_wiretap) == max_batch_size * number_of_batches
        assert len(data) == len(rows_from_wiretap)
        assert rows_from_wiretap == data
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


def test_data_formats(sdc_builder, sdc_executor):
    pytest.skip("Azure Data Lake uses DataParserService to parse DataFormats, we do not need to test every format here")


def test_data_format_whole_file(sdc_builder, sdc_executor, azure):
    """
    Read data from Azure using whole file format.

    The pipeline looks like:
        azure_data_lake_origin >> wiretap
    """
    fs = azure.datalake.file_system
    directory_name = get_random_string(string.ascii_letters, 10)
    object_name = get_random_string(string.ascii_letters, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='WHOLE_FILE',
                                          common_path=f'/{directory_name}')
    wiretap = pipeline_builder.add_wiretap()

    azure_data_lake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        data = [get_random_string(string.ascii_letters, 100) for _ in range(10)]
        fs.mkdir(directory_name)
        fs.touch(f'{directory_name}/{object_name}')
        fs.write(f'{directory_name}/{object_name}', '\n'.join(msg for msg in data))

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        output_records = [record.field for record in wiretap.output_records]
        # Whole file mode only returns metadata, but no actual data, so we can only test that a single file was passed
        # and that it has the correct name
        assert len(output_records) == 1
        assert output_records[0]['fileInfo']['file'] == f'/{directory_name}/{object_name}'
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


@pytest.mark.parametrize('threads', [5, 15])
def test_multithreading(sdc_builder, sdc_executor, azure, threads):
    """The connector will create multiple threads to consume from the queue, but the queue spooler needs to be
    single threaded always. Creating multiple dirs and subdirs with equal and different keys

    Pipeline:
        azure_data_lake_origin >> wiretap
    """
    fs = azure.datalake.file_system

    directory_name = '{}'.format(get_random_string(string.ascii_letters, 10))
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

    try:
        # Create the directory tree and populate with files. The content of each file is just the filename.
        # we create files with different names in the different folders (number of files in expected_walk)
        expected_output = []
        fs.mkdir(directory_name)

        # we will do the same in more initial paths, to create more files
        initial_path = ['1', '2', '3', '4', '5']
        for starting_point in initial_path:
            for path, data in subdirs_data.items():
                if path != '.':
                    fs.mkdir(os.path.join(directory_name, starting_point, path))
                filepath = os.path.join(directory_name, starting_point, path, 'file.txt')
                fs.touch(filepath)
                fs.write(filepath, filepath + data)
                expected_output += [filepath + data]
        time.sleep(5)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}',
                                              number_of_threads=threads)

        if Version(sdc_builder.version) >= Version('5.7.0'):
            azure_data_lake_origin.file_processing_delay_in_ms = 1000

        wiretap = builder.add_wiretap()

        azure_data_lake_origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(expected_output))
        sdc_executor.stop_pipeline(pipeline)

        actual_output = [record.field['text'].value for record in wiretap.output_records]

        # Verify all the expected records were produced by the origin.
        assert sorted(actual_output) == sorted(expected_output)
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


def test_resume_offset(sdc_builder, sdc_executor, azure):
    """
    Create a pipeline that reads half the data on a Data Lake Storage origin. Stop the pipeline, start it again and
    test that it resumes from the correct offset.

    The pipeline looks like:
        azure_data_lake_origin >> wiretap
    """
    fs = azure.datalake.file_system
    directory_name = get_random_string(string.ascii_letters, 10)
    object_name = get_random_string(string.ascii_letters, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    max_batch_size = 100
    number_of_batches = 10
    half_batches = 5

    azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='TEXT',
                                          common_path=f'/{directory_name}',
                                          batch_wait_time_in_ms=20_000,
                                          max_batch_size_in_records=max_batch_size)

    if Version(sdc_builder.version) >= Version('5.7.0'):
        azure_data_lake_origin.file_processing_delay_in_ms = 1000

    wiretap = pipeline_builder.add_wiretap()
    azure_data_lake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)

    try:
        data1 = [get_random_string(string.ascii_letters, 100) for _ in range(max_batch_size * half_batches)]
        fs.mkdir(directory_name)
        fs.touch(f'{directory_name}/{object_name}')
        fs.write(f'{directory_name}/{object_name}', '\n'.join(msg for msg in data1))
        time.sleep(5)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', max_batch_size * half_batches)
        sdc_executor.stop_pipeline(pipeline)

        first_data_half = [record.field['text'] for record in wiretap.output_records]
        wiretap.reset()

        data2 = [get_random_string(string.ascii_letters, 100) for _ in range(max_batch_size * half_batches)]
        fs.mkdir(directory_name)
        fs.touch(f'{directory_name}/{object_name}')
        fs.write(f'{directory_name}/{object_name}', '\n'.join(msg for msg in data2))

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', max_batch_size * half_batches)
        sdc_executor.stop_pipeline(pipeline)

        second_data_half = [record.field['text'] for record in wiretap.output_records]

        joined_result = first_data_half + second_data_half

        assert len(first_data_half) + len(second_data_half) == max_batch_size * number_of_batches
        assert len(first_data_half) + len(second_data_half) == len(data1) + len(data2)
        assert joined_result == data1 + data2
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


def test_empty_objects(sdc_builder, sdc_executor, azure):
    """
    Test that no records are generated if the origin is empty.

    The pipeline looks like:
        azure_data_lake_origin >> wiretap
    """
    fs = azure.datalake.file_system
    directory_name = get_random_string(string.ascii_letters, 10)
    object_name = get_random_string(string.ascii_letters, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='TEXT',
                                          common_path=f'/{directory_name}')
    wiretap = pipeline_builder.add_wiretap()

    azure_data_lake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        fs.mkdir(directory_name)
        fs.touch(f'{directory_name}/{object_name}')

        sdc_executor.start_pipeline(pipeline)
        time.sleep(30)
        sdc_executor.stop_pipeline(pipeline)

        assert 0 == len(wiretap.output_records)
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


def test_empty_folder(sdc_builder, sdc_executor, azure):
    """
    Test that no records are generated if the origin is empty.

    The pipeline looks like:
        azure_data_lake_origin >> wiretap
    """
    fs = azure.datalake.file_system
    directory_name = get_random_string(string.ascii_letters, 10)
    pipeline_builder = sdc_builder.get_pipeline_builder()

    azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='WHOLE_FILE',
                                          common_path=f'/{directory_name}')
    wiretap = pipeline_builder.add_wiretap()

    azure_data_lake_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        fs.mkdir(directory_name)

        sdc_executor.start_pipeline(pipeline)
        time.sleep(30)
        sdc_executor.stop_pipeline(pipeline)

        assert 0 == len(wiretap.output_records)
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)
