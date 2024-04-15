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
import tempfile

import avro
import avro.schema
import pytest
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import pyarrow as pa
import pyarrow.parquet as pq
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [azure('datalake'), sdc_min_version('5.5.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_origin_client_datalake_DataLakeStorageGen2DSource'
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


@pytest.mark.parametrize('read_order', ['LEXICOGRAPHICAL', 'TIMESTAMP'])
def test_data_lake_origin(sdc_builder, sdc_executor, azure, read_order):
    """ Test for Data Lake origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Data Lake Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The pipeline looks like:

    azure_data_lake_origin >> wiretap
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
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}',
                                              read_order=read_order)

        azure_data_lake_origin >> wiretap.destination

        data_lake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(data_lake_origin_pipeline)

        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline)
        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_data_lake_origin_with_avro(sdc_builder, sdc_executor, azure):
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
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage(name=STAGE_NAME)
        origin.set_attributes(data_format='AVRO',
                              avro_schema_location='SOURCE',
                              common_path=f'/{directory_name}')


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


@sdc_min_version('5.8.0')
def test_data_lake_origin_with_parquet(sdc_builder, sdc_executor, azure):
    """Ensure that the origin can properly read Parquet document."""
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = get_random_string(string.ascii_letters, 10)
    file = f'{directory_name}/{file_name}.parquet'
    total_records = 1000

    # create raw data
    data = []
    for id in range(total_records):
        data.append({"id": id, "text": get_random_string()})

    # build parquet data
    schema = pa.schema([('id', pa.uint32()), ('text', pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([record[key] for record in data]) for key in data[0].keys()],
        names=schema.names
    )
    table = pa.Table.from_batches([batch])

    dl_fs = azure.datalake.file_system
    try:
        # write parquet data to file and upload to ADLS
        dl_fs.mkdir(directory_name)
        dl_fs.touch(file)
        with tempfile.NamedTemporaryFile(mode='r+b') as fd:
            pq.write_table(table, fd.name)
            fd.flush()
            fd.seek(0)
            dl_fs.write(file, fd.read(), content_type='application/octet-stream')

        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage(name=STAGE_NAME)
        origin.set_attributes(data_format='PARQUET',
                              common_path=f'/{directory_name}')

        wiretap = builder.add_wiretap()
        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', total_records, timeout_sec=60)
        output_records = wiretap.output_records
        assert len(output_records) == len(data), 'Wrong number of records!'
        for out_record, record in zip(output_records, data):
            assert out_record.field == record, 'Wrong record!'

    finally:
        if pipeline:
            if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
                sdc_executor.stop_pipeline(pipeline)
            sdc_executor.remove_pipeline(pipeline)
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_parse_timestamp_data_lake_origin(sdc_builder, sdc_executor, azure):
    """ Test for time creation file in Data Lake origin stage. We do so by creating and modificating a file in
    Azure Data Lake Storage using the STF client, then reading last modification date from the file using the Blob
    Storage Origin Stage, to assert file's modification time ingested by the pipeline is the expected time from the
    moment the file was created. Assert the difference and discard the difference if they fluctuate in less than ten
    second.
    The pipeline looks like:

    azure_data_lake_origin >> wiretap
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
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        wiretap = builder.add_wiretap()
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}',
                                              include_metadata=True)

        azure_data_lake_origin >> wiretap.destination

        data_lake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(data_lake_origin_pipeline)

        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline)

        timestamp = wiretap.output_records[0].header.values["mtime"]
        time_modification_time_obtained = int(timestamp[:-3])

        assert abs(time_modification_time - time_modification_time_obtained) <= 10

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@sdc_min_version('5.7.0')
def test_data_lake_origin_offset_continuation_token(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Data Lake Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The pipeline looks like:

    azure_data_lake_origin >> wiretap

    We stop the pipeline and check the offset worked.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    path_parts = sorted([get_random_string(string.ascii_letters, 10) for _ in range(0, 100)])

    # Put files in the azure storage file system
    dl_fs = azure.datalake.file_system
    try:
        dl_fs.mkdir(directory_name)
        for path_part in path_parts[:80]:
            filepath = os.path.join(directory_name, str(path_part), 'file.txt')
            dl_fs.touch(filepath)
            dl_fs.write(filepath, path_part)
        time.sleep(5)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}',
                                              object_pool_size=10,
                                              max_results_per_page=5,
                                              number_of_threads=5,
                                              file_processing_delay_in_ms=1000)

        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(react_to_events=True)

        azure_data_lake_origin >> wiretap.destination
        azure_data_lake_origin >= finisher

        data_lake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(data_lake_origin_pipeline)

        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', 10, timeout_sec=120)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline)
        output_records_1 = [record.field['text'] for record in wiretap.output_records]

        wiretap.reset()
        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', 10, timeout_sec=120)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline)
        output_records_2 = [record.field['text'] for record in wiretap.output_records]

        for path_part in path_parts[80:]:
            filepath = os.path.join(directory_name, str(path_part), 'file.txt')
            dl_fs.touch(filepath)
            dl_fs.write(filepath, path_part)
        time.sleep(5)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        wiretap.reset()
        sdc_executor.start_pipeline(data_lake_origin_pipeline).wait_for_finished()
        output_records_3 = [record.field['text'] for record in wiretap.output_records]

        output_records = output_records_1 + output_records_2 + output_records_3
        assert path_parts == sorted(output_records)
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_data_lake_origin_stop_go(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Data Lake Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The pipeline looks like:

    azure_data_lake_origin >> wiretap

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
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}')

        azure_data_lake_origin >> wiretap.destination

        data_lake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(data_lake_origin_pipeline)

        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline)
        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

        messages = [f'message{i}' for i in range(11, 20)]
        file_name = 'test-data-2.txt'

        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Put files in the azure storage file system
        dl_fs = azure.datalake.file_system

        wiretap.reset()
        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', total_records)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline)

        output_records = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


def test_data_lake_origin_events(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Data Lake Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file.
    The origin produce events. A pipeline finisher is connected to the origin. It stops the pipeline.
    We assert the events are the expected ones.
    The pipeline looks like:

    azure_data_lake_origin >> wiretap
    azure_data_lake_origin >= [finisher, wiretap]

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
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        wiretap_events = builder.add_wiretap()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}')

        pipeline_finisher_executor = builder.add_stage('Pipeline Finisher Executor')
        pipeline_finisher_executor.set_attributes(
            stage_record_preconditions=["${record:eventType() == 'finished-file'}"])

        azure_data_lake_origin >> wiretap.destination
        azure_data_lake_origin >= [pipeline_finisher_executor, wiretap_events.destination]

        data_lake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(data_lake_origin_pipeline)

        sdc_executor.start_pipeline(data_lake_origin_pipeline).wait_for_finished()
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


def test_data_lake_origin_resume_offset(sdc_builder, sdc_executor, azure):
    """ Test for Data Lake origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Data Lake Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file. We then create more data, restart the pipeline, and read from wiretap to
    ensure that the stage properly resumes from where the offset left off. The pipeline looks like:
    The pipeline looks like:

    azure_data_lake_origin >> wiretap
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
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}')

        azure_data_lake_origin >> wiretap.destination

        data_lake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(data_lake_origin_pipeline)

        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', total_records_1)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline, wait=True)
        output_records = [record.field['text']
                          for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records

        # Try adding the second file and resuming from the offset
        dl_fs.touch(f'{directory_name}/{file2_name}')
        dl_fs.write(f'{directory_name}/{file2_name}', '\n'.join(msg for msg in messages2))
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        wiretap.reset()
        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', total_records_2)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline, wait=False)
        output_records = [record.field['text']
                          for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages2 == output_records
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@sdc_min_version('5.9.0')
@pytest.mark.parametrize('initial_threads', [1, 10])
def test_data_lake_origin_resume_parser_offset(sdc_builder, sdc_executor, azure, initial_threads):
    """ Test for Data Lake origin stage. We do so by creating a file in Azure Data Lake Storage using the
    STF client, then reading the file using the Data Lake Origin Stage, to assert data ingested by the pipeline
    is the expected data from the file. The pipeline looks like:
    The pipeline looks like:

    azure_data_lake_origin >> wiretap
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    file_name = 'test-data.txt'
    messages = [f'message{i}' for i in range(0, 2000)]

    # Put files in the azure storage file system
    dl_fs = azure.datalake.file_system
    try:
        dl_fs.mkdir(directory_name)
        dl_fs.touch(f'{directory_name}/{file_name}')
        dl_fs.write(f'{directory_name}/{file_name}', '\n'.join(msg for msg in messages))
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        wiretap = builder.add_wiretap()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              max_batch_size_in_records=100,
                                              number_of_threads=initial_threads,
                                              common_path=f'/{directory_name}')

        azure_data_lake_origin >> wiretap.destination

        data_lake_origin_pipeline = builder.build().configure_for_environment(azure)
        sdc_executor.add_pipeline(data_lake_origin_pipeline)

        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count', 100)
        sdc_executor.stop_pipeline(data_lake_origin_pipeline, wait=True)
        output_records_1 = [record.field['text'] for record in wiretap.output_records]

        azure_data_lake_origin.set_attributes(number_of_threads=1)

        wiretap.reset()
        sdc_executor.start_pipeline(data_lake_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(data_lake_origin_pipeline, 'input_record_count',
                                              len(messages) - len(output_records_1))
        sdc_executor.stop_pipeline(data_lake_origin_pipeline, wait=False)
        output_records_2 = [record.field['text'] for record in wiretap.output_records]

        # assert Data Lake files generated
        assert messages == output_records_1 + output_records_2, \
            f'''Number of records read {output_records_1 + output_records_2} should have been the
             same as messages written {messages}'''
    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        dl_fs.rmdir(directory_name, recursive=True)


@pytest.mark.parametrize('path_pattern', ['**', '**/*.txt', '*', '*/*', '*/*1/*', 'a/*', 'a/**', 'b/b*/**', 'c'])
def test_data_lake_origin_path_pattern(sdc_builder, sdc_executor, azure, path_pattern):
    """Test prefix pattern expressions for Data Lake origin.

    Prefix pattern (ANT pattern) takes care of the client when choosing which files to process, while common prefix is
    included in the query made to Data Lake to choose which files to retrieve. Common prefix does not allow
    expressions (because the Azure API does not allow it), so by combining both prefixes we can have expressions
    without retrieving everything from Azure.

    Pipeline:
        azure_data_lake_origin >> wiretap
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
        if path in expected_walk[path_pattern]:
            expected_output += [data]
    time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

    try:
        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{rootdir}',
                                              path_pattern=path_pattern)

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
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', rootdir)
        fs.rmdir(rootdir, recursive=True)


@pytest.mark.parametrize('action', ['ARCHIVE', 'DELETE'])
def test_file_postprocessing(sdc_builder, sdc_executor, azure, action):
    """Test file post-processing functionality in Data Lake Origin.

    The test creates a directory tree and populates it with files. Then it checks the files are ingested by
    the pipeline and post-processed accordingly (either removing the file from ADLS when 'DELETE' is
    configured or moving the files to the archive directory when 'ARCHIVE' is configured).

    Pipeline:  azure_data_lake_origin >> trash

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
        time.sleep(15)  # we are waiting for filesystem consistency, as we are retrieving files lexicographically

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{rootdir}',
                                              post_processing_option=action,
                                              post_processing_path=f'/{archive_dir}',
                                              error_handling_option='DELETE')

        trash = builder.add_stage('Trash')
        azure_data_lake_origin >> trash

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


@sdc_min_version('5.7.0')
@pytest.mark.parametrize(
    'num_records, num_reading_threads, file_processing_delay, batch_wait_time', [
        (150, 1, 1000, 2000),
        (300, 10, 10000, 15000),
        (1000, 10, 10000, 1000),
        (2000, 10, 3000, 1000)
    ])
def test_multithreading_multiple_batches(
        sdc_builder,
        sdc_executor,
        azure,
        num_records,
        num_reading_threads,
        file_processing_delay,
        batch_wait_time
):
    directory_name = get_random_string(string.ascii_letters, 10)
    dl_fs = azure.datalake.file_system

    builder = sdc_builder.get_pipeline_builder()
    # Use multiple threads to create the files in the Azure container to try to get files with the same timestamp
    num_writing_threads = 10
    writing_batch_size = 1
    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(
        fields_to_generate=[{'field': 'id', 'type': 'LONG_SEQUENCE'}],
        delay_between_batches=0,
        batch_size=writing_batch_size,
        records_to_be_generated=num_records,
        number_of_threads=num_writing_threads
    )

    azure_data_lake_destination = builder.add_stage(name=TARGET_STAGE_NAME)
    azure_data_lake_destination.set_attributes(data_format='JSON',
                                               directory_template=f'/{directory_name}',
                                               max_records_in_file=1)

    creation_wiretap = builder.add_wiretap()

    dev_data_generator >> [azure_data_lake_destination ,creation_wiretap.destination]

    azure_dest_pipeline = builder.build(
        title='Multi-threaded Writing Pipeline - Azure destination').configure_for_environment(azure)
    sdc_executor.add_pipeline(azure_dest_pipeline)

    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')
    azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='JSON',
                                          common_path=f'/{directory_name}',
                                          path_pattern='sdc**',
                                          max_batch_size_in_records=100,
                                          number_of_threads=num_reading_threads,
                                          file_processing_delay_in_ms=file_processing_delay,
                                          batch_wait_time_in_ms=batch_wait_time,
                                          read_order='TIMESTAMP')

    read_wiretap = builder.add_wiretap()

    azure_data_lake_origin >> read_wiretap.destination

    azure_origin_pipeline = builder.build(
        title='Multi-threaded Reading Pipeline - Azure origin').configure_for_environment(azure)
    azure_origin_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(azure_origin_pipeline)

    try:
        dl_fs.mkdir(directory_name)

        logger.info("Executing the pipeline that creates files in the Azure container ...")
        sdc_executor.start_pipeline(azure_dest_pipeline)

        logger.info("Executing the pipeline that reads files from the Azure container ...")
        sdc_executor.start_pipeline(azure_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(azure_origin_pipeline, 'input_record_count', num_records, timeout_sec=500)

        num_records_read = len(read_wiretap.output_records)
        assert num_records_read == num_records, \
            f'{num_records} files should have been read from the Azure container, but only {num_records_read} were read'

        records_created = [record.field['id'] for record in creation_wiretap.output_records]
        records_read = [record.field['id'] for record in read_wiretap.output_records]
        assert sorted(records_created) == sorted(records_read)
    finally:
        if sdc_executor.get_pipeline_status(azure_dest_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(azure_dest_pipeline)
        if sdc_executor.get_pipeline_status(azure_origin_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(azure_origin_pipeline)
        dl_fs.rmdir(directory_name, recursive=True)


@sdc_min_version('5.11.0')
def test_data_lake_large_parquet_file(sdc_builder, sdc_executor, azure):
    # This tests checks that big Parquet files can be correctly read from Azure. For the test we need a file with a few MBs,
    # 50k records with 12 double fields each amount to a file of about 5MB which is enough for the test.
    directory_name = get_random_string(string.ascii_letters, 10)
    dl_fs = azure.datalake.file_system

    builder = sdc_builder.get_pipeline_builder()
    # Use multiple threads to create the files in the Azure container to try to get files with the same timestamp
    num_writing_threads = 10
    num_records = 50000
    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(
        fields_to_generate=[{'field': 'id', 'type': 'LONG_SEQUENCE'},
                            {'field': 'double1', 'type': 'DOUBLE'},
                            {'field': 'double2', 'type': 'DOUBLE'},
                            {'field': 'double3', 'type': 'DOUBLE'},
                            {'field': 'double4', 'type': 'DOUBLE'},
                            {'field': 'double5', 'type': 'DOUBLE'},
                            {'field': 'double6', 'type': 'DOUBLE'},
                            {'field': 'double7', 'type': 'DOUBLE'},
                            {'field': 'double8', 'type': 'DOUBLE'},
                            {'field': 'double9', 'type': 'DOUBLE'},
                            {'field': 'double10', 'type': 'DOUBLE'},
                            {'field': 'double11', 'type': 'DOUBLE'},
                            {'field': 'double12', 'type': 'DOUBLE'}
                            ],
        batch_size=num_records,
        records_to_be_generated=num_records,
        number_of_threads=num_writing_threads
    )

    azure_data_lake_destination = builder.add_stage(name=TARGET_STAGE_NAME)
    azure_data_lake_destination.set_attributes(data_format='PARQUET',
                                               directory_template=f'/{directory_name}',
                                               max_records_in_file=num_records)

    creation_wiretap = builder.add_wiretap()

    dev_data_generator >> [azure_data_lake_destination, creation_wiretap.destination]

    azure_dest_pipeline = builder.build(
        title='Multi-threaded Writing Pipeline - Azure destination').configure_for_environment(azure)
    sdc_executor.add_pipeline(azure_dest_pipeline)

    builder = sdc_builder.get_pipeline_builder()
    read_wiretap = builder.add_wiretap()
    azure_data_lake_origin = builder.add_stage(name=STAGE_NAME)
    azure_data_lake_origin.set_attributes(data_format='PARQUET',
                                          common_path=f'/{directory_name}')

    azure_data_lake_origin >> read_wiretap.destination

    azure_origin_pipeline = builder.build().configure_for_environment(azure)
    sdc_executor.add_pipeline(azure_origin_pipeline)



    try:
        logger.info("Creating Azure destination directory: %s", directory_name)
        dl_fs.mkdir(directory_name)

        logger.info("Executing the pipeline that creates a large file in Azure...")
        sdc_executor.start_pipeline(azure_dest_pipeline).wait_for_finished(timeout_sec=300)

        logger.info("Executing the pipeline that reads the file from the Azure container ...")
        sdc_executor.start_pipeline(azure_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(azure_origin_pipeline, 'input_record_count', num_records, timeout_sec=300)
        sdc_executor.stop_pipeline(azure_origin_pipeline)

        num_records_read = len(read_wiretap.output_records)
        assert num_records_read == num_records, \
            f'{num_records} should have been read from the Azure container, but only {num_records_read} were read'

        records_created = [{'id': record.field['id'],
                            'double1': record.field['double1'],
                            'double2': record.field['double2'],
                            'double3': record.field['double3'],
                            'double4': record.field['double4'],
                            'double5': record.field['double5'],
                            'double6': record.field['double6'],
                            'double7': record.field['double7'],
                            'double8': record.field['double8'],
                            'double9': record.field['double9'],
                            'double10': record.field['double10'],
                            'double11': record.field['double11'],
                            'double12': record.field['double12']}
                           for record in creation_wiretap.output_records]
        records_read = [{'id' : record.field['id'],
                         'double1': record.field['double1'],
                         'double2': record.field['double2'],
                         'double3': record.field['double3'],
                         'double4': record.field['double4'],
                         'double5': record.field['double5'],
                         'double6': record.field['double6'],
                         'double7': record.field['double7'],
                         'double8': record.field['double8'],
                         'double9': record.field['double9'],
                         'double10': record.field['double10'],
                         'double11': record.field['double11'],
                         'double12': record.field['double12']}
                         for record in read_wiretap.output_records]

        created_set = {tuple(sorted(record.items())) for record in records_created}
        read_set = {tuple(sorted(record.items())) for record in records_read}

        assert created_set == read_set

    finally:
        logger.info("Deleting directory %s ...", directory_name)
        try:
            dl_fs.rmdir(directory_name, recursive=True)
            logger.info("Directory deleted successfully")
        except FileNotFoundError:
            logger.info("Directory does not exist")
        except Exception as e:
            logger.info("An error occurred when deleting the directory:", e)



def _adls_create_file(adls_client, file_content, file_path):
    """Create a file in ADLS with the specified content.  If the file already exist, overrite content with
    `file_content`.

    """
    res1 = adls_client.touch(file_path)
    res2 = adls_client.write(file_path, file_content)
    if not (res1.response.ok and res2.response.ok):
        raise RuntimeError(f'Could not create file: {file_path}')
