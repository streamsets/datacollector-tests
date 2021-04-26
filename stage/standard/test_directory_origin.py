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

import logging
import os
import string
import tempfile

import pytest
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.decorators import stub
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

NAMES = [
    ('max_size', get_random_string(string.ascii_lowercase, 255)),
    ('underscore', get_random_string(string.ascii_lowercase, 10) + '_' + get_random_string(string.ascii_lowercase, 10)),
    ('hyphen', get_random_string(string.ascii_lowercase, 10) + '-' + get_random_string(string.ascii_lowercase, 10)),
    ('plus', get_random_string(string.ascii_lowercase, 10) + '+' + get_random_string(string.ascii_lowercase, 10)),
    ('dot', get_random_string(string.ascii_lowercase, 10) + '.' + get_random_string(string.ascii_lowercase, 10)),
    ('numbers', get_random_string(string.ascii_lowercase, 10) + '1234567890'),
]


def test_data_types(sdc_builder, sdc_executor):
    pytest.skip("""No data types test for directory origin """)


@pytest.mark.parametrize('name_category,directory', NAMES, ids=[i[0] for i in NAMES])
def test_object_names_directory(sdc_builder, sdc_executor, name_category, directory):
    """ Test for different kind of valid names for directory.
        Wiretap is used to assert correctness.
        Pipeline structure: directory_origin >> trash
    """

    file_name = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'

    file_path = os.path.join(tempfile.gettempdir(), directory)
    _write_file_with_pipeline(sdc_executor, file_path, file_name, raw_str)

    builder = sdc_builder.get_pipeline_builder()
    directory_origin = builder.add_stage('Directory', type='origin')
    directory_origin.set_attributes(data_format='TEXT', file_name_pattern=f'{file_name}*',
                                    file_name_pattern_mode='GLOB',
                                    file_post_processing='DELETE', files_directory=file_path,
                                    process_subdirectories=True, read_order='TIMESTAMP')
    wiretap = builder.add_wiretap()

    directory_origin >> wiretap.destination
    directory_origin_pipeline = builder.build()
    sdc_executor.add_pipeline(directory_origin_pipeline)

    try:
        sdc_executor.start_pipeline(directory_origin_pipeline)
        sdc_executor.wait_for_pipeline_metric(directory_origin_pipeline, 'input_record_count', 1)
        _stop_pipeline(sdc_executor, directory_origin_pipeline)
        for record in wiretap.output_records:
            assert raw_str == record.field['text'].value
    finally:
        _stop_pipeline(sdc_executor, directory_origin_pipeline)


@pytest.mark.parametrize('name_category,file_name', NAMES, ids=[i[0] for i in NAMES])
def test_object_names_file(sdc_builder, sdc_executor, name_category, file_name):
    """Different kinds of valid file names are correctly processed."""
    files_directory = os.path.join('/tmp', get_random_string())
    FILE_CONTENTS = 'Hello World!'

    if name_category == 'max_size':
        # 12 characters are deleted because of /tmp/, _tmp_ and _0 suffix
        file_name = file_name[12:]
    file_path = os.path.join(files_directory, file_name)

    sdc_executor.execute_shell(f'mkdir -p {files_directory}')
    sdc_executor.write_file(file_path, FILE_CONTENTS)

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory', type='origin')
        directory.set_attributes(data_format='TEXT', file_name_pattern='*', files_directory=files_directory)
        wiretap = pipeline_builder.add_wiretap()
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

        directory >> [wiretap.destination, pipeline_finisher]
        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        output_records = [record.field['text'] for record in wiretap.output_records]
        assert output_records == [FILE_CONTENTS]
    finally:
        sdc_executor.execute_shell(f'rm -r {files_directory}')


@pytest.mark.parametrize('max_batch_size', [10, 100, 1000])
def test_multiple_batches(sdc_builder, sdc_executor, max_batch_size):
    """ Test for different size of batch.
        Wiretap is used to assert correctness.
        Two files are created.
        Pipeline structure: directory_origin >> wiretap.destination
                         directory_origin >= pipeline_finished_executor
   """
    expected_raw_str_1 = [f'Hello World{i}!' for i in range(100)]
    expected_raw_str_2 = [f'Hello World{i}!' for i in range(100, 200)]
    raw_str_1 = '\n'.join(expected_raw_str_1)
    raw_str_2 = '\n'.join(expected_raw_str_2)
    file_path = tempfile.gettempdir()
    file_name_prefix = get_random_string(string.ascii_letters, 10)
    file_name_1 = f'{file_name_prefix}_1'
    file_name_2 = f'{file_name_prefix}_2'
    _write_file_with_pipeline(sdc_executor, file_path, file_name_1, raw_str_1)
    _write_file_with_pipeline(sdc_executor, file_path, file_name_2, raw_str_2)

    builder = sdc_builder.get_pipeline_builder()
    directory_origin = builder.add_stage('Directory', type='origin')
    directory_origin.set_attributes(data_format='TEXT', file_name_pattern=f'{file_name_prefix}*',
                                    file_name_pattern_mode='GLOB',
                                    file_post_processing='DELETE', files_directory=file_path,
                                    process_subdirectories=True, read_order='TIMESTAMP',
                                    batch_size_in_recs=max_batch_size)
    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    directory_origin >> wiretap.destination
    directory_origin >= pipeline_finished_executor

    directory_origin_pipeline = builder.build()
    sdc_executor.add_pipeline(directory_origin_pipeline)

    try:
        sdc_executor.start_pipeline(directory_origin_pipeline).wait_for_finished()
        records = [record.field['text'] for record in wiretap.output_records]
        expected_raw_str = expected_raw_str_1 + expected_raw_str_2
        assert records == expected_raw_str

    finally:
        _stop_pipeline(sdc_executor, directory_origin_pipeline)


def test_dataflow_events(sdc_builder, sdc_executor):
    """ Test for different events generated by directory origin.
       Wiretap is used to assert correctness. Wiretap is connected to events flow.
       Two files are created.
       Pipeline structure:     directory_origin >> trash
                               directory_origin >= [pipeline_finished_executor, wiretap.destination]
   """

    raw_str = 'Hello World!'
    file_path = tempfile.gettempdir()
    file_name_prefix = get_random_string(string.ascii_letters, 10)
    file_name_1 = f'{file_name_prefix}_1'
    file_name_2 = f'{file_name_prefix}_2'
    _write_file_with_pipeline(sdc_executor, file_path, file_name_1, raw_str)

    builder = sdc_builder.get_pipeline_builder()
    directory_origin = builder.add_stage('Directory', type='origin')
    directory_origin.set_attributes(data_format='TEXT', file_name_pattern=f'{file_name_prefix}*',
                                    file_name_pattern_mode='GLOB',
                                    file_post_processing='DELETE', files_directory=file_path,
                                    process_subdirectories=True, read_order='TIMESTAMP',
                                    batch_size_in_recs=100)
    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])
    trash = builder.add_stage('Trash')
    directory_origin >> trash
    directory_origin >= [pipeline_finished_executor, wiretap.destination]

    directory_origin_pipeline = builder.build()
    sdc_executor.add_pipeline(directory_origin_pipeline)

    try:
        sdc_executor.start_pipeline(directory_origin_pipeline).wait_for_finished()
        _write_file_with_pipeline(sdc_executor, file_path, file_name_2, raw_str)
        sdc_executor.start_pipeline(directory_origin_pipeline).wait_for_finished()

        records = [record.header['values']['sdc.event.type'] for record in wiretap.output_records]
        expected_events = ['new-file', 'finished-file', 'no-more-data',
                           'new-file', 'finished-file', 'no-more-data']
        assert records == expected_events

    finally:
        _stop_pipeline(sdc_executor, directory_origin_pipeline)


def test_resume_offset(sdc_builder, sdc_executor):
    """ Test for stop and resume condition.
       Wiretap is used to assert correctness.
       One file is created, then processed. Pipeline is stopped.
       A second file is created and then processed.
       Pipeline structure: directory_origin >> wiretap.destination
                           directory_origin >= pipeline_finished_executor
   """
    expected_raw_str_1 = [f'Hello World{i}!' for i in range(100)]
    expected_raw_str_2 = [f'Hello World{i}!' for i in range(100, 200)]
    raw_str_1 = '\n'.join(expected_raw_str_1)
    raw_str_2 = '\n'.join(expected_raw_str_2)
    file_path = tempfile.gettempdir()
    file_name_prefix = get_random_string(string.ascii_letters, 10)
    file_name_1 = f'{file_name_prefix}_1'
    file_name_2 = f'{file_name_prefix}_2'
    _write_file_with_pipeline(sdc_executor, file_path, file_name_1, raw_str_1)

    builder = sdc_builder.get_pipeline_builder()
    directory_origin = builder.add_stage('Directory', type='origin')
    directory_origin.set_attributes(data_format='TEXT', file_name_pattern=f'{file_name_prefix}*',
                                    file_name_pattern_mode='GLOB',
                                    file_post_processing='DELETE', files_directory=file_path,
                                    process_subdirectories=True, read_order='TIMESTAMP',
                                    batch_size_in_recs=100)
    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    directory_origin >> wiretap.destination
    directory_origin >= pipeline_finished_executor

    directory_origin_pipeline = builder.build()
    sdc_executor.add_pipeline(directory_origin_pipeline)

    try:
        sdc_executor.start_pipeline(directory_origin_pipeline).wait_for_finished()
        _write_file_with_pipeline(sdc_executor, file_path, file_name_2, raw_str_2)
        sdc_executor.start_pipeline(directory_origin_pipeline).wait_for_finished()

        records = [record.field['text'] for record in wiretap.output_records]
        expected_raw_str = expected_raw_str_1 + expected_raw_str_2
        assert records == expected_raw_str

    finally:
        _stop_pipeline(sdc_executor, directory_origin_pipeline)


def test_empty_object_file(sdc_builder, sdc_executor):
    """ Test for empty file condition.
       Wiretap is used to assert no data was generated.
       One file empty file is created and then processed.
       Pipeline structure: directory_origin >> wiretap.destination
                          directory_origin >= pipeline_finished_executor
   """

    raw_str = ''
    file_path = tempfile.gettempdir()
    file_name = get_random_string(string.ascii_letters, 10)
    _write_file_with_pipeline(sdc_executor, file_path, file_name, raw_str)

    builder = sdc_builder.get_pipeline_builder()
    directory_origin = builder.add_stage('Directory', type='origin')
    directory_origin.set_attributes(data_format='TEXT', file_name_pattern=file_name,
                                    file_name_pattern_mode='GLOB',
                                    file_post_processing='DELETE', files_directory=file_path,
                                    process_subdirectories=True, read_order='TIMESTAMP',
                                    batch_size_in_recs=100)
    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    directory_origin >> wiretap.destination
    directory_origin >= pipeline_finished_executor

    directory_origin_pipeline = builder.build()
    sdc_executor.add_pipeline(directory_origin_pipeline)

    try:
        sdc_executor.start_pipeline(directory_origin_pipeline).wait_for_finished()

        records = [record.field['text'] for record in wiretap.output_records]
        expected_raw = []
        assert records == expected_raw

    finally:
        _stop_pipeline(sdc_executor, directory_origin_pipeline)

@stub
def test_data_format_avro(sdc_builder, sdc_executor):
    pass


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('csv_parser', ['UNIVOCITY', 'LEGACY_PARSER'])
def test_data_format_delimited(sdc_builder, sdc_executor, csv_parser):
    DATA = "A,B,C\n" \
           "1,2,3\n" \
           "10,20,30\n"
    work_dir = _prepare_work_dir(sdc_executor, DATA)

    # Create Pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory', type='origin')
    origin.file_name_pattern = '*.csv'
    origin.files_directory = work_dir

    origin.data_format = 'DELIMITED'
    origin.csv_parser = csv_parser
    origin.header_line = 'WITH_HEADER'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(pipeline)

    records = wiretap.output_records
    assert len(records) == 2

    assert records[0].field['A'] == "1"
    assert records[0].field['B'] == "2"
    assert records[0].field['C'] == "3"
    assert records[1].field['A'] == "10"
    assert records[1].field['B'] == "20"
    assert records[1].field['C'] == "30"


@stub
def test_data_format_excel(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_json(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_log(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_protobuf(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_text(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_sdc_record(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_whole_file(sdc_builder, sdc_executor):
    pass


@stub
def test_data_format_xml(sdc_builder, sdc_executor):
    pass


def _write_file_with_pipeline(sdc_executor, file_path, file_name, file_contents):
    """Help function to write file in the SDC - File system in directory file_path,
       with file_name name and with file_contents content.
       Pipeline structure: dev_raw_data_source >> local_fs
    """

    builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=file_contents, stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=file_path,
                            files_prefix=file_name, files_suffix='')

    dev_raw_data_source >> local_fs
    files_pipeline = builder.build(f'Generate files pipeline {file_path}')
    sdc_executor.add_pipeline(files_pipeline)

    sdc_executor.start_pipeline(files_pipeline).wait_for_finished()
    sdc_executor.remove_pipeline(files_pipeline)


def _stop_pipeline(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)

def _prepare_work_dir(sdc_executor, data):
    """Create work directory, insert test data, return the work directory."""
    work_dir = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir -p {work_dir}')
    sdc_executor.write_file(os.path.join(work_dir, 'input.csv'), data)
    return work_dir
