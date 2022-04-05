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

from streamsets.testframework.markers import cluster, azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@sdc_min_version("3.19.0")
def test_file_event_filepath_when_whole_file_mode_disabled(sdc_builder, sdc_executor):
    """
    We want to make sure that the filepath attribute of an event generated by the LocalFS destination contains only
    a file path and doesn't contain a schema. A misbehaviour normally happened before when an old file was found
    during a pipelined start-up. This wasn't observed for events that were sent for files created during pipeline
    execution. The goal is to make the behaviour consistent, and since the name of the attribute is the filepath
    not URI we decided to remove the schema part from it.

    Pipeline:
              Dev Raw Data Source >> Local FS >= Wiretap

    When the pipeline stops we assert the filepath attribute of the event generate by LocalFS.
    """

    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source')
    data_source.stop_after_first_batch = True
    data_source.data_format = 'DELIMITED'
    data_source.delimiter_format_type = 'CUSTOM'
    data_source.header_line = 'WITH_HEADER'
    data_source.raw_data = 'HEADER\nVALUE\n'

    fs = builder.add_stage('Local FS')
    fs.directory_template = f'/tmp/{get_random_string(string.ascii_letters)}'
    fs.files_prefix = 'sdc'
    fs.data_format = 'DELIMITED'
    fs.delimiter_format = 'CUSTOM'
    fs.header_line = 'WITH_HEADER'

    wiretap = builder.add_wiretap()

    data_source >> fs >= wiretap.destination

    pipeline = builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.execute_shell(f'mkdir -p {fs.directory_template}')
        sdc_executor.write_file(os.path.join(fs.directory_template, '_tmp_sdc_0'), 'HEADER\nDATA\n')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        history = sdc_executor.get_pipeline_history(pipeline)
        pipeline_record_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        stage_record_count = len(wiretap.output_records)

        assert stage_record_count == 2
        assert pipeline_record_count == 3
        for event_record in wiretap.output_records:
            assert event_record.get_field_data('/filepath').value.startswith(f'{fs.directory_template}/sdc_')

    finally:
        sdc_executor.execute_shell(f'rm -fr {fs.directory_template}')


def test_file_event_filepath_when_whole_file_mode_enabled(sdc_builder, sdc_executor):
    """
    We want to make sure that the /targetFileInfo/path and /sourceFileInfo/file attributes of the event generated by
    the LocalFS destination in the WHOLE_FILE mode contains only a file path and doesn't contain schema.
    A misbehaviour was not observed before for the WHOLE_FILE mode. The goal is to make sure the behaviour
    is not broken by the fix.

    Pipeline:
              Directory >> Local FS >= Trash

    Before starting the pipeline we put a file to the source directory, this is the new file.
    We also put a file to the destination folder, this is the "old" file.
    When the pipeline stops we assert the /targetFileInfo/path and /sourceFileInfo/file attributes of the event
    generated by LocalFS.
    """

    base_folder = f'/tmp/{get_random_string(string.ascii_letters)}'

    builder = sdc_builder.get_pipeline_builder()

    src = builder.add_stage('Directory')
    src.files_directory = f'{base_folder}/input'
    src.file_name_pattern = '*'
    src.data_format = 'WHOLE_FILE'
    src.batch_size_in_recs = 1
    src.batch_wait_time_in_secs = 1

    fs = builder.add_stage('Local FS')
    fs.directory_template = f'{base_folder}/output'
    fs.files_prefix = 'sdc'
    fs.file_name_expression = '-output'
    fs.data_format = 'WHOLE_FILE'
    fs.file_type = 'WHOLE_FILE'

    wiretap = builder.add_wiretap()

    src >> fs >= wiretap.destination

    pipeline = builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.execute_shell(f'mkdir -p {src.files_directory}')
        sdc_executor.execute_shell(f'mkdir -p {fs.directory_template}')

        sdc_executor.write_file(os.path.join(src.files_directory, 'input.txt'), 'HEADER\nVALUE\n')
        sdc_executor.write_file(os.path.join(fs.directory_template, '_tmp_sdc-output'), 'HEADER\nDATA\n')

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)
        history = sdc_executor.get_pipeline_history(pipeline)
        pipeline_record_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        stage_record_count = len(wiretap.output_records)

        assert stage_record_count == 1
        assert pipeline_record_count == 3
        for event_record in wiretap.output_records:
            assert event_record.get_field_data('/targetFileInfo/path').value == f'{fs.directory_template}/sdc-output'
            assert event_record.get_field_data('/sourceFileInfo/file').value == f'{src.files_directory}/input.txt'

    finally:
        sdc_executor.execute_shell(f'rm -fr {base_folder}')


@sdc_min_version('5.0.0')
def test_file_ref_from_event(sdc_builder, sdc_executor):
    """Test that Whole File Transformer is able to process file reference in a CLOSED_FILE event as a serialized FileRef.
    """

    pipeline_name = f'FileRef in Event - {get_random_string(string.ascii_letters, 10)}'

    directory_template = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 16))

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(stop_after_first_batch=True,
                                       data_format='JSON',
                                       raw_data='{"text": "In a hole in the ground there lived a hobbit."}')

    schema_generator = pipeline_builder.add_stage('Schema Generator')
    schema_generator.set_attributes(schema_name='tolkien')

    local_fs_main = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs_main.set_attributes(data_format='AVRO',
                                 avro_schema_location = 'HEADER')

    whole_file_transformer = pipeline_builder.add_stage('Whole File Transformer')
    whole_file_transformer.set_attributes(job_type='AVRO_PARQUET')

    local_fs_event = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs_event.set_attributes(directory_template=directory_template,
                                  data_format='WHOLE_FILE',
                                  file_name_expression='${record:value(\'/fileInfo/filename\')}',
                                  file_exists='OVERWRITE')

    dev_raw_data_source >> schema_generator >> local_fs_main >= whole_file_transformer
    whole_file_transformer >> local_fs_event

    pipeline = pipeline_builder.build(pipeline_name)

    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_status('FINISHED')

    local_fs_destination_created_files_count = sdc_executor.execute_shell(f'ls {directory_template} | wc -l').stdout
    if local_fs_destination_created_files_count is None:
        raise Exception('Void records count. Probably your Log4J layout is not standard.')
    if local_fs_destination_created_files_count == '':
        raise Exception('Empty records count. Probably your Log4J layout is not standard.')
    local_fs_destination_created_files_total = int(local_fs_destination_created_files_count)
    assert local_fs_destination_created_files_total == 1, f'Expected 1 file, and counted {local_fs_destination_created_files_total}'
