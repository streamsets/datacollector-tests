import logging
import os
from uuid import uuid4
import pytest
import textwrap

from streamsets.testframework.markers import cluster, azure, sdc_min_version

logger = logging.getLogger(__name__)


FILE_WRITER_SCRIPT = """
    file_contents = '''{file_contents}'''
    for record in records:
        with open('{filepath}', 'w') as f:
            f.write(file_contents.decode('utf8').encode('{encoding}'))
"""

FILE_WRITER_SCRIPT_BINARY = """
    with open('{filepath}', 'wb') as f:
        f.write({file_contents})
"""


@pytest.fixture
def shell_executor(sdc_executor):
    def shell_executor_(script):
        builder = sdc_executor.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='noop', stop_after_first_batch=True)
        shell = builder.add_stage('Shell')
        shell.set_attributes(script=script,
                             environment_variables=([]))
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> [trash, shell]
        pipeline = builder.build('Shell executor pipeline')

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.remove_pipeline(pipeline)
    return shell_executor_


@pytest.fixture
def file_writer(sdc_executor):
    def file_writer_(filepath, file_contents, encoding='utf8', file_data_type='NOT_BINARY'):
        builder = sdc_executor.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='noop', stop_after_first_batch=True)
        jython_evaluator = builder.add_stage('Jython Evaluator')

        file_writer_script = FILE_WRITER_SCRIPT_BINARY if file_data_type == 'BINARY' else FILE_WRITER_SCRIPT
        jython_evaluator.script = textwrap.dedent(file_writer_script).format(filepath=str(filepath),
                                                                             file_contents=file_contents,
                                                                             encoding=encoding)
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> jython_evaluator >> trash
        pipeline = builder.build('File writer pipeline')

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.remove_pipeline(pipeline)
    return file_writer_


@sdc_min_version("3.20.0")
def test_file_event_filepath_when_whole_file_mode_disabled(sdc_builder, sdc_executor, shell_executor, file_writer):
    builder = sdc_builder.get_pipeline_builder()

    data_source = builder.add_stage('Dev Raw Data Source')
    data_source.stop_after_first_batch = True
    data_source.data_format = 'DELIMITED'
    data_source.delimiter_format_type = 'CUSTOM'
    data_source.header_line = 'WITH_HEADER'
    data_source.raw_data = 'HEADER\nVALUE\n'

    fs = builder.add_stage('Local FS')
    fs.directory_template = f'/tmp/{str(uuid4())}'
    fs.files_prefix = 'sdc'
    fs.data_format = 'DELIMITED'
    fs.delimiter_format = 'CUSTOM'
    fs.header_line = 'WITH_HEADER'

    trash = builder.add_stage('Trash')

    data_source >> fs
    fs >= trash

    pipeline = builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        shell_executor(f'mkdir -p {fs.directory_template}')
        file_writer(os.path.join(fs.directory_template, '_tmp_sdc_0'),
                    'HEADER\nDATA\n')

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True).snapshot
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('FINISHED')
        stage = snapshot[fs.instance_name]

        assert len(stage.event_records) > 0
        for record in stage.event_records:
            assert record.get_field_data('/filepath').value.startswith(f'{fs.directory_template}/sdc_')

    finally:
        shell_executor(f'rm -fr {fs.directory_template}')


def test_file_event_filepath_when_whole_file_mode_enabled(
        sdc_builder, sdc_executor, shell_executor, file_writer):

    base_folder = f'/tmp/{str(uuid4())}'

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

    dst = builder.add_stage('Trash')

    src >> fs
    fs >= dst

    pipeline = builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        shell_executor(f'mkdir -p {src.files_directory}')
        shell_executor(f'mkdir -p {fs.directory_template}')

        file_writer(os.path.join(src.files_directory, 'input.txt'), 'HEADER\nVALUE\n')
        file_writer(os.path.join(fs.directory_template, '_tmp_sdc-output'), 'HEADER\nDATA\n')

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline,
                                                 start_pipeline=True, batches=1).snapshot
        sdc_executor.stop_pipeline(pipeline, force=True)
        stage = snapshot[fs.instance_name]

        assert len(stage.event_records) > 0
        for record in stage.event_records:
            assert record.get_field_data('/targetFileInfo/path').value == f'{fs.directory_template}/sdc-output'
            assert record.get_field_data('/sourceFileInfo/file').value == f'{src.files_directory}/input.txt'

    finally:
        shell_executor(f'rm -fr {base_folder}')
