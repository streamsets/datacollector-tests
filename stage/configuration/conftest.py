import json
import textwrap

import pytest
from streamsets.sdk.models import Configuration

FILE_WRITER_SCRIPT = """
    file_contents = '''{file_contents}'''
    for record in records:
        with open('{filepath}', 'w') as f:
            f.write(file_contents.decode('utf8').encode('{encoding}'))
"""


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
    return hook


@pytest.fixture
def file_writer(sdc_executor):
    """Writes a file to SDC's local FS.

    Args:
        filepath (:obj:`str`): The absolute path to which to write the file.
        file_contents (:obj:`str`): The file contents.
        encoding (:obj:`str`, optional): The file encoding. Default: ``'utf8'``
    """
    def file_writer_(filepath, file_contents, encoding='utf8'):
        builder = sdc_executor.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='noop', stop_after_first_batch=True)
        jython_evaluator = builder.add_stage('Jython Evaluator')
        jython_evaluator.script = textwrap.dedent(FILE_WRITER_SCRIPT).format(filepath=str(filepath),
                                                                             file_contents=file_contents,
                                                                             encoding=encoding)
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> jython_evaluator >> trash
        pipeline = builder.build('File writer pipeline')

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.remove_pipeline(pipeline)
    return file_writer_


@pytest.fixture
def shell_executor(sdc_executor):
    def shell_executor_(script, environment_variables=None):
        builder = sdc_executor.get_pipeline_builder()
        dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='noop', stop_after_first_batch=True)
        shell = builder.add_stage('Shell')
        shell.set_attributes(script=script,
                             environment_variables=(Configuration(**environment_variables)._data
                                                    if environment_variables
                                                    else []))
        trash = builder.add_stage('Trash')
        dev_raw_data_source >> [trash, shell]
        pipeline = builder.build('Shell executor pipeline')

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_executor.remove_pipeline(pipeline)
    return shell_executor_

@pytest.fixture
def avro_file_writer(sdc_executor):
    def avro_file_writer_(tmp_directory, avro_records, avro_schema):
        """Here we are using dev raw data source to read the json data. Then we will use localfs stage to write
        avro file. Function requires avro records and avro schema for generating avro file.
        Pipeline : dev_raw_data_source >> local_fs.
        """
        raw_data = ''.join(json.dumps(avro_record) for avro_record in avro_records)

        pipeline_builder = sdc_executor.get_pipeline_builder()
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
        local_fs = pipeline_builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(data_format='AVRO',
                                avro_schema_location='INLINE',
                                avro_schema=json.dumps(avro_schema),
                                directory_template=tmp_directory,
                                files_prefix='sdc-${sdc:id()}', files_suffix='txt', max_records_in_file=5)

        dev_raw_data_source >> local_fs
        files_pipeline = pipeline_builder.build('Generate files pipeline')
        sdc_executor.add_pipeline(files_pipeline)

        # generate some batches/files
        sdc_executor.start_pipeline(files_pipeline).wait_for_finished(timeout_sec=5)
    return avro_file_writer_

