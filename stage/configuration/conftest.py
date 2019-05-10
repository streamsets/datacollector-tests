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
def snapshot_content():
    def snapshot_content_(snapshot, directory):
        """This is common function can be used at in may TCs to get snapshot content.
        """
        processed_data = []
        for snapshot_batch in snapshot.snapshot_batches:
            for value in snapshot_batch[directory.instance_name].output_lanes.values():
                for record in value:
                    processed_data.append(str(record.field['text']))
        return '\n'.join(processed_data)
    return snapshot_content_
