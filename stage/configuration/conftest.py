import csv
import io
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
        write_file_with_pipeline(sdc_executor, filepath, file_contents, encoding)
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


def write_file_with_pipeline(sdc_executor, filepath, file_contents, encoding='utf8'):
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


@pytest.fixture
def delimited_file_writer(sdc_executor):
    def delimited_file_writer_(filepath, file_contents_list, delimiter_format, delimiter_character, encoding='utf8'):
        delimited_file_contents = get_file_content(file_contents_list, delimiter_format, delimiter_character)
        write_file_with_pipeline(sdc_executor, filepath, delimited_file_contents, encoding)
    return delimited_file_writer_


def get_file_content(file_contents, delimiter_format, delimiter_character):
    if delimiter_format in ['EXCEL']:
        return get_excel_compatible_csv(file_contents)
    elif delimiter_format in ['POSTGRES_CSV', 'CSV']:
        return '\n'.join([','.join(t1) for t1 in file_contents])
    elif delimiter_format == 'RFC4180':
        #  As per https://tools.ietf.org/html/rfc4180 last record may or may not have line break.
        return '\n'.join([','.join(t1) for t1 in file_contents]) + '\n'
    elif delimiter_format in ['TDF', 'POSTGRES_TEXT', 'MYSQL']:
        return '\n'.join(['\t'.join(t1) for t1 in file_contents])
    elif delimiter_format in ['CUSTOM', 'POSTGRES_TEXT']:
        return '\n'.join([delimiter_character.join(t1) for t1 in file_contents])


def get_excel_compatible_csv(data):
    content = None
    queue = io.StringIO()
    try:
        data[1][1] = data[1][1] + '\nSTR'
        writer = csv.writer(queue, dialect='excel', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerows(data)
        content = queue.getvalue()
    finally:
        queue.close()
    return content


@pytest.fixture
def compressed_file_writer(sdc_executor):
    """Writes compressed file to local fs.
    To write to any file other than the compressed file make compression_format='NONE' and
    compression_codec='NONE'.
    Args:
        tmp_directory (:obj:`str`): The absolute path to which to write the file.
        local_fs_data_format: Format in which data should be generated.
        compression_format: Compression format = COMPRESSED_FILE.
        file_content (:obj:`str`): The file contents.
        compression_codec (:obj:`str`): Compression format in which we have to write the file
                                        By default GZIP file will be generated
        files_prefix (:obj:`str`): File name format to be generated.
    """
    def compressed_file_writer_(tmp_directory, local_fs_data_format, compression_format, file_content,
                                compression_codec='GZIP', files_prefix='sdc-${sdc:id()}'):
        ext_map = {'BINARY': 'bin', 'TEXT': 'txt', 'DELIMITED': 'csv', 'JSON': 'json', 'LOG': 'log',
                   'PROTOBUF': 'proto', 'SDC_JSON': 'json', 'XML': 'xml'}
        extension = ext_map[local_fs_data_format]
        dev_raw_data_source_data_format = local_fs_data_format
        if local_fs_data_format in ['LOG', 'XML']:
            local_fs_data_format = 'TEXT'
            dev_raw_data_source_data_format = 'TEXT'
        if local_fs_data_format == 'SDC_JSON':
            dev_raw_data_source_data_format = 'JSON'

        attributes = {'data_format': local_fs_data_format,
                      'compression_format': compression_format,
                      'directory_template': tmp_directory,
                      'files_prefix': files_prefix,
                      'files_suffix': extension,
                      'compression_codec': compression_codec}

        pipeline_builder = sdc_executor.get_pipeline_builder()
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format=dev_raw_data_source_data_format, raw_data=file_content,
                                           stop_after_first_batch=True)
        local_fs = pipeline_builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(**attributes)

        dev_raw_data_source >> local_fs
        files_pipeline = pipeline_builder.build('Generate files pipeline')
        sdc_executor.add_pipeline(files_pipeline)

        # generate some batches/files
        sdc_executor.start_pipeline(files_pipeline).wait_for_finished(timeout_sec=30)

    return compressed_file_writer_
