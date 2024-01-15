# Copyright 2017 StreamSets Inc.
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

import csv
import datetime
import json
import logging
import os
import random
import re
import string
import tempfile
import textwrap
import time
from uuid import uuid4

import pytest

from streamsets.sdk.exceptions import StartError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

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
        file_data_type (:obj:`str`, optional): The file which type of data containing . Default: ``'NOT_BINARY'``
    """
    def file_writer_(filepath, file_contents, encoding='utf8', file_data_type='NOT_BINARY'):
        write_file_with_pipeline(sdc_executor, filepath, file_contents, encoding, file_data_type)
    return file_writer_


def write_file_with_pipeline(sdc_executor, filepath, file_contents, encoding='utf8', file_data_type='NOT_BINARY'):
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
def list_dir(sdc_executor):
    def list_dir_(data_format, files_directory, file_name_pattern, recursive=True, batches=1, batch_size=10):
        builder = sdc_executor.get_pipeline_builder()
        directory = builder.add_stage('Directory', type='origin')
        directory.set_attributes(data_format=data_format,
                                 file_name_pattern=file_name_pattern,
                                 file_name_pattern_mode='GLOB',
                                 files_directory=files_directory,
                                 process_subdirectories=recursive)
        if Version(sdc_executor.version) >= Version('5.9.0'):
            directory.set_attributes(file_processing_delay_in_ms=0)

        trash = builder.add_stage('Trash')
        events_wiretap = builder.add_wiretap()

        pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
        pipeline_finisher.set_attributes(preconditions=['${record:eventType() == \'no-more-data\'}'],
                                         on_record_error='DISCARD')

        directory >> trash
        directory >= [events_wiretap.destination, pipeline_finisher]

        pipeline = builder.build('List dir pipeline')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        files = [str(record.field['filepath']) for record in events_wiretap.output_records
                 if record.header.values['sdc.event.type'] == 'new-file']

        sdc_executor.remove_pipeline(pipeline)

        return files
    return list_dir_


# pylint: disable=pointless-statement, too-many-locals


def test_directory_origin(sdc_builder, sdc_executor):
    """Test Directory Origin. We test by making sure files are pre-created
     and then have the Directory Origin read those files. The pipelines looks like:

        directory >> wiretap

    """
    raw_data = 'Hello!'
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string())
    sub_tmp_directory1 = os.path.join(tmp_directory, get_random_string())
    sub_tmp_directory2 = os.path.join(tmp_directory, get_random_string())

    sdc_executor.execute_shell(f'mkdir -p {sub_tmp_directory1}')

    try:
        sdc_executor.write_file(os.path.join(sub_tmp_directory1, 'sdc1.txt'), raw_data)
        sdc_executor.write_file(os.path.join(sub_tmp_directory1, 'sdc2.txt'), raw_data)

        sdc_executor.execute_shell(f'mkdir -p {sub_tmp_directory2}')
        sdc_executor.write_file(os.path.join(sub_tmp_directory2, 'sdc1.txt'), raw_data)
        sdc_executor.write_file(os.path.join(sub_tmp_directory2, 'sdc2.txt'), raw_data)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory', type='origin')
        directory.set_attributes(data_format='TEXT',
                                 file_name_pattern='sdc*.txt',
                                 file_name_pattern_mode='GLOB',
                                 file_post_processing='DELETE',
                                 files_directory=tmp_directory,
                                 process_subdirectories=True,
                                 read_order='TIMESTAMP')
        if Version(sdc_builder.version) >= Version('5.9.0'):
            directory.set_attributes(file_processing_delay_in_ms=0)

        wiretap = pipeline_builder.add_wiretap()
        directory >> wiretap.destination
        directory_pipeline = pipeline_builder.build('Directory Origin pipeline')
        sdc_executor.add_pipeline(directory_pipeline)

        cmd = sdc_executor.start_pipeline(directory_pipeline)
        cmd.wait_for_pipeline_batch_count(5)
        sdc_executor.stop_pipeline(directory_pipeline)

        # assert all the data captured have the same raw_data
        output = [record.field['text'].value for record in wiretap.output_records]
        assert len(output) == 4
        for record in output:
            assert raw_data == record
        for record in wiretap.output_records:
            assert record.header['sourceId'] is not None
            assert record.header['stageCreator'] is not None
    finally:
        sdc_executor.execute_shell(f'rm -fr {tmp_directory}')


@pytest.mark.parametrize('no_of_threads', [1, 5])
@sdc_min_version('3.1.0.0')
def test_directory_origin_order_by_timestamp(sdc_builder, sdc_executor, no_of_threads):
    """Test Directory Origin. We make sure we covered race condition
    when directory origin is configured order by last modified timestamp.
    The default wait time for directory spooler is 5 seconds,
    when the files are modified between 5 seconds make sure all files are processed.
    The pipelines looks like:

        dev_raw_data_source >> local_fs
        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # 1st pipeline which writes one record per file with interval 0.1 seconds
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      delay_between_batches=10)

    dev_data_generator.fields_to_generate = [{'field': 'text', 'precision': 10, 'scale': 2, 'type': 'STRING'}]

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=os.path.join(tmp_directory),
                            files_prefix='sdc-${sdc:id()}', files_suffix='txt', max_records_in_file=1)

    dev_data_generator >> local_fs

    # run the 1st pipeline to create the directory and starting files
    files_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(files_pipeline)
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(1)
    sdc_executor.stop_pipeline(files_pipeline)

    # 2nd pipeline which reads the files using Directory Origin stage
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(batch_wait_time_in_secs=1,
                             data_format='TEXT',
                             file_name_pattern='sdc*.txt',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             number_of_threads=no_of_threads)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    trash = pipeline_builder.add_stage('Trash')
    directory >> trash

    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.start_pipeline(directory_pipeline)

    # re-run the 1st pipeline
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(files_pipeline)

    # wait till 2nd pipeline reads all files
    time.sleep(10)
    sdc_executor.stop_pipeline(directory_pipeline)

    # Validate history is as expected
    file_pipeline_history = sdc_executor.get_pipeline_history(files_pipeline)
    msgs_sent_count1 = file_pipeline_history.entries[4].metrics.counter('pipeline.batchOutputRecords.counter').count
    msgs_sent_count2 = file_pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    directory_pipeline_history = sdc_executor.get_pipeline_history(directory_pipeline)
    msgs_result_count = directory_pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    assert msgs_result_count == msgs_sent_count1 + msgs_sent_count2


@pytest.mark.parametrize('no_of_threads', [10])
@sdc_min_version('3.2.0.0')
def test_directory_origin_in_whole_file_dataformat(sdc_builder, sdc_executor, no_of_threads):
    """Test Directory Origin. We make sure multiple threads on whole data format works correct.
    The pipelines looks like:

        dev_raw_data_source >> local_fs
        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # 1st pipeline which writes one record per file with interval 0.1 seconds
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    batch_size = 100
    dev_data_generator.set_attributes(batch_size=batch_size,
                                      delay_between_batches=10,
                                      number_of_threads=no_of_threads)

    dev_data_generator.fields_to_generate = [{'field': 'text', 'precision': 10, 'scale': 2, 'type': 'STRING'}]

    max_records_in_file = 10
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=os.path.join(tmp_directory),
                            files_prefix='sdc-${sdc:id()}',
                            files_suffix='txt',
                            max_records_in_file=max_records_in_file)

    dev_data_generator >> local_fs

    number_of_batches = 5
    # run the 1st pipeline to create the directory and starting files
    files_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(files_pipeline)
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(number_of_batches)
    sdc_executor.stop_pipeline(files_pipeline)

    # get the how many records are sent
    file_pipeline_history = sdc_executor.get_pipeline_history(files_pipeline)
    msgs_sent_count = file_pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    # compute the expected number of batches to process all files
    no_of_input_files = (msgs_sent_count / max_records_in_file)

    # 2nd pipeline which reads the files using Directory Origin stage in whole data format
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(batch_wait_time_in_secs=1,
                             data_format='WHOLE_FILE',
                             max_files_in_directory=1000,
                             files_directory=tmp_directory,
                             file_name_pattern='*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             number_of_threads=no_of_threads,
                             process_subdirectories=True,
                             read_order='TIMESTAMP')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    localfs = pipeline_builder.add_stage('Local FS', type='destination')
    localfs.set_attributes(data_format='WHOLE_FILE',
                           file_name_expression='${record:attribute(\'filename\')}')

    directory >> localfs

    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.start_pipeline(directory_pipeline).wait_for_pipeline_batch_count(no_of_input_files)
    sdc_executor.stop_pipeline(directory_pipeline)

    directory_pipeline_history = sdc_executor.get_pipeline_history(directory_pipeline)
    msgs_result_count = directory_pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    assert msgs_result_count == no_of_input_files


@pytest.mark.parametrize('no_of_threads', [5])
@sdc_min_version('3.2.0.0')
def test_directory_origin_multiple_batches_no_initial_file(sdc_builder, sdc_executor, no_of_threads):
    """Test Directory Origin. We use the directory origin to read a batch of 100 files,
    after some times we will read a new batch of 100 files. No initial file configured.
    This test has been written to avoid regression, especially of issues raised in ESC-371
    The pipelines look like:

        Execute_shell 1 writes 100 files in tmp_directory
        Execute_shell 2 writes 100 files in tmp_directory_2 lexicographically greater than the files written
        by Execute_shell 1
        Pipeline 1 (Directory Origin in SDC UI): directory >> local_fs
        Pipeline 2 (tmp_directory to tmp_directory_2 in SDC UI): directory_2 >> local_fs_2

        The test works as follows:
            1) Execute_shell 1 writes files with prefix SDC1 to directory tmp_directory
            2) Pipeline 1 is started and directory origin read files from directory tmp_directory. Pipeline is
                stopped because when we read lexicographicaly, we strongly recommend to avoid modification
            3) Execute_shell 2 writes files with prefix SDC2 to directory tmp_directory_2
            4) Pipeline 2 reads files from directory tmp_directory_2 and writes them to directory tmp_directory, then
                it is stopped
            5) Pipeline 1 is started and it will read files Pipeline 2 wrote to directory tmp_directory
            6) Test checks that all the corresponding files from directory tmp_directory are read and then test ends
    """

    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    tmp_directory_2 = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    sdc_executor.execute_shell(f'mkdir {tmp_directory}')
    msgs_sent_count = 100
    sdc_executor.execute_shell(
        '\n'.join([f'echo {str(uuid4())} > {tmp_directory}/sdc1-{str(uuid4())}.txt' for _ in range(msgs_sent_count)]))

    # 2nd pipeline which reads the files using Directory Origin stage in whole data format
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(batch_wait_time_in_secs=1,
                             data_format='WHOLE_FILE',
                             max_files_in_directory=20000,
                             files_directory=tmp_directory,
                             file_name_pattern='*',
                             file_name_pattern_mode='GLOB',
                             number_of_threads=no_of_threads,
                             process_subdirectories=True,
                             read_order='LEXICOGRAPHICAL',
                             batch_size_in_recs=5,
                             file_post_processing='DELETE')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='WHOLE_FILE',
                            file_name_expression='${record:attribute(\'filename\')}')

    directory >> local_fs

    directory_pipeline = pipeline_builder.build(title='Directory Origin')
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', msgs_sent_count, timeout_sec=120)
    sdc_executor.stop_pipeline(directory_pipeline)
    history = sdc_executor.get_pipeline_history(directory_pipeline)
    msgs_result_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    assert msgs_result_count == msgs_sent_count

    sdc_executor.execute_shell(f'mkdir {tmp_directory_2}')
    msgs_sent_count_2 = 110
    sdc_executor.execute_shell(
        '\n'.join(
            [f'echo {str(uuid4())} > {tmp_directory_2}/sdc2-{str(uuid4())}.txt' for _ in range(msgs_sent_count_2)]))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory_2 = pipeline_builder.add_stage('Directory', type='origin')
    directory_2.set_attributes(batch_wait_time_in_secs=1,
                               data_format='WHOLE_FILE',
                               max_files_in_directory=20000,
                               files_directory=tmp_directory_2,
                               file_name_pattern='*',
                               file_name_pattern_mode='GLOB',
                               number_of_threads=no_of_threads,
                               process_subdirectories=True,
                               read_order='LEXICOGRAPHICAL',
                               batch_size_in_recs=5,
                               file_post_processing='DELETE')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    local_fs_2 = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs_2.set_attributes(data_format='WHOLE_FILE',
                              file_name_expression='${record:attribute(\'filename\')}',
                              directory_template=tmp_directory,
                              files_prefix='')

    directory_2 >> local_fs_2

    directory_pipeline_2 = pipeline_builder.build(title='tmp_directory to tmp_directory_2')
    sdc_executor.add_pipeline(directory_pipeline_2)
    sdc_executor.start_pipeline(directory_pipeline_2)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline_2, 'input_record_count', msgs_sent_count_2,
                                          timeout_sec=240)
    sdc_executor.stop_pipeline(directory_pipeline_2)

    # Wait until the pipeline reads all the expected files
    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', msgs_sent_count_2,
                                          timeout_sec=360)
    sdc_executor.stop_pipeline(directory_pipeline)

    history = sdc_executor.get_pipeline_history(directory_pipeline)
    msgs_result_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    assert msgs_result_count == msgs_sent_count_2


def test_directory_timestamp_ordering(sdc_builder, sdc_executor):
    """This test is mainly for SDC-10019.  The bug that was fixed there involves a race condition.  It only manifests if
    the files are ordered in increasing timestamp order and reverse alphabetical order AND the processing time required
    for a batch is sufficiently high.  That's why the pipeline is configured to write relatively large files (200k
    records, gzipped).

    Functionally, the test simply ensures that the second pipeline (with the directory origin) reads the same number of
    batches as was written by the first pipeline, and hence all data is read.  If the test times out, that essentially
    means that bug has occurred.
    """
    max_records_per_file = random.randint(100000, 300000)
    # randomize the batch size
    batch_size = random.randint(100, 5000)
    # generate enough batches to have 20 or so files
    num_batches = random.randint(15, 25) * max_records_per_file/batch_size

    random_str = get_random_string(string.ascii_letters, 10)
    tmp_directory = os.path.join(tempfile.gettempdir(), 'directory_timestamp_ordering', random_str, 'data')
    scratch_directory = os.path.join(tempfile.gettempdir(), 'directory_timestamp_ordering', random_str, 'scatch')
    logger.info('Test run information: num_batches=%d, batch_size=%d, max_records_per_file=%d, tmp_directory=%s, scratch_directory=%s',
                num_batches, batch_size, max_records_per_file, tmp_directory, scratch_directory)

    # use one pipeline to generate the .txt.gz files to be consumed by the directory pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [{'field': 'text', 'precision': 10, 'scale': 2, 'type': 'STRING'}]
    dev_data_generator.set_attributes(delay_between_batches=0, batch_size=batch_size)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}',
                            files_suffix='txt',
                            compression_codec='GZIP',
                            max_records_in_file=max_records_per_file)

    dev_data_generator >> local_fs
    shell_executor = pipeline_builder.add_stage('Shell')
    shell_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'file-closed'}"])
    shell_executor.set_attributes(environment_variables=[{'key': 'FILENAME', 'value': '${record:value(\'/filename\')}'},
                                                         {'key': 'FILEPATH', 'value': '${record:value(\'/filepath\')}'}])
    # this script will rename the completed txt.gz file to be of the form WORD_TIMESTAMP.txt.gz where WORD is chosen from
    # a reverse-alphabetical list of cycling words and TIMESTAMP is the current timestamp, and also ; this ensures that newer files
    # (i.e. those written later in the pipeline execution) will sometimes have earlier lexicographical orderings to
    # trigger SDC-10091

    shell_executor.set_attributes(script=f'''\
#!/bin/bash

if [[ ! -s {scratch_directory}/count.txt ]]; then
  echo '0' > {scratch_directory}/count.txt
fi
COUNT=$(cat {scratch_directory}/count.txt)
echo $(($COUNT+1)) > {scratch_directory}/count.txt

if [[ ! -s {scratch_directory}/words.txt ]]; then
  mkdir -p {scratch_directory}
  echo 'eggplant
dill
cucumber
broccoli
apple' > {scratch_directory}/words.txt
  WORD=fig
else
  WORD=$(head -1 {scratch_directory}/words.txt)
  grep -v $WORD {scratch_directory}/words.txt > {scratch_directory}/words_new.txt
  mv {scratch_directory}/words_new.txt {scratch_directory}/words.txt
fi

RAND_NUM=$(($RANDOM % 10))
SUBDIR="subdir${{RAND_NUM}}"
cd $(dirname $FILEPATH)
mkdir -p $SUBDIR
mv $FILENAME $SUBDIR/${{WORD}}_$COUNT.txt.gz
    ''')
    local_fs >= shell_executor
    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # generate the input files
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(num_batches)
    sdc_executor.stop_pipeline(files_pipeline)

    # create the actual directory origin pipeline, which will read the generated *.txt.gz files (across
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='TEXT',
                             file_name_pattern='*.txt.gz',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             compression_format='COMPRESSED_FILE',
                             batch_size_in_recs=batch_size)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build('Directory Origin pipeline')
    sdc_executor.add_pipeline(directory_pipeline)

    # if we set the batch size to the same value in the directory origin pipeline, it should read exactly as many batches
    # as were written by the first pipeline
    sdc_executor.start_pipeline(directory_pipeline).wait_for_pipeline_batch_count(num_batches)
    sdc_executor.stop_pipeline(directory_pipeline)


@sdc_min_version('3.0.0.0')
def test_directory_origin_avro_produce_less_file(sdc_builder, sdc_executor):
    """Test Directory Origin in Avro data format. The sample Avro file has 5 lines and
    the batch size is 1. The pipeline should produce the event, "new-file", "finished-file"
     and "no-more-data", and 1 record

    The pipelines looks like:

        directory >> wiretap

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    avro_records = setup_avro_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='AVRO',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             batch_size_in_recs=1)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    records_wiretap = pipeline_builder.add_wiretap()
    events_wiretap = pipeline_builder.add_wiretap()

    directory >> records_wiretap.destination
    directory >= [events_wiretap.destination]

    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data

    assert 'new-file' == events_wiretap.output_records[0].header.values['sdc.event.type']
    assert 'finished-file' == events_wiretap.output_records[1].header.values['sdc.event.type']
    assert 'no-more-data' == events_wiretap.output_records[2].header.values['sdc.event.type']

    assert 5 == len(records_wiretap.output_records)
    for i in range(0, 5):
        assert records_wiretap.output_records[i].get_field_data('/name') == avro_records[i].get('name')
        assert records_wiretap.output_records[i].get_field_data('/age') == avro_records[i].get('age')
        assert records_wiretap.output_records[i].get_field_data('/emails') == avro_records[i].get('emails')
        assert records_wiretap.output_records[i].get_field_data('/boss') == avro_records[i].get('boss')


@sdc_min_version('3.8.0')
def test_directory_origin_multiple_threads_no_more_data_sent_after_all_data_read(sdc_builder, sdc_executor):
    """Test that directory origin with more than one threads read all data from all the files in a folder before
    sending no more data event.

    The pipelines looks like:

        directory >> wiretap
        directory >= pipeline finisher executor
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             file_name_pattern='test*.csv',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='NONE',
                             files_directory='/resources/resources/directory_origin',
                             read_order='LEXICOGRAPHICAL',
                             batch_size_in_recs=10,
                             batch_wait_time_in_secs=60,
                             number_of_threads=3,
                             on_record_error='STOP_PIPELINE')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    wiretap = pipeline_builder.add_wiretap()
    directory >> wiretap.destination

    pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher_executor.set_attributes(preconditions=['${record:eventType() == \'no-more-data\'}'],
                                              on_record_error='DISCARD')

    directory >= pipeline_finisher_executor

    directory_pipeline = pipeline_builder.build(
        title='test_directory_origin_multiple_threads_no_more_data_sent_after_all_data_read')
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.start_pipeline(directory_pipeline).wait_for_finished()

    temp_data_from_csv_file = (read_csv_file('./resources/directory_origin/test4.csv', ',', True))
    data_from_csv_files = [f'{row[0]},{row[1]},{row[2]}' for row in temp_data_from_csv_file]
    temp_data_from_csv_file = (read_csv_file('./resources/directory_origin/test5.csv', ',', True))
    for row in temp_data_from_csv_file:
        data_from_csv_files.append(f'{row[0]},{row[1]},{row[2]}')
    temp_data_from_csv_file = (read_csv_file('./resources/directory_origin/test6.csv', ',', True))
    for row in temp_data_from_csv_file:
        data_from_csv_files.append(f'{row[0]},{row[1]},{row[2]}')

    # assert all the data captured have the same raw_data
    output_records = wiretap.output_records
    output_records_text_fields = [f'{record.field["Name"]},{record.field["Job"]},{record.field["Salary"]}' for record in
                                  output_records]

    assert len(data_from_csv_files) == len(output_records_text_fields)
    assert sorted(data_from_csv_files) == sorted(output_records_text_fields)
    assert sdc_executor.get_pipeline_status(directory_pipeline).response.json().get('status') == 'FINISHED'


@sdc_min_version('3.0.0.0')
def test_directory_origin_avro_produce_full_file(sdc_builder, sdc_executor):
    """ Test Directory Origin in Avro data format. The sample Avro file has 5 lines and
    the batch size is 10. The pipeline should produce the event, "new-file", "finished-file"
    and "no-more-data" and 5 records

    The pipelines looks like:

        directory >> wiretap

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    avro_records = setup_avro_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='AVRO',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             batch_size_in_recs=10)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    records_wiretap = pipeline_builder.add_wiretap()
    events_wiretap = pipeline_builder.add_wiretap()

    directory >> records_wiretap.destination
    directory >= [events_wiretap.destination]

    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 5)
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    assert 'new-file' == events_wiretap.output_records[0].header.values['sdc.event.type']
    assert 'finished-file' == events_wiretap.output_records[1].header.values['sdc.event.type']
    assert 'no-more-data' == events_wiretap.output_records[2].header.values['sdc.event.type']

    assert 5 == len(records_wiretap.output_records)

    for i in range(0, 5):
        assert records_wiretap.output_records[i].get_field_data('/name') == avro_records[i].get('name')
        assert records_wiretap.output_records[i].get_field_data('/age') == avro_records[i].get('age')
        assert records_wiretap.output_records[i].get_field_data('/emails') == avro_records[i].get('emails')
        assert records_wiretap.output_records[i].get_field_data('/boss') == avro_records[i].get('boss')


@sdc_min_version('3.12.0')
@pytest.mark.parametrize('csv_record_type', ['LIST_MAP','LIST'])
def test_directory_origin_bom_file(sdc_builder, sdc_executor, csv_record_type):
    """ Test Directory Origin with file in CSV data format and containing BOM.
    The file(file_with_bom.csv) is present in resources/directory_origin. To view the
    BOM bytes, we can use "hexdump -C file_with_bom.csv". The first 3 bytes(ef bb bf)
    are BOM.

    The pipeline looks like:

        directory >> wiretap

    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')

    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='file_with_bom.csv',
                             file_name_pattern_mode='GLOB',
                             files_directory='/resources/resources/directory_origin',
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             root_field_type=csv_record_type)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    wiretap = pipeline_builder.add_wiretap()

    directory >> wiretap.destination
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(directory_pipeline)

    # contents of file_with_bom.csv: <BOM>abc,123,xyz
    if csv_record_type == 'LIST_MAP':
        assert 'abc' == wiretap.output_records[0].get_field_data('/0')
        assert '123' == wiretap.output_records[0].get_field_data('/1')
        assert 'xyz' == wiretap.output_records[0].get_field_data('/2')
    else:
        assert 'abc' == wiretap.output_records[0].get_field_data('/0').get('value')
        assert '123' == wiretap.output_records[0].get_field_data('/1').get('value')
        assert 'xyz' == wiretap.output_records[0].get_field_data('/2').get('value')


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('csv_record_type', ['LIST_MAP', 'LIST'])
def test_directory_origin_csv_produce_full_file(sdc_builder, sdc_executor, csv_record_type):
    """ Test Directory Origin in CSV data format. The sample CSV file has 3 lines and
    the batch size is 10. The pipeline should produce the event, "new-file", "finished-file"
    and "no-more-data" and 3 records

    The pipelines looks like:

        directory >> wiretap

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_basic_dilimited_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')

    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             root_field_type=csv_record_type,
                             batch_size_in_recs=10)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    records_wiretap = pipeline_builder.add_wiretap()
    events_wiretap = pipeline_builder.add_wiretap()

    directory >> records_wiretap.destination
    directory >= [events_wiretap.destination]

    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 3)
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured has the same csv_records
    assert 3 == len(records_wiretap.output_records)
    for i in range(0, len(csv_records)):
        records = records_wiretap.output_records[i].get_field_data(0)
        for j in range(0, 2):
            if csv_record_type == 'LIST_MAP':
                assert csv_records[i].split(',')[j] == list(records.values())[j]
            elif csv_record_type == 'LIST':
                assert csv_records[i].split(',')[j] == records[j].get('value')

    # assert events are correct
    assert 3 == len(events_wiretap.output_records)
    assert 'new-file' == events_wiretap.output_records[0].header.values['sdc.event.type']
    assert 'finished-file' == events_wiretap.output_records[1].header.values['sdc.event.type']
    assert 'no-more-data' == events_wiretap.output_records[2].header.values['sdc.event.type']


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('csv_record_type', ['LIST_MAP', 'LIST'])
@pytest.mark.parametrize('header_line', ['WITH_HEADER', 'IGNORE_HEADER', 'NO_HEADER'])
def test_directory_origin_csv_produce_less_file(sdc_builder, sdc_executor, csv_record_type, header_line):
    """ Test Directory Origin in CSV data format. The sample CSV file has 3 lines and
    the batch size is 1. The pipeline should produce the event, "new-file", "finished-file"
    and "no-more-data" and 3 (or 2) records

    The pipelines looks like:

        directory >> records_wiretap.destination
        directory >= [events_wiretap.destination]

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_basic_dilimited_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             header_line=header_line,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             root_field_type=csv_record_type,
                             batch_size_in_recs=1)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    records_wiretap = pipeline_builder.add_wiretap()
    events_wiretap = pipeline_builder.add_wiretap()

    directory >> records_wiretap.destination
    directory >= [events_wiretap.destination]

    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured has the same csv_records values
    # if WITH_HEADER, header is not treated as a record (2 records)
    # if IGNORE_HEADER, header is discarded (2 records)
    # if NO_HEADER, the possible header is treated as a record (3 records)
    # if LIST_MAP, field is OrderedDict with 0,1,...
    # if LIST, field has values
    # offset is used to include or avoid header in the values check

    if header_line == 'WITH_HEADER' or header_line == 'IGNORE_HEADER':
        assert 2 == len(records_wiretap.output_records)
        offset = 1
    elif header_line == 'NO_HEADER':
        assert 3 == len(records_wiretap.output_records)
        offset = 0

    for i in range(offset, len(csv_records)):
        records = records_wiretap.output_records[i - offset].get_field_data(0)
        for j in range(0, 2):
            if csv_record_type == 'LIST_MAP':
                assert csv_records[i].split(',')[j] == list(records.values())[j]
            elif csv_record_type == 'LIST':
                assert csv_records[i].split(',')[j] == records[j].get('value')

    # assert events are correct
    assert 3 == len(events_wiretap.output_records)
    assert 'new-file' == events_wiretap.output_records[0].header.values['sdc.event.type']
    assert 'finished-file' == events_wiretap.output_records[1].header.values['sdc.event.type']
    assert 'no-more-data' == events_wiretap.output_records[2].header.values['sdc.event.type']


@sdc_min_version('3.0.0.0')
def test_directory_origin_csv_custom_file(sdc_builder, sdc_executor):
    """ Test Directory Origin in custom CSV data format. The sample CSV file has 1 custom CSV

    The pipelines looks like:

        directory >> wiretap

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_custom_delimited_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             delimiter_format_type='CUSTOM',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    wiretap = pipeline_builder.add_wiretap()

    directory >> wiretap.destination
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(directory_pipeline)

    assert 1 == len(wiretap.output_records)
    assert wiretap.output_records[0].get_field_data('/0') == ' '.join(csv_records)


@sdc_min_version('3.8.0')
def test_directory_origin_multi_char_delimited(sdc_builder, sdc_executor):
    """
    Test Directory Origin with multi-character delimited format. This will generate a sample file with the custom
    multi-char delimiter then read it with the test pipeline.

    The pipeline looks like:

        directory >> wiretap

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    # crazy delimiter
    delim = '_/-\\_'
    custom_delimited_lines = [
      f"first{delim}second{delim}third",
      f"1{delim}11{delim}111",
      f"2{delim}22{delim}222",
      f"31{delim}3,3{delim}3,_/-_3,3"
    ]
    setup_dilimited_file(sdc_executor, tmp_directory, custom_delimited_lines)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             delimiter_format_type='MULTI_CHARACTER',
                             multi_character_field_delimiter=delim,
                             header_line='WITH_HEADER',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             batch_size_in_recs=3)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    wiretap = pipeline_builder.add_wiretap()

    directory >> wiretap.destination
    directory_pipeline = pipeline_builder.build('Multi Char Delimited Directory')
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 3)
    sdc_executor.stop_pipeline(directory_pipeline)

    assert 3 == len(wiretap.output_records)
    assert wiretap.output_records[0].get_field_data('/first') == '1'
    assert wiretap.output_records[0].get_field_data('/second') == '11'
    assert wiretap.output_records[0].get_field_data('/third') == '111'
    assert wiretap.output_records[1].get_field_data('/first') == '2'
    assert wiretap.output_records[1].get_field_data('/second') == '22'
    assert wiretap.output_records[1].get_field_data('/third') == '222'
    assert wiretap.output_records[2].get_field_data('/first') == '31'
    assert wiretap.output_records[2].get_field_data('/second') == '3,3'
    assert wiretap.output_records[2].get_field_data('/third') == '3,_/-_3,3'


@sdc_min_version('3.0.0.0')
def test_directory_origin_csv_custom_comment_file(sdc_builder, sdc_executor):
    """ Test Directory Origin in custom CSV data format with comment enabled. The sample CSV file have
    1 delimited line follow by 1 comment line and 1 delimited line

    The pipelines looks like:

        directory >> wiretap

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_dilimited_with_comment_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             delimiter_format_type='CUSTOM',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             enable_comments=True,
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             batch_size_in_recs=10)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    wiretap = pipeline_builder.add_wiretap()

    directory >> wiretap.destination
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 2)
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    assert 2 == len(wiretap.output_records)
    assert wiretap.output_records[0].get_field_data('/0') == csv_records[0]
    assert wiretap.output_records[1].get_field_data('/0') == csv_records[2]


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('ignore_empty_line', [True, False])
def test_directory_origin_custom_csv_empty_line_file(sdc_builder, sdc_executor, ignore_empty_line):
    """ Test Directory Origin in custom CSV data format with empty line enabled and disabled.
    The sample CSV file has 2 CSV records and 1 empty line.
    The pipeline should produce 2 when empty line is enabled and 3 when empty line is disabled

    The pipelines looks like:

        directory >> wiretap

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_dilimited_with_empty_line_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             delimiter_format_type='CUSTOM',
                             ignore_empty_lines=ignore_empty_line,
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             batch_size_in_recs=10)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    wiretap = pipeline_builder.add_wiretap()

    expected_record_size = len(csv_records)
    if ignore_empty_line:
        expected_record_size = 2

    directory >> wiretap.destination
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', expected_record_size)
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    assert expected_record_size == len(wiretap.output_records)

    assert wiretap.output_records[0].get_field_data('/0') == csv_records[0]

    if ignore_empty_line:
        assert wiretap.output_records[1].get_field_data('/0') == csv_records[2]
    else:
        assert wiretap.output_records[2].get_field_data('/0') == csv_records[2]


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('batch_size', [15])#4,5,6
def test_directory_origin_csv_record_overrun_on_batch_boundary(sdc_builder, sdc_executor, batch_size):
    """ Test Directory Origin in Delimited data format. The long delimited record in [2,4,5,8,9]th in the file
    the long delimited record should be ignored in the batch

    The pipelines looks like:

    directory >> wiretap.destination
    directory >= pipeline_finisher

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_long_dilimited_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             max_record_length_in_chars=10,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             batch_size_in_recs=batch_size)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(preconditions=['${record:eventType() == \'no-more-data\'}'])

    wiretap = pipeline_builder.add_wiretap()

    directory >> wiretap.destination
    directory >= pipeline_finisher

    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline).wait_for_finished()

    # assert all the data captured has the same raw_data
    # select shorter records, expected result
    expected_data = []
    for csv_record in csv_records:
        if 10 > len(csv_record):
            expected_data.append(csv_record)

    output_data = []
    for record in wiretap.output_records:
        output_data.append(','.join(str(e) for e in record.get_field_data(0).values()))

    assert expected_data == output_data

    # assert that the batch count is:
    # 3 batches for batchsize 6, 4 batches for batchsize 5, 5 batches for batchsize 3
    history = sdc_executor.get_pipeline_history(directory_pipeline)
    if batch_size == 3:
        assert 5 == history.latest.metrics.counter('pipeline.batchCount.counter').count
    elif batch_size == 5:
        assert 4 == history.latest.metrics.counter('pipeline.batchCount.counter').count
    elif batch_size == 6:
        assert 3 == history.latest.metrics.counter('pipeline.batchCount.counter').count


# SDC-10424
@sdc_min_version('3.5.3')
def test_directory_post_delete_on_batch_failure(sdc_builder, sdc_executor):
    """Make sure that post-actions are not executed on batch failure."""
    raw_data = '1\n2\n3\n4\n5'
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # 1st pipeline which generates the required files for Directory Origin
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.stop_after_first_batch = True
    origin.set_attributes(data_format='TEXT', raw_data=raw_data)

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=os.path.join(tmp_directory, '${YYYY()}-${MM()}-${DD()}-${hh()}'),
                            files_prefix='sdc-${sdc:id()}',
                            files_suffix='txt',
                            max_records_in_file=100)

    origin >> local_fs
    files_pipeline = builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # Generate exactly one input file
    sdc_executor.start_pipeline(files_pipeline).wait_for_finished()

    # 2nd pipeline which reads the files using Directory Origin stage
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='TEXT',
                             file_name_pattern='sdc*.txt',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True,
                             read_order='TIMESTAMP')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    shell = builder.add_stage('Shell')
    shell.script = "return -1"
    shell.on_record_error = "STOP_PIPELINE"

    directory >> shell
    directory_pipeline = builder.build('Directory Origin pipeline')
    sdc_executor.add_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline, wait=False).wait_for_status(status='RUN_ERROR', ignore_errors=True)

    # The main check is now - the pipeline should not drop the input file
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Directory')
    origin.set_attributes(data_format='WHOLE_FILE',
                          file_name_pattern='sdc*.txt',
                          file_name_pattern_mode='GLOB',
                          file_post_processing='DELETE',
                          files_directory=tmp_directory,
                          process_subdirectories=True,
                          read_order='TIMESTAMP')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        origin.set_attributes(file_processing_delay_in_ms=0)

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build('Validation')
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)

    assert 1 == len(wiretap.output_records)


# SDC-13559: Directory origin fires one batch after another when Allow Late directories is in effect
def test_directory_allow_late_directory_wait_time(sdc_builder, sdc_executor):
    """Test to ensure that when user explicitly enables "Allow Late Directory" and the directory doesn't exists,
    the origin won't go into a mode where it will generate one batch after another, ignoring the option Batch Wait
    Time completely."""
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'TEXT'
    directory.file_name_pattern = 'sdc*.txt'
    directory.files_directory = '/i/do/not/exists'
    directory.allow_late_directory = True
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.file_processing_delay_in_ms = 0

    trash = builder.add_stage('Trash')

    directory >> trash
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline)
    # We let the pipeline run for ~10 seconds - enough time to validate whether the origin is creating one batch
    # after another or not.
    time.sleep(10)
    sdc_executor.stop_pipeline(pipeline)

    # The origin and/or pipeline can still generate some batches, so we don't test precise number, just that is
    # really small (less then 1 batch/second).
    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('pipeline.batchCount.counter').count < 5


# Test for SDC-13476
def test_directory_origin_read_different_file_type(sdc_builder, sdc_executor):
    """Test Directory Origin. We make sure we covered race condition
    when directory origin is configured with JSON data format but files directory have txt files.
    It shows the relative stage errors depending on the type of file we try to read from files directory.
    The pipelines looks like:

        dev_raw_data_source >> local_fs
        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    generate_files(sdc_builder, sdc_executor, tmp_directory)

    # 2nd pipeline which reads the files using Directory Origin stage
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='JSON',
                             file_name_pattern='*',
                             number_of_threads=10,
                             file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             error_directory=tmp_directory,
                             read_order='LEXICOGRAPHICAL',
                             batch_wait_time_in_secs=1)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(preconditions=['${record:eventType() == \'no-more-data\'}'])

    wiretap = builder.add_wiretap()

    directory >> wiretap.destination
    directory >= pipeline_finisher

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert 0 == len(wiretap.output_records)
    history = sdc_executor.get_pipeline_history(pipeline)
    assert history.latest.metrics.counter('stage.Directory_01.stageErrors.counter').count > 0


@pytest.mark.parametrize('no_of_threads', [4])
@sdc_min_version('3.2.0.0')
def test_directory_origin_multiple_threads_timestamp_ordering(sdc_builder, sdc_executor, no_of_threads):
    """Test Directory Origin. We test that we read the same amount of files that we write with no reprocessing
    of files and no NoSuchFileException in the sdc logs

    Pipeline looks like:

    Shell executor that creates the files.
    Directory Origin >> Trash

    """

    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    sdc_executor.execute_shell(f'mkdir {tmp_directory}')
    msgs_sent_count = 400
    sdc_executor.execute_shell(
        '\n'.join([f'echo {str(uuid4())} > {tmp_directory}/sdc1-{str(uuid4())}.txt' for _ in range(msgs_sent_count)]))

    # 2nd pipeline which reads the files using Directory Origin stage in whole data format
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(batch_wait_time_in_secs=1,
                             data_format='WHOLE_FILE',
                             max_files_in_directory=1000,
                             files_directory=tmp_directory,
                             file_name_pattern='*',
                             file_name_pattern_mode='GLOB',
                             number_of_threads=no_of_threads,
                             process_subdirectories=True,
                             read_order='TIMESTAMP',
                             file_post_processing='DELETE')
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    trash = pipeline_builder.add_stage('Trash')

    directory >> trash

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.preconditions = ['${record:eventType() == \'no-more-data\'}']

    directory >= finisher

    directory_pipeline = pipeline_builder.build(title='Directory Origin')
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.start_pipeline(directory_pipeline).wait_for_finished()

    assert 0 == len(sdc_executor.get_stage_errors(directory_pipeline, directory))

    directory_pipeline_history = sdc_executor.get_pipeline_history(directory_pipeline)
    msgs_result_count = directory_pipeline_history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

    assert msgs_result_count == msgs_sent_count


# Test for SDC-13486
def test_directory_origin_error_file_to_error_dir(sdc_builder, sdc_executor):
    """ Test Directory Origin. Create two files in tmp_directory file1.txt which is correctly parsed by directory
    origin and file2.txt which is not correctly parsed by directory origin and hence it is sent to tmp_error_directory
    by that directory origin. After that we check with another directory origin reading from tmp_error_directory that
    we get an error_record specifying that file2.txt cannot be parsed again so we have checked that file2.txt was moved
    to tmp_error_directory by the first directory origin.

    Pipelines look like:

        dev_raw_data_source >> local_fs (called Custom Generate file1.txt pipeline)
        dev_raw_data_source >> local_fs (called Custom Generate file2.txt pipeline)
        dev_raw_data_source >= shell (events lane for the same pipeline as in above comment)
        directory >> trash (called Directory Read file1.txt and file2.txt)
        directory >> trash (called Directory Read file2.txt from error directory)

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    tmp_error_directory = os.path.join(tempfile.mkdtemp(prefix="err_dir_", dir=tempfile.gettempdir()))

    headers = "publication_title	print_identifier	online_identifier\n"

    # Generate file1.txt with good data.
    raw_data = headers + "abcd  efgh    ijkl\n"
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True,
                                       event_data='create-directory')
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=tmp_directory,
                            files_prefix='file1', files_suffix='txt')

    dev_raw_data_source >> local_fs

    files_pipeline = pipeline_builder.build('Custom Generate file1.txt pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    logger.debug("Creating file1.txt")
    sdc_executor.start_pipeline(files_pipeline).wait_for_finished(timeout_sec=5)

    # Generate file2.txt with bad data and create error directory.
    raw_data = headers + f'''ab	"	"'	''abcd		efgh\n'''
    dev_raw_data_source.set_attributes(raw_data=raw_data)
    local_fs.set_attributes(files_prefix='file2')

    shell = pipeline_builder.add_stage('Shell')
    shell.set_attributes(preconditions=["${record:value('/text') == 'create-directory'}"],
                         script=f'''mkdir {tmp_error_directory}''')

    dev_raw_data_source >= shell

    files_pipeline_2 = pipeline_builder.build('Custom Generate file2.txt pipeline')
    sdc_executor.add_pipeline(files_pipeline_2)

    logger.debug("Creating file2.txt")
    sdc_executor.start_pipeline(files_pipeline_2).wait_for_finished(timeout_sec=5)

    # 1st Directory pipeline which tries to read both file1.txt and file2.txt.
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(file_name_pattern='*.txt',
                             number_of_threads=2,
                             file_name_pattern_mode='GLOB',
                             file_post_processing='NONE',
                             files_directory=tmp_directory,
                             error_directory=tmp_error_directory,
                             read_order='LEXICOGRAPHICAL',
                             data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             delimiter_format_type='TDF')  # Tab separated values.
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    trash = builder.add_stage('Trash')
    directory >> trash

    pipeline_dir = builder.build('Directory Read file1.txt and file2.txt')
    sdc_executor.add_pipeline(pipeline_dir)

    sdc_executor.start_pipeline(pipeline_dir)
    sdc_executor.wait_for_pipeline_metric(pipeline_dir, 'input_record_count', 1, timeout_sec=300)
    time.sleep(5)

    assert 1 == len(sdc_executor.get_stage_errors(pipeline_dir, directory))
    assert "file2" in sdc_executor.get_stage_errors(pipeline_dir, directory)[0].error_message

    sdc_executor.stop_pipeline(pipeline_dir)

    # 2nd Directory pipeline which will read from error directory to check file2.txt is there.
    builder = sdc_builder.get_pipeline_builder()
    directory_error = builder.add_stage('Directory', type='origin')
    directory_error.set_attributes(file_name_pattern='*.txt',
                                   number_of_threads=2,
                                   file_name_pattern_mode='GLOB',
                                   file_post_processing='NONE',
                                   files_directory=tmp_error_directory,
                                   error_directory=tmp_error_directory,
                                   read_order='LEXICOGRAPHICAL',
                                   data_format='DELIMITED',
                                   header_line='WITH_HEADER',
                                   delimiter_format_type='TDF')  # Tab separated values.
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    trash_2 = builder.add_stage('Trash')
    directory_error >> trash_2

    pipeline_error_dir = builder.build('Directory Read file2.txt from error directory')
    sdc_executor.add_pipeline(pipeline_error_dir)

    sdc_executor.start_pipeline(pipeline_error_dir)

    assert 1 == len(sdc_executor.get_stage_errors(pipeline_error_dir, directory))
    assert "file2" in sdc_executor.get_stage_errors(pipeline_error_dir, directory)[0].error_message

    sdc_executor.stop_pipeline(pipeline_error_dir)


def generate_files(sdc_builder, sdc_executor, tmp_directory):
    raw_data = 'Hello!'

    # pipeline which generates the required files for Directory Origin
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}',
                            files_suffix='txt',
                            max_records_in_file=1)

    dev_raw_data_source >> local_fs
    files_pipeline = builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # generate some batches/files
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(files_pipeline)


@pytest.mark.parametrize('read_order', ['TIMESTAMP', 'LEXICOGRAPHICAL'])
@pytest.mark.parametrize('file_post_processing', ['DELETE', 'ARCHIVE'])
def test_directory_no_post_process_older_files(sdc_builder, sdc_executor, read_order, file_post_processing):
    """Only files that have been processed by the origin are post-processed."""
    files_directory = os.path.join('/tmp', get_random_string())
    archive_directory = os.path.join('/tmp', get_random_string())

    # We'll write 4 files, but only read the last two and ensure that those are the only ones left in the files
    # directory when the pipeline finishes.
    FIRST_FILE_TO_PROCESS = 'file-2.txt'
    UNPROCESSED_FILES = ['file-0.txt', 'file-1.txt']
    PROCESSED_FILES = ['file-2.txt', 'file-3.txt']

    sdc_executor.execute_shell(f'mkdir -p {files_directory} {archive_directory}')

    for i in range(4):
        sdc_executor.write_file(os.path.join(files_directory, f'file-{i}.txt'), str(i))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory')
    directory.set_attributes(archive_directory=archive_directory, data_format='TEXT',
                             file_name_pattern='*',
                             file_post_processing=file_post_processing,
                             files_directory=files_directory,
                             first_file_to_process=FIRST_FILE_TO_PROCESS,
                             read_order=read_order)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    trash = pipeline_builder.add_stage('Trash')
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(preconditions=["${record:eventType() == 'no-more-data'}"],
                                     on_record_error='DISCARD')

    directory >> trash
    directory >= pipeline_finisher

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert sorted(sdc_executor.execute_shell(f'ls {files_directory}').stdout.split()) == UNPROCESSED_FILES

    if file_post_processing == 'ARCHIVE':
        assert sorted(sdc_executor.execute_shell(f'ls {archive_directory}').stdout.split()) == PROCESSED_FILES


def setup_avro_file(sdc_executor, tmp_directory):
    """Setup 5 avro records and save in local system. The pipelines looks like:

        dev_raw_data_source >> local_fs

    """
    avro_records = [
    {
        "name": "sdc1",
        "age": 3,
        "emails": ["sdc1@streamsets.com", "sdc@company.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc2",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc3",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc4",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    },
    {
        "name": "sdc5",
        "age": 3,
        "emails": ["sdc0@streamsets.com", "sdc@gmail.com"],
        "boss": {
            "name": "sdc0",
            "age": 3,
            "emails": ["sdc0@streamsets.com", "sdc1@apache.org"],
            "boss": None
        }
    }]

    avro_schema = {
        "type": "record",
        "name": "Employee",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "emails", "type": {"type": "array", "items": "string"}},
            {"name": "boss" ,"type": ["Employee", "null"]}
        ]
    }

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

    return avro_records


def setup_basic_dilimited_file(sdc_executor, tmp_directory):
    """Setup simple 3 csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    csv_records = ["A,B", "c,d", "e,f"]
    return setup_dilimited_file(sdc_executor, tmp_directory, csv_records)


def setup_custom_delimited_file(sdc_executor, tmp_directory):
    """Setup 1 custom csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    csv_records = ["A^!B !^$^A"]
    return setup_dilimited_file(sdc_executor, tmp_directory, csv_records)


def setup_long_dilimited_file(sdc_executor, tmp_directory):
    """Setup 10 csv records and some records contain long charsets
    and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    csv_records = [
        "a,b,c,d",
        "e,f,g,h",
        "aaa,bbb,ccc,ddd",
        "i,j,k,l",
        "aa1,bb1,cc1,dd1",
        "aa2,bb2,cc2,dd2",
        "m,n,o,p",
        "q,r,s,t",
        "aa3,bb3,cc3,dd3",
        "aa4,bb5,cc5,dd5"
    ]

    return setup_dilimited_file(sdc_executor, tmp_directory, csv_records)


def setup_dilimited_with_comment_file(sdc_executor, tmp_directory):
    """Setup 3 csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    csv_records = [
        "a,b",
        "# This is comment",
        "c,d"
    ]

    return setup_dilimited_file(sdc_executor, tmp_directory, csv_records)


def setup_dilimited_with_empty_line_file(sdc_executor, tmp_directory):
    """Setup 3 csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    csv_records = [
        "a,b",
        "",
        "c,d"
    ]

    return setup_dilimited_file(sdc_executor, tmp_directory, csv_records)


def setup_dilimited_file(sdc_executor, tmp_directory, csv_records):
    """Setup csv records and save in local system. The pipelines looks like:

            dev_raw_data_source >> local_fs

    """
    raw_data = "\n".join(csv_records)
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, stop_after_first_batch=True)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix='csv')

    dev_raw_data_source >> local_fs
    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # generate some batches/files
    sdc_executor.start_pipeline(files_pipeline).wait_for_finished(timeout_sec=5)

    return csv_records


def read_csv_file(file_path, delimiter, remove_header=False):
    """ Reads a csv file with records separated by delimiter"""
    rows = []
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter)
        for row in csv_reader:
            rows.append(row)
    if remove_header:
        rows = rows[1:]
    return rows


@sdc_min_version('3.8.0')
def test_directory_origin_stop_resume(sdc_builder, sdc_executor):
    """Test that directory origin can stop and resume.
    test4.csv file from resources directory is used.
    The test reads the first ten records, stops and resume.
        directory >> wiretap.destination
        directory >= pipeline_finisher_executor
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             file_name_pattern='test4.csv',
                             file_name_pattern_mode='GLOB',
                             file_post_processing='NONE',
                             files_directory='/resources/resources/directory_origin',
                             read_order='LEXICOGRAPHICAL',
                             batch_size_in_recs=1)
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(preconditions=['${record:eventType() == \'no-more-data\'}'])

    directory >> wiretap.destination
    directory >= pipeline_finisher

    directory_pipeline = pipeline_builder.build(title='test_directory_stop_resume')
    sdc_executor.add_pipeline(directory_pipeline)
    sdc_executor.start_pipeline(directory_pipeline)
    sdc_executor.wait_for_pipeline_metric(directory_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(directory_pipeline)

    sdc_executor.update_pipeline(directory_pipeline)

    sdc_executor.start_pipeline(directory_pipeline).wait_for_finished()

    # assert all the data captured have the same raw_data
    output_records_text_fields = [f'{record.field["Name"]},{record.field["Job"]},{record.field["Salary"]}' for record in
                                  wiretap.output_records]

    temp_data_from_csv_file = (read_csv_file('./resources/directory_origin/test4.csv', ',', True))
    data_from_csv_files = [f'{row[0]},{row[1]},{row[2]}' for row in temp_data_from_csv_file]

    assert len(data_from_csv_files) == len(output_records_text_fields)
    assert sorted(data_from_csv_files) == sorted(output_records_text_fields)

def _write_pipeline(sdc_executor, number_files, tmp_write_directory, tmp_in_directory):
    """Pipeline to write.
    The pipelines looks like:

        directory >> wiretap

    """
    pipeline_builder = sdc_executor.get_pipeline_builder()

    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE',
                             file_name_pattern='^.*\.(pdf)$',
                             file_name_pattern_mode='REGEX',
                             file_post_processing='NONE',
                             files_directory=tmp_write_directory,
                             batch_size_in_recs=100,
                             process_subdirectories=True,
                             read_order='TIMESTAMP')
    if Version(sdc_executor.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='WHOLE_FILE',
                            directory_template=os.path.join(tmp_in_directory),
                            files_prefix='sdc-${sdc:id()}',
                            files_suffix='pdf',
                            max_records_in_file=1)

    directory >> local_fs

    files_pipeline = pipeline_builder.build('Write pipeline')

    # run the 'Write pipeline' to create the directory and starting files
    sdc_executor.add_pipeline(files_pipeline)
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(number_files)
    sdc_executor.stop_pipeline(files_pipeline)

    number_processed_files = int(sdc_executor.execute_shell(f'ls {tmp_in_directory} | wc -l').stdout)

    return number_processed_files


def test_directory_origin_read_while_writing(sdc_builder, sdc_executor):
    """Test Directory Origin. We run the pipeline to read files created, and at the same time, we write a large number
    of files with the same timestamp. We test if we get all the files created.
    The pipelines looks like:

        directory >> wiretap

    """
    number_files = 1000
    tmp_write_directory = os.path.join(tempfile.gettempdir(), get_random_string())
    tmp_in_directory = os.path.join(tempfile.gettempdir(), get_random_string())
    tmp_out_directory = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir {tmp_write_directory}')
    sdc_executor.execute_shell(f'mkdir {tmp_in_directory}')
    sdc_executor.execute_shell(f'mkdir {tmp_out_directory}')
    raw_data = 'hello'

    pipeline_builder = sdc_executor.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       stop_after_first_batch=False)

    local_fs_create = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs_create.set_attributes(data_format='TEXT',
                                   directory_template=os.path.join(tmp_write_directory),
                                   files_suffix='txt',
                                   max_records_in_file=1)

    dev_raw_data_source >> local_fs_create

    write_pipeline = pipeline_builder.build('Create files pipeline')

    try:
        # run the 'Create files pipeline' to create the directory and starting files
        sdc_executor.add_pipeline(write_pipeline)
        sdc_executor.start_pipeline(write_pipeline).wait_for_pipeline_batch_count(number_files)
        sdc_executor.stop_pipeline(write_pipeline)

        number_processed_files = int(sdc_executor.execute_shell(f'ls {tmp_write_directory} | wc -l').stdout)

        # 2nd pipeline to read the files at the same time the 3rd pipeline write it.
        pipeline_builder = sdc_builder.get_pipeline_builder()

        directory = pipeline_builder.add_stage('Directory', type='origin')
        directory.set_attributes(data_format='TEXT',
                                 file_name_pattern='^.*\.(txt)$',
                                 file_name_pattern_mode='REGEX',
                                 file_post_processing='NONE',
                                 files_directory=tmp_in_directory,
                                 batch_size_in_recs=100,
                                 process_subdirectories=True,
                                 read_order='TIMESTAMP')
        if Version(sdc_builder.version) >= Version('5.9.0'):
            directory.set_attributes(file_processing_delay_in_ms=0)

        local_fs = pipeline_builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(data_format='TEXT',
                                directory_template=os.path.join(tmp_out_directory),
                                files_suffix='txt',
                                max_records_in_file=1)

        directory >> local_fs

        directory_pipeline = pipeline_builder.build('Directory Origin pipeline')
        sdc_executor.add_pipeline(directory_pipeline)

        # 3rd pipeline to read the files created and write to the directory "tmp_in_directory"
        pipeline_builder = sdc_executor.get_pipeline_builder()

        directory_write = pipeline_builder.add_stage('Directory', type='origin')
        directory_write.set_attributes(data_format='TEXT',
                                       file_name_pattern='^.*\.(txt)$',
                                       file_name_pattern_mode='REGEX',
                                       file_post_processing='NONE',
                                       files_directory=tmp_write_directory,
                                       batch_size_in_recs=100,
                                       process_subdirectories=True,
                                       read_order='TIMESTAMP')
        if Version(sdc_builder.version) >= Version('5.9.0'):
            directory.set_attributes(file_processing_delay_in_ms=0)

        local_fs_write = pipeline_builder.add_stage('Local FS', type='destination')
        local_fs_write.set_attributes(data_format='TEXT',
                                      directory_template=os.path.join(tmp_in_directory),
                                      files_suffix='txt',
                                      max_records_in_file=1)

        directory_write >> local_fs_write

        files_pipeline = pipeline_builder.build('Write pipeline')
        sdc_executor.add_pipeline(files_pipeline)

        # run the 2nd and the 3rd pipeline
        cdm = sdc_executor.start_pipeline(directory_pipeline)
        sdc_executor.start_pipeline(files_pipeline)

        cdm.wait_for_pipeline_batch_count(number_processed_files)

        sdc_executor.stop_pipeline(directory_pipeline)
        sdc_executor.stop_pipeline(files_pipeline)

        # assert it captured all the files write in the directory
        history = sdc_executor.get_pipeline_history(directory_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == number_processed_files

    finally:
        sdc_executor.execute_shell(f'rm -fr {tmp_write_directory}')
        sdc_executor.execute_shell(f'rm -fr {tmp_in_directory}')
        sdc_executor.execute_shell(f'rm -fr {tmp_out_directory}')


@pytest.mark.parametrize('total_time', [10])
@pytest.mark.parametrize('num_of_dirs', [100])
@sdc_min_version('5.3.0')
def test_directory_origin_add_missing_dirs(sdc_builder, sdc_executor, total_time, num_of_dirs):
    """Run two pipelines in parallel. One creates and deletes multiple directories. The other one reads these
    newly created directories avoiding to fail if it is removed while reading."""

    temp_dir = sdc_executor.execute_shell(f'mktemp -d').stdout.rstrip()

    mkdir_builder = sdc_builder.get_pipeline_builder()
    mkdir_source = mkdir_builder.add_stage('Jython Scripting')
    mkdir_script = """
    try:
        sdc.importLock()
        import random
        import string
        import os
        import shutil
    finally:
        sdc.importUnlock()
    from timeit import default_timer as timer


    def randomword(length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))


    PREFIX = "%s"
    TOTAL_TIME = %d
    NUM_OF_DIRS = %d
    start = timer()
    while timer() - start < TOTAL_TIME:
        dirs = []
        for i in range(NUM_OF_DIRS):
            dir_name = PREFIX + "/" + randomword(10)
            os.mkdir(dir_name)
            dirs.append( dir_name )
        for dir_name in dirs:
            shutil.rmtree(dir_name)
    """ % (temp_dir, total_time, num_of_dirs)
    # textwrap.dedent helps to strip leading whitespaces for valid Python indentation
    mkdir_source.set_attributes(user_script=textwrap.dedent(mkdir_script))
    mkdir_wiretap = mkdir_builder.add_wiretap()
    mkdir_source >> mkdir_wiretap.destination
    mkdir_pipeline = mkdir_builder.build()

    rddir_builder = sdc_builder.get_pipeline_builder()
    rddir_source = rddir_builder.add_stage('Directory')
    rddir_source.set_attributes(files_directory=temp_dir,
                                file_name_pattern="*",
                                read_order='TIMESTAMP',
                                process_subdirectories=True,
                                data_format="DELIMITED")
    if Version(sdc_builder.version) >= Version('5.9.0'):
        rddir_source.set_attributes(file_processing_delay_in_ms=0)

    rddir_wiretap = rddir_builder.add_wiretap()
    rddir_source >> rddir_wiretap.destination
    rddir_pipeline = rddir_builder.build()

    sdc_executor.add_pipeline(mkdir_pipeline)
    sdc_executor.add_pipeline(rddir_pipeline)
    sdc_executor.start_pipeline(rddir_pipeline)
    sdc_executor.start_pipeline(mkdir_pipeline).wait_for_finished()
    sdc_executor.stop_pipeline(rddir_pipeline, force=True)
    sdc_executor.execute_shell(f'rm -rf {temp_dir}')

    # look for an IOException related to temp_dir in the pipeline logs
    logs = sdc_executor.get_logs(pipeline=rddir_pipeline)
    pattern = ".*IOException.+" + temp_dir
    regex = re.compile(pattern)
    matches = re.findall(regex, str(logs))
    if len(matches) > 0:
        for match in matches:
            logger.debug("Try to access a deleted directory: '%s'", match)
        assert False, 'Should not reach here. Tried to read some directory that have been deleted.'


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('_delay_between_batches', [100])
@pytest.mark.parametrize('_records_to_be_generated', [100])
@pytest.mark.parametrize('_batch_size', [1])
@pytest.mark.parametrize('_ignore_temporary_files', [True,False])
def test_directory_origin_ignore_tmp_files(sdc_builder, sdc_executor,
                                           _delay_between_batches, _records_to_be_generated, _batch_size,
                                           _ignore_temporary_files):
    """Run two pipelines in parallel. One creates multiples records and stores them into a LocalFS destination.
    The other reads from a Directory origin which points to the same path as the previous LocalFS destination.
    We set the flag to ignore temporary files to avoid generating duplicates.
        Dev Data Generator >> Local FS
        Directory >> Trash
    """

    temp_dir = sdc_executor.execute_shell(f'mktemp -d').stdout.rstrip()

    producer_builder = sdc_builder.get_pipeline_builder()
    producer_origin = producer_builder.add_stage('Dev Data Generator')
    producer_origin.set_attributes(delay_between_batches=_delay_between_batches,
                                   records_to_be_generated=_records_to_be_generated,
                                   batch_size=_batch_size)
    producer_origin.fields_to_generate = [{'field': 'foo', 'type': 'STRING'}]
    producer_destination = producer_builder.add_stage('Local FS')
    producer_destination.set_attributes(directory_template=temp_dir,
                                        data_format="JSON")
    producer_origin >> producer_destination
    producer_pipeline = producer_builder.build()

    consumer_builder = sdc_builder.get_pipeline_builder()
    consumer_origin = consumer_builder.add_stage('Directory')
    consumer_origin.set_attributes(files_directory=temp_dir,
                                   file_name_pattern="*",
                                   ignore_temporary_files=_ignore_temporary_files,
                                   data_format="JSON")
    if Version(sdc_builder.version) >= Version('5.9.0'):
        consumer_origin.set_attributes(file_processing_delay_in_ms=0)

    consumer_trash = consumer_builder.add_stage('Trash')
    consumer_origin >> consumer_trash
    consumer_pipeline = consumer_builder.build()

    sdc_executor.add_pipeline(producer_pipeline)
    sdc_executor.add_pipeline(consumer_pipeline)
    sdc_executor.start_pipeline(consumer_pipeline)
    sdc_executor.start_pipeline(producer_pipeline).wait_for_finished()

    record_count = _records_to_be_generated + 0 if _ignore_temporary_files else 1
    try:
        sdc_executor.wait_for_pipeline_metric(consumer_pipeline, 'output_record_count', record_count)
        sdc_executor.stop_pipeline(consumer_pipeline, force=True)
        if _ignore_temporary_files:
            assert_record_count(sdc_executor, consumer_pipeline, record_count)

    finally:
        sdc_executor.execute_shell(f'rm -rf {temp_dir}')


@sdc_min_version('5.3.0')
@pytest.mark.parametrize('batch_size_in_recs', [1, 50, 100])
@pytest.mark.parametrize('num_threads', [1, 2])
def test_directory_origin_number_of_batches_generated(
        sdc_builder,
        sdc_executor,
        shell_executor,
        file_writer,
        batch_size_in_recs,
        num_threads
):
    """
    Check that no empty batches are generated once all the data is read when using a single thread or multiple ones.
    """
    files_directory = os.path.join('/tmp', get_random_string())
    file_name_1 = 'temp_1.txt'
    file_name_2 = 'temp_2.txt'

    records_per_file = 50
    number_of_batches = 2 * (records_per_file // batch_size_in_recs + 1) + 1
    file_contents_1 = '\n'.join(['Doc 1 - Line {}'.format(i) for i in range(0, records_per_file)])
    file_contents_2 = '\n'.join(['Doc 2 - Line {}'.format(i) for i in range(0, records_per_file)])

    pipeline = None

    try:
        logger.debug(f'Creating files directory {files_directory}...')
        shell_executor(f'mkdir {files_directory}')
        logger.debug(f'Creating file {files_directory}...')
        file_writer(os.path.join(files_directory, file_name_1), file_contents_1)
        logger.debug(f'Creating file {files_directory}...')
        file_writer(os.path.join(files_directory, file_name_2), file_contents_2)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory')
        directory.set_attributes(
            data_format='TEXT',
            files_directory=files_directory,
            file_name_pattern='*.txt',
            batch_size_in_recs=batch_size_in_recs,
            number_of_threads=num_threads
        )
        if Version(sdc_builder.version) >= Version('5.9.0'):
            directory.set_attributes(file_processing_delay_in_ms=0)

        wiretap = pipeline_builder.add_wiretap()

        directory >> wiretap.destination
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 100)

        # Wait for 55 seconds to give time for further batches to be generated
        # (a bit less than the default 60 seconds poolingTimeoutSecs to avoid flakiness)
        time.sleep(55)
        sdc_executor.stop_pipeline(pipeline)

        # Assert that we get the correct number of batches
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == number_of_batches

        # Assert we have processed 100 records
        assert len(wiretap.output_records) == 100
    finally:
        logger.info(f'Delete directory in {files_directory}...')
        shell_executor(f'rm -r {files_directory}')

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


@sdc_min_version('5.6.0')
def test_directory_origin_compression_library_snappy(sdc_builder, sdc_executor, shell_executor):
    """
    Tests the Compression Library option to indicate which library has been used to compress a file. Checks this option
    can be used to decompress a file compressed with Snappy with an HDFS target stage, which uses a custom Snappy
    library.

    The test creates the following pipeline to create the file:
        Dev Raw Data >> Local FS

    And then uses the pipeline below to test the Compression Library option and check the file can be decompressed:
        Directory >> Local FS
    """
    files_directory = os.path.join('/tmp', get_random_string())
    compressed_file_prefix = 'compressed_sdc'
    decompressed_file_prefix = 'decompressed_sdc'
    file_content = 'Oh! Bello papaguena! Tu le Bella comme le papaya.'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='TEXT',
        raw_data=file_content,
        stop_after_first_batch=True
    )

    local_fs = pipeline_builder.add_stage('Local FS')
    local_fs.set_attributes(
        directory_template=files_directory,
        files_prefix=compressed_file_prefix,
        data_format='TEXT',
        compression_codec='SNAPPY'
    )

    dev_raw_data_source >> local_fs
    file_generator_pipeline = pipeline_builder.build().configure_for_environment()
    sdc_executor.add_pipeline(file_generator_pipeline)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory')
    directory.set_attributes(
        files_directory=files_directory,
        file_name_pattern='*.snappy',
        data_format='TEXT',
        compression_format='COMPRESSED_FILE',
        compression_library='SNAPPY'
    )
    if Version(sdc_builder.version) >= Version('5.9.0'):
        directory.set_attributes(file_processing_delay_in_ms=0)

    local_fs = pipeline_builder.add_stage('Local FS')
    local_fs.set_attributes(
        directory_template=files_directory,
        files_prefix=decompressed_file_prefix,
        data_format='TEXT'
    )

    directory >> local_fs
    pipeline = pipeline_builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info(f'Creating the Snappy compressed file in {files_directory}...')
        sdc_executor.start_pipeline(file_generator_pipeline).wait_for_finished()

        logger.info('Checking the file has been created...')
        shell_result = sdc_executor.execute_shell(f'cat {files_directory}/{compressed_file_prefix}*')
        assert shell_result.stderr == ''
        assert shell_result.exit_code == '0'

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Checking the file has been decompressed successfully...')
        shell_result = sdc_executor.execute_shell(f'cat {files_directory}/{decompressed_file_prefix}*')
        assert shell_result.stderr == ''
        assert shell_result.exit_code == '0'
        assert shell_result.stdout == file_content + '\n'
    finally:
        logger.info(f'Delete directory in {files_directory}...')
        shell_executor(f'rm -r {files_directory}')

        if file_generator_pipeline and \
                (sdc_executor.get_pipeline_status(file_generator_pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(file_generator_pipeline)

        if pipeline and (sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING'):
            sdc_executor.stop_pipeline(pipeline)


def assert_record_count(sdc_executor, pipeline, expected_count):
    history = sdc_executor.get_pipeline_history(pipeline)
    output_record_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    assert output_record_count == expected_count


@sdc_min_version('5.9.0')
def test_directory_origin_with_file_processing_delay(sdc_builder, sdc_executor, keep_data):
    """
    Test Directory Origin with the File Processing Delay parameter set to 100 seconds.
    Files should be found only after the delay time has passed from their last modified time,
    and not before then.

        directory >> wiretap

    """

    delay_seconds = 100
    margin_seconds = 20
    raw_data = "Hello World"
    random_string = get_random_string()
    filename1 = "sdc-" + random_string + "-1.txt"
    filename2 = "sdc-" + random_string + "-2.txt"
    tmp_directory = os.path.join(tempfile.gettempdir(), random_string)
    file1 = os.path.join(tmp_directory, filename1)
    file2 = os.path.join(tmp_directory, filename2)
    sdc_executor.execute_shell(f'mkdir -p {tmp_directory}')
    sdc_executor.write_file(file1, raw_data)
    time.sleep(delay_seconds + margin_seconds)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(batch_size_in_recs=1,
                             data_format='TEXT',
                             file_name_pattern='sdc*.txt',
                             file_processing_delay_in_ms=delay_seconds * 1000,
                             files_directory=tmp_directory,
                             read_order='TIMESTAMP')
    wiretap = pipeline_builder.add_wiretap()
    directory >> wiretap.destination
    pipeline = pipeline_builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)
        cmd = sdc_executor.start_pipeline(pipeline)
        cmd.wait_for_pipeline_output_records_count(1, timeout_sec=1800)
        assert len(wiretap.output_records) == 1

        date_format = '%Y-%m-%d %H:%M:%S'
        sdc_executor.write_file(file2, raw_data)
        assert len(wiretap.output_records) == 1

        sys_time = sdc_executor.execute_shell(f"date '+{date_format}'").stdout.strip()
        sys_datetime = datetime.datetime.strptime(sys_time, date_format)
        touch_datetime = sys_datetime + datetime.timedelta(seconds=delay_seconds)
        sdc_executor.execute_shell(f"touch -d '{touch_datetime}' {file2}")

        sys_time = sdc_executor.execute_shell(f"date '+{date_format}'").stdout.strip()
        read_time = datetime.datetime.strptime(sys_time, date_format)
        min_read_time = touch_datetime + datetime.timedelta(seconds=delay_seconds)
        while read_time < (min_read_time - datetime.timedelta(seconds=margin_seconds)):
            assert len(wiretap.output_records) == 1
            time.sleep(margin_seconds)
            sys_time = sdc_executor.execute_shell(f"date '+{date_format}'").stdout.strip()
            read_time = datetime.datetime.strptime(sys_time, date_format)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=3000)
        sys_time = sdc_executor.execute_shell(f"date '+{date_format}'").stdout.strip()
        read_time = datetime.datetime.strptime(sys_time, date_format)

        assert len(wiretap.output_records) == 2
        assert read_time >= min_read_time

    finally:
        sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            sdc_executor.execute_shell(f'rm -fr {tmp_directory}')


@sdc_min_version('5.9.0')
def test_directory_origin_with_max_total_spool_files(sdc_builder, sdc_executor, keep_data):
    """
    Test Directory Origin with the File Processing Delay parameter set to 100 seconds.
    Files should be found only after the delay time has passed from their last modified time,
    and not before then.

        directory >> wiretap

    """

    raw_data = "Hello World"
    random_str = get_random_string()
    tmp_directory = sdc_executor.execute_shell('mktemp -d').stdout.rstrip()
    sdc_executor.write_file(os.path.join(tmp_directory, "sdc-" + random_str + "-1.txt"), raw_data)
    sdc_executor.write_file(os.path.join(tmp_directory, "sdc-" + random_str + "-2.txt"), raw_data)
    sdc_executor.write_file(os.path.join(tmp_directory, "sdc-" + random_str + "-3.txt"), raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(batch_size_in_recs=1,
                             data_format='TEXT',
                             file_name_pattern='sdc*.txt',
                             file_processing_delay_in_ms=0,
                             files_directory=tmp_directory,
                             max_files_hard_limit=2,
                             read_order='TIMESTAMP')
    wiretap = pipeline_builder.add_wiretap()
    directory >> wiretap.destination
    pipeline = pipeline_builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)
        with pytest.raises(StartError) as exception:
            sdc_executor.start_pipeline(pipeline)

        expected_error = "SPOOLDIR_50 - Exceeded max number"
        assert expected_error in f'{exception.value}',\
            f"Expected '{expected_error}' exception, instead got '{exception.value}'"

    finally:
        if not keep_data:
            sdc_executor.execute_shell(f'rm -fr {tmp_directory}')
