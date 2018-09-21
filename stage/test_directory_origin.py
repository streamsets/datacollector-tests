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

import logging
import os
import pytest
import random
import string
import tempfile
import time

from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=pointless-statement, too-many-locals


def test_directory_origin(sdc_builder, sdc_executor):
    """Test Directory Origin. We test by making sure files are pre-created using Local FS destination stage pipeline
    and then have the Directory Origin read those files. The pipelines looks like:

        dev_raw_data_source >> local_fs
        directory >> trash

    """
    raw_data = 'Hello!'
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # 1st pipeline which generates the required files for Directory Origin
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT',
                            directory_template=os.path.join(tmp_directory, '${YYYY()}-${MM()}-${DD()}-${hh()}'),
                            files_prefix='sdc-${sdc:id()}', files_suffix='txt', max_records_in_file=100)

    dev_raw_data_source >> local_fs
    files_pipeline = pipeline_builder.build('Generate files pipeline')
    sdc_executor.add_pipeline(files_pipeline)

    # generate some batches/files
    sdc_executor.start_pipeline(files_pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(files_pipeline)

    # 2nd pipeline which reads the files using Directory Origin stage
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='TEXT', file_name_pattern='sdc*.txt', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build('Directory Origin pipeline')
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    for record in snapshot.snapshot_batches[0][directory.instance_name].output:
        assert raw_data == record.value['value']['text']['value']
        assert record.header['sourceId'] is not None
        assert record.header['stageCreator'] is not None


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
                             data_format='TEXT', file_name_pattern='sdc*.txt',
                             file_name_pattern_mode='GLOB', file_post_processing='DELETE',
                             files_directory=tmp_directory, process_subdirectories=True,
                             read_order='TIMESTAMP', number_of_threads=no_of_threads)
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
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build('Directory Origin pipeline')
    sdc_executor.add_pipeline(directory_pipeline)

    # if we set the batch size to the same value in the directory origin pipeline, it should read exactly as many batches
    # as were written by the first pipeline
    sdc_executor.start_pipeline(directory_pipeline).wait_for_pipeline_batch_count(num_batches)
    sdc_executor.stop_pipeline(directory_pipeline)

