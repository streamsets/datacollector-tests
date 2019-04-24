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

import json
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


@sdc_min_version('3.0.0.0')
def test_directory_origin_avro_produce_less_file(sdc_builder, sdc_executor):
    """Test Directory Origin in Avro data format. The sample Avro file has 5 lines and
    the batch size is 1. The pipeline should produce the event, "new-file" and 1 record

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    avro_records = setup_avro_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='AVRO', file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=1).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    output_records = snapshot[directory.instance_name].output
    event_records = snapshot[directory.instance_name].event_records

    assert 1 == len(event_records)
    assert 1 == len(output_records)

    assert 'new-file' == event_records[0].header['values']['sdc.event.type']

    assert output_records[0].get_field_data('/name') == avro_records[0].get('name')
    assert output_records[0].get_field_data('/age') == avro_records[0].get('age')
    assert output_records[0].get_field_data('/emails') == avro_records[0].get('emails')
    assert output_records[0].get_field_data('/boss') == avro_records[0].get('boss')


@sdc_min_version('3.0.0.0')
def test_directory_origin_avro_produce_full_file(sdc_builder, sdc_executor):
    """ Test Directory Origin in Avro data format. The sample Avro file has 5 lines and
    the batch size is 10. The pipeline should produce the event, "new-file" and "finished-file"
    and 5 records

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    avro_records = setup_avro_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='AVRO', file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=10).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    output_records = snapshot[directory.instance_name].output
    event_records = snapshot[directory.instance_name].event_records

    assert 2 == len(event_records)
    assert 5 == len(output_records)

    assert 'new-file' == event_records[0].header['values']['sdc.event.type']
    assert 'finished-file' == event_records[1].header['values']['sdc.event.type']

    for i in range(0, 5):
        assert output_records[i].get_field_data('/name') == avro_records[i].get('name')
        assert output_records[i].get_field_data('/age') == avro_records[i].get('age')
        assert output_records[i].get_field_data('/emails') == avro_records[i].get('emails')
        assert output_records[i].get_field_data('/boss') == avro_records[i].get('boss')

@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('csv_record_type', ['LIST_MAP', 'LIST'])
def test_directory_origin_csv_produce_full_file(sdc_builder, sdc_executor, csv_record_type):
    """ Test Directory Origin in CSV data format. The sample CSV file has 3 lines and
    the batch size is 10. The pipeline should produce the event, "new-file" and "finished-file"
    and 3 records

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_basic_dilimited_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')

    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP',
                             root_field_type=csv_record_type)

    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=10).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same csv_records
    output_records = snapshot[directory.instance_name].output
    event_records = snapshot[directory.instance_name].event_records

    assert 2 == len(event_records)
    assert 3 == len(output_records)

    assert 'new-file' == event_records[0].header['values']['sdc.event.type']
    assert 'finished-file' == event_records[1].header['values']['sdc.event.type']

    for i in range(0, 3):
        csv_record_fields = csv_records[i].split(',')
        for j in range(0, len(csv_record_fields)):
            if type(output_records[i].get_field_data(f'/{j}')) is dict:
               output_records[i].get_field_data(f'/{j}').get('value') == csv_record_fields[j]
            else:
                output_records[i].get_field_data(f'/{j}') == csv_record_fields[j]


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('csv_record_type', ['LIST_MAP', 'LIST'])
@pytest.mark.parametrize('header_line', ['WITH_HEADER', 'IGNORE_HEADER', 'NO_HEADER'])
def test_directory_origin_csv_produce_less_file(sdc_builder, sdc_executor, csv_record_type, header_line):
    """ Test Directory Origin in CSV data format. The sample CSV file has 3 lines and
    the batch size is 1. The pipeline should produce the event, "new-file"
    and 1 record

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_basic_dilimited_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             header_line=header_line, process_subdirectories=True,
                             read_order='TIMESTAMP', root_field_type=csv_record_type)
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=1).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same csv_records
    output_records = snapshot[directory.instance_name].output
    event_records = snapshot[directory.instance_name].event_records

    assert 1 == len(event_records)
    assert 1 == len(output_records)

    assert 'new-file' == event_records[0].header['values']['sdc.event.type']

    csv_record_fields = csv_records[0].split(',')
    for j in range(0, len(csv_record_fields)):
        name = csv_record_fields[j] if header_line == 'WITH_HEADER' and csv_record_type == 'LIST_MAP' else j
        if type(output_records[0].get_field_data(f'/{name}')) is dict:
           output_records[0].get_field_data(f'/{name}').get('value') == csv_record_fields[j]
        else:
            output_records[0].get_field_data(f'/{name}') == csv_record_fields[j]


@sdc_min_version('3.0.0.0')
def test_directory_origin_csv_custom_file(sdc_builder, sdc_executor):
    """ Test Directory Origin in custom CSV data format. The sample CSV file has 1 custom CSV

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_custom_delimited_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED', delimiter_format_type='CUSTOM',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=1).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    output_records = snapshot[directory.instance_name].output

    assert 1 == len(output_records)
    assert output_records[0].get_field_data('/0') == ' '.join(csv_records)

@sdc_min_version('3.8.0')
def test_directory_origin_multi_char_delimited(sdc_builder, sdc_executor):
    """
    Test Directory Origin with multi-character delimited format. This will generate a sample file with the custom
    multi-char delimiter then read it with the test pipeline.

    The pipeline looks like:

        directory >> trash

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
    directory.set_attributes(data_format='DELIMITED', delimiter_format_type='MULTI_CHARACTER',
                             multi_character_field_delimiter=delim,
                             header_line='WITH_HEADER',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build('Multi Char Delimited Directory')
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=3).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    output_records = snapshot[directory.instance_name].output

    assert 3 == len(output_records)
    assert output_records[0].get_field_data('/first') == '1'
    assert output_records[0].get_field_data('/second') == '11'
    assert output_records[0].get_field_data('/third') == '111'
    assert output_records[1].get_field_data('/first') == '2'
    assert output_records[1].get_field_data('/second') == '22'
    assert output_records[1].get_field_data('/third') == '222'
    assert output_records[2].get_field_data('/first') == '31'
    assert output_records[2].get_field_data('/second') == '3,3'
    assert output_records[2].get_field_data('/third') == '3,_/-_3,3'

@sdc_min_version('3.0.0.0')
def test_directory_origin_csv_custom_comment_file(sdc_builder, sdc_executor):
    """ Test Directory Origin in custom CSV data format with comment enabled. The sample CSV file have
    1 delimited line follow by 1 comment line and 1 delimited line

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_dilimited_with_comment_file(sdc_executor, tmp_directory)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED', delimiter_format_type='CUSTOM',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             enable_comments = True,
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=10).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    output_records = snapshot[directory.instance_name].output

    assert 2 == len(output_records)

    assert output_records[0].get_field_data('/0') == csv_records[0]
    assert output_records[1].get_field_data('/0') == csv_records[2]


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('ignore_empty_line', [True, False])
def test_directory_origin_custom_csv_empty_line_file(sdc_builder, sdc_executor, ignore_empty_line):
    """ Test Directory Origin in custom CSV data format with empty line enabled and disabled.
    The sample CSV file has 2 CSV records and 1 empty line.
    The pipeline should produce 2 when empty line is enabled and 3 when empty line is disabled

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_dilimited_with_empty_line_file(sdc_executor, tmp_directory)
    empty_line_position = [1]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED', delimiter_format_type='CUSTOM',
                             ignore_empty_lines = ignore_empty_line,
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE',
                             files_directory=tmp_directory,
                             process_subdirectories=True, read_order='TIMESTAMP')
    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=10).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    output_records = snapshot[directory.instance_name].output

    expected_record_size = len(csv_records)
    if ignore_empty_line:
        expected_record_size = 2
    assert expected_record_size == len(output_records)

    assert output_records[0].get_field_data('/0') == csv_records[0]

    if ignore_empty_line:
        assert output_records[1].get_field_data('/0') == csv_records[2]
    else:
        assert output_records[2].get_field_data('/0') == csv_records[2]


@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('batch_size', [3,4,5,6])
def test_directory_origin_csv_record_overrun_on_batch_boundary(sdc_builder, sdc_executor, batch_size):
    """ Test Directory Origin in Delimited data format. The long delimited record in [2,4,5,8,9]th in the file
    the long delimited record should be ignored in the batch

    The pipelines looks like:

        directory >> trash

    """
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    csv_records = setup_long_dilimited_file(sdc_executor, tmp_directory)
    long_dilimited_record_position = [2,4,5,8,9]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             file_name_pattern='sdc*', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory,
                             max_record_length_in_chars=10,
                             process_subdirectories=True, read_order='TIMESTAMP')


    trash = pipeline_builder.add_stage('Trash')

    directory >> trash
    directory_pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(directory_pipeline)

    snapshot = sdc_executor.capture_snapshot(directory_pipeline, start_pipeline=True, batch_size=batch_size).snapshot
    sdc_executor.stop_pipeline(directory_pipeline)

    # assert all the data captured have the same raw_data
    output_records = snapshot[directory.instance_name].output

    expected_batch_size = batch_size
    for i in range(0, batch_size):
        if i in long_dilimited_record_position:
            expected_batch_size = expected_batch_size - 1

    assert expected_batch_size == len(output_records)

    j = 0
    for i in range(0, batch_size):
        if j not in long_dilimited_record_position:
            csv_record_fields = csv_records[j].split(',')
            for k in range(0, len(csv_record_fields)):
                output_records[0].get_field_data(f'/{k}') == csv_record_fields[k]
            j = j + 1


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
    trash = builder.add_stage('Trash')
    origin >> trash

    pipeline = builder.build('Validation')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)
    assert 1 == len(snapshot[origin.instance_name].output)


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
