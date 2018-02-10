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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
import os
import pytest
import string
import tempfile
import time

from testframework.markers import sdc_min_version
from testframework.utils import get_random_string

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
    file_pipeline_history = sdc_executor.pipeline_history(files_pipeline)
    msgs_sent_count1 = file_pipeline_history.entries[4].metrics.counter('pipeline.batchOutputRecords.counter').count
    msgs_sent_count2 = file_pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    directory_pipeline_history = sdc_executor.pipeline_history(directory_pipeline)
    msgs_result_count = directory_pipeline_history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

    assert msgs_result_count == msgs_sent_count1 + msgs_sent_count2
