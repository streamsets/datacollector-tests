# Copyright 2018 StreamSets Inc.
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.parametrize('dir_name', ('dataDir', 'configDir'))
@sdc_min_version('3.6.0')
def test_protect_read(dir_name, sdc_builder, sdc_executor):
    """Ensure that pipeline can't read from SDC internal directory."""
    input_dir = sdc_executor.api_client.get_sdc_directories()[dir_name]

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='TEXT', file_name_pattern='*', file_name_pattern_mode='GLOB',
                             files_directory=input_dir, process_subdirectories=True)
    trash = builder.add_stage('Trash')

    directory >> trash
    pipeline = builder.build(f'Read from {input_dir}')
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # Start pipeline and wait for it to die
    sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status(status='START_ERROR', ignore_errors=True)

    # Validate how the pipeline failed in an expected manner - security exception about not having proper access
    status = sdc_executor.get_pipeline_status(pipeline).response.json()
    assert 'java.lang.SecurityException' in status['message']
    assert 'is not allowed access to Data Collector internal directories' in status['message']


@pytest.mark.parametrize('dir_name', ('dataDir', 'configDir', 'resourcesDir'))
@sdc_min_version('3.6.0')
def test_protect_write(dir_name, sdc_builder, sdc_executor):
    """Ensure that pipeline can't write into SDC internal directory."""
    output_dir = sdc_executor.api_client.get_sdc_directories()[dir_name]

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='TEXT', raw_data='nothing important')
    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=output_dir,
                            files_prefix='sdc-${sdc:id()}', files_suffix='txt', max_records_in_file=100)

    source >> local_fs
    pipeline = builder.build(f'Write to {output_dir}')
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # Start pipeline and wait for it to die
    sdc_executor.start_pipeline(pipeline, wait=False).wait_for_status(status='START_ERROR', ignore_errors=True)

    # Validate how the pipeline failed in an expected manner - security exception about not having proper access
    status = sdc_executor.get_pipeline_status(pipeline).response.json()
    assert 'java.lang.SecurityException' in status['message']
    assert 'is not allowed access to Data Collector internal directories' in status['message']

