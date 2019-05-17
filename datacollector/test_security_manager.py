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
import pytest

from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
        data_collector.sdc_properties['security_manager.sdc_manager.enable'] = 'true'
    return hook


@pytest.mark.parametrize('dir_name', ('dataDir', 'configDir'))
@sdc_min_version('3.7.0')
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
@sdc_min_version('3.7.0')
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


@sdc_min_version('3.7.0')
def test_access_to_ldap_conf(sdc_builder, sdc_executor):
    """Read ldap-login.conf file that is whitelisted by default"""
    ldap_login = sdc_executor.api_client.get_sdc_directories()['configDir'] + "/ldap-login.conf"

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.set_attributes(data_format='TEXT', raw_data='nothing important', stop_after_first_batch=True)

    jython = builder.add_stage('Jython Evaluator')
    jython.script = f"""# Script
for record in records:
  lines = []
  file = open("{ldap_login}", "r")
  for line in file:
    lines.append(line)

  record.value['lines'] = lines

  output.write(record)
    """

    trash = builder.add_stage('Trash')

    source >> jython >> trash

    pipeline = builder.build(f'Reading ldap-login.conf')
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # Generate snapshot
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    assert len(snapshot[jython].output) == 1

    # There should be field lines that is an array of 2+ entries (each row)
    record = snapshot[jython].output[0]
    assert len(record.field['lines']) > 2
