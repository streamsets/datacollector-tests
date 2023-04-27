# Copyright 2021 StreamSets Inc.
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
import string
import tempfile

from datetime import datetime, timedelta
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@sdc_min_version('3.22.0')
@pytest.mark.parametrize('late_records_time, expected_late_records', [
    ('${1 * SECONDS}', True),
    ('${2 * HOURS}', False),
])
def test_local_fs_late_records_time_limit(sdc_builder, sdc_executor, late_records_time, expected_late_records):
    """A simple basic test to see the behaviour of Late Record Time Limit in Local FS stage". Pipeline looks like:
        dev_raw_data_source >> field_type_converter >> delay >> local_fs
    """

    tmp_directory_local_fs = '/tmp/out/${YYYY()}-${MM()}-${DD()}-${hh()}-${mm()}'
    tmp_directory_local_fs_late_record = '/tmp/late/{}'.format(get_random_string(string.ascii_letters, 10))

    timestamp = datetime.now() - timedelta(hours=1)
    raw_data = [{'id': 1, 'timestamp': str(timestamp)},
                {'id': 2, 'timestamp': str(timestamp)},
                {'id': 3, 'timestamp': str(timestamp)},
                {'id': 4, 'timestamp': str(timestamp)},
                {'id': 5, 'timestamp': str(timestamp)}]

    field_type_converter_configs = [
        {
            'fields': ['/timestamp'],
            'targetType': 'DATE',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS'
        }]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join(json.dumps(rec) for rec in raw_data),
                                       stop_after_first_batch=False)

    field_type_converter_fields = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter_fields.set_attributes(conversion_method='BY_FIELD',
                                               field_type_converter_configs=field_type_converter_configs)

    delay = pipeline_builder.add_stage('Delay')
    # milliseconds to delay between batches, so as we get time to disconnect network
    delay.set_attributes(delay_between_batches=30000)

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='JSON',
                            directory_template=tmp_directory_local_fs,
                            time_basis='${record:value("/timestamp")}',
                            idle_timeout='${1 * HOURS}',
                            late_record_time_limit_in_secs=late_records_time,
                            late_record_handling='SEND_TO_LATE_RECORDS_FILE',
                            late_record_directory_template=tmp_directory_local_fs_late_record)

    dev_raw_data_source >> field_type_converter_fields >> delay >> local_fs
    local_fs_pipeline = pipeline_builder.build('Late Records Time Limit Pipeline').configure_for_environment()

    sdc_executor.add_pipeline(local_fs_pipeline)

    try:
        # Start pipeline and read how many files are created in late folder:
        sdc_executor.start_pipeline(local_fs_pipeline).wait_for_pipeline_batch_count(3)
        sdc_executor.stop_pipeline(local_fs_pipeline)
        num_late_files = int(sdc_executor.execute_shell(f'find /tmp/late -type f | wc -l').stdout)

        if expected_late_records:
            assert num_late_files > 0
        else:
            assert num_late_files == 0

    finally:
        logger.info('Deleting files created by Local FS in %s and %s ...', tmp_directory_local_fs,
                    tmp_directory_local_fs_late_record)
        sdc_executor.execute_shell(f'rm -R /tmp/out/')
        sdc_executor.execute_shell(f'rm -R {tmp_directory_local_fs_late_record}')


@sdc_min_version('5.6.0')
def test_local_fs_whole_file_with_colon_in_file_name(sdc_builder, sdc_executor):
    """
    Tests Local FS cannot copy files via WhOle File Format if they have a colon character in the file name and throws a
    record error but continues to process the remaining files.

    The test creates a file with a colon on the name and another file with an accepted name and then tries to copy them
    to the local file system, but only the second file should be copied. The file with the incorrect name is set to be
    processed first to check the remaining files are processed.

    The pipeline looks like:
        Directory >> Local FS >= Wiretap
    """
    temp_dir = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    temp_dir_input = temp_dir + '/input/'
    temp_dir_output = temp_dir + '/output/'

    random_file_name = get_random_string(string.ascii_letters, 10)

    correct_file_name = f"2_correct_name_{random_file_name}"
    correct_file_path = temp_dir_input + correct_file_name
    final_correct_file_path = temp_dir_output + correct_file_name
    correct_file_content = 'This file should be correctly processed' \
                           '.'
    incorrect_file_name = f"1_incorrect:name_{random_file_name}"
    incorrect_file_content = 'This file should raise a Record Error.'
    incorrect_file_path = temp_dir_input + incorrect_file_name
    final_incorrect_file_path = temp_dir_output + incorrect_file_name

    logger.info(f'Creating directory {temp_dir}...')
    sdc_executor.execute_shell(f"mkdir {temp_dir}")

    logger.info(f'Creating directory {temp_dir_input}...')
    sdc_executor.execute_shell(f"mkdir {temp_dir_input}")

    logger.info(f'Creating files {incorrect_file_path} and {correct_file_path} in {temp_dir_input}...')
    sdc_executor.execute_shell(f"echo {incorrect_file_content} > {incorrect_file_path}")
    sdc_executor.execute_shell(f"echo {correct_file_content} > {correct_file_path}")

    logger.info('Checking the files have been correctly created...')
    assert sdc_executor.execute_shell(f'cat {correct_file_path}').stdout == correct_file_content + '\n'
    assert sdc_executor.execute_shell(f'cat {incorrect_file_path}').stdout == incorrect_file_content + '\n'

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory')
    directory.set_attributes(
        data_format='WHOLE_FILE',
        files_directory=temp_dir_input,
        file_name_pattern='*'
    )

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(
        data_format='WHOLE_FILE',
        file_type='WHOLE_FILE',
        files_prefix='',
        directory_template=temp_dir_output,
        file_name_expression='${record:value("/fileInfo/filename")}'
    )

    wiretap = builder.add_wiretap()

    directory >> local_fs >= wiretap.destination

    pipeline = builder.build().configure_for_environment()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        output_records = wiretap.output_records
        assert len(output_records) == 1
        assert correct_file_path in output_records[0].field['sourceFileInfo']['file'].value
        assert sdc_executor.execute_shell(f'cat {final_correct_file_path}').stdout == correct_file_content + '\n'

        error_records = wiretap.error_records
        assert len(error_records) == 1
        assert error_records[0].header['errorCode'] == 'HADOOPFS_64'
        assert "No such file or directory" in sdc_executor.execute_shell(f'cat {final_incorrect_file_path}').stderr

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)

        logger.info(f'Deleting the folder {temp_dir} and its contents...')
        sdc_executor.execute_shell(f'rm -R {temp_dir}')
