# Copyright 2020 StreamSets Inc.
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
import string
import tempfile

import pytest
from streamsets.testframework.markers import ftp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

FTP_DEST_CLIENT_NAME = 'com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget'


@ftp
@sdc_min_version('3.9.0')
def test_ftp_destination(sdc_builder, sdc_executor, ftp):
    """Smoke test FTP destination. We first create a local file using Local FS destination stage and use that file
    for FTP destination stage to see if it gets successfully uploaded.
    The pipelines looks like:
        dev_raw_data_source >> local_fs
        directory >> sftp_ftp_client
    """
    # Our destination FTP file name
    ftp_file_name = get_random_string(string.ascii_letters, 10)
    # Local temporary directory where we will create a source file to be uploaded to FTP server
    local_tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # Build source file pipeline logic
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='Hello World!', stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(directory_template=local_tmp_directory, data_format='TEXT')

    dev_raw_data_source >> local_fs
    local_fs_pipeline = builder.build('Local FS Pipeline')

    builder = sdc_builder.get_pipeline_builder()

    # Build FTP destination pipeline logic
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern='sdc*', files_directory=local_tmp_directory)

    sftp_ftp_client = builder.add_stage(name=FTP_DEST_CLIENT_NAME)
    sftp_ftp_client.file_name_expression = ftp_file_name

    directory >> sftp_ftp_client
    sftp_ftp_client_pipeline = builder.build('FTP Destination Pipeline Simple').configure_for_environment(ftp)

    sdc_executor.add_pipeline(local_fs_pipeline, sftp_ftp_client_pipeline)

    # Start source file creation pipeline and assert file has been created with expected number of records
    sdc_executor.start_pipeline(local_fs_pipeline).wait_for_finished()
    history = sdc_executor.get_pipeline_history(local_fs_pipeline)

    try:
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1

        # Start FTP upload (destination) file pipeline and assert pipeline has processed expected number of files
        sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)
        history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1

        # Read FTP destination file and compare our source data to assert
        assert ftp.get_string(ftp_file_name) == dev_raw_data_source.raw_data

    finally:
        # Delete the test FTP destination file we created
        client = ftp.client
        client.delete(ftp_file_name)
        client.quit()


@ftp
@sdc_min_version('5.7.0')
@pytest.mark.parametrize('prefix', ['_tmp_', '_alex_', ""])
def test_configured_prefix(sdc_builder, sdc_executor, ftp, prefix):
    """Test for configured temporary prefixes.
    We first create a local file using Local FS destination stage and use that file for FTP destination stage to see
    if it gets successfully uploaded.
    The pipelines looks like:
        dev_raw_data_source >> local_fs
        directory >> sftp_ftp_client >= wiretap.destination
    """
    # Our destination FTP file name
    ftp_file_name = get_random_string(string.ascii_letters, 10)
    # Local temporary directory where we will create a source file to be uploaded to FTP server
    local_tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # Build source file pipeline logic
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='Hello World!', stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(directory_template=local_tmp_directory, data_format='TEXT')

    dev_raw_data_source >> local_fs
    local_fs_pipeline = builder.build('Local FS Pipeline')

    builder = sdc_builder.get_pipeline_builder()

    # Build FTP destination pipeline logic
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern='sdc*', files_directory=local_tmp_directory)

    consent = False
    if prefix == "":
        consent = True

    sftp_ftp_client = builder.add_stage(name=FTP_DEST_CLIENT_NAME).set_attributes(file_name_expression=ftp_file_name,
                                                                                  temporary_prefix=prefix,
                                                                                  user_consent_for_prefix_omission=consent)

    wiretap = builder.add_wiretap()

    directory >> sftp_ftp_client >= wiretap.destination

    sftp_ftp_client_pipeline = builder.build().configure_for_environment(ftp)

    sdc_executor.add_pipeline(local_fs_pipeline, sftp_ftp_client_pipeline)

    # Start source file creation pipeline and assert file has been created with expected number of records
    sdc_executor.start_pipeline(local_fs_pipeline).wait_for_finished()

    try:
        # Start FTP upload (destination) file pipeline and assert pipeline has processed expected number of files
        sdc_executor.start_pipeline(sftp_ftp_client_pipeline)
        sdc_executor.wait_for_pipeline_metric(sftp_ftp_client_pipeline, 'output_record_count', 1)
        sdc_executor.stop_pipeline(sftp_ftp_client_pipeline)

        assert ftp_file_name in str(wiretap.output_records)

        # Read FTP destination file and compare our source data to assert
        assert ftp.get_string(ftp_file_name) == dev_raw_data_source.raw_data

    finally:
        # Delete the test FTP destination file we created
        client = ftp.client
        client.delete(ftp_file_name)
        client.quit()
