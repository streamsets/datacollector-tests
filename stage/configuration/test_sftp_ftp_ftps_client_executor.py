
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
from streamsets.testframework.markers import ftp, sftp, sdc_min_version
from streamsets.testframework.utils import get_random_string

REMOTE_DESTINATION_STAGE = 'com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget'
REMOTE_EXECUTOR_STAGE = 'com_streamsets_pipeline_stage_executor_remote_RemoteLocationDExecutor'

EXISTING_FILE_DATA = 'This is old information'
INPUT_DATA = 'Hello World'
FTP_CHROOT_DIR = '/'
SFTP_CHROOT_DIR = '/sftp_dir'

pytestmark = sdc_min_version('3.16.0')

logger = logging.getLogger(__name__)

from streamsets.testframework.decorators import stub


@sdc_min_version("3.22.0")
@sftp
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'NONE'},
                                              {'authentication': 'PASSWORD'},
                                              {'authentication': 'PRIVATE_KEY'}])
def test_authentication(sdc_builder, sdc_executor, sftp, stage_attributes):
    """Test SFTP and FTP/FTPS executor. We first create a local file using shell and use
    that file for SFTP/FTP/FTPS executor. We then assert the ingested data using wiretap.
    The pipelines look like:
        Local FS  >>  FTP/SFTP Destination
        Local FS  >=  Pipeline Finisher
                      FTP/SFTP Destination >= FTP/SFTP Executor
    """
    # Our origin SFTP/FTP/FTPS file name
    sftp_ftp_file_name = get_random_string(string.ascii_letters, 10)
    local_tmp_directory = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_file_name = f'sdc-{get_random_string(string.ascii_letters, 5)}'
    raw_text_data = 'Hello World!'

    sdc_executor.execute_shell(f'mkdir {local_tmp_directory}/')
    sdc_executor.execute_shell(f'echo {raw_text_data} >> {local_tmp_directory}/{local_file_name}')

    # Build Consumer Pipeline
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern='sdc*', files_directory=local_tmp_directory)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')

    sftp_ftp_client = builder.add_stage(name=REMOTE_DESTINATION_STAGE)
    sftp_ftp_client.set_attributes(file_name_expression=sftp_ftp_file_name)

    wiretap = builder.add_wiretap()

    directory >> sftp_ftp_client >= wiretap.destination
    directory >= pipeline_finished_executor

    sftp_ftp_client.authentication = stage_attributes['authentication']

    sftp_ftp_client_pipeline = builder.build('SFTP Executor Pipeline - Authentication').configure_for_environment(sftp)

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    # Start SFTP/FTP/FTPS upload (destination) file pipeline and assert pipeline has processed expected number of files
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_finished()
    history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)

    try:
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count >= 5
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0

        assert sftp.get_string(os.path.join(sftp.path, sftp_ftp_file_name)).strip() == raw_text_data

        # Delete the test SFTP origin file we created
        transport, client = sftp.client
        client.remove(os.path.join(sftp.path, sftp_ftp_file_name))
    finally:
        client.close()
        transport.close()
        sdc_executor.execute_shell(f'rm -R {local_tmp_directory}')


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_data_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'file_exists_action': 'OVERWRITE', 'task': 'MOVE_FILE'},
                                              {'file_exists_action': 'TO_ERROR', 'task': 'MOVE_FILE'}])
def test_file_exists_action(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_file_name_expression(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_client_certificate_keystore_type': 'JKS',
                                               'use_client_certificate_for_ftps': True},
                                              {'ftps_client_certificate_keystore_type': 'PKCS12',
                                               'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_data_channel_protection_level': 'CLEAR'},
                                              {'ftps_data_channel_protection_level': 'PRIVATE'}])
def test_ftps_data_channel_protection_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_mode': 'EXPLICIT'}, {'ftps_mode': 'IMPLICIT'}])
def test_ftps_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE'}])
def test_ftps_truststore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE'}])
def test_ftps_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'ALLOW_ALL'},
                                              {'ftps_truststore_provider': 'FILE'},
                                              {'ftps_truststore_provider': 'JVM_DEFAULT'}])
def test_ftps_truststore_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE', 'ftps_truststore_type': 'JKS'},
                                              {'ftps_truststore_provider': 'FILE', 'ftps_truststore_type': 'PKCS12'}])
def test_ftps_truststore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': True}])
def test_known_hosts_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'path_relative_to_user_home_directory': False},
                                              {'path_relative_to_user_home_directory': True}])
def test_path_relative_to_user_home_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'PLAIN_TEXT'}])
def test_private_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'FILE'}])
def test_private_key_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY'}])
def test_private_key_passphrase(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'FILE'},
                                              {'authentication': 'PRIVATE_KEY', 'private_key_provider': 'PLAIN_TEXT'}])
def test_private_key_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_resource_url(sdc_builder, sdc_executor):
    pass


@sdc_min_version('3.22.0')
@sftp
def test_sftp_protocol(sdc_builder, sdc_executor, sftp):
    """Test SFTP executor. We first create a local file using shell and use that file for SFTP/FTP/FTPS executor.
    We then assert the ingested data using wiretap.
    The pipelines look like:
        Local FS  >>  SFTP Destination
        Local FS  >=  Pipeline Finisher
                      SFTP Destination >= SFTP Executor
    """

    # Our destination SFTP/FTP/FTPS file name
    sftp_ftp_file_name = get_random_string(string.ascii_letters, 10)
    # Local temporary directory where we will create a source file to be uploaded to SFTP/FTP/FTPS server
    local_tmp_directory = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_file_name = f'sdc-{get_random_string(string.ascii_letters, 5)}'
    raw_data = 'Hello World!'

    sdc_executor.execute_shell(f'mkdir {local_tmp_directory}/')
    sdc_executor.execute_shell(f'echo {raw_data} >> {local_tmp_directory}/{local_file_name}')

    # Build Consumer Pipeline
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern='sdc*', files_directory=local_tmp_directory)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')

    destination_stage = builder.add_stage(name=REMOTE_DESTINATION_STAGE)
    destination_stage.set_attributes(file_name_expression=sftp_ftp_file_name)

    wiretap = builder.add_wiretap()

    directory >> destination_stage >= wiretap.destination
    directory >= pipeline_finished_executor

    destination_stage.protocol = 'SFTP'

    sftp_ftp_client_pipeline = builder.build('SFTP Destination Pipeline - Protocol').configure_for_environment(sftp)

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    # Start SFTP/FTP/FTPS upload (destination) file pipeline and assert pipeline has processed expected number of files
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_finished()
    history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)

    try:
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count >= 5
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0

        # Read SFTP destination file and compare our source data to assert
        assert sftp.get_string(os.path.join(sftp.path, sftp_ftp_file_name)).strip() == raw_data

        # Delete the test SFTP origin file we created
        transport, client = sftp.client
        client.remove(os.path.join(sftp.path, sftp_ftp_file_name))

    finally:
        client.close()
        transport.close()
        sdc_executor.execute_shell(f'rm -R {local_tmp_directory}')


@sdc_min_version('3.22.0')
@ftp
@pytest.mark.parametrize('stage_attributes', [{'protocol': 'FTP'},
                                              {'protocol': 'FTPS'}
                                              ])
def test_ftp_protocol(sdc_builder, sdc_executor, ftp, stage_attributes):
    """Test FTP/FTPS executor. We first create a local file using shell and use that file for SFTP/FTP/FTPS executor.
    We then assert the ingested data using wiretap.
    The pipelines look like:
        Local FS  >>  FTP Destination
        Local FS  >=  Pipeline Finisher
                      FTP Destination >= FTP Executor
    """
    if ('FTPS' in ftp.ftp_type) and stage_attributes['protocol'] == 'FTP':
        pytest.skip('FTP protocol only runs with ftp-type FTP')
    elif ftp.ftp_type == 'FTP' and stage_attributes['protocol'] == 'FTPS':
        pytest.skip('FTPS protocol only runs with ftp-type FTPS')

    # Our destination SFTP/FTP/FTPS file name
    sftp_ftp_file_name = get_random_string(string.ascii_letters, 10)
    # Local temporary directory where we will create a source file to be uploaded to SFTP/FTP/FTPS server
    local_tmp_directory = os.path.join('~', tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_file_name = f'sdc-{get_random_string(string.ascii_letters, 5)}'
    raw_data = 'Hello World!'

    sdc_executor.execute_shell(f'mkdir {local_tmp_directory}/')
    sdc_executor.execute_shell(f'echo {raw_data} >> {local_tmp_directory}/{local_file_name}')

    # Build Consumer Pipeline
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern='sdc*', files_directory=local_tmp_directory)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')

    destination_stage = builder.add_stage(name=REMOTE_DESTINATION_STAGE)
    destination_stage.set_attributes(file_name_expression=sftp_ftp_file_name)

    wiretap = builder.add_wiretap()

    directory >> destination_stage >= wiretap.destination
    directory >= pipeline_finished_executor

    destination_stage.protocol = stage_attributes['protocol']

    if stage_attributes['protocol'] == 'FTP':
        sftp_ftp_client_pipeline = builder.build('FTP Destination Pipeline - Protocol').configure_for_environment(ftp)
    else:
        sftp_ftp_client_pipeline = builder.build('FTPS Destination Pipeline - Protocol').configure_for_environment(ftp)

    sdc_executor.add_pipeline(sftp_ftp_client_pipeline)

    # Start SFTP/FTP/FTPS upload (destination) file pipeline and assert pipeline has processed expected number of files
    sdc_executor.start_pipeline(sftp_ftp_client_pipeline).wait_for_finished()
    history = sdc_executor.get_pipeline_history(sftp_ftp_client_pipeline)

    try:
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count >= 1
        assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count >= 5
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0

        # Read FTP destination file and compare our source data to assert
        assert ftp.get_string(os.path.join(ftp.path, sftp_ftp_file_name)).strip() == raw_data
    finally:
        # Delete the test FTP destination file we created
        client = ftp.client
        client.delete(sftp_ftp_file_name)
        client.quit()
        sdc_executor.execute_shell(f'rm -R {local_tmp_directory}')


@stub
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': False}, {'strict_host_checking': True}])
def test_strict_host_checking(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'MOVE_FILE'}])
def test_target_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'DELETE_FILE'}, {'task': 'MOVE_FILE'}])
def test_task(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': False},
                                              {'use_client_certificate_for_ftps': True}])
def test_use_client_certificate_for_ftps(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}, {'authentication': 'PRIVATE_KEY'}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass


def _create_file_on_local_fs_pipeline(sdc_builder, local_tmp_directory):
    # Build source file pipeline logic
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=INPUT_DATA, stop_after_first_batch=True)

    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(directory_template=local_tmp_directory, data_format='TEXT')

    dev_raw_data_source >> local_fs
    return builder.build('Dev Raw Data to Local FS - Producer Pipeline')


def _create_producer_consumer_pipeline(sdc_builder, local_tmp_directory, file_name):
    """Helper function to create 2 pipelines:

    1.  Producer pipeline to create a local file in running SDC's docker container
        Pipeline looks like:
            Dev Raw Data Source >> Local FS

    2.  Consumer Pipeline that reads from Local FS and writes to FTP/SFTP Destination.
        Destination is connected to FTP/SFTP Executor which is the stage that is being tested here.
        The Executor stage is configured to point to the same server as the Destination stage.
        Pipeline looks like:
            Local FS  >>  FTP/SFTP Destination
            Local FS  >=  Pipeline Finisher
                          FTP/SFTP Destination >= FTP/SFTP Executor

    The basic operation of the consumer pipeline is as follows:
        1.  Read the file from local tmpfs and write to FTP/SFTP Destination
        2.  Perform Post Processing Action on the file specified by the file_name_expression in the remote location
    """
    local_fs_pipeline = _create_file_on_local_fs_pipeline(sdc_builder, local_tmp_directory)

    # Build Consumer Pipeline
    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='WHOLE_FILE', file_name_pattern='sdc*', files_directory=local_tmp_directory)

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')

    destination_stage = builder.add_stage(name=REMOTE_DESTINATION_STAGE)
    destination_stage.set_attributes(file_name_expression=file_name)

    executor_stage = builder.add_stage(name=REMOTE_EXECUTOR_STAGE)
    wiretap = builder.add_wiretap()

    directory >> destination_stage >= [executor_stage, wiretap.destination]
    directory >= pipeline_finished_executor

    test_executor_pipeline = builder.build('Local FS - Remote Target - Remote Executor -- Consumer Pipeline')
    return local_fs_pipeline, test_executor_pipeline, wiretap


def _execute_producer_pipeline(sdc_executor, local_fs_pipeline):
    # Start source file creation pipeline and assert file has been created with expected number of records
    sdc_executor.start_pipeline(local_fs_pipeline).wait_for_finished()
    history = sdc_executor.get_pipeline_history(local_fs_pipeline)

    assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 1
    assert history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count == 1


@ftp
def test_ftp_delete_file(sdc_builder, sdc_executor, ftp):
    _test_delete_file(sdc_builder, sdc_executor, ftp, FTP_CHROOT_DIR)


@sftp
def test_sftp_delete_file(sdc_builder, sdc_executor, sftp):
    _test_delete_file(sdc_builder, sdc_executor, sftp, SFTP_CHROOT_DIR)


def _test_delete_file(sdc_builder, sdc_executor, client, chroot_dir):
    """Test delete functionality.
        Verify that the file has been deleted from the remote location after pipeline
        has finished.
    """
    file_name = get_random_string(string.ascii_letters, 10)
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    local_fs_pipeline, executor_pipeline, wiretap_stage = _create_producer_consumer_pipeline(sdc_builder,
                                                                                             tmp_directory,
                                                                                             file_name)

    sdc_executor.add_pipeline(local_fs_pipeline)
    _execute_producer_pipeline(sdc_executor, local_fs_pipeline)

    executor_pipeline.configure_for_environment(client)

    executor_stage = executor_pipeline[3]
    executor_stage.set_attributes(task='DELETE_FILE', file_name_expression=file_name)
    sdc_executor.add_pipeline(executor_pipeline)

    try:
        sdc_executor.start_pipeline(executor_pipeline).wait_for_finished()

        logger.info("Checking if file %s is present on the remote server at path: %s ...",
                    file_name, chroot_dir)
        assert file_name not in client.list_files(chroot_dir)
    finally:
        pass  # No cleanup required here


@ftp
def test_ftp_move_file(sdc_builder, sdc_executor, ftp):
    _test_move_file(sdc_builder, sdc_executor, ftp, FTP_CHROOT_DIR)


@sftp
def test_sftp_move_file(sdc_builder, sdc_executor, sftp):
    _test_move_file(sdc_builder, sdc_executor, sftp, SFTP_CHROOT_DIR)


def _test_move_file(sdc_builder, sdc_executor, client, chroot_dir):
    """Test move functionality.
        Verify that the file has been moved from it's current directory to the new directory in the remote location
        The new directory did not contain the file previously
    """
    file_name = get_random_string(string.ascii_letters, 10)
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    local_fs_pipeline, executor_pipeline, wiretap_stage = _create_producer_consumer_pipeline(sdc_builder,
                                                                                             tmp_directory,
                                                                                             file_name)

    sdc_executor.add_pipeline(local_fs_pipeline)
    _execute_producer_pipeline(sdc_executor, local_fs_pipeline)

    executor_pipeline.configure_for_environment(client)

    executor_stage = executor_pipeline[3]
    target_dir = get_random_string(string.ascii_letters, 10)
    executor_stage.set_attributes(task='MOVE_FILE',
                                  file_name_expression=file_name,
                                  target_directory=target_dir)
    sdc_executor.add_pipeline(executor_pipeline)

    new_location = os.path.join(chroot_dir, target_dir)
    try:
        sdc_executor.start_pipeline(executor_pipeline).wait_for_finished()

        logger.info("Checking if file %s is present on the remote server at path: %s ...",
                    file_name, chroot_dir)
        assert file_name not in client.list_files(chroot_dir)
        logger.info("Checking if file %s is present on the remote server at new location: %s ...",
                    file_name, new_location)
        assert file_name in client.list_files(new_location)
    finally:
        client.rm(os.path.join(new_location, file_name))
        client.rmdir(new_location)


@ftp
def test_ftp_move_overwrite_if_file_already_exists(sdc_builder, sdc_executor, ftp):
    _test_move_overwrite_if_file_already_exists(sdc_builder, sdc_executor, ftp, FTP_CHROOT_DIR)


@sftp
def test_sftp_move_overwrite_if_file_already_exists(sdc_builder, sdc_executor, sftp):
    _test_move_overwrite_if_file_already_exists(sdc_builder, sdc_executor, sftp, SFTP_CHROOT_DIR)


def _test_move_overwrite_if_file_already_exists(sdc_builder, sdc_executor, client, chroot_dir):
    """Test move functionality.
    Verify that the file has been moved from it's current directory to the new directory in the remote location
    The new directory did contain the a file with the filename previously.
    Verify that the old file has been overridden with the contents of the new file
    """
    file_name = get_random_string(string.ascii_letters, 10)
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    local_fs_pipeline, executor_pipeline, wiretap_stage = _create_producer_consumer_pipeline(sdc_builder,
                                                                                             tmp_directory,
                                                                                             file_name)

    sdc_executor.add_pipeline(local_fs_pipeline)
    _execute_producer_pipeline(sdc_executor, local_fs_pipeline)

    executor_pipeline.configure_for_environment(client)

    executor_stage = executor_pipeline[3]
    target_dir = get_random_string(string.ascii_letters, 10)
    executor_stage.set_attributes(task='MOVE_FILE',
                                  file_name_expression=file_name,
                                  target_directory=target_dir,
                                  file_exists_action='OVERWRITE')
    sdc_executor.add_pipeline(executor_pipeline)

    new_location = os.path.join(chroot_dir, target_dir)
    try:
        client.mkdir(new_location)
        client.put_string(os.path.join(new_location, file_name), EXISTING_FILE_DATA)

        sdc_executor.start_pipeline(executor_pipeline).wait_for_finished()

        logger.info("Checking if file %s is present on the remote server at path: %s ...",
                    file_name, chroot_dir)
        assert file_name not in client.list_files(chroot_dir)
        logger.info("Checking if file %s is present on the remote server at new location: %s ...",
                    file_name, new_location)
        assert file_name in client.list_files(new_location)
        logger.info("Checking that the existing file %s at location %s has been overwritten with newer file...",
                    file_name, new_location)
        assert client.get_string(os.path.join(new_location, file_name)).rstrip('\n') == INPUT_DATA
    finally:
        client.rm(os.path.join(new_location, file_name))
        client.rmdir(new_location)


@ftp
def test_ftp_move_send_to_error_if_file_already_exists(sdc_builder, sdc_executor, ftp):
    _test_move_send_to_error_if_file_already_exists(sdc_builder, sdc_executor, ftp, FTP_CHROOT_DIR)


@sftp
def test_sftp_move_send_to_error_if_file_already_exists(sdc_builder, sdc_executor, sftp):
    _test_move_send_to_error_if_file_already_exists(sdc_builder, sdc_executor, sftp, SFTP_CHROOT_DIR)


def _test_move_send_to_error_if_file_already_exists(sdc_builder, sdc_executor, client, chroot_dir):
    """Test move functionality.
    Verify that the file has been note moved from it's current directory to the new directory in the remote location
    The new directory did contain the a file with the filename previously.
    Verify that the executor stage produced an error record
    Verify that the new file is still present in it's current location
    Verify that the old file's contents have not been changed
    """
    file_name = get_random_string(string.ascii_letters, 10)
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    local_fs_pipeline, executor_pipeline, wiretap_stage = _create_producer_consumer_pipeline(sdc_builder,
                                                                                             tmp_directory,
                                                                                             file_name)

    sdc_executor.add_pipeline(local_fs_pipeline)
    _execute_producer_pipeline(sdc_executor, local_fs_pipeline)

    executor_pipeline.configure_for_environment(client)

    executor_stage = executor_pipeline[3]
    target_dir = get_random_string(string.ascii_letters, 10)
    executor_stage.set_attributes(task='MOVE_FILE',
                                  file_name_expression=file_name,
                                  target_directory=target_dir,
                                  file_exists_action='TO_ERROR')
    sdc_executor.add_pipeline(executor_pipeline)

    new_location = os.path.join(chroot_dir, target_dir)
    try:
        client.mkdir(new_location)
        client.put_string(os.path.join(new_location, file_name), EXISTING_FILE_DATA)

        sdc_executor.start_pipeline(executor_pipeline)
        sdc_executor.get_pipeline_status(executor_pipeline).wait_for_status('FINISHED')

        logger.info("Checking if file %s is STILL present on the remote server at path: %s ...",
                    file_name, chroot_dir)
        assert file_name in client.list_files(chroot_dir)
        logger.info("Checking if file %s is present on the remote server at new location: %s ...",
                    file_name, new_location)
        assert file_name in client.list_files(new_location)
        logger.info("Checking that the existing file %s at location %s has NOT been overwritten with newer file...",
                    file_name, new_location)
        assert client.get_string(os.path.join(new_location, file_name)) == EXISTING_FILE_DATA

        logger.info("Checking for 'file already exists' error code in executor stage's error records")

        assert 'REMOTE_LOCATION_EXECUTOR_04' in [record.header['errorCode']
                                                 for record in wiretap_stage.error_records]
    finally:
        client.rm(os.path.join(chroot_dir, file_name))
        client.rm(os.path.join(new_location, file_name))
        client.rmdir(new_location)


@ftp
def test_ftp_file_specified_by_file_path_not_present(sdc_builder, sdc_executor, ftp):
    _test_file_specified_by_file_path_not_present(sdc_builder, sdc_executor, ftp, FTP_CHROOT_DIR)


@sftp
def test_sftp_file_specified_by_file_path_not_present(sdc_builder, sdc_executor, sftp):
    _test_file_specified_by_file_path_not_present(sdc_builder, sdc_executor, sftp, SFTP_CHROOT_DIR)


def _test_file_specified_by_file_path_not_present(sdc_builder, sdc_executor, client, chroot_dir):
    """Test to verify that error record is generated when file is not present in remote server
    """
    file_name = get_random_string(string.ascii_letters, 10)
    nonexistent_file_name = get_random_string(string.ascii_letters, 10)
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    local_fs_pipeline, executor_pipeline, wiretap_stage = _create_producer_consumer_pipeline(sdc_builder,
                                                                                             tmp_directory,
                                                                                             file_name)

    sdc_executor.add_pipeline(local_fs_pipeline)
    _execute_producer_pipeline(sdc_executor, local_fs_pipeline)

    executor_pipeline.configure_for_environment(client)

    executor_stage = executor_pipeline[3]
    executor_stage.set_attributes(task='DELETE_FILE', file_name_expression=nonexistent_file_name)
    sdc_executor.add_pipeline(executor_pipeline)

    try:
        sdc_executor.start_pipeline(executor_pipeline).wait_for_finished()

        logger.info("Checking for 'file not present' error code in executor stage's error records")
        assert 'REMOTE_LOCATION_EXECUTOR_02' in [record.header['errorCode']
                                                 for record in wiretap_stage.error_records]
    finally:
        client.rm(os.path.join(chroot_dir, file_name))  # This file is remaining from when destination stage copied it
