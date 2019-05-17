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
import sched
import string
import time

import pytest
from hadoop.io import SequenceFile
from streamsets.testframework.markers import cluster, large, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_LISTENING_PORT = 20000

# Duration for which pipeline would run in case of long-running test/s.
LARGE_TEST_DURATION_IN_SECS = 3600
# Duration at which pipeline status will be checked for long-running test/s.
LARGE_TEST_STATUS_CHECK_DURATION_IN_SECS = int(LARGE_TEST_DURATION_IN_SECS / 10)
# Statuses that signify a pipeline that isn't failing.
SUCCESS_STATUSES = ['EDITED', 'STARTING', 'RUNNING']

# Current time in seconds since the epoch
CURRENT_TIME = time.time()

PRODUCT_DATA = [
    {'name': 'iphone', 'price': 649.99, 'release': CURRENT_TIME},
    {'name': 'pixel', 'price': 649.89, 'release': CURRENT_TIME - 60 * 5},  # -5 minutes
    {'name': 'galaxy', 'price': 549.89, 'release': CURRENT_TIME - 60 * 10}  # -10 minutes
]

PRODUCT_DATA_FIX = [
    {'name': 'iphone', 'price': 649.99, 'release': 1.5535},
    {'name': 'pixel', 'price': 649.89, 'release': 1.5545},
    {'name': 'galaxy', 'price': 549.89, 'release': 1.5565}
]


def create_hadoop_fs_dest_pipeline(pipeline_builder, pipeline_title, hdfs_directory, hadoop_fs):
    """Helper function to create and return a pipeline with Hadoop FS destination
    The Deduplicator assures there is only one ingest to HDFS. The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> hadoop_fs
                                                   >> trash
    """
    raw_data = '\n'.join(json.dumps(product) for product in PRODUCT_DATA)
    logger.info('Pipeline will write to HDFS directory %s ...', hdfs_directory)

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    # triggered the destination file to be closed after writing all data.
    hadoop_fs.set_attributes(max_records_in_file=len(PRODUCT_DATA))

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> hadoop_fs
    record_deduplicator >> trash
    return pipeline_builder.build(title=pipeline_title)


@cluster('cdh', 'hdp')
def test_hadoop_fs_destination(sdc_builder, sdc_executor, cluster):
    """Test Hadoop FS destination with file prefix and suffix
    The data is written to a HDFS file, then check the contents.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    FILES_PREFIX, FILES_SUFFIX = 'stages', 'txt'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             files_prefix=FILES_PREFIX,
                             files_suffix=FILES_SUFFIX)

    pipeline = create_hadoop_fs_dest_pipeline(pipeline_builder,
                                              'Hadoop FS Destination',
                                              hdfs_directory,
                                              hadoop_fs)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_fs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_fs_files) == 1

        hdfs_fs_filename = hdfs_fs_files[0]
        assert hdfs_fs_filename.startswith(FILES_PREFIX)
        assert hdfs_fs_filename.endswith(FILES_SUFFIX)

        with cluster.hdfs.client.read(f'{hdfs_directory}/{hdfs_fs_filename}') as reader:
            file_contents = reader.read()
        assert {tuple(json.loads(line).items())
                for line in file_contents.decode().split()} == {tuple(stage.items())
                                                                for stage in PRODUCT_DATA}
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hadoop_fs_destination_user(sdc_builder, sdc_executor, cluster):
    """Test Hadoop FS destination with HDFS user
    The data is written to a HDFS file, then check the owner of the file.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    HDFS_USER = 'foo'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             hdfs_user=HDFS_USER)

    pipeline = create_hadoop_fs_dest_pipeline(pipeline_builder,
                                              'Hadoop FS Destination User',
                                              hdfs_directory,
                                              hadoop_fs)

    pipeline.configure_for_environment(cluster)
    hadoop_fs.hdfs_user = HDFS_USER  # Some cluster environments overwrite this property
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_files) == 1
        status = cluster.hdfs.client.status(f'{hdfs_directory}/{hdfs_files[0]}')
        assert status['owner'] == HDFS_USER
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hadoop_fs_destination_time_basis(sdc_builder, sdc_executor, cluster):
    """Test Hadoop FS destination with time basis
    The data is written to a HDFS file in every 10 minute interval based on release time.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    DIR_TEMPLATE = hdfs_directory + '/${YY()}${MM()}${DD()}${hh()}${every(10, mm())}'
    TIME_BASIS = "${time:millisecondsToDateTime(record:value('/release') * 1000)}"
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=DIR_TEMPLATE,
                             time_basis=TIME_BASIS)

    pipeline = create_hadoop_fs_dest_pipeline(pipeline_builder,
                                              'Hadoop FS Destination Time Basis',
                                              hdfs_directory,
                                              hadoop_fs)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_dirs = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_dirs) == 2
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hadoop_fs_origin_simple(sdc_builder, sdc_executor, cluster):
    """Write a simple file into a Hadoop FS folder with a randomly-generated name and confirm that the Hadoop FS origin
    successfully reads it. Because cluster mode pipelines don't support snapshots, we do this verification using a
    second standalone pipeline whose origin is an SDC RPC written to by the Hadoop FS pipeline. Specifically, this would
    look like:
    Hadoop FS pipeline:
        hadoop_fs_origin >> sdc_rpc_destination
    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """
    hadoop_fs_folder = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))

    # Build the Hadoop FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    hadoop_fs = builder.add_stage('Hadoop FS', type='origin')
    hadoop_fs.data_format = 'TEXT'
    hadoop_fs.input_paths.append(hadoop_fs_folder)

    sdc_rpc_destination = builder.add_stage('SDC RPC', type='destination')
    sdc_rpc_destination.sdc_rpc_connection.append('{}:{}'.format(sdc_executor.server_host,
                                                                 SDC_RPC_LISTENING_PORT))
    sdc_rpc_destination.sdc_rpc_id = get_random_string(string.ascii_letters, 10)

    hadoop_fs >> sdc_rpc_destination
    hadoop_fs_pipeline = builder.build(title='Hadoop FS pipeline').configure_for_environment(cluster)
    hadoop_fs_pipeline.configuration['executionMode'] = 'CLUSTER_BATCH'

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    sdc_rpc_origin = builder.add_stage('SDC RPC', type='origin')
    sdc_rpc_origin.sdc_rpc_listening_port = SDC_RPC_LISTENING_PORT
    sdc_rpc_origin.sdc_rpc_id = sdc_rpc_destination.sdc_rpc_id
    # Since YARN jobs take a while to get going, set RPC origin batch wait time to 5 min. to avoid
    # getting an empty batch in the snapshot.
    sdc_rpc_origin.batch_wait_time_in_secs = 300

    trash = builder.add_stage('Trash')

    sdc_rpc_origin >> trash
    snapshot_pipeline = builder.build(title='Snapshot pipeline')

    # Add both pipelines we just created to SDC and start writing files to Hadoop FS with the HDFS client.
    sdc_executor.add_pipeline(hadoop_fs_pipeline, snapshot_pipeline)

    try:
        lines_in_file = ['hello', 'hi', 'how are you?']

        logger.debug('Writing file %s/file.txt to Hadoop FS ...', hadoop_fs_folder)
        cluster.hdfs.client.makedirs(hadoop_fs_folder)
        cluster.hdfs.client.write(os.path.join(hadoop_fs_folder, 'file.txt'), data='\n'.join(lines_in_file))

        # So here's where we do the clever stuff. We use SDC's capture snapshot endpoint to start and begin
        # capturing a snapshot from the snapshot pipeline. We do this, however, without using the synchronous
        # wait_for_finished function. That way, we can switch over and start the Hadoop FS pipeline. Once that one
        # completes, we can go back and do an assert on the snapshot pipeline's snapshot.
        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline, start_pipeline=True,
                                                                  wait=False)

        logger.debug('Starting Hadoop FS pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(hadoop_fs_pipeline)

        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=120).snapshot
        sdc_executor.stop_pipeline(snapshot_pipeline, force=True)
        lines_from_snapshot = [record.field['text'].value
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == lines_in_file
    finally:
        cluster.hdfs.client.delete(hadoop_fs_folder, recursive=True)


@sdc_min_version('3.2.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('filename', ['file.txt', '_tmp_file.txt', '.tmp_file.txt'])
def test_hadoop_fs_origin_standalone(sdc_builder, sdc_executor, cluster, filename):
    """Write a simple file into a Hadoop FS folder with a randomly-generated name and confirm that the Hadoop FS origin
    successfully reads it. Specifically, this would look like:
    Hadoop FS pipeline:
        hadoop_fs_origin >> trash
    """
    hadoop_fs_folder = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))

    # Build the Hadoop FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    hadoop_fs = builder.add_stage('Hadoop FS Standalone', type='origin')
    hadoop_fs.set_attributes(data_format='WHOLE_FILE', files_directory=hadoop_fs_folder, file_name_pattern='*')

    trash = builder.add_stage('Trash')

    hadoop_fs >> trash
    hadoop_fs_pipeline = builder.build(title='Hadoop FS pipeline').configure_for_environment(cluster)
    hadoop_fs_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(hadoop_fs_pipeline)

    try:
        lines_in_file = ['hello', 'hi', 'how are you?']

        logger.debug('Writing file %s/file.txt to Hadoop FS ...', hadoop_fs_folder)
        cluster.hdfs.client.makedirs(hadoop_fs_folder)
        cluster.hdfs.client.write(os.path.join(hadoop_fs_folder, filename), data='\n'.join(lines_in_file))

        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(hadoop_fs_pipeline, start_pipeline=True, wait=False)

        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=120).snapshot
        sdc_executor.stop_pipeline(hadoop_fs_pipeline, force=True)

        lines_from_snapshot = [record.field['fileInfo']['file']
                               for record in snapshot[hadoop_fs_pipeline[0].instance_name].output]

        assert lines_from_snapshot[0] == f"{hadoop_fs_folder}/{filename}"
    finally:
        cluster.hdfs.client.delete(hadoop_fs_folder, recursive=True)


@sdc_min_version('3.8.0')
@cluster('cdh', 'hdp')
def test_hadoop_fs_origin_standalone_glob_pattern(sdc_builder, sdc_executor, cluster):
    """Write two file into two different paths sharing a base root and try to read them using glob pattern.
     Specifically, this would look like:
     Hadoop FS pipeline:
        hadoop_fs_origin >> hadoop_fs_destination
    """
    DATA = [
        {'first_name': 'Alex', 'last_name': 'Sanchez'},
        {'first_name': 'Danilo', 'last_name': 'Viana'},
        {'first_name': 'Dima', 'last_name': 'Spivak'}
    ]

    random_string = get_random_string(string.ascii_letters, 10)
    hadoop_fs_folder_base = f'/tmp/{random_string}'

    # Build the Hadoop FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    hadoop_fs_origin = builder.add_stage('Hadoop FS Standalone', type='origin')
    hadoop_fs_origin.set_attributes(data_format='JSON', files_directory=f'{hadoop_fs_folder_base}/*/*',
                                    file_name_pattern='*', read_order='TIMESTAMP')

    hadoop_fs_destination = builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs_destination.set_attributes(data_format='JSON',
                                         directory_template=f'/tmp/out/{random_string}')

    hadoop_fs_origin >> hadoop_fs_destination
    hadoop_fs_pipeline = builder.build(title='Hadoop FS pipeline').configure_for_environment(cluster)
    hadoop_fs_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(hadoop_fs_pipeline)

    try:
        danilo = f'{{"first_name": "Danilo", "last_name": "Viana"}}'
        danilo_file = 'danilo.json'
        alex = f'{{"first_name": "Alex", "last_name": "Sanchez"}}'
        alex_file = 'alex.json'
        dima = f'{{"first_name": "Dima", "last_name": "Spivak"}}'
        dima_file = 'dima.json'

        logger.debug('Writing to directory %s ...', hadoop_fs_folder_base)
        cluster.hdfs.client.makedirs(hadoop_fs_folder_base)
        cluster.hdfs.client.makedirs(f'{hadoop_fs_folder_base}/spain')
        cluster.hdfs.client.makedirs(f'{hadoop_fs_folder_base}/usa')
        cluster.hdfs.client.makedirs(f'{hadoop_fs_folder_base}/spain/development')
        cluster.hdfs.client.makedirs(f'{hadoop_fs_folder_base}/spain/support')
        cluster.hdfs.client.makedirs(f'{hadoop_fs_folder_base}/usa/productivity')

        cluster.hdfs.client.write(os.path.join(hadoop_fs_folder_base, 'spain', 'development', alex_file), data=alex)
        cluster.hdfs.client.write(os.path.join(hadoop_fs_folder_base, 'spain', 'support', danilo_file), data=danilo)
        cluster.hdfs.client.write(os.path.join(hadoop_fs_folder_base, 'usa', 'productivity', dima_file), data=dima)

        sdc_executor.start_pipeline(hadoop_fs_pipeline).wait_for_pipeline_batch_count(3)

        hdfs_fs_files = cluster.hdfs.client.list(f'/tmp/out/{random_string}')
        assert len(hdfs_fs_files) == 1

        hdfs_fs_filename = hdfs_fs_files[0]

        with cluster.hdfs.client.read(f'/tmp/out/{random_string}/{hdfs_fs_filename}') as reader:
            file_contents = reader.read()

        assert {tuple(json.loads(line).items()) for line in
                file_contents.decode().split()} == {tuple(stage.items()) for stage in DATA}
    finally:
        sdc_executor.stop_pipeline(hadoop_fs_pipeline)
        cluster.hdfs.client.delete(hadoop_fs_folder_base, recursive=True)
        cluster.hdfs.client.delete(f'/tmp/out/{random_string}', recursive=True)


# Test developed to avoid multi-threading issues causing duplicated parsing of records, raised in SDC-10704
@sdc_min_version('3.2.0.0')
@cluster('cdh', 'hdp')
def test_hadoop_fs_origin_standalone_multi_thread(sdc_builder, sdc_executor, cluster):
    """
    - Write multiple files into a Hadoop FS folder with a randomly-generated name
    - Add a field renamer to add some delay
    - Confirm that the Hadoop FS origin successfully reads it only once
    Specifically, this would look like: Hadoop FS pipeline: hadoop_fs_origin >> trash
    """
    hadoop_fs_folder = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    hadoop_fs_filename = '_tmp_file.txt'
    hdfs_lines_per_file = 5500
    hdfs_files_count = 100
    hdfs_number_of_threads = 3

    # Build the Hadoop FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    hadoop_fs = builder.add_stage('Hadoop FS Standalone', type='origin')
    hadoop_fs.set_attributes(data_format='DELIMITED', files_directory=hadoop_fs_folder,
                             file_name_pattern='*', number_of_threads=hdfs_number_of_threads)

    field_renamer = builder.add_stage('Field Renamer')
    field_renamer.fields_to_rename = [{'fromFieldExpression': '/(.*)',
                                       'toFieldExpression': '/field$1'}]

    trash = builder.add_stage('Trash')

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    hadoop_fs >> field_renamer >> trash
    hadoop_fs >= pipeline_finished_executor

    hadoop_fs_pipeline = builder.build(title='Hadoop FS pipeline multithread').configure_for_environment(cluster)
    hadoop_fs_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(hadoop_fs_pipeline)

    try:
        file_line = (
            'CEEE11949C17DF1A1A742EDB90FA8C59,C361A55D95993FB3E20BB25E0A0FBC78,VTS,CRD,12,0.5,0.5,2.5,0,15.5,1,'
            '2013-01-13 02:07:00,2013-01-13 02:16:00,5,540,3.63,-73.982124,40.731152,-73.954063,'
            '40.77449,5222071822065944')
        file_lines = [file_line for _ in range(hdfs_lines_per_file)]

        cluster.hdfs.client.makedirs(hadoop_fs_folder)

        logger.debug('Writing files in %s', hadoop_fs_folder)
        for i in range(hdfs_files_count):
            cluster.hdfs.client.write(os.path.join(hadoop_fs_folder, f'{i}{hadoop_fs_filename}'),
                                      data='\n'.join(file_lines))

        logger.debug('Starting snapshot pipeline and capturing snapshot ...')

        # We want to verify the amount of records
        sdc_executor.start_pipeline(hadoop_fs_pipeline).wait_for_finished(timeout_sec=180)

        history = sdc_executor.get_pipeline_history(hadoop_fs_pipeline)

        input_records_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count

        assert input_records_count == hdfs_lines_per_file * hdfs_files_count
        assert output_records_count == hdfs_lines_per_file * hdfs_files_count + hdfs_number_of_threads
    finally:
        cluster.hdfs.client.delete(hadoop_fs_folder, recursive=True)


def _check_pipeline_status(pipeline, sdc_executor, is_pipeline_stopped):
    """Make sure the pipeline has not entered any of the unsuccessful states.
    While pipeline is running, pipeline needs to have exactly one entry for status of 'RUNNING'.
    After the pipeline is stopped,
    pipeline needs to have exactly one entry for status of 'RUNNING' and 'STOPPED' each."""
    history = sdc_executor.get_pipeline_history(pipeline)
    logger.debug('Inside _check_pipeline_status at %s ...', time.ctime())
    assert sum(1 for entry in history.entries if entry['status'] == 'RUNNING') == 1
    if is_pipeline_stopped:
        SUCCESS_STATUSES.extend(['STOPPING', 'STOPPED'])
        assert sum(1 for entry in history.entries if entry['status'] == 'STOPPED') == 1
    error_msg = f'History entries={[entry["status"] for entry in history.entries]}'
    assert not any(item not in SUCCESS_STATUSES for item in [entry['status'] for entry in history.entries]), error_msg


@large
@cluster('cdh', 'hdp')
def test_kerberos_ticket_expiration_hadoop_fs_destination(sdc_builder, sdc_executor, cluster):
    """ Test Hadoop FS destination stage against kerberized cluster where Kerberos ticket will be expired
    every few minutes (e.g. 10 minutes). And the pipeline will be run for an hour.
    The purpose is to test if the pipeline runs for an hour by renewing kerberos ticket.
    Data is sent to Hadoop FS destination using Dev Data Generator stage.
    Using pipeline history, checks are done at regular intervals that pipeline did not enter
    any of the not successful statuses. And to make sure it ran without any glitches.

    The pipeline looks like:
        dev_data_generator >> hadoop_fs
    """
    if 'hdfs' not in cluster.kerberized_services:
        pytest.skip('test_kerberos_ticket_expiration_hadoop_fs_destination runs only for kerberized HDFS.')

    hdfs_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    logger.info('Pipeline will write to HDFS directory %s ...', hdfs_directory)

    builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      delay_between_batches=1000,
                                      fields_to_generate=[{'type': 'STRING',
                                                           'precision': 10,
                                                           'scale': 2,
                                                           'field': 'random_string'}])

    hadoop_fs = builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON', directory_template=hdfs_directory,
                             files_prefix='stages', files_suffix='txt')

    dev_data_generator >> hadoop_fs

    pipeline = builder.build(title='Kerberos tkt expiration- Hadoop FS Dest').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        scheduler = sched.scheduler(time.time, time.sleep)
        sdc_executor.start_pipeline(pipeline)
        # Keep checking the status by scheduling the verify task.
        for i in range(0, LARGE_TEST_DURATION_IN_SECS - 10, LARGE_TEST_STATUS_CHECK_DURATION_IN_SECS):
            scheduler.enter(i, 1, _check_pipeline_status, argument=(pipeline, sdc_executor, False))
        # Schedule to stop the pipeline after LARGE_TEST_DURATION_IN_SECS.
        logger.debug('LARGE_TEST_DURATION_IN_SECS = %d', LARGE_TEST_DURATION_IN_SECS)
        scheduler.enter(LARGE_TEST_DURATION_IN_SECS, 1, sdc_executor.stop_pipeline, argument=(pipeline,))

        scheduler.run()
        _check_pipeline_status(pipeline, sdc_executor, True)

    finally:
        # remove HDFS files
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)

@sdc_min_version('3.0.0')
@cluster('cdh', 'hdp')
def test_hadoop_fs_destination_sequence_files(sdc_builder, sdc_executor, cluster):
    """Test Hadoop FS destination configuring File Type to Sequence File.
    We use sequence files with a EL expression in the sequence key file.
    We use SequenceFile module to read the generated file.
    Hadoop File is copied to local file system.
    """

    # Configure Prefix and Sufix and Directory
    FILES_PREFIX, FILES_SUFFIX = 'tst', 'seq'
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'

    # Get Pipeline Builder
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Stage
    raw_data = '\n'.join(json.dumps(product) for product in PRODUCT_DATA_FIX)
    logger.info('Pipeline will write to HDFS directory %s ...', hdfs_directory)
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    # Create Hadoop FS Destination
    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             files_prefix=FILES_PREFIX,
                             files_suffix=FILES_SUFFIX,
                             file_type='SEQUENCE_FILE',
                             compression_type='RECORD',
                             sequence_file_key='${record:value(\'/sequenceKey\')}'
                             )

    # triggered the destination file to be closed after writing all data.
    hadoop_fs.set_attributes(max_records_in_file=len(PRODUCT_DATA_FIX))

    dev_raw_data_source >> hadoop_fs

    # Build and Start Pipeline. After first batch it finishes.
    pipeline = pipeline_builder.build('Hadoop FS Destination Sequence Key').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=10)

    try:

        # Check that just one file is in the directory
        hdfs_fs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_fs_files) == 1
        # Check the prefix and suffix
        hdfs_fs_filename = hdfs_fs_files[0]
        assert hdfs_fs_filename.startswith(FILES_PREFIX)
        assert hdfs_fs_filename.endswith(FILES_SUFFIX)

        # Download the file from HDFS to Local File System
        cluster.hdfs.client.download(f'{hdfs_directory}/{hdfs_fs_filename}', f'/tmp/{hdfs_fs_filename}')

        # Read the sequence file
        reader = SequenceFile.Reader(f'/tmp/{hdfs_fs_filename}')
        key_class = reader.getKeyClass()
        value_class = reader.getValueClass()
        key = key_class()
        value = value_class()

        # Convert list of dict to list of bytes
        product_data_expected = [json.dumps(row, separators=(',', ':')).encode() for row in PRODUCT_DATA_FIX]

        for i in range(2):
            # Read the information
            reader.next(key, value)

            # Check if name, price and release are in value
            assert product_data_expected[i] == value.toString()

        reader.close()

    finally:
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)
