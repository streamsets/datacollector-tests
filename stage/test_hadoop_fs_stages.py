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
import string
from pathlib import Path
from uuid import uuid4

from testframework import sdc
from testframework.markers import *
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000


@cluster('cdh', 'hdp')
def test_hadoop_fs_destination(sdc_builder, sdc_executor, cluster):
    """Test Hadoop FS destination stage. This is achieved by using a deduplicator which assures us that there is
    only one ingest to HDFS. We then read that to assert what we ingest. The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> hadoop_fs
                                                   >> trash
    """
    product_data = [dict(name='iphone', price=649.99),
                    dict(name='pixel', price=649.89)]
    raw_data = ''.join([json.dumps(product) for product in product_data])
    hdfs_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    logger.info(f'Pipeline will write to HDFS directory {hdfs_directory} ...')

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    hadoop_fs = builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON', directory_template=hdfs_directory,
                             files_prefix='stages', files_suffix='txt')

    dev_raw_data_source >> record_deduplicator >> hadoop_fs
    record_deduplicator >> trash

    pipeline = builder.build(title='Hadoop FS pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(product_data))
        # Wait for pipeline to be stopped in order to ensure file has been written.
        sdc_executor.stop_pipeline(pipeline)

        hdfs_fs_files = cluster.hdfs.client.list(hdfs_directory)
        # There should only be one file written.
        assert len(hdfs_fs_files) == 1

        # Check that file prefix and suffix are respected.
        hdfs_fs_filename = hdfs_fs_files[0]
        assert hdfs_fs_filename.startswith('stages') and hdfs_fs_filename.endswith('txt')

        with cluster.hdfs.client.read(str(Path(hdfs_directory, hdfs_fs_filename))) as reader:
            file_contents = reader.read()

        assert {tuple(json.loads(line).items())
                for line in file_contents.decode().split()} == {tuple(stage.items())
                                                                for stage in product_data}
    finally:
        # remove HDFS files
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
    hadoop_fs_folder = os.path.join(os.sep, get_random_string(string.ascii_letters, 10))

    # Build the Hadoop FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    hadoop_fs = builder.add_stage('Hadoop FS', type='origin')
    hadoop_fs.data_format = 'TEXT'
    hadoop_fs.input_paths.append(hadoop_fs_folder)

    sdc_rpc_destination = builder.add_stage('SDC RPC', type='destination')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host,
                                                              SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = get_random_string(string.ascii_letters, 10)

    hadoop_fs >> sdc_rpc_destination
    hadoop_fs_pipeline = builder.build(title='Hadoop FS pipeline').configure_for_environment(cluster)
    hadoop_fs_pipeline.configuration['executionMode'] = 'CLUSTER_BATCH'

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    sdc_rpc_origin = builder.add_stage('SDC RPC', type='origin')
    sdc_rpc_origin.rpc_port = SDC_RPC_PORT
    sdc_rpc_origin.rpc_id = sdc_rpc_destination.rpc_id
    # Since YARN jobs take a while to get going, set RPC origin batch wait time to 5 min. to avoid
    # getting an empty batch in the snapshot.
    sdc_rpc_origin.batch_wait_time_in_secs = 300

    trash = builder.add_stage(label='Trash')

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
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline,
                                                                  start_pipeline=True)

        logger.debug('Starting Hadoop FS pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(hadoop_fs_pipeline)

        snapshot = snapshot_pipeline_command.wait_for_finished().snapshot
        sdc_executor.stop_pipeline(snapshot_pipeline, force=True)
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == lines_in_file
    finally:
        cluster.hdfs.client.delete(hadoop_fs_folder, recursive=True)


@cluster('cdh', 'hdp')
@upgrade
def test_hadoop_fs_strict_impersonation(args, sdc_builder, cluster):
    """ Test strict impersonation (SDC-3704) of Hadoop FS target. The pipeline would look like:

        dev_data_generator >> hadoop_fs

    Note: Test is expected to fail till TEST-138 is addressed.
    """
    hdfs_path = os.path.join(os.sep, "tmp", str(uuid4()))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [{'field': 'text', 'precision': 10, 'scale': 2, 'type': 'STRING'}]
    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='TEXT', directory_template=hdfs_path, files_prefix='sdc-${sdc:id()}')

    dev_data_generator >> hadoop_fs
    pipeline = pipeline_builder.build(title='Hadoop FS impersonation pipeline').configure_for_environment(cluster)

    try:
        data_collector = sdc.DataCollector(version=args.post_upgrade_sdc_version or args.sdc_version,
                                           username=args.sdc_username, password=args.sdc_password,
                                           server_url=args.sdc_server_url,
                                           tear_down_on_exit=not args.keep_sdc_instances,
                                           always_pull=args.sdc_always_pull, enable_kerberos=args.kerberos)
        data_collector.configure_for_environment(cluster)
        data_collector.set_user('admin')
        data_collector.add_pipeline(pipeline)
        data_collector.sdc_properties['stage.conf_hadoop.always.impersonate.current.user'] = 'true'
        data_collector.start()
        # Run at least one batch of data to Hadoop FS.
        data_collector.capture_snapshot(pipeline=pipeline, start_pipeline=True).wait_for_finished()
        data_collector.stop_pipeline(pipeline).wait_for_stopped()

        # Validate that the files were created with proper user name.
        hdfs_fs_files = cluster.hdfs.client.list(hdfs_path)
        assert len(hdfs_fs_files) == 1

        status = cluster.hdfs.client.status(f'{hdfs_path}/{hdfs_fs_files[0]}')
        assert status['owner'] == 'admin'
    finally:
        if data_collector.tear_down_on_exit:
            data_collector.tear_down()
        cluster.hdfs.client.delete(hdfs_path, recursive=True)
