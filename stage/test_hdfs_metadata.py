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

import json
import logging
import string

import pytest
from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PRODUCT_DATA = [
    {'name': 'iphone', 'price': 649.99},
    {'name': 'pixel', 'price': 649.89}
]

def create_hdfs_metadata_pipeline(pipeline_builder, pipeline_title, hdfs_directory, hdfs_metadata):
    """Helper function to create and return a pipeline with HDFS File Metadata
    The Deduplicator assures there is only one ingest to HDFS. The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> hadoop_fs >= hdfs_metadata
                                                   >> trash
    """
    raw_data = '\n'.join(json.dumps(product) for product in PRODUCT_DATA)
    logger.info('Pipeline will write to HDFS directory %s ...', hdfs_directory)

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             max_records_in_file=len(PRODUCT_DATA))

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> hadoop_fs >= hdfs_metadata
    record_deduplicator >> trash
    return pipeline_builder.build(title=pipeline_title)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_remove(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Remove File Task
    The data is written to a HDFS file then removed by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='REMOVE_FILE',
                                 file_path="${record:value('/filepath')}")

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Remove Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_files) == 0
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_rename(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Rename File Task
    The data is written to a HDFS file then renamed by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    NEW_FILE_NAME = 'new_file_name.txt'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 rename=True,
                                 new_name=NEW_FILE_NAME)

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Rename Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_files) == 1
        assert hdfs_files[0] == NEW_FILE_NAME
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_move_rename(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Move and Rename File Task
    The data is written to a HDFS file then moved and renamed by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    new_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    NEW_FILE_NAME = 'new_moved_file_name.txt'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 move_file=True,
                                 rename=True,
                                 new_location=new_directory,
                                 new_name=NEW_FILE_NAME)

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Move Rename Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        assert len(cluster.hdfs.client.list(hdfs_directory)) == 0
        hdfs_files = cluster.hdfs.client.list(new_directory)
        assert len(hdfs_files) == 1
        assert hdfs_files[0] == NEW_FILE_NAME
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s %s ...', hdfs_directory, new_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)
        cluster.hdfs.client.delete(new_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_change_ownership(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Change Ownership Task
    The data is written to a HDFS file then changed owenr and group by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    NEW_OWNER, NEW_GROUP = 'eddie', 'sdcuser'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 hdfs_user='hdfs',
                                 set_ownership=True,
                                 new_owner=NEW_OWNER,
                                 new_group=NEW_GROUP)

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Ownership Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_files) == 1
        status = cluster.hdfs.client.status(f'{hdfs_directory}/{hdfs_files[0]}')
        assert (status['owner'], status['group']) == (NEW_OWNER, NEW_GROUP)
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_change_permission_octal(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Change Permission Task
    The data is written to a HDFS file then changed permissions by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    NEW_PERMISSION = '600'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 set_permissions=True,
                                 new_permissions=NEW_PERMISSION)

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Octal Permission Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_files) == 1
        status = cluster.hdfs.client.status(f'{hdfs_directory}/{hdfs_files[0]}')
        assert status['permission'] == NEW_PERMISSION
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_change_permission_unix(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Change Permission Task
    The data is written to a HDFS file then changed permissions by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 set_permissions=True,
                                 new_permissions='rw-------')

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Unix Permission Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_files) == 1
        status = cluster.hdfs.client.status(f'{hdfs_directory}/{hdfs_files[0]}')
        assert status['permission'] == '600'
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_change_acls(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Change ACLs Task
    The data is written to a HDFS file then changed ACLs by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 set_acls=True,
                                 new_acls='user::rwx,user:eddie:rw-,group::r--,other::---')

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata ACLs Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = cluster.hdfs.client.list(hdfs_directory)
        assert len(hdfs_files) == 1
        status = cluster.hdfs.client.acl_status(f'{hdfs_directory}/{hdfs_files[0]}')
        assert 'user:eddie:rw-' in status['entries']
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_create(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Create File Task
    The data is written to a HDFS file but removed by HDFS File Metadata executor.
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CREATE_EMPTY_FILE',
                                 file_path="${record:value('/filepath')}.empty")

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Create Task',
                                             hdfs_directory,
                                             hdfs_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(cluster))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))
        hdfs_files = sorted(cluster.hdfs.client.list(hdfs_directory))
        assert len(hdfs_files) == 2
        empty_file = hdfs_files[1]
        assert empty_file.endswith('.empty')
        assert cluster.hdfs.client.content(f'{hdfs_directory}/{empty_file}')['length'] == 0
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)


@cluster('cdh', 'hdp')
def test_hdfs_metadata_path_el_error(sdc_builder, sdc_executor, cluster):
    """HDFS File Metadata with Invalid EL in File Path
    Check the pipeline is in START_ERROR status and validate the error.
    The ValidationError looks like:
    {
      'pipelineIssues': [],
      'stageIssues': {
        'HDFSFileMetadata_01': [
          {
            'configName': 'filePath',
            'additionalInfo': None,
            'configGroup': 'TASKS',
            'serviceName': None,
            'count': 1,
            'level': 'STAGE_CONFIG',
            'instanceName': 'HDFSFileMetadata_01',
            'message': 'HDFS_METADATA_007 - Invalid EL expression: CTRCMN_0101 ...'
          }
        ]
      },
      'issueCount': 1
    }
    """
    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    ERROR_PATH = "${record:value('/filepath')" # missing close bracket
    pipeline_builder = sdc_builder.get_pipeline_builder()

    hdfs_metadata = pipeline_builder.add_stage('HDFS File Metadata', type='executor')
    hdfs_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path=ERROR_PATH,
                                 rename=True,
                                 new_name='new_file_name.txt')

    pipeline = create_hdfs_metadata_pipeline(pipeline_builder,
                                             'HDFS File Metadata Path Error',
                                             hdfs_directory,
                                             hdfs_metadata)
    pipeline.configuration['shouldRetry'] = False
    pipeline.configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline, wait=False)
        sdc_executor.get_pipeline_status(pipeline).wait_for_status('START_ERROR', ignore_errors=True)
        sdc_executor.validate_pipeline(pipeline)
        assert False, 'Should not reach here.'
    except Exception as error:
        logger.info('Validating the error: %s', error)
        assert error.issues['issueCount'] == 1
        stage_error = next(iter(error.issues['stageIssues'].values()))[0]
        assert stage_error['configName'] == 'filePath'
        assert stage_error['message'].startswith('HDFS_METADATA_007 - Invalid EL expression')
    finally:
        logger.info('Deleting Hadoop FS directory %s ...', hdfs_directory)
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)
