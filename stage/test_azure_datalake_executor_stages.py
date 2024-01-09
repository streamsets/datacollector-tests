# Copyright 2019 StreamSets Inc.
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

"""A module to test the ADLS Gen2 File Metadata stages"""
# Tests based on test_hdfs_metadata.py
# Some helper code based on test_azure_datalake_gen2_stages.py

import json
import logging
import string

import pytest
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

ADLS_GEN2_FILE_METADATA = 'ADLS Gen2 File Metadata'

STORAGE_V1 = 'Storage'
STORAGE_V2 = 'StorageV2'

ADLS_GEN2_TARGET = 'com_streamsets_pipeline_stage_destination_datalake_gen2_DataLakeGen2DTarget'

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def storage_type_check(azure, executor_name):
    if executor_name == ADLS_GEN2_FILE_METADATA and azure.storage_type != STORAGE_V2:
        pytest.skip('ADLS Gen2 tests require storage type to be of Gen2.')


PRODUCT_DATA = [
    {'name': 'iphone', 'price': 649.99},
    {'name': 'pixel', 'price': 649.89}
]


def create_adls_metadata_pipeline(pipeline_builder, azure, pipeline_title, directory_name, files_prefix, files_suffix,
                                  adls_metadata):
    """Helper function to create and return a pipeline with ADLS File Metadata
    The Deduplicator assures there is only one ingest to ADLS. The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> adls_destination >= adls_metadata
                                                   >> trash
    """
    raw_data = '\n'.join(json.dumps(product) for product in PRODUCT_DATA)
    logger.info('Pipeline will write to ADLS directory %s ...', directory_name)

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    if azure.storage_type == STORAGE_V2:
        adls_stage_name = ADLS_GEN2_TARGET
    adls_destination = pipeline_builder.add_stage(name=adls_stage_name)
    adls_destination.set_attributes(data_format='JSON',
                                    directory_template=f'/{directory_name}',
                                    files_prefix=files_prefix,
                                    files_suffix=files_suffix,
                                    max_records_in_file=len(PRODUCT_DATA))

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> adls_destination >= adls_metadata
    record_deduplicator >> trash
    return pipeline_builder.build(title=pipeline_title)


def get_dir(azure, directory_name):
    """Helper function to get the contents of a directory in ADLS
    """
    dl_fs = azure.datalake.file_system

    if azure.storage_type == STORAGE_V1:
        dl_files = dl_fs.ls(directory_name)
    if azure.storage_type == STORAGE_V2:
        paths = dl_fs.ls(directory_name).response.json()['paths']
        dl_files = [item['name'] for item in paths] if paths else []

    return dl_files


def get_perms(azure, directory_or_file_name):
    """Helper function to get the permissions of a file or directory of files in ADLS
    """
    dl_fs = azure.datalake.file_system

    if azure.storage_type == STORAGE_V1:
        dl_files = dl_fs.ls(directory_or_file_name)
        perms = [dl_fs.get_acl_status(item)['permission'] for item in dl_files] if dl_files else []
    if azure.storage_type == STORAGE_V2:
        paths = dl_fs.ls(directory_or_file_name).response.json()['paths']
        perms = [item['permissions'] for item in paths] if paths else []

    return perms


def get_size(azure, directory_or_file_name):
    """Helper function to get the size of a file or directory of files in ADLS
    """
    dl_fs = azure.datalake.file_system

    if azure.storage_type == STORAGE_V1:
        dl_files = dl_fs.ls(directory_or_file_name)
        sizes = [dl_fs.df(item)['length'] for item in dl_files] if dl_files else []
    if azure.storage_type == STORAGE_V2:
        paths = dl_fs.ls(directory_or_file_name).response.json()['paths']
        sizes = [int(item['contentLength']) for item in paths] if paths else []

    return sizes


def delete_dir(azure, directory_name):
    """Helper function to delete a directory and its files in ADLS
    """
    dl_fs = azure.datalake.file_system

    if azure.storage_type == STORAGE_V1:
        dl_files = dl_fs.ls(directory_name)
    if azure.storage_type == STORAGE_V2:
        paths = dl_fs.ls(directory_name).response.json()['paths']
        dl_files = [item['name'] for item in paths] if paths else []

    logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
    # Note: Non-empty directory is not allowed to be removed, hence remove all files first.
    for dl_file in dl_files:
        dl_fs.rm(dl_file)
    dl_fs.rmdir(directory_name)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_metadata_remove(sdc_builder, sdc_executor, azure):
    """ADLS File Metadata with Remove File Task
    The data is written to an ADLS file then removed by ADLS File Metadata executor.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = 'json'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    adls_metadata = pipeline_builder.add_stage(ADLS_GEN2_FILE_METADATA, type='executor')
    adls_metadata.set_attributes(task='REMOVE_FILE',
                                 file_path="${record:value('/filepath')}")

    pipeline = create_adls_metadata_pipeline(pipeline_builder,
                                             azure,
                                             f'ADLS File Metadata Remove Task ({ADLS_GEN2_FILE_METADATA})',
                                             directory_name,
                                             files_prefix,
                                             files_suffix,
                                             adls_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(azure))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))

        dl_files = get_dir(azure, directory_name)
        assert len(dl_files) == 0
    finally:
        sdc_executor.stop_pipeline(pipeline)
        delete_dir(azure, directory_name)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_metadata_rename(sdc_builder, sdc_executor, azure):
    """ADLS File Metadata with Rename File Task
    The data is written to an ADLS file then renamed by ADLS File Metadata executor.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = 'json'
    new_file_name = 'new_file_name.txt'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    adls_metadata = pipeline_builder.add_stage(ADLS_GEN2_FILE_METADATA, type='executor')
    adls_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 rename=True,
                                 new_name=new_file_name)

    pipeline = create_adls_metadata_pipeline(pipeline_builder,
                                             azure,
                                             f'ADLS File Metadata Rename Task ({ADLS_GEN2_FILE_METADATA})',
                                             directory_name,
                                             files_prefix,
                                             files_suffix,
                                             adls_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(azure))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))

        dl_files = get_dir(azure, directory_name)
        assert len(dl_files) == 1
        assert dl_files[0] == directory_name + "/" + new_file_name
    finally:
        sdc_executor.stop_pipeline(pipeline)
        delete_dir(azure, directory_name)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_metadata_move_rename(sdc_builder, sdc_executor, azure):
    """ADLS File Metadata with Move and Rename File Task
    The data is written to an ADLS file then moved and renamed by ADLS File Metadata executor.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = 'json'
    new_file_name = 'new_file_name.txt'
    new_directory_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    adls_metadata = pipeline_builder.add_stage(ADLS_GEN2_FILE_METADATA, type='executor')
    adls_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 move_file=True,
                                 rename=True,
                                 new_location=f'/{new_directory_name}',
                                 new_name=new_file_name)

    pipeline = create_adls_metadata_pipeline(pipeline_builder,
                                             azure,
                                             f'ADLS File Metadata Move Rename Task ({ADLS_GEN2_FILE_METADATA})',
                                             directory_name,
                                             files_prefix,
                                             files_suffix,
                                             adls_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(azure))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))

        assert len(get_dir(azure, directory_name)) == 0
        dl_files = get_dir(azure, new_directory_name)
        assert len(dl_files) == 1
        assert dl_files[0] == new_directory_name + "/" + new_file_name
    finally:
        sdc_executor.stop_pipeline(pipeline)
        delete_dir(azure, directory_name)
        delete_dir(azure, new_directory_name)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_metadata_change_permission_octal(sdc_builder, sdc_executor, azure):
    """ADLS File Metadata with Change Permission Task
    The data is written to an ADLS file then changed permissions by ADLS File Metadata executor.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = 'json'
    new_permission = 'rw-------'
    new_permission_octal = '600'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    adls_metadata = pipeline_builder.add_stage(ADLS_GEN2_FILE_METADATA, type='executor')
    adls_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 set_permissions=True,
                                 new_permissions=new_permission_octal)

    pipeline = create_adls_metadata_pipeline(pipeline_builder,
                                             azure,
                                             f'ADLS File Metadata Octal Permission Task ({ADLS_GEN2_FILE_METADATA})',
                                             directory_name,
                                             files_prefix,
                                             files_suffix,
                                             adls_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(azure))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))

        dl_files = get_dir(azure, directory_name)
        assert len(dl_files) == 1
        perms = get_perms(azure, dl_files[0])
        # Gen2 report the perms back with different syntax
        assert perms[0] == new_permission
    finally:
        sdc_executor.stop_pipeline(pipeline)
        delete_dir(azure, directory_name)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_metadata_change_permission_unix(sdc_builder, sdc_executor, azure):
    """ADLS File Metadata with Change Permission Task
    The data is written to an ADLS file then changed permissions by ADLS File Metadata executor.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = 'json'
    new_permission = 'rw-------'
    new_permission_octal = '600'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    adls_metadata = pipeline_builder.add_stage(ADLS_GEN2_FILE_METADATA, type='executor')
    adls_metadata.set_attributes(task='CHANGE_EXISTING_FILE',
                                 file_path="${record:value('/filepath')}",
                                 set_permissions=True,
                                 new_permissions=new_permission)

    pipeline = create_adls_metadata_pipeline(pipeline_builder,
                                             azure,
                                             f'ADLS File Metadata Unix Permission Task ({ADLS_GEN2_FILE_METADATA})',
                                             directory_name,
                                             files_prefix,
                                             files_suffix,
                                             adls_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(azure))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))

        dl_files = get_dir(azure, directory_name)
        assert len(dl_files) == 1
        perms = get_perms(azure, dl_files[0])
        assert perms[0] == new_permission
    finally:
        sdc_executor.stop_pipeline(pipeline)
        delete_dir(azure, directory_name)


@azure('datalake')
@sdc_min_version('3.9.0')
def test_datalake_metadata_create(sdc_builder, sdc_executor, azure, executor_name):
    """ADLS File Metadata with Create File Task
    The data is written to an ADLS file but removed by ADLS File Metadata executor.
    """
    directory_name = get_random_string(string.ascii_letters, 10)
    files_prefix = get_random_string(string.ascii_letters, 10)
    files_suffix = 'json'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    adls_metadata = pipeline_builder.add_stage(ADLS_GEN2_FILE_METADATA, type='executor')
    adls_metadata.set_attributes(task='CREATE_EMPTY_FILE',
                                 file_path="${record:value('/filepath')}.empty")

    pipeline = create_adls_metadata_pipeline(pipeline_builder,
                                             azure,
                                             f'ADLS File Metadata Create Task ({ADLS_GEN2_FILE_METADATA})',
                                             directory_name,
                                             files_prefix,
                                             files_suffix,
                                             adls_metadata)
    sdc_executor.add_pipeline(pipeline.configure_for_environment(azure))

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(PRODUCT_DATA))

        dl_files = sorted(get_dir(azure, directory_name))
        assert len(dl_files) == 2
        empty_file = dl_files[1]
        assert empty_file.endswith('.empty')
        sizes = sorted(get_size(azure, dl_files[1]))
        assert sizes[0] == 0
    finally:
        sdc_executor.stop_pipeline(pipeline)
        delete_dir(azure, directory_name)
