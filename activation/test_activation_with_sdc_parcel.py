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

# This module runs pipelines against SDC installed on CDH cluster with a parcel containing activation jars.

import json
import logging
import pytest
import requests
import string
import time

from streamsets.testframework.markers import cluster, sdc_activation, sdc_min_version
from streamsets.testframework.utils import get_random_string

from .utils.utils_activation import ACTIVATION_SUPPORT_SDC_MIN_VERSION, register_and_activate_sdc

# For CDH
# Current time in seconds since the epoch
CURRENT_TIME = time.time()

PRODUCT_DATA = [
    {'name': 'iphone', 'price': 649.99, 'release': CURRENT_TIME},
    {'name': 'pixel', 'price': 649.89, 'release': CURRENT_TIME - 60 * 5},  # -5 minutes
    {'name': 'galaxy', 'price': 549.89, 'release': CURRENT_TIME - 60 * 10}  # -10 minutes
]

# Skip all tests in this module if --sdc-version < 3.15.0
pytestmark = sdc_min_version(ACTIVATION_SUPPORT_SDC_MIN_VERSION)

logger = logging.getLogger(__name__)


@cluster('cdh')
@sdc_activation
@pytest.mark.parametrize('activate_sdc', [False, True])
def test_sdc_parcel_activation_hadoop_fs_destination(sdc_executor, activate_sdc, cluster):
    """SDC containing activation jars is installed with a parcel on CDH cluster.

    The pipelines should not run when SDC is not activated. activate_sdc = False
    The pipelines should be able to run when SDC is activated. activate_sdc = true
    """
    if activate_sdc:
        register_and_activate_sdc(sdc_executor)
        _test_basic_stage(sdc_executor)
        _test_hadoop_fs_destination(sdc_executor, cluster)
    else:
        expected = ("403 Client Error: Forbidden for url:")
        with pytest.raises(requests.exceptions.HTTPError, match=expected):
            # Note here: the following is expected to produce 403 even though it is a basic stage.
            # Reason is : SDC Parcel contains all stage libs and so now it needs registration and activation
            # in order to run any pipelines.
            _test_basic_stage(sdc_executor)
        with pytest.raises(requests.exceptions.HTTPError, match=expected):
            _test_hadoop_fs_destination(sdc_executor, cluster)


def _test_basic_stage(sdc_executor):
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'
    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> trash
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)
    snap_data = snapshot[pipeline.origin_stage.instance_name]
    assert len(snap_data.output) == 1
    assert snap_data.output[0].field['emp_id'].value == '123456'


def _test_hadoop_fs_destination(sdc_executor, cluster):
    """Test Hadoop FS destination with file prefix and suffix
    The data is written to a HDFS file, then check the contents.
    """

    hdfs_directory = f'/tmp/out/{get_random_string(string.ascii_letters, 10)}'
    FILES_PREFIX, FILES_SUFFIX = 'stages', 'txt'
    pipeline_builder = sdc_executor.get_pipeline_builder()

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='JSON',
                             directory_template=hdfs_directory,
                             files_prefix=FILES_PREFIX,
                             files_suffix=FILES_SUFFIX)

    pipeline = _create_hadoop_fs_dest_pipeline(pipeline_builder,
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


def _create_hadoop_fs_dest_pipeline(pipeline_builder, pipeline_title, hdfs_directory, hadoop_fs):
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
