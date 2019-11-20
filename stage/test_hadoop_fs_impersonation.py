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

# Note: This module lives separate from other related ones as it has custom SDC hook logic.

import logging
import os
from uuid import uuid4

import pytest

from streamsets.testframework.markers import cluster

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.set_user('admin')
        data_collector.sdc_properties['stage.conf_hadoop.always.impersonate.current.user'] = 'true'

    return hook


@pytest.fixture(autouse=True)
def impersonation_check(sdc_executor):
    if sdc_executor.sdc_configuration.get('stage.conf_hadoop.always.impersonate.current.user') != 'true':
        pytest.skip('Hadoop FS impersonation requires stage.conf_hadoop.'
                    'always.impersonate.current.user to be set to true')


@cluster('cdh', 'hdp')
def test_hadoop_fs_strict_impersonation(sdc_builder, sdc_executor, cluster):
    """ Test strict impersonation (SDC-3704) of Hadoop FS target. The pipeline would look like:

        dev_data_generator >> hadoop_fs
    """
    hdfs_path = os.path.join(os.sep, "tmp", str(uuid4()))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [{'field': 'text', 'precision': 10, 'scale': 2, 'type': 'STRING'}]
    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(data_format='TEXT', directory_template=hdfs_path, files_prefix='sdc-${sdc:id()}')

    dev_data_generator >> hadoop_fs
    pipeline = pipeline_builder.build(title='Hadoop FS impersonation pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True)
        sdc_executor.stop_pipeline(pipeline)

        # Validate that the files were created with proper user name.
        hdfs_fs_files = cluster.hdfs.client.list(hdfs_path)
        assert len(hdfs_fs_files) == 1

        status = cluster.hdfs.client.status(f'{hdfs_path}/{hdfs_fs_files[0]}')
        assert status['owner'] == 'admin'
    finally:
        cluster.hdfs.client.delete(hdfs_path, recursive=True)
