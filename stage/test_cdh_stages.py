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

import logging
import string

import pytest
from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Spark executor was renamed in SDC-10697, so we need to reference it by name.
SPARK_EXECUTOR_STAGE_NAME = 'com_streamsets_datacollector_pipeline_executor_spark_SparkDExecutor'


@cluster('cdh')
def test_spark_executor(sdc_builder, sdc_executor, cluster):
    """Test Spark executor stage. This is acheived by using 2 pipelines. The 1st pipeline would generate the
    application resource file (Python in this case) which will be used by the 2nd pipeline for spark-submit. Spark
    executor will do the spark-submit and we assert that it has submitted the job to Yarn.
    We will also verify that the job generates an event that contains the proper information.

    The pipelines would
    look like:

        dev_raw_data_source >> local_fs >= pipeline_finisher_executor

        dev_raw_data_source >> record_deduplicator >> spark_executor
                               record_deduplicator >> trash
    """
    # STF-1156: STF Does not properly configure Spark Executor for Secured Cluster
    if cluster.hdfs.is_kerberized:
        pytest.skip('Spark Executor tests on secured cluster are not supported.')

    python_data = 'print("Hello World!")'
    tmp_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    python_suffix = 'py'
    application_name = ''.join(['stf_', get_random_string(string.ascii_letters, 10)])

    # build the 1st pipeline - file generator
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=python_data)
    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix=python_suffix, max_records_in_file=1)
    # we use the finisher so as local_fs can generate event with file_path being generated
    pipeline_finisher_executor = builder.add_stage('Pipeline Finisher Executor')

    dev_raw_data_source >> local_fs >= pipeline_finisher_executor

    pipeline = builder.build(title='To File pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    # run the pipeline and capture the file path
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    file_path = snapshot[local_fs.instance_name].event_records[0].field['filepath'].value

    # build the 2nd pipeline - spark executor
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data='dummy')
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    trash2 = builder.add_stage('Trash')
    spark_executor = builder.add_stage(name=SPARK_EXECUTOR_STAGE_NAME)
    spark_executor.set_attributes(minimum_number_of_worker_nodes=1,
                                  maximum_number_of_worker_nodes=1,
                                  application_name=application_name,
                                  deploy_mode='CLUSTER',
                                  driver_memory='10m',
                                  executor_memory='10m',
                                  application_resource=file_path,
                                  language='PYTHON')

    dev_raw_data_source >> record_deduplicator >> spark_executor >= trash2
    record_deduplicator >> trash

    pipeline = builder.build(title='Spark executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    snapshot2 = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=1).snapshot
    sdc_executor.stop_pipeline(pipeline)

    assert 'default user (sdc)' == snapshot2[spark_executor.instance_name].event_records[0].field['submitter'].value
    assert snapshot2[spark_executor.instance_name].event_records[0].field['timestamp'].value

    # assert Spark executor has triggered the YARN job
    assert cluster.yarn.wait_for_app_to_register(application_name)
