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

import logging
import string
import time

import pytest
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Spark executor was renamed in SDC-10697, so we need to reference it by name.
SPARK_EXECUTOR_STAGE_NAME = 'com_streamsets_datacollector_pipeline_executor_spark_SparkDExecutor'

@stub
def test_additional_files(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_jars(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_spark_arguments(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_spark_arguments_and_values(sdc_builder, sdc_executor):
    pass


@stub
def test_application_arguments(sdc_builder, sdc_executor):
    pass


@stub
def test_application_name(sdc_builder, sdc_executor):
    pass


@stub
def test_application_resource(sdc_builder, sdc_executor):
    pass


@stub
def test_custom_java_home(sdc_builder, sdc_executor):
    pass


@stub
def test_custom_spark_home(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'language': 'PYTHON'}])
def test_dependencies(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'deploy_mode': 'CLIENT'}, {'deploy_mode': 'CLUSTER'}])
def test_deploy_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_driver_memory(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': False}, {'dynamic_allocation': True}])
def test_dynamic_allocation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_verbose_logging': False}, {'enable_verbose_logging': True}])
def test_enable_verbose_logging(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_environment_variables(sdc_builder, sdc_executor):
    pass


@stub
def test_executor_memory(sdc_builder, sdc_executor):
    pass


@stub
def test_kerberos_keytab(sdc_builder, sdc_executor):
    pass


@stub
def test_kerberos_principal(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'language': 'JVM'}, {'language': 'PYTHON'}])
def test_language(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'language': 'JVM'}])
def test_main_class(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': True}])
def test_maximum_number_of_worker_nodes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'deploy_mode': 'CLUSTER'}])
def test_maximum_time_to_wait_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': True}])
def test_minimum_number_of_worker_nodes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': False}])
def test_number_of_worker_nodes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_proxy_user(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@cluster('cdh')
@sdc_min_version('3.16.0')
def test_wait_for_spark_app_id(sdc_builder, sdc_executor, cluster):
    """Test Spark executor stage. This is acheived by using 2 pipelines. The 1st pipeline would generate the
    application resource file (Python in this case) which will be used by the 2nd pipeline for spark-submit. Spark
    executor will do the spark-submit and we assert that it has submitted the job to Yarn.
    We will also verify that the job generates an event that contains the proper information,
    especially the Spark AppID

    The pipelines would
    look like:

        dev_raw_data_source >> local_fs >= pipeline_finisher_executor

        dev_raw_data_source >> record_deduplicator >> spark_executor
                               record_deduplicator >> trash
    """
    # STF-1156: STF Does not properly configure Spark Executor for Secured Cluster
    if cluster.hdfs.is_kerberized:
        pytest.skip('Spark Executor tests on secured cluster are not supported.')

    submit_timeout_secs = 10
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
                                  language='PYTHON',
                                  spark_app_submission_time_in_s=submit_timeout_secs)

    dev_raw_data_source >> record_deduplicator >> spark_executor >= trash2
    record_deduplicator >> trash

    pipeline = builder.build(title='Spark executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    snapshot2 = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, batches=1).snapshot
    sdc_executor.stop_pipeline(pipeline)

    assert 'default user (sdc)' == snapshot2[spark_executor.instance_name].event_records[0].field['submitter'].value
    assert snapshot2[spark_executor.instance_name].event_records[0].field['timestamp'].value
    assert snapshot2[spark_executor.instance_name].event_records[0].field['app-id'].value

    # assert Spark executor has triggered the YARN job
    assert cluster.yarn.wait_for_app_to_register(application_name)
