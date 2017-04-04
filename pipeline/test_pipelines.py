# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
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
import pytest
from uuid import uuid4
from os.path import dirname, join, realpath

from testframework.markers import *
from testframework import environment, sdc, sdc_api, sdc_models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#
# Basic cluster mode tests.
#

@cluster_test
def test_hdfs_origin_to_hbase_destination(args):
    cluster = environment.Cluster(cluster_server=args.cluster_server)
    pipeline = sdc_models.Pipeline(
        join(dirname(realpath(__file__)), 'pipelines', 'pipeline_1.json')
    ).configure_for_environment(cluster)

    # Generate a random string to use when naming the HDFS input path folder and the HBase table.
    random_name = str(uuid4())
    # And generate a short list of data for a file in HDFS and (hopefully) rows in an HBase table.
    random_data = ['hello', 'hi', 'how are you']

    try:
        # Create HDFS input path folder and write a file with three lines of text.
        hdfs_input_path = os.path.join(os.sep, random_name)
        cluster.hdfs.client.makedirs(hdfs_input_path)
        cluster.hdfs.client.write(hdfs_path=os.path.join(hdfs_input_path,
                                                         'file.txt'),
                                  data='\n'.join(random_data))

        # Create an HBase table with one column family.
        logger.info('Creating table %s...', random_name)
        cluster.hbase.client.create_table(name=random_name, families={'cf1': {}})

        # Update our pipeline stages to use the input path and table name we used above.
        pipeline.stages['HadoopFS_01'].input_paths = [os.path.join(os.sep, random_name)]
        pipeline.stages['HBase_01'].table_name = random_name

        # Start an SDC instance, import and start the pipeline, and then wait until it finishes.
        with sdc.DataCollector(version=args.sdc_version) as data_collector:
            data_collector.add_pipeline(pipeline)
            data_collector.start()
            data_collector.start_pipeline(pipeline).wait_for_finished()

        assert random_data == [row[0].decode()
                               for row in cluster.hbase.client.table(name=random_name).scan()]
    finally:
        # Clean up after ourselves.
        cluster.hdfs.client.delete(hdfs_input_path, recursive=True)
        cluster.hbase.client.delete_table(name=random_name, disable=True)


@cluster_test
def test_hdfs_origin_to_hbase_destination_missing_configs(args):
    cluster = environment.Cluster(cluster_server=args.cluster_server)
    pipeline = sdc_models.Pipeline(
        join(dirname(realpath(__file__)), 'pipelines', 'pipeline_1.json')
    ).configure_for_environment(cluster)

    # Update our pipeline stages to use the input path and table name we used above.
    pipeline.stages['HadoopFS_01'].input_paths = []
    pipeline.stages['HBase_01'].table_name = ''

    # Start an SDC instance, import the pipeline, start it, and then wait until it finishes running.
    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        with pytest.raises(sdc_api.StartError):
            data_collector.start_pipeline(pipeline).wait_for_finished()


#
# Error record handing.
#

def test_error_records_stop_pipeline_on_required_field(args):
    pipeline = sdc_models.Pipeline('pipelines/random_expression_trash.json')

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'STOP_PIPELINE'
    pipeline.stages['ExpressionEvaluator_01'].stage_required_fields = ['/b']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        with pytest.raises(sdc_api.RunError) as exception_info:
            data_collector.start_pipeline(pipeline).wait_for_finished()
        # Stage precondition: CONTAINER_0050 - The stage requires records to include the following.
        assert("CONTAINER_0050" in exception_info.value.message)


def test_error_records_stop_pipeline_on_record_precondition(args):
    pipeline = sdc_models.Pipeline('pipelines/random_expression_trash.json')

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'STOP_PIPELINE'
    pipeline.stages['ExpressionEvaluator_01'].stage_record_preconditions = ['${1 == 2}']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        with pytest.raises(sdc_api.RunError) as exception_info:
            data_collector.start_pipeline(pipeline).wait_for_finished()
        # Stage precondition: CONTAINER_0051 - Unsatisfied precondition.
        assert("CONTAINER_0051" in exception_info.value.message)


def test_error_records_to_error_on_required_field(args):
    pipeline = sdc_models.Pipeline('pipelines/random_expression_trash.json')

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'TO_ERROR'
    pipeline.stages['ExpressionEvaluator_01'].stage_required_fields = ['/b']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # All records should go to error stream.
        input_records = snapshot[0]['DevDataGenerator_01'].output
        stage = snapshot[0]['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == len(input_records)


def test_error_records_to_error_on_record_precondition(args):
    pipeline = sdc_models.Pipeline('pipelines/random_expression_trash.json')

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'TO_ERROR'
    pipeline.stages['ExpressionEvaluator_01'].stage_record_preconditions = ['${1 == 2}']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # All records should go to error stream.
        input_records = snapshot[0]['DevDataGenerator_01'].output
        stage = snapshot[0]['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == len(input_records)


def test_error_records_discard_on_required_field(args):
    pipeline = sdc_models.Pipeline('pipelines/random_expression_trash.json')

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'DISCARD'
    pipeline.stages['ExpressionEvaluator_01'].stage_required_fields = ['/b']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
        stage = snapshot[0]['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == 0


def test_error_records_discard_on_record_precondition(args):
    pipeline = sdc_models.Pipeline('pipelines/random_expression_trash.json')

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'DISCARD'
    pipeline.stages['ExpressionEvaluator_01'].stage_record_preconditions = ['${1 == 2}']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
        stage = snapshot[0]['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == 0
