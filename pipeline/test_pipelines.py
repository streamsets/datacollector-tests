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
# Utility functions
#

def pipeline_file_path(file, dir='pipelines'):
    return join(dirname(realpath(__file__)), dir, file)

#
# Basic cluster mode tests.
#

@cluster_test
def test_hdfs_origin_to_hbase_destination(args):
    cluster = environment.Cluster(cluster_server=args.cluster_server)
    pipeline = sdc_models.Pipeline(pipeline_filepath('pipeline_1.json')).configure_for_environment(cluster)

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
    pipeline = sdc_models.Pipeline(pipeline_file_path('pipeline_1.json')).configure_for_environment(cluster)

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
# Strict impersonation (SDC-3704).
#

@cluster_test
def test_strict_impersonation_hdfs(args):
    cluster = environment.Cluster(cluster_server=args.cluster_server)
    pipeline = sdc_models.Pipeline(
        pipeline_file_path('test_strict_impersonation_hdfs.json')
    ).configure_for_environment(cluster)

    hdfs_path = os.path.join(os.sep, "tmp", str(uuid4()))
    pipeline.stages['HadoopFS_01'].output_path = hdfs_path

    dc = sdc.DataCollector(version=args.sdc_version)
    dc.set_user('admin')
    dc.add_pipeline(pipeline)
    dc.sdc_properties['stage.conf_hadoop.always.impersonate.current.user'] = 'true'

    # Run at least one batch of data (write something).
    dc.start()
    dc.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished()
    dc.stop_pipeline(pipeline).wait_for_stopped()

    # Validate that the files were created with proper user name.
    entries = cluster.hdfs.client.list(hdfs_path)
    assert len(entries) == 1

    status = cluster.hdfs.client.status("{0}/{1}".format(hdfs_path, entries[0]))
    assert status['owner'] == 'admin'

#
# Pipeline rules.
#

def test_basic_data_rules(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('data_rules.json'))
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_pipeline(pipeline)
    dc.start()

    # Run at least 100 batches.
    dc.start_pipeline(pipeline).wait_for_pipeline_batch_count(100)

    alerts = dc.get_alerts().for_pipeline(pipeline)
    assert len(alerts) == 2

    # The order of alerts is arbitrary, so clean up
    data_alert = alerts[0] if alerts[0].label == "data-rule-data-lane" else alerts[1]
    event_alert = alerts[1] if alerts[1].label == "data-rule-event-lane" else alerts[0]

    assert data_alert.label == "data-rule-data-lane"
    assert data_alert.alert_texts == ["data-rule-data-lane"]
    assert event_alert.label == "data-rule-event-lane"
    assert event_alert.alert_texts == ["data-rule-event-lane"]

    # Tear down.
    dc.stop_pipeline(pipeline).wait_for_stopped()
    dc.tear_down()


def test_basic_drift_rules(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('drift_rule.json'))
    dc = sdc.DataCollector(version=args.sdc_version)
    dc.add_pipeline(pipeline)
    dc.start()

    # For drift rules, running a single batch is sufficient.
    dc.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

    alerts = dc.get_alerts().for_pipeline(pipeline)
    assert len(alerts) == 2

    # The order of alerts is arbitrary, so clean up
    data_alert = alerts[0] if alerts[0].label == "drift-rule-data-lane" else alerts[1]
    event_alert = alerts[1] if alerts[1].label == "drift-rule-event-lane" else alerts[0]

    assert data_alert.label == "drift-rule-data-lane"
    assert data_alert.alert_texts == ["drift-rule-data-lane"]
    assert event_alert.label == "drift-rule-event-lane"
    assert event_alert.alert_texts == ["drift-rule-event-lane"]

    # Tear down.
    dc.stop_pipeline(pipeline).wait_for_stopped()
    dc.tear_down()


#
# Error record handing.
#

def test_error_records_stop_pipeline_on_required_field(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

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
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

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
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'TO_ERROR'
    pipeline.stages['ExpressionEvaluator_01'].stage_required_fields = ['/b']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # All records should go to error stream.
        input_records = snapshot['DevDataGenerator_01'].output
        stage = snapshot['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == len(input_records)


def test_error_records_to_error_on_record_precondition(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'TO_ERROR'
    pipeline.stages['ExpressionEvaluator_01'].stage_record_preconditions = ['${1 == 2}']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # All records should go to error stream.
        input_records = snapshot['DevDataGenerator_01'].output
        stage = snapshot['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == len(input_records)


def test_error_records_discard_on_required_field(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'DISCARD'
    pipeline.stages['ExpressionEvaluator_01'].stage_required_fields = ['/b']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
        stage = snapshot['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == 0


def test_error_records_discard_on_record_precondition(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

    pipeline.stages['ExpressionEvaluator_01'].stage_on_record_error = 'DISCARD'
    pipeline.stages['ExpressionEvaluator_01'].stage_record_preconditions = ['${1 == 2}']

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        data_collector.stop_pipeline(pipeline)
        # Output of the stage should be empty as all records were discarded (doesn't fit the condition).
        stage = snapshot['ExpressionEvaluator_01']
        assert len(stage.output) == 0
        assert len(stage.error_records) == 0


#
# Pipeline level ELs
#


def test_pipeline_el_user(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

    pipeline.stages['ExpressionEvaluator_01'].header_expressions = [
        {"attributeToSet": "user", "headerAttributeExpression": "${pipeline:user()}"},
    ]

    with sdc.DataCollector(version=args.sdc_version) as dc:
        dc.add_pipeline(pipeline)
        dc.add_user("arvind", roles=["admin"])
        dc.add_user("girish", roles=["admin"])
        dc.start()

        # Run the pipeline as one user.
        dc.set_user("arvind")
        snapshot = dc.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        dc.stop_pipeline(pipeline).wait_for_stopped()

        record = snapshot['ExpressionEvaluator_01'].output[0]
        assert record.header["user"] == "arvind"

        # And then try different user.
        dc.set_user("girish")
        snapshot = dc.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        dc.stop_pipeline(pipeline).wait_for_stopped()

        record = snapshot['ExpressionEvaluator_01'].output[0]
        assert record.header["user"] == "girish"

def test_pipeline_el_name_title_id_version(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('random_expression_trash.json'))

    pipeline.title = "Most Pythonic Pipeline"
    pipeline.metadata["dpm.pipeline.version"] = 42
    pipeline.stages['ExpressionEvaluator_01'].header_expressions = [
        {"attributeToSet": "title", "headerAttributeExpression": "${pipeline:title()}"},
        {"attributeToSet": "name", "headerAttributeExpression": "${pipeline:name()}"},
        {"attributeToSet": "version", "headerAttributeExpression": "${pipeline:version()}"},
        {"attributeToSet": "id", "headerAttributeExpression": "${pipeline:id()}"},
    ]

    with sdc.DataCollector(version=args.sdc_version) as dc:
        dc.add_pipeline(pipeline)
        dc.start()

        snapshot = dc.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        dc.stop_pipeline(pipeline).wait_for_stopped()

        record = snapshot['ExpressionEvaluator_01'].output[0]
        assert record.header["name"] == pipeline.id
        assert record.header["id"] == pipeline.id
        assert record.header["title"] == pipeline.title
        assert record.header["version"] == '42'



def test_str_unescape_and_replace_el(args):
    pipeline = sdc_models.Pipeline(pipeline_file_path('string_el_pipeline.json'))

    with sdc.DataCollector(version=args.sdc_version) as data_collector:
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        snapshot = data_collector.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        input_records = snapshot['DevRawDataSource_01'].output
        stage = snapshot['ExpressionEvaluator_01']
        assert len(stage.output) == len(input_records)
        el_out = stage.output[0]
        assert input_records[0].value['value']['text']['value'] == 'here\nis\tsome\ndata'
        assert el_out.value['value']['text']['value'] == 'here\nis\tsome\ndata'
        assert el_out.value['value']['transformed']['value'] == 'here<NEWLINE>is\tsome<NEWLINE>data'
        assert el_out.value['value']['transformed2']['value'] == 'here<NEWLINE>is<TAB>some<NEWLINE>data'

