# Copyright 2017 StreamSets Inc.
#
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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import json
import logging
import os
import string

import pytest
import sqlalchemy

import test_apache
from testframework.markers import *
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000
SNAPSHOT_TIMEOUT_SEC = 120

DEFAULT_IMPALA_DB = 'default'

@cluster('cdh')
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

    hadoop_fs_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_hdfs_cluster_ClusterHdfsDSource')
    hadoop_fs_origin.data_format = 'TEXT'
    hadoop_fs_origin.input_paths.append(hadoop_fs_folder)

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host,
                                                              SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = get_random_string(string.ascii_letters, 10)

    hadoop_fs_origin >> sdc_rpc_destination
    hadoop_fs_pipeline = builder.build(title='Hadoop FS pipeline').configure_for_environment(cluster)
    hadoop_fs_pipeline.configuration['executionMode'] = 'CLUSTER_BATCH'

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
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
        sdc_executor.start_pipeline(hadoop_fs_pipeline).wait_for_finished()

        snapshot = snapshot_pipeline_command.wait_for_finished().snapshot
        sdc_executor.stop_pipeline(snapshot_pipeline)
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == lines_in_file
    finally:
        cluster.hdfs.client.delete(hadoop_fs_folder, recursive=True)


@cluster('cdh')
def test_hbase_destination(sdc_builder, sdc_executor, cluster):
    """Simple HBase destination test.

    dev_raw_data_source >> hbase
    """
    dumb_haiku = ['I see you driving',
                  'Round town with the girl I love',
                  "And I'm like haiku."]
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = '\n'.join(dumb_haiku)

    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/text'
    hbase.fields = [dict(columnValue='/text',
                         columnStorageType='TEXT',
                         columnName='cf1:cq1')]

    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf1': {}})

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()

        assert [record.value['value']['text']['value']
                for record in snapshot[dev_raw_data_source.instance_name].output] == dumb_haiku
    finally:
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


@cluster('cdh')
def test_hbase_lookup_processor(sdc_builder, sdc_executor, cluster):
    """Simple HBase Lookup processor test.

    Pipeline will enrich records with the name of Grand Tours by adding a field containing the year
    of their first editions, which will come from an HBase table.

    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Generate some silly data.
    bike_races = [dict(name='Tour de France', first_edition='1903'),
                  dict(name="Giro d'Italia", first_edition='1909'),
                  dict(name='Vuelta a Espana', first_edition='1935')]

    # Convert to raw data for the Dev Raw Data Source.
    raw_data = '\n'.join(bike_race['name'] for bike_race in bike_races)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/text')}",
                              columnExpr='info:first_edition',
                              outputFieldPath='/founded',
                              timestampExpr='')]
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})
        # Use HappyBase's `Batch` instance to avoid unnecessary calls to HBase.
        batch = cluster.hbase.client.table(table_name).batch()
        for bike_race in bike_races:
            # Use of str.encode() below is because HBase (and HappyBase) speaks in byte arrays.
            batch.put(bike_race['name'].encode(), {b'info:first_edition': bike_race['first_edition'].encode()})
        batch.send()

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert [dict(name=record.value['value']['text']['value'],
                     first_edition=record.value['value']['founded']['value'])
                for record in snapshot[hbase_lookup.instance_name].output] == bike_races
    finally:
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh')
def test_kafka_destination(sdc_builder, sdc_executor, cluster):
    """Send simple text messages into Kafka Destination from Dev Raw Data Source and
       confirm that Kafka successfully reads them using KafkaConsumer from cluster.
       Specifically, this would look like:

       Kafka Destination Origin pipeline:
           dev_raw_data_source >> kafka_destination

    """

    kafka_topic_name = get_random_string(string.ascii_letters, 10)

    # Build the Kafka destination pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'Hello World!'

    kafka_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=cluster.kafka.standalone_stage_lib)
    kafka_destination.kafka_topic_name = kafka_topic_name
    kafka_destination.data_format = 'TEXT'

    dev_raw_data_source >> kafka_destination
    kafka_destination_pipeline = builder.build(title='Kafka Destination pipeline').configure_for_environment(cluster)
    kafka_destination_pipeline.configuration['rateLimit'] = 1

    sdc_executor.add_pipeline(kafka_destination_pipeline)

    # Specify timeout so that iteration of consumer is stopped after that time and
    # specify auto_offset_reset to get messages from beginning.
    consumer = cluster.kafka.consumer(consumer_timeout_ms=1000, auto_offset_reset='earliest')
    consumer.subscribe([kafka_topic_name])

    # Send messages using pipeline to Kafka Destination.
    logger.debug('Starting Kafka Destination pipeline and waiting for it to produce 10 records ...')
    sdc_executor.start_pipeline(kafka_destination_pipeline).wait_for_pipeline_batch_count(10)

    logger.debug('Stopping Kafka Destination pipeline and getting the count of records produced in total ...')
    sdc_executor.stop_pipeline(kafka_destination_pipeline).wait_for_stopped()

    history = sdc_executor.pipeline_history(kafka_destination_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    logger.debug('No. of messages sent in the pipeline = %s', msgs_sent_count)

    msgs_received = [message.value.decode().strip() for message in consumer]
    logger.debug('No. of messages received in Kafka Consumer = %d', (len(msgs_received)))

    logger.debug('Verifying messages with Kafka consumer client ...')
    assert msgs_sent_count == len(msgs_received)
    assert msgs_received == [dev_raw_data_source.raw_data] * msgs_sent_count


@cluster('cdh')
@pytest.mark.parametrize('execution_mode', ('cluster', 'standalone'), ids=('cluster_mode', 'standalone_mode'))
def test_kafka_origin(sdc_builder, sdc_executor, cluster, execution_mode):
    """Write simple text messages into Kafka and confirm that Kafka successfully reads them.
    Because cluster mode pipelines don't support snapshots, we do this verification using a
    second standalone pipeline whose origin is an SDC RPC written to by the Kafka Consumer pipeline.
    Specifically, this would look like:

    Kafka Consumer Origin pipeline:
        kafka_consumer >> sdc_rpc_destination

    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """

    is_cluster_mode = True if execution_mode == 'cluster' else False
    # Build the Kafka consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    kafka_consumer = builder.add_stage(name='com_streamsets_pipeline_stage_origin_kafka_KafkaDSource',
                                       library=cluster.kafka.cluster_stage_lib if is_cluster_mode
                                       else cluster.kafka.standalone_stage_lib)
    kafka_consumer.data_format = 'TEXT'
    kafka_consumer.kafka_topic_name = get_random_string(string.ascii_letters, 10)
    if is_cluster_mode:
        kafka_consumer.max_rate_per_partition = 10
    kafka_consumer.consumer_configs = [{"key": "auto.offset.reset", "value": "earliest"}]

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.rpc_connections.append('{}:{}'.format(sdc_executor.server_host,
                                                              SDC_RPC_PORT))
    sdc_rpc_destination.rpc_id = get_random_string(string.ascii_letters, 10)

    kafka_consumer >> sdc_rpc_destination
    kafka_consumer_pipeline = builder.build(title='{0} pipeline'.format(execution_mode)).configure_for_environment(cluster)
    kafka_consumer_pipeline.configuration['executionMode'] = 'CLUSTER_YARN_STREAMING' if is_cluster_mode else 'STANDALONE'

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.rpc_port = SDC_RPC_PORT
    sdc_rpc_origin.rpc_id = sdc_rpc_destination.rpc_id
    # Since YARN jobs take a while to get going, set RPC origin batch wait time to 5 min. to avoid
    # getting an empty batch in the snapshot.
    sdc_rpc_origin.batch_wait_time_in_secs = 300

    trash = builder.add_stage(label='Trash')

    sdc_rpc_origin >> trash
    snapshot_pipeline = builder.build(title='{0} Snapshot pipeline'.format(execution_mode))

    # Add both pipelines we just created to SDC.
    sdc_executor.add_pipeline(kafka_consumer_pipeline, snapshot_pipeline)

    try:
        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline,
                                                                  start_pipeline=True)

        logger.debug('Starting Kafka Consumer pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(kafka_consumer_pipeline).wait_for_status('RUNNING')

        producer = cluster.kafka.producer()
        for _ in range(20):
            producer.send(kafka_consumer.kafka_topic_name, b'Hello World from SDC & DPM!')
        producer.flush()

        # Verify the messages are received correctly.
        logger.debug('Finish the snapshot and verify')
        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC).snapshot
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == ['Hello World from SDC & DPM!'] * 20

    finally:
        sdc_executor.stop_pipeline(snapshot_pipeline)
        sdc_executor.stop_pipeline(kafka_consumer_pipeline)


@cluster('cdh')
def test_kudu_destination(sdc_builder, sdc_executor, cluster):
    """Simple Dev Raw Data Source to Kudu pipeline.

    dev_raw_data_source >> kudu
    """
    # Generate some data.
    tour_de_france_contenders = [dict(favorite_rank=1, name='Chris Froome', wins=3),
                                 dict(favorite_rank=2, name='Greg LeMond', wins=3),
                                 dict(favorite_rank=4, name='Vincenzo Nibali', wins=1),
                                 dict(favorite_rank=3, name='Nairo Quintana', wins=0)]
    raw_data = ''.join([json.dumps(contender) for contender in tour_de_france_contenders])

    # For a little more coverage, we'll map the "favorite_rank" record field to the "rank" column in Kudu.
    # These rankings are Dima's opinion and not reflective of the views of StreamSets, Inc.
    field_to_column_mapping = [dict(field='/favorite_rank', columnName='rank')]

    kudu_table_name = get_random_string(string.ascii_letters, 10)

    # Build the pipeline.
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    kudu = builder.add_stage('Kudu',
                             type='destination').set_attributes(table_name='impala::{}.{}'.format(DEFAULT_IMPALA_DB,
                                                                                                  kudu_table_name),
                                                                default_operation='INSERT',
                                                                field_to_column_mapping=field_to_column_mapping)
    dev_raw_data_source >> kudu

    pipeline = builder.build().configure_for_environment(cluster)
    pipeline.delivery_guarantee = 'AT_MOST_ONCE'
    # We want to write data once and then stop, but Dev Raw Data Source will keep looping, so we set the rate limit to
    # a low value and will rely upon pipeline metrics to know when to stop the pipeline.
    pipeline.rate_limit = 4

    metadata = sqlalchemy.MetaData()
    tdf_contenders_table = sqlalchemy.Table(kudu_table_name,
                                            metadata,
                                            sqlalchemy.Column('rank', sqlalchemy.Integer, primary_key=True),
                                            sqlalchemy.Column('name', sqlalchemy.String),
                                            sqlalchemy.Column('wins', sqlalchemy.Integer),
                                            impala_partition_by='HASH PARTITIONS 16',
                                            impala_stored_as='KUDU')

    try:
        logger.info('Creating Kudu table %s ...', kudu_table_name)
        engine = cluster.kudu.engine
        tdf_contenders_table.create(engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(len(tour_de_france_contenders))
        sdc_executor.stop_pipeline(pipeline)

        connection = engine.connect()
        result = connection.execute(sqlalchemy.sql.select([tdf_contenders_table]).order_by('rank'))
        assert list(result) == [tuple([item['favorite_rank'], item['name'], item['wins']])
                                for item in sorted(tour_de_france_contenders, key=lambda key: key['favorite_rank'])]
    finally:
        logger.info('Dropping Kudu table %s ...', kudu_table_name)
        tdf_contenders_table.drop(engine)


@cluster('cdh')
def test_solr_destination(sdc_builder, sdc_executor, cluster):
    """Test Solr target pipeline in CDH environment. Note: CDH 5.x at this time runs with Solr 4.x variants.
    This version of Solr does not support API operations for schema and collections.
    """
    test_apache.basic_solr_target('cdh', sdc_builder, sdc_executor, cluster)


@cluster('cdh')
def test_hive_query_executor(sdc_builder, sdc_executor, cluster):
    """Test Hive query executor stage. This is acheived by using a deduplicator which assures us that there is
    only one successful ingest. The pipeline would look like:

        dev_raw_data_source >> record_deduplicator >> hive_query
                                                   >> trash
    """
    hive_table_name = get_random_string(string.ascii_letters, 10)
    hive_queries = ["CREATE TABLE ${record:value('/text')} (id int, name string)"]
    hive_cursor = cluster.hive.client.cursor()

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=hive_table_name)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    hive_query = builder.add_stage('Hive Query', type='executor').set_attributes(queries=hive_queries)

    dev_raw_data_source >> record_deduplicator >> hive_query
    record_deduplicator >> trash

    pipeline = builder.build(title='Hive query executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        # assert successful query execution of the pipeline
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline).wait_for_stopped()
        assert snapshot[hive_query.instance_name].event_records[0].header['sdc.event.type'] == 'successful-query'

        # assert Hive table creation
        assert hive_cursor.table_exists(hive_table_name)

        # Re-running the same query to create Hive table should fail the query. So assert the failure.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)
        assert snapshot[hive_query.instance_name].event_records[0].header['sdc.event.type'] == 'failed-query'
    finally:
        # drop the Hive table
        hive_cursor.execute(f'DROP TABLE `{hive_table_name}`')


@cluster('cdh')
def test_mapreduce_executor(sdc_builder, sdc_executor, cluster):
    """Test MapReduce executor stage. This is acheived by using a deduplicator which assures us that there is
    only one successful ingest and that we ingest to HDFS. The executor then triggers MapReduce job which should
    convert the ingested HDFS Avro data to Parquet. The pipeline would look like:

        dev_raw_data_source >> record_deduplicator >> hadoop_fs >= mapreduce
                                                   >> trash
    """
    hdfs_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    product_data = [dict(name='iphone', price=649.99),
                    dict(name='pixel', price=649.89)]
    raw_data = ''.join([json.dumps(product) for product in product_data])
    avro_schema = ('{ "type" : "record", "name" : "STF", "fields" : '
                   '[ { "name" : "name", "type" : "string" }, { "name" : "price", "type" : "double" } ] }')

    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=raw_data)
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    hadoop_fs = builder.add_stage('Hadoop FS', type='destination')
    # max_records_in_file enables to close the file and generate the event
    hadoop_fs.set_attributes(avro_schema=avro_schema, avro_schema_location='INLINE', data_format='AVRO',
                             directory_template=hdfs_directory, files_prefix='sdc-${sdc:id()}', max_records_in_file=1)
    mapreduce = builder.add_stage('MapReduce', type='executor')
    mapreduce.job_type = 'AVRO_PARQUET'
    mapreduce.output_directory = hdfs_directory

    dev_raw_data_source >> record_deduplicator >> hadoop_fs >= mapreduce
    record_deduplicator >> trash

    pipeline = builder.build(title='MapReduce executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)

        # assert events (MapReduce) generated
        assert len(snapshot[mapreduce.instance_name].event_records) == len(product_data)

        # make sure MapReduce job is done and is successful
        for event in snapshot[mapreduce.instance_name].event_records:
            job_id = event.value['value']['job-id']['value']
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'SUCCEEDED'

        # assert parquet data is same as what is ingested
        for event in snapshot[hadoop_fs.instance_name].event_records:
            file_path = event.value['value']['filepath']['value']
            hdfs_parquet_file_path = '{}.parquet'.format(file_path)
            hdfs_data = cluster.hdfs.get_data_from_parquet(hdfs_parquet_file_path)
            assert hdfs_data[0] in product_data
    finally:
        # remove HDFS files
        cluster.hdfs.client.delete(hdfs_directory, recursive=True)

@cluster('cdh')
def test_spark_executor(sdc_builder, sdc_executor, cluster):
    """Test Spark executor stage. This is acheived by using 2 pipelines. The 1st pipeline would generate the
    application resource file (Python in this case) which will be used by the 2nd pipeline for spark-submit. Spark
    executor will do the spark-submit and we assert that it has submitted the job to Yarn. The pipelines would
    look like:

        dev_raw_data_source >> local_fs >= pipeline_finisher_executor

        dev_raw_data_source >> record_deduplicator >> spark_executor
                               record_deduplicator >> trash
    """
    python_data = 'print("Hello World!")'
    tmp_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    python_suffix = 'py'
    yarn_application_name = ''.join(['stf_', get_random_string(string.ascii_letters, 10)])

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
    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished().snapshot
    file_path = snapshot[local_fs.instance_name].event_records[0].value['value']['filepath']['value']

    # build the 2nd pipeline - spark executor
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT', raw_data='dummy')
    record_deduplicator = builder.add_stage('Record Deduplicator')
    trash = builder.add_stage('Trash')
    spark_executor = builder.add_stage('Spark Executor', type='executor')
    spark_executor.set_attributes(cluster_manager='YARN', min_executors=1, max_executors=1,
                                  yarn_application_name=yarn_application_name, yarn_deploy_mode='CLUSTER',
                                  yarn_driver_memory='10m', yarn_executor_memory='10m',
                                  yarn_resource_file=file_path, yarn_resource_language='PYTHON')

    dev_raw_data_source >> record_deduplicator >> spark_executor
    record_deduplicator >> trash

    pipeline = builder.build(title='Spark executor pipeline').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
    sdc_executor.stop_pipeline(pipeline)

    # assert Spark executor has triggered the YARN job
    assert cluster.yarn.wait_for_app_to_register(yarn_application_name)
