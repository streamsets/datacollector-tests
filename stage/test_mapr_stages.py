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

import pytest
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_LISTENING_RPC_PORT = 20000


@cluster('mapr')
@sdc_min_version('3.0.0.0')
def test_mapr_json_db_cdc_origin(sdc_builder, sdc_executor, cluster):
    """Insert, update, delete a handful of records in the MapR-DB json table using a pipeline.
    After that create another pipeline with CDC Consumer and verify with snapshot that MapR DB CDC
    consumer gets the correct data.

    dev_raw_data_source >> expression evaluator >> field_remover >> mapr_db_json
    mapr_db_cdc_consumer >> trash
    """
    if not cluster.version[len('mapr'):].startswith('6'):
        pytest.skip('MapR CDC test only runs against cluster with MapR version 6.')
    table_name = get_random_string(string.ascii_letters, 10)
    topic_name = f'{table_name}-topic'
    table_path = f'/user/sdc/{table_name}'
    stream_name = f'/{get_random_string(string.ascii_letters, 10)}'

    # Generate some data.
    test_data = [dict(_id='1', name='Sachin Tendulkar', operation='insert',
                      average=53.79, is_alive=True, runs_bf=1592129437, innings=329),
                 dict(_id='2', name='Don Bradman', operation='insert',
                      average=53.79, is_alive=False, runs_bf=69969798, innings=80),
                 dict(_id='3', name='Gary Sobers', operation='insert',
                      average=57.78, is_alive=True, runs_bf=80323867, innings=160),
                 dict(_id='1', name='Sachin', operation='update'),
                 dict(_id='2', name='Don', operation='update'),
                 dict(_id='3', operation='delete')]
    raw_data = ''.join(json.dumps(record) for record in test_data)

    # Build the MapR JSON DB pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                           stop_after_first_batch=True,
                                                                                           raw_data=raw_data)
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    header_attribute_expressions = ("${record:value('/operation')=='insert'?1:"
                                    "record:value('/operation')=='update'?3:"
                                    "record:value('/operation')=='delete'?2:1}")
    expression_evaluator.set_attributes(header_attribute_expressions=[
        {'attributeToSet': 'sdc.operation.type',
         'headerAttributeExpression': header_attribute_expressions}
    ])
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/operation'])
    mapr_db_json = pipeline_builder.add_stage('MapR DB JSON', type='destination')
    mapr_db_json.set_attributes(table_name=table_path, row_key='/_id')

    dev_raw_data_source >> expression_evaluator >> field_remover >> mapr_db_json
    json_db_pipeline = pipeline_builder.build('MapR Json DB').configure_for_environment(cluster)

    # Build the MapR DB CDC Consumer pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    mapr_db_cdc_consumer = pipeline_builder.add_stage('MapR DB CDC Consumer', type='origin')
    mapr_db_cdc_consumer.set_attributes(mapr_streams_configuration=[dict(key='auto.offset.reset',
                                                                         value='earliest')],
                                        number_of_threads=1,
                                        topic_list=[dict(key=f'{stream_name}:{topic_name}',
                                                         value=f'{table_path}')])

    trash = pipeline_builder.add_stage('Trash')
    mapr_db_cdc_consumer >> trash
    cdc_pipeline = pipeline_builder.build('MapR DB CDC Consumer').configure_for_environment(cluster)

    try:
        logger.info('Creating MapR-DB table %s ...', table_path)
        cluster.execute_command('table', 'create', http_request_method='POST',
                                data={'path': table_path,
                                      'defaultreadperm': 'p',
                                      'tabletype': 'json',
                                      'defaultwriteperm': 'p'})

        logger.info('Creating MapR stream %s ...', stream_name)
        cluster.execute_command('stream', 'create', http_request_method='POST',
                                data={'path': stream_name,
                                      'ischangelog': 'true',
                                      'consumeperm': 'p',
                                      'defaultpartitions': 1})

        changelog = f'{stream_name}:{topic_name}'
        logger.info('Creating MapR-DB table changelog %s ...', changelog)
        cluster.execute_command('table', 'changelog', 'add', http_request_method='POST',
                                data={'path': table_path,
                                      'changelog': changelog})

        sdc_executor.add_pipeline(json_db_pipeline, cdc_pipeline)
        cdc_pipeline_command = sdc_executor.capture_snapshot(cdc_pipeline, start_pipeline=True, wait=False)
        sdc_executor.start_pipeline(json_db_pipeline)

        # Verify with a snapshot.
        snapshot = cdc_pipeline_command.wait_for_finished(timeout_sec=120).snapshot
        sdc_executor.stop_pipeline(cdc_pipeline)

        actual = [record.value2 for record in snapshot[mapr_db_cdc_consumer].output]
        for record in test_data:
            # In the pipeline, Field Remover stage removed field 'operation' and so it will not be present in actual.
            # Remove it from test_data, for verification with assert.
            record.pop('operation')
        assert actual == test_data

    finally:
        logger.info('Deleting MapR-DB table changelog %s ...', f'{stream_name}:{topic_name}')
        cluster.execute_command('table', 'changelog', 'remove', http_request_method='POST',
                                data={'path': table_path,
                                      'changelog': f'{stream_name}:{topic_name}'})
        logger.info('Deleting MapR stream %s ...', stream_name)
        cluster.execute_command('stream', 'delete', http_request_method='POST', data={'path': stream_name})
        logger.info('Deleting MapR-DB table %s ...', table_path)
        cluster.execute_command('table', 'delete', http_request_method='POST', data={'path': table_path})


@cluster('mapr')
def test_mapr_db_destination(sdc_builder, sdc_executor, cluster):
    """Write a handful of records to the MapR-DB destination and confirm their presence with an HBase client.

    dev_raw_data_source >> mapr_db
    """
    # Generate some data.
    bike_brands = [dict(name='Cannondale'),
                   dict(name='Specialized'),
                   dict(name='Bianchi'),
                   dict(name='BMC')]
    raw_data = ''.join(json.dumps(brand) for brand in bike_brands)

    table_name = '/user/sdc/{}'.format(get_random_string(string.ascii_letters, 10))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data)

    mapr_db = pipeline_builder.add_stage('MapR DB', type='destination')
    mapr_db.set_attributes(table_name=table_name,
                           row_key='/name',
                           fields=[dict(columnValue='/name',
                                        columnStorageType='TEXT',
                                        columnName='cf1:cq1')])

    dev_raw_data_source >> mapr_db
    pipeline = pipeline_builder.build().configure_for_environment(cluster)

    try:
        logger.info('Creating MapR-DB table %s ...', table_name)
        cluster.execute_command('table', 'create', http_request_method='POST',
                                data={'path': table_name,
                                      'defaultreadperm': 'p',
                                      'defaultwriteperm': 'p'})
        cluster.execute_command('table', 'cf', 'create', http_request_method='POST',
                                data={'path': table_name, 'cfname': 'cf1'})

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(len(bike_brands))

        rows = [(key, value) for key, value in cluster.mapr_db.client.table(name=table_name).scan()]
        # Bike brands are stored in a list of dicts ('name' => brand). Manipulate this to match what we
        # expect our MapR-DB rows to look like (including putting them in lexicographic order).
        assert sorted((bike_brand['name'].encode(), {b'cf1:cq1': bike_brand['name'].encode()})
                      for bike_brand in bike_brands) == rows
    finally:
        logger.info('Deleting MapR-DB table %s ...', table_name)
        cluster.execute_command('table', 'delete', http_request_method='POST', data={'path': table_name})
        sdc_executor.stop_pipeline(pipeline)


@cluster('mapr')
def test_mapr_fs_origin(sdc_builder, sdc_executor, cluster):
    """Write a simple file into a MapR FS folder with a randomly-generated name and confirm that the MapR FS origin
    successfully reads it. Because cluster mode pipelines don't support snapshots, we do this verification using a
    second standalone pipeline whose origin is an SDC RPC written to by the MapR FS pipeline. Specifically, this would
    look like:

    MapR FS pipeline:
        mapr_fs_origin >> sdc_rpc_destination

    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """
    mapr_fs_folder = os.path.join(os.sep, get_random_string(string.ascii_letters, 10))

    # Build the MapR FS pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    mapr_fs_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_maprfs_ClusterMapRFSDSource')
    mapr_fs_origin.data_format = 'TEXT'
    mapr_fs_origin.input_paths.append(mapr_fs_folder)

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.sdc_rpc_connection.append('{}:{}'.format(sdc_executor.server_host,
                                                                 SDC_RPC_LISTENING_PORT))
    sdc_rpc_destination.sdc_rpc_id = get_random_string(string.ascii_letters, 10)

    mapr_fs_origin >> sdc_rpc_destination
    mapr_fs_pipeline = builder.build(title='MapR FS pipeline').configure_for_environment(cluster)
    mapr_fs_pipeline.configuration['executionMode'] = 'CLUSTER_BATCH'

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.sdc_rpc_listening_port = SDC_RPC_LISTENING_PORT
    sdc_rpc_origin.sdc_rpc_id = sdc_rpc_destination.sdc_rpc_id
    # Since YARN jobs take a while to get going, set RPC origin batch wait time to 5 min. to avoid
    # getting an empty batch in the snapshot.
    sdc_rpc_origin.batch_wait_time_in_secs = 300

    trash = builder.add_stage(label='Trash')

    sdc_rpc_origin >> trash
    snapshot_pipeline = builder.build(title='Snapshot pipeline')

    # Add both pipelines we just created to SDC and start writing files to MapR FS with the HDFS client.
    sdc_executor.add_pipeline(mapr_fs_pipeline, snapshot_pipeline)

    try:
        lines_in_file = ['hello', 'hi', 'how are you?']

        logger.debug('Writing file %s/file.txt to MapR FS ...', mapr_fs_folder)
        cluster.mapr_fs.client.makedirs(mapr_fs_folder)
        cluster.mapr_fs.client.write(os.path.join(mapr_fs_folder, 'file.txt'), data='\n'.join(lines_in_file))

        # So here's where we do the clever stuff. We use SDC's capture snapshot endpoint to start and begin
        # capturing a snapshot from the snapshot pipeline. We do this, however, without using the synchronous
        # wait_for_finished function. That way, we can switch over and start the MapR FS pipeline. Once that one
        # completes, we can go back and do an assert on the snapshot pipeline's snapshot.
        logger.debug('Starting snapshot pipeline and capturing snapshot ...')
        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline, start_pipeline=True,
                                                                  wait=False)

        logger.debug('Starting MapR FS pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(mapr_fs_pipeline).wait_for_finished()

        snapshot = snapshot_pipeline_command.wait_for_finished().snapshot
        lines_from_snapshot = [record.value['value']['text']['value']
                               for record in snapshot[snapshot_pipeline[0].instance_name].output]

        assert lines_from_snapshot == lines_in_file
    finally:
        cluster.mapr_fs.client.delete(mapr_fs_folder, recursive=True)
        # Force stop the pipeline to avoid hanging until the SDC RPC stage's max batch wait time is reached.
        sdc_executor.stop_pipeline(pipeline=snapshot_pipeline, force=True)


@cluster('mapr')
def test_mapr_fs_destination(sdc_builder, sdc_executor, cluster):
    """Write a few files into MapR-FS and confirm their presence with an HDFS client.
    We use a record deduplicator processor in between our dev raw data source origin and MapR-FS
    destination in order to add determinism to the files written to MapR-FS.

    dev_raw_data_source >> record_deduplicator >> mapr_fs
                                               >> to_error
    """
    # Generate some data.
    giro_stages = [dict(start='Alghero', finish='Olbia'),
                   dict(start='Olbia', finish='Tortoli'),
                   dict(start='Tortoli', finish='Cagliari')]
    raw_data = ''.join(json.dumps(stage) for stage in giro_stages)

    output_folder_path = Path('/', 'tmp', str(uuid4()))
    logger.info('Pipeline will write to folder %s ...', output_folder_path)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    to_error = pipeline_builder.add_stage('To Error')

    mapr_fs = pipeline_builder.add_stage('MapR FS', type='destination')
    mapr_fs.set_attributes(data_format='JSON',
                           directory_template=str(output_folder_path),
                           files_prefix='stages',
                           files_suffix='txt')

    dev_raw_data_source >> record_deduplicator >> mapr_fs
    record_deduplicator >> to_error

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(giro_stages))
        # Wait for pipeline to be stopped in order to ensure file has been written.
        sdc_executor.stop_pipeline(pipeline)

        mapr_fs_files = cluster.mapr_fs.client.list(str(output_folder_path))
        # With only 3 unique records, there should only be one file written.
        assert len(mapr_fs_files) == 1

        # Check that file prefix and suffix are respected.
        mapr_fs_filename = mapr_fs_files[0]
        assert mapr_fs_filename.startswith('stages') and mapr_fs_filename.endswith('txt')

        with cluster.mapr_fs.client.read(str(Path(output_folder_path, mapr_fs_filename))) as reader:
            file_contents = reader.read()

        assert {tuple(json.loads(line).items())
                for line in file_contents.decode().split()} == {tuple(stage.items())
                                                                for stage in giro_stages}
    finally:
        logger.info('Deleting MapR-FS directory %s ...', output_folder_path)
        cluster.mapr_fs.client.delete(str(output_folder_path), recursive=True)


@cluster('mapr')
def test_mapr_standalone_streams(sdc_builder, sdc_executor, cluster):
    """This test will start MapR Streams producer and consumer pipelines which check for integrity of data
    from a MapR Streams producer to MapR Streams consumer. Both the pipelines run as standalone. Specifically, this
    would look like:

    MapR Streams producer pipeline:
        dev_raw_data_source >> mapr_streams_producer

    MapR Streams consumer pipeline:
        mapr_streams_consumer >> trash
    """
    # MapR Stream name has to be pre-created in MapR cluster. Clusterdock MapR image has this already.
    stream_name = '/sample-stream'
    stream_topic_name = stream_name + ':' + get_random_string(string.ascii_letters, 10)

    # Build the MapR Stream producer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'Hello World!'

    mapr_streams_producer = builder.add_stage('MapR Streams Producer')
    mapr_streams_producer.topic = stream_topic_name
    mapr_streams_producer.data_format = 'TEXT'

    dev_raw_data_source >> mapr_streams_producer
    producer_pipeline = builder.build('MapR Streams producer pipeline - standalone').configure_for_environment(cluster)
    producer_pipeline.rate_limit = 1

    # Build the MapR Stream consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    mapr_streams_consumer = builder.add_stage('MapR Streams Consumer')
    mapr_streams_consumer.topic_name = stream_topic_name
    mapr_streams_consumer.data_format = 'TEXT'

    trash = builder.add_stage(label='Trash')

    mapr_streams_consumer >> trash
    consumer_pipeline = builder.build('MapR Streams consumer pipeline - standalone').configure_for_environment(cluster)
    consumer_pipeline.rate_limit = 1

    sdc_executor.add_pipeline(producer_pipeline, consumer_pipeline)

    # Run pipelines and assert the data flow. To do that, the sequence of steps is as follows:
    # 1. Start MapR Stream producer and make sure to wait till some batches generate
    # 2. Start MapR Stream consumer via capture snapshot feature to make sure data flow can be captured
    # 3. Capture snapshot on the MapR Stream consumer
    # 4. Compare and assert snapshot result to the data injected at the producer
    try:
        sdc_executor.start_pipeline(producer_pipeline).wait_for_pipeline_batch_count(5)
        snapshot_pipeline_command = sdc_executor.capture_snapshot(consumer_pipeline, start_pipeline=True,
                                                                  wait=False)
        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=120).snapshot
        snapshot_data = snapshot[consumer_pipeline[0].instance_name].output[0].value['value']['text']['value']
        assert dev_raw_data_source.raw_data == snapshot_data
    finally:
        sdc_executor.stop_pipeline(consumer_pipeline)
        sdc_executor.stop_pipeline(producer_pipeline)


@cluster('mapr')
def test_mapr_cluster_streams(sdc_builder, sdc_executor, cluster):
    """This test will start MapR Streams producer and consumer pipelines which check for integrity of data flow
    from a MapR Streams producer to MapR Streams consumer. Producer pipeline runs as standalone while the consumer
    one runs on cluster. Since cluster pipeline cannot be snapshot, we use RPC stage to snapshot the data.
    The pipeline would look like:

    MapR Streams producer pipeline:
        dev_raw_data_source >> mapr_streams_producer

    MapR Streams consumer pipeline:
        mapr_streams_consumer >> sdc_rpc_destination

    Snapshot pipeline:
        sdc_rpc_origin >> trash
    """
    # MapR Stream name has to be pre-created in MapR cluster. Clusterdock MapR image has this already.
    stream_name = '/sample-stream'
    stream_topic_name = stream_name + ':' + get_random_string(string.ascii_letters, 10)
    sdc_rpc_id = get_random_string(string.ascii_letters, 10)

    # Build the MapR Stream producer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = 'Hello World!'

    mapr_streams_producer = builder.add_stage('MapR Streams Producer')
    mapr_streams_producer.topic = stream_topic_name
    mapr_streams_producer.data_format = 'TEXT'

    dev_raw_data_source >> mapr_streams_producer
    producer_pipeline = builder.build('Streams Producer - cluster').configure_for_environment(cluster)
    producer_pipeline.rate_limit = 1

    # Build the MapR Stream consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()

    mapr_streams_consumer = builder.add_stage('MapR Streams Consumer')
    mapr_streams_consumer.topic_name = stream_topic_name
    mapr_streams_consumer.data_format = 'TEXT'

    sdc_rpc_destination = builder.add_stage(name='com_streamsets_pipeline_stage_destination_sdcipc_SdcIpcDTarget')
    sdc_rpc_destination.sdc_rpc_connection.append('{}:{}'.format(sdc_executor.server_host, SDC_RPC_LISTENING_PORT))
    sdc_rpc_destination.sdc_rpc_id = sdc_rpc_id

    mapr_streams_consumer >> sdc_rpc_destination
    consumer_pipeline = builder.build('Streams Consumer - cluster').configure_for_environment(cluster)
    consumer_pipeline.configuration['executionMode'] = 'CLUSTER_YARN_STREAMING'
    consumer_pipeline.rate_limit = 1

    # Build the Snapshot pipeline.
    builder = sdc_builder.get_pipeline_builder()

    sdc_rpc_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_sdcipc_SdcIpcDSource')
    sdc_rpc_origin.sdc_rpc_listening_port = SDC_RPC_LISTENING_PORT
    sdc_rpc_origin.sdc_rpc_id = sdc_rpc_id
    # Since YARN jobs take a while to get going, set RPC origin batch wait time to 5 min. to avoid
    # getting an empty batch in the snapshot.
    sdc_rpc_origin.batch_wait_time_in_secs = 300

    trash = builder.add_stage(label='Trash')

    sdc_rpc_origin >> trash
    snapshot_pipeline = builder.build('Snapshot pipeline - cluster')

    sdc_executor.add_pipeline(producer_pipeline, consumer_pipeline, snapshot_pipeline)

    # Run pipelines and assert the data flow. To do that, the sequence of steps is as follows:
    # 1. Start MapR Stream producer and make sure to wait till some output generates - ensures topic creation
    # 2. Start RPC origin (snapshot_pipeline) where snapshot can be captured
    # 3. Start MapR Stream consumer and make sure to wait till some output generates - ensures cluster streaming
    # 4. Initiate and capture snapshot on the RPC origin pipeline
    # 5. Compare and assert snapshot result to the data injected at the MapR Stream producer
    try:
        sdc_executor.start_pipeline(producer_pipeline).wait_for_pipeline_output_records_count(5)
        # RUNNING ensures RPC origin is started
        sdc_executor.start_pipeline(snapshot_pipeline)

        consumer_start_cmd = sdc_executor.start_pipeline(consumer_pipeline)
        consumer_start_cmd.wait_for_pipeline_output_records_count(5)

        snapshot_pipeline_command = sdc_executor.capture_snapshot(snapshot_pipeline, start_pipeline=False,
                                                                  wait=False)
        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=120).snapshot
        snapshot_data = snapshot[snapshot_pipeline[0].instance_name].output[0].value['value']['text']['value']

        assert dev_raw_data_source.raw_data == snapshot_data
    finally:
        # Force stop the pipeline to avoid hanging until the SDC RPC stage's max batch wait time is reached.
        sdc_executor.stop_pipeline(pipeline=snapshot_pipeline, force=True)
        sdc_executor.stop_pipeline(producer_pipeline)
        sdc_executor.stop_pipeline(consumer_pipeline)
