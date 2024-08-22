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
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@cluster('mapr')
@sdc_min_version('3.0.0.0')
def test_mapr_json_db_cdc_origin(sdc_builder, sdc_executor, cluster):
    """Insert, update, delete a handful of records in the MapR-DB json table using a pipeline.
    After that create another pipeline with CDC Consumer and verify that MapR DB CDC
    consumer gets the correct data. Also we do tht with the non-CDC consumer to verify also that it works properly

    dev_raw_data_source >> expression evaluator >> field_remover >> mapr_db_json
    mapr_db_cdc_consumer >> wiretap
    mapr_db_json >> wiretap
    """
    if not cluster.version[len('mapr'):].startswith('6') and not cluster.version[len('mapr'):].startswith('7'):
        pytest.skip('MapR CDC test only runs against cluster with MapR version > 6.')
    if cluster.mep_version == "4.0":
        pytest.skip('MapR CDC test are written only for MEP 5 and above.')

    table_name = get_random_string(string.ascii_letters, 10)
    topic_name = f'{table_name}-topic'
    table_path = f'/user/sdc/{table_name}'
    stream_name = f'/{get_random_string(string.ascii_letters, 10)}'

    # Generate some data, including null values.
    test_data = [dict(_id='1', name='Sachin Tendulkar', operation='insert',
                      average=53.79, is_alive=True, runs_bf=1592129437, innings=329),
                 dict(_id='2', name='Don Bradman', operation='insert',
                      average=53.79, is_alive=False, runs_bf=69969798, innings=80),
                 dict(_id='3', name='Gary Sobers', operation='insert',
                      average=57.78, is_alive=True, runs_bf=80323867, innings=160),
                 dict(_id='4', name=None, operation='insert',
                      average=58.23, is_alive=True, runs_bf=70323867, innings=140),
                 dict(_id='1', name='Sachin', operation='update'),
                 dict(_id='2', name='Don', operation='update'),
                 dict(_id='3', operation='delete')]
    raw_data = ''.join(json.dumps(record) for record in test_data)

    # Expected final data, field remover stage will have the operation field removed
    final_data = [dict(_id='1', name='Sachin', average=53.79, is_alive=True, runs_bf=1592129437, innings=329),
                  dict(_id='2', name='Don', average=53.79, is_alive=False, runs_bf=69969798, innings=80),
                  dict(_id='4', name=None, average=58.23, is_alive=True, runs_bf=70323867, innings=140)]

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
    mapr_db_json_destination = pipeline_builder.add_stage('MapR DB JSON', type='destination')
    mapr_db_json_destination.set_attributes(table_name=table_path, row_key='/_id')

    dev_raw_data_source >> expression_evaluator >> field_remover >> mapr_db_json_destination
    json_db_destination_pipeline = pipeline_builder.build('MapR Json DB Destination').configure_for_environment(cluster)

    # Build the MapR DB CDC Consumer pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    mapr_db_cdc_consumer = pipeline_builder.add_stage('MapR DB CDC Consumer', type='origin')
    mapr_db_cdc_consumer.set_attributes(mapr_streams_configuration=[dict(key='auto.offset.reset',
                                                                         value='earliest')],
                                        number_of_threads=1,
                                        topic_list=[dict(key=f'{stream_name}:{topic_name}',
                                                         value=f'{table_path}')])

    wiretap_cdc = pipeline_builder.add_wiretap()
    mapr_db_cdc_consumer >> wiretap_cdc.destination
    cdc_pipeline = pipeline_builder.build('MapR DB CDC Consumer').configure_for_environment(cluster)

    # Build the MapR DB JSON Consumer pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    mapr_db_json_origin = pipeline_builder.add_stage('MapR DB JSON Origin')
    mapr_db_json_origin.set_attributes(table_name=table_path)
    wiretap_json = pipeline_builder.add_wiretap()
    mapr_db_json_origin >> wiretap_json.destination
    json_db_origin_pipeline = pipeline_builder.build('MapR Json DB Origin').configure_for_environment(cluster)

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

        sdc_executor.add_pipeline(json_db_destination_pipeline, cdc_pipeline, json_db_origin_pipeline)
        sdc_executor.start_pipeline(json_db_destination_pipeline)

        sdc_executor.start_pipeline(cdc_pipeline)
        sdc_executor.start_pipeline(json_db_origin_pipeline)

        sdc_executor.wait_for_pipeline_metric(cdc_pipeline, 'input_record_count', 1, timeout_sec=300)
        sdc_executor.wait_for_pipeline_metric(json_db_origin_pipeline, 'input_record_count', 1, timeout_sec=300)

        sdc_executor.stop_pipeline(cdc_pipeline)
        sdc_executor.stop_pipeline(json_db_origin_pipeline)

        actual_cdc = [record.field for record in wiretap_cdc.output_records]
        for record in test_data:
            # In the pipeline, Field Remover stage removed field 'operation' and so it will not be present in actual.
            # Remove it from test_data, for verification with assert.
            record.pop('operation')

        actual_json = [record.field for record in wiretap_json.output_records]

        assert actual_cdc == test_data
        assert actual_json == final_data
    finally:
        logger.info('Deleting MapR-DB table changelog %s ...', f'{stream_name}:{topic_name}')
        cluster.execute_command('table', 'changelog', 'remove', http_request_method='POST',
                                data={'path': table_path, 'changelog': f'{stream_name}:{topic_name}'})
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
    bike_brands = [dict(name='Bianchi'),
                   dict(name='BMC'),
                   dict(name='Cannondale'),
                   dict(name='Specialized')]
    raw_data = ''.join(json.dumps(brand) for brand in bike_brands)

    table_name = '/user/sdc/{}'.format(get_random_string(string.ascii_letters, 10))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

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
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        table = cluster.mapr_db.client.table(name=table_name)
        # Due to the following bug in MapR 6.0.1 MEP 5.0, MapR DB table.scan() call hangs and times out.
        # https://mapr.com/support/s/article/Hung-issue-when-using-HappyBase-python-to-SCAN-MapRDB?language=ja%29
        # Hence read the database table by using table.row() call instead of whole table scan.
        result = [(bike_brand['name'].encode(), table.row(bike_brand['name'].encode()))
                  for bike_brand in bike_brands]
        # Bike brands are stored in a list of dicts ('name' => brand). Manipulate this to match what we
        # expect our MapR-DB rows to look like (including putting them in lexicographic order).
        assert [(bike_brand['name'].encode(), {b'cf1:cq1': bike_brand['name'].encode()})
                for bike_brand in bike_brands] == result

    finally:
        logger.info('Deleting MapR-DB table %s ...', table_name)
        cluster.execute_command('table', 'delete', http_request_method='POST', data={'path': table_name})


@cluster('mapr')
def test_mapr_fs_standalone_origin(sdc_builder, sdc_executor, cluster):
    """Write a simple file into a MapR FS folder with a randomly-generated name and confirm that the MapR FS origin
    successfully reads it.

    Specifically, this would look like:

    MapR FS pipeline:
        mapr_fs_standalone_origin >> wiretap
    """
    mapr_fs_folder = os.path.join(os.sep, get_random_string(string.ascii_letters, 10))

    # Build the MapR FS Standalone pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    mapr_fs_standalone_origin = builder.add_stage(name='com_streamsets_pipeline_stage_origin_maprfs_MapRFSDSource')
    mapr_fs_standalone_origin.data_format = 'TEXT'
    mapr_fs_standalone_origin.files_directory = mapr_fs_folder
    mapr_fs_standalone_origin.file_name_pattern = '*'

    wiretap = builder.add_wiretap()

    pipeline_finished_executor = builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    mapr_fs_standalone_origin >> wiretap.destination
    mapr_fs_standalone_origin >= pipeline_finished_executor

    mapr_fs_pipeline = builder.build(title='MapR FS Standalone pipeline')
    sdc_executor.add_pipeline(mapr_fs_pipeline)

    try:
        lines_in_file = ['hello', 'hi', 'how are you?']

        logger.debug('Writing file %s/file.txt to MapR FS ...', mapr_fs_folder)
        cluster.mapr_fs.client.makedirs(mapr_fs_folder)
        cluster.mapr_fs.client.write(os.path.join(mapr_fs_folder, 'file.txt'), data='\n'.join(lines_in_file))

        # Start Pipeline.
        logger.debug('Starting MapR FS pipeline and waiting for it to finish ...')
        sdc_executor.start_pipeline(mapr_fs_pipeline).wait_for_finished()

        assert lines_in_file == [record.field['text'] for record in wiretap.output_records]

    finally:
        cluster.mapr_fs.client.delete(mapr_fs_folder, recursive=True)


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
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

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
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

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


def _test_mapr_standalone_multitopic_streams_generic(sdc_builder, sdc_executor, cluster, with_timestamp):
    """
    Utility method to run the multitopic streams test so we can version-guard the 'with timestamp' option
    """
    # MapR Stream name has to be pre-created in MapR cluster. Clusterdock MapR image has this already.
    stream_name = '/sample-stream'
    stream_topic_name = stream_name + ':' + get_random_string(string.ascii_letters, 10)
    stream_topic_name_2 = stream_name + ':' + get_random_string(string.ascii_letters, 10)
    wait_batches = 5
    stream_producer_values = ['Hello World!', 'Goodbye World!']

    # Build the MapR Stream producer pipeline.
    producer_pipeline = generate_streams_producer(sdc_builder, stream_topic_name, stream_producer_values[0], cluster)

    # Build the MapR Stream producer pipeline.
    producer_pipeline_2 = generate_streams_producer(sdc_builder, stream_topic_name_2, stream_producer_values[1], cluster)

    # Build the MapR Stream consumer pipeline.
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')

    mapr_streams_consumer = builder.add_stage('MapR Multitopic Streams Consumer')
    mapr_streams_consumer.set_attributes(data_format='TEXT',
                                         topic_list=[stream_topic_name, stream_topic_name_2],
                                         auto_offset_reset='EARLIEST',
                                         consumer_group=get_random_string(string.ascii_letters, 10),
                                         number_of_threads=10)
    if with_timestamp:
        mapr_streams_consumer.set_attributes(include_timestamps=True)
    wiretap = builder.add_wiretap()
    mapr_streams_consumer >> wiretap.destination
    consumer_pipeline = builder.build('MapR Multitopic Consumer Pipeline').configure_for_environment(cluster)
    consumer_pipeline.configuration['executionMode'] = 'STANDALONE'
    consumer_pipeline.configuration['shouldRetry'] = False

    sdc_executor.add_pipeline(producer_pipeline, producer_pipeline_2, consumer_pipeline)

    # Run pipelines and assert the data flow. To do that, the sequence of steps is as follows:
    # 1. Start MapR Stream producer and make sure to wait till some batches generate
    # 2. Start MapR Stream consumer via wiretap utility to make sure data flow can be captured
    # 3. Compare and assert wiretap result to the data injected at the producer
    sdc_executor.start_pipeline(producer_pipeline).wait_for_pipeline_batch_count(wait_batches)
    sdc_executor.start_pipeline(consumer_pipeline)
    sdc_executor.wait_for_pipeline_metric(consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(consumer_pipeline)

    wiretap_data = [record.field['text'].value for record in wiretap.output_records]
    sdc_executor.stop_pipeline(producer_pipeline)
    assert len(wiretap_data) > 0
    assert stream_producer_values[0] in wiretap_data
    if with_timestamp:
        record_header = [record.header for record in wiretap.output_records]
        for element in record_header:
            assert 'timestamp' in element['values']
            assert 'timestampType' in element['values']

    wiretap.reset()

    sdc_executor.start_pipeline(producer_pipeline_2).wait_for_pipeline_batch_count(wait_batches)

    sdc_executor.start_pipeline(consumer_pipeline)
    sdc_executor.wait_for_pipeline_metric(consumer_pipeline, 'input_record_count', 1)
    sdc_executor.stop_pipeline(consumer_pipeline)

    wiretap_data = [record.field['text'].value for record in wiretap.output_records]
    sdc_executor.stop_pipeline(producer_pipeline_2)
    assert len(wiretap_data) > 0
    assert stream_producer_values[1] in wiretap_data
    if with_timestamp:
        record_header = [record.header for record in wiretap.output_records]
        for element in record_header:
            assert 'timestamp' in element['values']
            assert 'timestampType' in element['values']


@cluster('mapr')
@sdc_min_version('3.7.0')
def test_mapr_standalone_multitopic_streams(sdc_builder, sdc_executor, cluster):
    """This test will start MapR Streams producer and consumer pipelines which check for integrity of data
    from a MapR Streams producer to MapR Streams consumer. Both the pipelines run as standalone. Specifically, this
    would look like:

    MapR Streams producer pipeline:
        dev_raw_data_source >> mapr_streams_producer

    MapR Streams consumer pipeline:
        mapr_streams_consumer >> wiretap
    """
    if cluster.mep_version < '6.0':
        pytest.skip('MapR Streams are currently only supported on latest version of MEP (e.g. MEP > 6)')
    _test_mapr_standalone_multitopic_streams_generic(sdc_builder, sdc_executor, cluster, False)


@cluster('mapr')
@sdc_min_version('3.16.0')
def test_mapr_standalone_multitopic_streams_with_timestamp(sdc_builder, sdc_executor, cluster):
    """This test will start MapR Streams producer and consumer pipelines which check for integrity of data
    from a MapR Streams producer to MapR Streams consumer, including the message timestamp.
    Both the pipelines run as standalone. Specifically, this would look like:

    MapR Streams producer pipeline:
        dev_raw_data_source >> mapr_streams_producer

    MapR Streams consumer pipeline:
        mapr_streams_consumer >> wiretap
    """
    if cluster.mep_version < '6.0':
        pytest.skip('MapR Streams are currently only supported on latest version of MEP (e.g. MEP > 6)')
    _test_mapr_standalone_multitopic_streams_generic(sdc_builder, sdc_executor, cluster, True)


def generate_streams_producer(sdc_builder, topic_name, value, cluster):
    builder = sdc_builder.get_pipeline_builder()
    builder.add_error_stage('Discard')
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = value
    mapr_streams_producer = builder.add_stage('MapR Streams Producer')
    mapr_streams_producer.data_format = 'TEXT'
    mapr_streams_producer.runtime_topic_resolution = True
    mapr_streams_producer.topic_expression = topic_name
    dev_raw_data_source >> mapr_streams_producer
    producer_pipeline = builder.build('Second MapR Streams producer pipeline - standalone').configure_for_environment(
        cluster)
    producer_pipeline.rate_limit = 1
    producer_pipeline.configuration['executionMode'] = 'STANDALONE'
    producer_pipeline.configuration['shouldRetry'] = False
    return producer_pipeline


@cluster('mapr')
@sdc_min_version('4.4.0')
@pytest.mark.parametrize('input_records', [2000])
def test_mapr_db_cdc_origin_preview(sdc_builder, sdc_executor, cluster, input_records):
    """We had an issue in which preview pipeline committed records read from streams, which made actual runs not
    read those records. This test will preview the pipeline and then assert we have the expected number of records.

    dev_data_generator >> expression evaluator >> field_remover >> mapr_db_json
    mapr_db_cdc_consumer >> wiretap
    """
    if not cluster.version[len('mapr'):].startswith('6') and not cluster.version[len('mapr'):].startswith('7'):
        pytest.skip('MapR CDC test only runs against cluster with MapR version > 6.')
    if cluster.mep_version == "4.0":
        pytest.skip('MapR CDC test are written only for MEP 5 and above.')

    table_name = get_random_string(string.ascii_letters, 10)
    topic_name = f'{table_name}-topic'
    table_path = f'/user/sdc/{table_name}'
    stream_name = f'/{get_random_string(string.ascii_letters, 10)}'

    # Build the MapR JSON DB pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(records_to_be_generated=input_records)
    dev_data_generator.fields_to_generate = [
        {'field': '_id', 'type': 'STRING'},
        {'field': 'name', 'type': 'STRING'},
        {'field': 'address', 'type': 'STRING'},
        {'field': 'mail', 'type': 'STRING'},
    ]

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
    mapr_db_json_destination = pipeline_builder.add_stage('MapR DB JSON', type='destination')
    mapr_db_json_destination.set_attributes(table_name=table_path, row_key='/_id')

    dev_data_generator >> expression_evaluator >> field_remover >> mapr_db_json_destination
    json_db_destination_pipeline = pipeline_builder.build('MapR Json DB Destination').configure_for_environment(cluster)

    # Build the MapR DB CDC Consumer pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    mapr_db_cdc_consumer = pipeline_builder.add_stage('MapR DB CDC Consumer', type='origin')
    mapr_db_cdc_consumer.set_attributes(mapr_streams_configuration=[dict(key='auto.offset.reset',
                                                                         value='earliest')],
                                        number_of_threads=1,
                                        topic_list=[dict(key=f'{stream_name}:{topic_name}',
                                                         value=f'{table_path}')])

    wiretap_cdc = pipeline_builder.add_wiretap()
    mapr_db_cdc_consumer >> wiretap_cdc.destination
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

        sdc_executor.add_pipeline(json_db_destination_pipeline, cdc_pipeline)
        sdc_executor.start_pipeline(json_db_destination_pipeline).wait_for_finished()

        preview = sdc_executor.run_pipeline_preview(cdc_pipeline, timeout=30000).preview
        assert preview is not None
        assert preview.issues.issues_count == 0

        # We first assert preview has the default 10 records
        assert len(preview[mapr_db_cdc_consumer].output) == 10

        sdc_executor.start_pipeline(cdc_pipeline)
        sdc_executor.wait_for_pipeline_metric(cdc_pipeline, 'input_record_count', input_records, timeout_sec=90)

        actual_cdc = [record.field for record in wiretap_cdc.output_records]

        # And second that we actually consumed the input_records records, and not 10 less
        assert len(actual_cdc) == input_records
    finally:
        if sdc_executor.get_pipeline_status(cdc_pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(cdc_pipeline)
        logger.info('Deleting MapR-DB table changelog %s ...', f'{stream_name}:{topic_name}')
        cluster.execute_command('table', 'changelog', 'remove', http_request_method='POST',
                                data={'path': table_path, 'changelog': f'{stream_name}:{topic_name}'})
        logger.info('Deleting MapR stream %s ...', stream_name)
        cluster.execute_command('stream', 'delete', http_request_method='POST', data={'path': stream_name})
        logger.info('Deleting MapR-DB table %s ...', table_path)
        cluster.execute_command('table', 'delete', http_request_method='POST', data={'path': table_path})
