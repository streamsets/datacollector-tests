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

import collections
import json
import logging
import pytest
import sqlalchemy
import string

from datetime import datetime
from urllib.parse import urlparse
from streamsets.testframework.markers import database, cluster
from streamsets.testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DEV_DATA_GEN_FIELD_LIST = [{'field': 'stringField', 'type': 'STRING', 'precision': 10, 'scale': 2},
                           {'field': 'intField', 'type': 'INTEGER', 'precision': 10, 'scale': 2},
                           {'field': 'longField', 'type': 'LONG', 'precision': 10, 'scale': 2},
                           {'field': 'floatField', 'type': 'FLOAT', 'precision': 10, 'scale': 2},
                           {'field': 'doubleField', 'type': 'DOUBLE', 'precision': 10, 'scale': 2},
                           {'field': 'dateField', 'type': 'DATE', 'precision': 10, 'scale': 2},
                           {'field': 'decimalField', 'type': 'DECIMAL', 'precision': 10, 'scale': 5}]


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')

    return hook


def get_qualified_table_name(db, table_name):
    return f'`{db}`.`{table_name}`' if db else f'`{table_name}`'


def get_table_location(hive_cursor, db, table_name):
    hive_cursor.execute('DESC FORMATTED {0}'.format(get_qualified_table_name(db, table_name)))
    rows = [row for row in hive_cursor.fetchall()]
    url_result = urlparse([r[1] for r in rows if 'Location:' in r[0]][0])
    return url_result.path


def get_table_columns_and_type(hive_cursor, db, table_name):
    hive_cursor.execute('DESC {0}'.format(get_qualified_table_name(db, table_name)))
    return collections.OrderedDict({col_name: col_type for col_name, col_type, comment in hive_cursor.fetchall()})


@cluster('cdh')
@database
def test_query_with_parquet(sdc_builder, sdc_executor, cluster, database):
    """Validate end-to-end case with stopping the pipeline and executing the map/reduce job after it read all the
    data from database. Addresses Hive drift synchronization solution in parquet data format. The pipeline looks like:

        jdbc_query_consumer >= pipeline_finisher_executor
        jdbc_query_consumer >> expression_evaluator >> field_remover >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
        hadoop_fs >= mapreduce
    """
    table_name = get_random_string(string.ascii_lowercase, 20)  # lowercase for db compatibility (e.g. PostgreSQL)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String(25)),
                             sqlalchemy.Column('dt', sqlalchemy.String(20)))
    rows_in_database = [{'id': 1, 'name': 'Ji Sun', 'dt': '2017-05-03'},
                        {'id': 2, 'name': 'Jarcec', 'dt': '2017-05-03'},
                        {'id': 3, 'name': 'Santhosh', 'dt': '2017-05-03'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    jdbc_query_consumer.set_attributes(incremental_mode=False, sql_query=f'SELECT * FROM {table_name};')
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_expressions=[{'attributeToSet': 'dt',
                                                             'headerAttributeExpression': "${record:value('/dt')}"}])
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/dt'])
    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='PARQUET', database_expression=f'{database.database}',
                                 table_name="${record:attribute('jdbc.tables')}")
    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    # max_records_in_file enables to close the file and generate the event
    hadoop_fs.set_attributes(avro_schema_location='HEADER', data_format='AVRO', directory_in_header=True,
                             max_records_in_file=1)
    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')
    mapreduce = pipeline_builder.add_stage('MapReduce', type='executor')
    mapreduce.set_attributes(job_type='AVRO_PARQUET',
                             output_directory="${file:parentPath(file:parentPath(record:value('/filepath')))}")
    pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')

    jdbc_query_consumer >= pipeline_finisher_executor
    jdbc_query_consumer >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    hadoop_fs >= mapreduce
    pipeline = pipeline_builder.build(title='Hive drift test').configure_for_environment(cluster, database)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        logger.info('Adding %s rows into %s database ...', len(rows_in_database), database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), rows_in_database)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        # assert events (MapReduce) generated
        assert len(snapshot[mapreduce.instance_name].event_records) == len(rows_in_database)
        # make sure MapReduce job is done and is successful
        for event in snapshot[mapreduce.instance_name].event_records:
            job_id = event.value['value']['job-id']['value']
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'SUCCEEDED'
        # assert data
        hive_cursor.execute(f'RELOAD `{table_name}`')
        hive_cursor.execute(f'SELECT * from `{table_name}')
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in rows_in_database]
        assert sorted(hive_values) == sorted(raw_values)
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute(f'DROP TABLE `{table_name}`')


@cluster('cdh')
def test_null_fields(sdc_builder, sdc_executor, cluster):
    """Validate Null fields of different data type and see hive store and returns NULL. The pipeline looks like:

        dev_data_generator >> field_value_replacer >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
        dev_data_generator >= pipeline_finisher
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      delay_between_batches=10)

    dev_data_generator.fields_to_generate = DEV_DATA_GEN_FIELD_LIST

    field_value_replacer = pipeline_builder.add_stage('Value Replacer')
    field_value_replacer.fields_to_null = [{'fieldsToNull': ['/' + field_and_type_info['field']
                                                             for field_and_type_info in DEV_DATA_GEN_FIELD_LIST]}]
    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO', database_expression='default',
                                 partition_configuration=[],
                                 decimal_scale_expression='5',
                                 decimal_precision_expression='10',
                                 table_name=table_name)

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    # max_records_in_file enables to close the file and generate the event
    hadoop_fs.set_attributes(avro_schema_location='HEADER', data_format='AVRO', directory_in_header=True,
                             max_records_in_file=1)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    dev_data_generator >> field_value_replacer >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    dev_data_generator >= pipeline_finisher  # stop after first batch

    pipeline = pipeline_builder.build(title='Hive drift test - Null values').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        hive_cursor.execute(f'RELOAD `{table_name}`')
        hive_cursor.execute(f'SELECT * from `{table_name}`')
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        logger.debug('read_data = {}'.format(hive_values))
        assert 1 == len(hive_values)
        hive_row = hive_values[0]
        for value in hive_row:
            assert not value
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute(f'DROP TABLE `{table_name}`')


@cluster('cdh')
@pytest.mark.parametrize('db', ['', 'default', 'custom'])
@pytest.mark.parametrize('stored_as_avro', [True, False])
@pytest.mark.parametrize('external_table', [True, False])
@pytest.mark.parametrize('partitioned', [True, False])
def test_cold_start(sdc_builder, sdc_executor, cluster, db, stored_as_avro, external_table, partitioned):
    """Validate Cold Start no table and no data. This test also tests different types of table and methods of creation.
       The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    db_for_path = 'default' if not db else f'{db}.db' if db != 'default' else db
    database_location_for_table_path = (f'/tmp/sdc/hive/warehouse/{db_for_path}'
                                        if external_table else f'/user/hive/warehouse/{db_for_path}')

    table_path_template = f'{database_location_for_table_path}/{table_name}' if external_table else ''

    raw_data = [dict(id=1, name='abc'), dict(id=2, name='def'), dict(id=3, name='ghi')]

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[{'attributeToSet': 'db',
                                                                       'headerAttributeExpression': db},
                                                                      {'attributeToSet': 'table_name',
                                                                       'headerAttributeExpression': table_name}])

    partition_configuration = [{'name': 'dt', 'valueType': 'STRING',
                                'valueEL': '${YYYY()}-${MM()}-${DD()}'}] if partitioned else []
    partition_path_template = 'dt=${YYYY()}-${MM()}-${DD()}' if partitioned else ''

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="${record:attribute('db')}",
                                 external_table=external_table,
                                 table_path_template=table_path_template,
                                 partition_configuration=partition_configuration,
                                 partition_path_template=partition_path_template,
                                 decimal_scale_expression='5',
                                 decimal_precision_expression='10',
                                 table_name="${record:attribute('table_name')}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')
    hive_metastore.set_attributes(stored_as_avro=stored_as_avro)

    dev_raw_data_source >> expression_evaluator >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - Cold Start').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    if db:
        hive_cursor.execute(f'CREATE DATABASE IF NOT EXISTS`{db}`')
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        hive_cursor.execute('RELOAD {0}'.format(get_qualified_table_name(db, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(get_qualified_table_name(db, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in raw_data]
        if partitioned:
            for i in range(len(raw_values)):
                raw_values[i] = raw_values[i] + [datetime.now().strftime('%Y-%m-%d')]
        assert sorted(hive_values) == sorted(raw_values)

    finally:
        logger.info('Dropping table %s in Hive...', get_qualified_table_name(db, table_name))
        hive_cursor.execute('DROP TABLE {0}'.format(get_qualified_table_name(db, table_name)))
        if db and db != 'default':
            logger.info('Dropping Database %s in Hive...', db)
            hive_cursor.execute('DROP DATABASE IF EXISTS`{0}`'.format(db))
        if external_table:
            logger.info('Deleting Hadoop FS directory %s ...', database_location_for_table_path)
            cluster.hdfs.client.delete(database_location_for_table_path, recursive=True)


@cluster('cdh')
@pytest.mark.parametrize('db', ['custom'])
@pytest.mark.parametrize('external_table', [True, False])
@pytest.mark.parametrize('custom_database_location', ['/tmp/sdc/hive/warehouse/custom', ''])
@pytest.mark.parametrize('custom_table_location', ['/tmp/sdc/hive/warehouse/table', ''])
def test_database_and_table_location(sdc_builder, sdc_executor, cluster,
                  db, external_table, custom_database_location, custom_table_location):
    """Validate combination of custom and default location for database and table and assert data is read properly
       and locations are right. The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    if custom_table_location and not external_table:
        pytest.skip('Test skipped : SDC-5459: Hive processor is ignoring location for internal tables')

    table_path = (f'{custom_table_location}' if custom_table_location
                  else (f'{custom_database_location}/{table_name}' if custom_database_location
                        else f'/tmp/sdc/hive/warehouse/{db}.db/{table_name}' if external_table else ''))

    raw_data = [dict(id=1, name='abc'), dict(id=2, name='def'), dict(id=3, name='ghi')]

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[{'attributeToSet': 'db',
                                                                       'headerAttributeExpression': db},
                                                                      {'attributeToSet': 'table_name',
                                                                       'headerAttributeExpression': table_name}])

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="${record:attribute('db')}",
                                 external_table=external_table,
                                 table_path_template=table_path,
                                 partition_configuration=[],
                                 decimal_scale_expression='5',
                                 decimal_precision_expression='10',
                                 table_name="${record:attribute('table_name')}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    dev_raw_data_source >> expression_evaluator >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - Location').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()

    create_database_command = (f'CREATE DATABASE IF NOT EXISTS `{db}` LOCATION "{custom_database_location}"'
                               if custom_database_location else f'CREATE DATABASE IF NOT EXISTS `{db}`')
    hive_cursor.execute(create_database_command)

    # internal table_with_custom_location
    if not external_table and custom_table_location:
        create_table_command_template_prefix = ('CREATE TABLE IF NOT EXISTS {0} (id int, name string)'
                                                ' STORED AS AVRO').format(get_qualified_table_name(db, table_name))
        create_table_command = create_table_command_template_prefix + f' LOCATION "{custom_table_location}"'
        hive_cursor.execute(create_table_command)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=10)

        location_of_table = get_table_location(hive_cursor, db, table_name)

        hive_cursor.execute('RELOAD {0}'.format(get_qualified_table_name(db, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(get_qualified_table_name(db, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in raw_data]
        assert sorted(hive_values) == sorted(raw_values)
        # check location of the table
        expected_location_of_table = custom_table_location or (f'{custom_database_location}/{table_name}'
                                                               if custom_database_location else table_path
                                                               if external_table else
                                                               f'/user/hive/warehouse/{db}.db/{table_name}')

        assert expected_location_of_table == location_of_table
    finally:
        logger.info('Dropping table %s in Hive...', get_qualified_table_name(db, table_name))
        hive_cursor.execute('DROP TABLE {0}'.format(get_qualified_table_name(db, table_name)))
        logger.info('Dropping Database %s in Hive...', db)
        hive_cursor.execute('DROP DATABASE IF EXISTS`{0}`'.format(db))

        # delete only if external table
        if external_table:
            if table_path:
                logger.info('Deleting Hadoop FS directory %s ...', table_path)
                cluster.hdfs.client.delete(table_path, recursive=True)
            db_location_to_delete = custom_database_location or f'/tmp/sdc/hive/warehouse/{db}.db'
            logger.info('Deleting Hadoop FS directory %s ...', db_location_to_delete)
            cluster.hdfs.client.delete(db_location_to_delete, recursive=True)


@cluster('cdh')
@pytest.mark.parametrize('sdc_hive_type_info',
                         [('BOOLEAN', 'BOOLEAN', True), ('STRING', 'STRING', True), ('INTEGER', 'INT', True),
                          ('SHORT', 'INT', True), ('LONG', 'BIGINT', True), ('FLOAT', 'FLOAT', True),
                          ('DOUBLE', 'DOUBLE', True), ('DECIMAL', 'DECIMAL(4,2)', True), ('BYTE_ARRAY', 'BINARY', True),
                          ('BYTE', None, False), ('MAP', None, False),
                          ('LIST_MAP', None, False)])
def test_sdc_types(sdc_builder, sdc_executor, cluster, sdc_hive_type_info):
    """Validate Different Types of SDC Fields (supported and unsupported for hive) and assert data present
       or error records. The pipeline looks like:

        dev_data_generator >> expression_evaluator >> groovy_evaluator >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
        dev_data_generator >= pipeline_finisher
    """

    sdc_type, hive_type, supported = sdc_hive_type_info
    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      delay_between_batches=10)

    dev_data_generator.fields_to_generate = [{'field': 'id', 'precision': 10, 'scale': 2, 'type': 'INTEGER'}]

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[{'attributeToSet': 'sdc_type',
                                                                       'headerAttributeExpression': sdc_type}])

    groovy_evaluator = pipeline_builder.add_stage('Groovy Evaluator', type='processor')

    script = """
        def get_value_for_sdc_type(sdc_type) {
            switch (sdc_type) {
                case 'BOOLEAN':
                    return true;
                case 'INTEGER':
                    return 1;
                case 'SHORT':
                    return ((short)1);
                case 'LONG':
                    return 1L;
                case 'FLOAT':
                    return 1.0f;
                case 'DOUBLE':
                    return 1.0d;
                case 'DECIMAL':
                    return BigDecimal.valueOf(12.12);
                case 'BYTE_ARRAY':
                    return 'abc'.getBytes();
                case 'BYTE':
                    return 'abc'.getBytes()[0];
                case 'MAP':
                    def map_field = sdcFunctions.createMap(false);
                    [firstName:'John', lastName:'Doe'].each {
                        map_field.put(it.key, it.value)
                    };
                    return map_field;
                case 'LIST_MAP':
                    def list_map_field = sdcFunctions.createMap(true);
                    [firstName:'John', lastName:'Doe'].each {
                      list_map_field.put(it.key, it.value)
                    };
                    return list_map_field;
                case 'LIST':
                    return ['abc', 'def'];
                default:
                    return 'abc'
            }
        }
        for (record in records) {
          try {
            def sdc_type = record.attributes['sdc_type']
            record.value['custom'] = get_value_for_sdc_type(sdc_type)
            output.write(record)
          } catch (e) {
            log.error(e.toString(), e)
            error.write(record, e.toString())
          }
        }
    """
    groovy_evaluator.set_attributes(enable_invokedynamic_compiler_option=True,
                                    record_processing_mode='BATCH', script=script)

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="default",
                                 external_table=False,
                                 partition_configuration=[],
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name=table_name)

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    dev_data_generator >> expression_evaluator >> groovy_evaluator >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    dev_data_generator >= pipeline_finisher

    pipeline = pipeline_builder.build(title='Hive drift test - SDC Types').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        stage = snapshot[hive_metadata.instance_name]
        list_of_record_output_from_hive_metastore = stage.output_lanes[hive_metadata.output_lanes[0]]
        if not supported:
            assert len(stage.error_records) == 1
            assert len(list_of_record_output_from_hive_metastore) == 0
        else:
            assert len(stage.error_records) == 0
            assert len(list_of_record_output_from_hive_metastore) == 1

            column_and_types_from_hive = get_table_columns_and_type(hive_cursor, None, table_name)
            assert column_and_types_from_hive['custom'].upper() == hive_type

            record_output_from_hive_metastore = list_of_record_output_from_hive_metastore[0]

            hive_cursor.execute('RELOAD {0}'.format(get_qualified_table_name(None, table_name)))
            hive_cursor.execute('SELECT * from {0}'.format(get_qualified_table_name(None, table_name)))
            hive_values = [list(row) for row in hive_cursor.fetchall()]
            assert len(hive_values) == 1

            hive_values = hive_values[0]
            custom_value = record_output_from_hive_metastore.value2['custom']
            # hive client returns the binary as string
            custom_value = custom_value.decode() if hive_type == 'BINARY' else custom_value
            expected_row_values = [record_output_from_hive_metastore.value2['id'], custom_value]

            assert hive_values == expected_row_values
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@cluster('cdh')
@pytest.mark.parametrize('partition_type_and_value', [('INT', 1), ('BIGINT', 1), ('STRING', 'abc')])
def test_partition_types(sdc_builder, sdc_executor, cluster, partition_type_and_value):
    """Validate different supported partition types and assert data is read properly. The pipeline looks like:

        dev_raw_data_source >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    partition_type, partition_value = partition_type_and_value

    raw_data = [collections.OrderedDict(id=1, name='abc', value=partition_value, part=partition_value)]

    dev_raw_data_source_data = ''.join(json.dumps({k: v for k, v in d.items() if k != 'part'}) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    partition_configuration = [{'name': 'part', 'valueType': partition_type, 'valueEL': '${record:value("/value")}'}]

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="default",
                                 external_table=False,
                                 partition_configuration=partition_configuration,
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name=table_name)

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    dev_raw_data_source >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - SDC Types').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        hive_cursor.execute('RELOAD {0}'.format(get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(get_qualified_table_name(None, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in raw_data]
        assert len(hive_values) == 1
        assert hive_values == raw_values
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))
