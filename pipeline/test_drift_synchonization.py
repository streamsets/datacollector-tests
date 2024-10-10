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
import string
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from itertools import groupby, chain
from operator import itemgetter
from urllib.parse import urlparse

import pytest
import sqlalchemy
from streamsets.testframework.environments.cloudera import ClouderaManagerCluster
from streamsets.testframework.environments.hortonworks import AmbariCluster
from streamsets.testframework.markers import database, cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

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
    if 'hive' in cluster.kerberized_services:
        pytest.skip('Test runs only in non-kerberized environment till SDC-9324 is fixed.')
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
    expression_evaluator.set_attributes(header_attribute_expressions=[{'attributeToSet': 'dt',
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
    wiretap = pipeline_builder.add_wiretap()
    pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')

    jdbc_query_consumer >= pipeline_finisher_executor
    jdbc_query_consumer >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    hadoop_fs >= mapreduce
    mapreduce >= wiretap.destination

    pipeline = pipeline_builder.build(title='Hive drift test').configure_for_environment(cluster, database)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)
        logger.info('Adding %s rows into %s database ...', len(rows_in_database), database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), rows_in_database)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # assert events (MapReduce) generated
        assert len(wiretap.output_records) == len(rows_in_database)
        # make sure MapReduce job is done and is successful
        for event in wiretap.output_records:
            job_id = event.field['job-id'].value
            assert cluster.yarn.wait_for_job_to_end(job_id) == 'SUCCEEDED'

        # verify inserted data
        hive_cursor.execute(f'RELOAD {_get_qualified_table_name(None, table_name)}')
        hive_cursor.execute(f'SELECT * from {_get_qualified_table_name(None, table_name)}')
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in rows_in_database]
        assert sorted(hive_values) == sorted(raw_values)
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute(f'DROP TABLE `{table_name}`')


@cluster('cdh', 'hdp')
def test_null_fields(sdc_builder, sdc_executor, cluster):
    """Validate Null fields of different data type and see hive store and returns NULL. The pipeline looks like:

        dev_data_generator >> field_value_replacer >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
        dev_data_generator >= pipeline_finisher
    """
    if getattr(cluster, 'kerberized_services', False) and 'hive' in cluster.kerberized_services:
        pytest.skip('Test runs only in non-kerberized environment till SDC-9324 is fixed.')

    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=1,
                                      delay_between_batches=10)

    dev_data_generator.fields_to_generate = DEV_DATA_GEN_FIELD_LIST

    field_value_replacer = pipeline_builder.add_stage('Field Replacer')
    field_value_replacer.replacement_rules = [{'setToNull': True, 'fields': '/stringField'},
                                              {'setToNull': True, 'fields': '/intField'},
                                              {'setToNull': True, 'fields': '/longField'},
                                              {'setToNull': True, 'fields': '/floatField'},
                                              {'setToNull': True, 'fields': '/doubleField'},
                                              {'setToNull': True, 'fields': '/dateField'},
                                              {'setToNull': True, 'fields': '/decimalField'},
                                              ]
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


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
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
    if getattr(cluster, 'kerberized_services', False) and 'hive' in cluster.kerberized_services:
        pytest.skip('Test runs only in non-kerberized environment till SDC-9324 is fixed.')

    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

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
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in raw_data]
        if partitioned:
            for i in range(len(raw_values)):
                raw_values[i] = raw_values[i] + [datetime.now().strftime('%Y-%m-%d')]
        assert sorted(hive_values) == sorted(raw_values)

    finally:
        logger.info('Dropping table %s in Hive...', _get_qualified_table_name(db, table_name))
        hive_cursor.execute('DROP TABLE {0}'.format(_get_qualified_table_name(db, table_name)))
        if db and db != 'default':
            logger.info('Dropping Database %s in Hive...', db)
            hive_cursor.execute('DROP DATABASE IF EXISTS`{0}`'.format(db))
        if external_table:
            logger.info('Deleting Hadoop FS directory %s ...', database_location_for_table_path)
            cluster.hdfs.client.delete(database_location_for_table_path, recursive=True)


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
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
    if getattr(cluster, 'kerberized_services', False) and 'hive' in cluster.kerberized_services:
        pytest.skip('Test runs only in non-kerberized environment till SDC-9324 is fixed.')
    table_name = get_random_string(string.ascii_lowercase, 20)

    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    if custom_table_location and not external_table:
        pytest.skip('Test skipped : SDC-5459: Hive processor is ignoring location for internal tables')

    if custom_database_location and not custom_table_location and not external_table:
        pytest.skip(f'''Test skipped : COLLECTOR-3273: internal tables with custom location outside database
        managed location not supported''')

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
    # CDH 7 works in a bit more mysterious ways and it seems that the default creation path doesn't allow us
    # to write data into HDFS if Hive metastore created the directory for the table first.
    if isinstance(cluster, ClouderaManagerCluster) and cluster.version.startswith('cdh7'):
        hadoop_fs.impersonation_user = "root"

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
                                                ' STORED AS AVRO').format(_get_qualified_table_name(db, table_name))
        create_table_command = create_table_command_template_prefix + f' LOCATION "{custom_table_location}"'
        hive_cursor.execute(create_table_command)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=10)

        location_of_table = _get_table_location(hive_cursor, db, table_name)

        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in raw_data]
        assert sorted(hive_values) == sorted(raw_values)

        # check location of the table
        if custom_table_location:
            expected_location_of_table = custom_table_location
        elif custom_database_location:
            expected_location_of_table = f'{custom_database_location}/{table_name}'
        elif external_table:
            expected_location_of_table = table_path
        else:
            if isinstance(cluster, ClouderaManagerCluster) and cluster.version.startswith('cdh7'):
                # https://docs.cloudera.com/cdp/latest/data-migration/topics/cdp-data-migration-table-create.html
                # CDH 7 uses ACID for managed tables, so "internal non-acid tables" are slashed into external directory.
                expected_location_of_table = f'{_get_hive_warehouse_external_dir(hive_cursor)}/{db}.db/{table_name}'
            else:
                expected_location_of_table = f'{_get_hive_warehouse_dir(hive_cursor)}/{db}.db/{table_name}'

        assert expected_location_of_table == location_of_table

    finally:
        logger.info('Dropping table %s in Hive...', _get_qualified_table_name(db, table_name))
        hive_cursor.execute('DROP TABLE {0}'.format(_get_qualified_table_name(db, table_name)))
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


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_partition_locations(sdc_builder, sdc_executor, cluster):
    """Store data into a Hive managed table with multicolumn partitioning, and then check that the partition locations
    specified by Hive Metadata is actually where Hive Metastore have created them.

    Pipeline configuration:
        dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
        hive_metadata >> [hadoop_fs, wiretap]
        hive_metadata >> hive_metastore

    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    raw_data = [dict(id=1, name='abc', part1=get_random_string(), part2=get_random_string(), part3=get_random_string()),
                dict(id=2, name='def', part1=get_random_string(), part2=get_random_string(), part3=get_random_string()),
                dict(id=3, name='ghi', part1=get_random_string(), part2=get_random_string(), part3=get_random_string())]

    # Dev raw data source
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=''.join(json.dumps(d) for d in raw_data),
                                       stop_after_first_batch=True)

    # Expression evaluator
    database = get_random_string(string.ascii_lowercase, 20)
    table_name = get_random_string(string.ascii_lowercase, 20)
    header_attributes = [{'attributeToSet': 'database', 'headerAttributeExpression': database},
                         {'attributeToSet': 'table_name', 'headerAttributeExpression': table_name},
                         {'attributeToSet': 'part1', 'headerAttributeExpression': "${record:value('/part1')}"},
                         {'attributeToSet': 'part2', 'headerAttributeExpression': "${record:value('/part2')}"},
                         {'attributeToSet': 'part3', 'headerAttributeExpression': "${record:value('/part3')}"}]
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=header_attributes)

    # Field Remover
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/part1', '/part2', '/part3'])

    # Hive Metadata
    part1_name = 'part_' + get_random_string()
    part2_name = 'part_' + get_random_string()
    part3_name = 'part_' + get_random_string()
    partition_configuration = [{'name': part1_name, 'valueType': 'STRING', 'valueEL': "${record:attribute('part1')}"},
                               {'name': part2_name, 'valueType': 'STRING', 'valueEL': "${record:attribute('part2')}"},
                               {'name': part3_name, 'valueType': 'STRING', 'valueEL': "${record:attribute('part3')}"}]
    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="${record:attribute('database')}",
                                 table_name="${record:attribute('table_name')}",
                                 external_table=False,
                                 partition_configuration=partition_configuration)

    # Hadoop FS
    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    # Hive Metastore
    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    wiretap = pipeline_builder.add_wiretap()

    # Build pipeline
    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> [hive_metastore, wiretap.destination]

    pipeline = pipeline_builder.build(title='Hive drift - Test Partition Locations').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    qualified_table_name = _get_qualified_table_name(database, table_name)
    hive_cursor = cluster.hive.client.cursor()
    hive_cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
    hive_cursor.execute(f'USE {database}')

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        partition_metadata = [rec for rec in wiretap.output_records if rec.field['type'] == 'PARTITION']

        # Query the partition locations to the Hive database and compare them against the location generated by Hive
        # Metadata. Also, we are using the default location and therefore 'customLocation' must be always False.
        for raw, rec in zip(raw_data, partition_metadata):
            hive_cursor.execute(f"SHOW TABLE EXTENDED LIKE '{table_name}' PARTITION ({part1_name}='{raw['part1']}', {part2_name}='{raw['part2']}', {part3_name}='{raw['part3']}')")

            real_location = [row[0] for row in hive_cursor.fetchall() if row[0].startswith('location:')][0]
            assert 'customLocation' in rec.field and rec.field['customLocation'] == False
            assert real_location.endswith(str(rec.field['location']))

    finally:
        logger.info('Dropping table %s in Hive...', qualified_table_name)
        hive_cursor.execute(f'DROP TABLE IF EXISTS {qualified_table_name}')

        logger.info('Dropping database %s in Hive...', database)
        hive_cursor.execute(f'DROP DATABASE IF EXISTS {database}')


@cluster('cdh', 'hdp')
@pytest.mark.parametrize('sdc_type, hive_type, supported',
                         [('BOOLEAN', 'BOOLEAN', True), ('STRING', 'STRING', True), ('INTEGER', 'INT', True),
                          ('SHORT', 'INT', True), ('LONG', 'BIGINT', True), ('FLOAT', 'FLOAT', True),
                          ('DOUBLE', 'DOUBLE', True), ('DECIMAL', 'DECIMAL(4,2)', True),
                          ('BYTE_ARRAY', 'BINARY', True), ('BYTE', None, False),
                          ('MAP', None, False), ('LIST_MAP', None, False)])
def test_sdc_types(sdc_builder, sdc_executor, cluster, sdc_type, hive_type, supported):
    """Validate Different Types of SDC Fields (supported and unsupported for hive) and assert data present
       or error records. The pipeline looks like:

        dev_data_generator >> expression_evaluator >> groovy_evaluator >> hive_metadata
        hive_metadata >> [hadoop_fs, wiretap.destination]
        hive_metadata >> hive_metastore
        dev_data_generator >= pipeline_finisher
    """
    if getattr(cluster, 'kerberized_services', False) and 'hive' in cluster.kerberized_services:
        pytest.skip('Test runs only in non-kerberized environment till SDC-9324 is fixed.')

    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

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
    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> expression_evaluator >> groovy_evaluator >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore
    dev_data_generator >= pipeline_finisher

    pipeline = pipeline_builder.build(title='Hive drift test - SDC Types').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if not supported:
            assert len(wiretap.error_records) == 1
            assert len(wiretap.output_records) == 0
        else:
            assert len(wiretap.error_records) == 0
            assert len(wiretap.output_records) == 1

            column_and_types_from_hive = _get_table_columns_and_type(hive_cursor, None, table_name)
            assert column_and_types_from_hive['custom'].upper() == hive_type

            hive_cursor.execute(f'RELOAD {_get_qualified_table_name(None, table_name)}')
            hive_cursor.execute(f'SELECT * from {_get_qualified_table_name(None, table_name)}')
            hive_values = [list(row) for row in hive_cursor.fetchall()]
            assert len(hive_values) == 1

            hive_values = hive_values[0]
            custom_value = wiretap.output_records[0].field['custom'].value
            # hive client returns the binary as string
            custom_value = custom_value.decode() if hive_type == 'BINARY' else custom_value
            expected_row_values = [wiretap.output_records[0].field['id'], custom_value]
            assert hive_values == expected_row_values
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('partition_type, partition_value', [('INT', 1), ('BIGINT', 1), ('STRING', 'abc')])
def test_partition_types(sdc_builder, sdc_executor, cluster, partition_type, partition_value):
    """Validate different supported partition types and assert data is read properly. The pipeline looks like:

        dev_raw_data_source >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    if getattr(cluster, 'kerberized_services', False) and 'hive' in cluster.kerberized_services:
        pytest.skip('Test runs only in non-kerberized environment till SDC-9324 is fixed.')

    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = [OrderedDict(id=1, name='abc', value=partition_value, part=partition_value)]

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

    pipeline = pipeline_builder.build(title='Hive drift test - Partition Types').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in raw_data]
        assert len(hive_values) == 1
        assert hive_values == raw_values
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_multiplexing(sdc_builder, sdc_executor, cluster):
    """Validate multiplexing tables and assert data is right. The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_suffix = get_random_string(string.ascii_lowercase, 10)

    raw_data = [dict(id=1, name='San Francisco', table=f'towns_{table_suffix}', country='US', year='2016'),
                dict(id=2, customer='John', value=200, table=f'invoice_{table_suffix}', country='India', year='2015'),
                dict(id=3, name='Friedberg', table=f'towns_{table_suffix}', country='Germany', year='2017'),
                dict(id=4, customer='James', value=300, table=f'invoice_{table_suffix}',
                     country='Argentina', year='2014')]

    table_to_rows = {k: [[val for key, val in v.items() if key != 'table']
                         for v in sorted(list(g), key=itemgetter('id'))]
                     for k, g in groupby(sorted(raw_data, key=itemgetter('table')), key=itemgetter('table'))}

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    partition_configuration = [{'name': 'country', 'valueType': 'STRING', 'valueEL': '${record:attribute("country")}'},
                               {'name': 'year', 'valueType': 'STRING', 'valueEL': '${record:attribute("year")}'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'table',
                                                          'headerAttributeExpression': "${record:value('/table')}"},
                                                         {'attributeToSet': 'country',
                                                          'headerAttributeExpression': "${record:value('/country')}"},
                                                         {'attributeToSet': 'year',
                                                          'headerAttributeExpression': "${record:value('/year')}"}]

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/table', '/country', '/year'])

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="default",
                                 external_table=False,
                                 partition_configuration=partition_configuration,
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name='${record:attribute("table")}')

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - Multiplexing').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        for table_name, expected_rows in table_to_rows.items():
            logger.info('Validating table %s in Hive...', table_name)
            hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
            hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
            hive_values = [list(row) for row in hive_cursor.fetchall()]
            assert sorted(hive_values, key=itemgetter(0)) == expected_rows
    finally:
        for table_name in set([r['table'] for r in raw_data]):
            logger.info('Dropping table %s in Hive...', table_name)
            hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_special_characters_in_partition_value(sdc_builder, sdc_executor, cluster):
    """Validate special characters for partition value . The pipeline looks like:

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)
    partition_values = OrderedDict([("-", True), ("_", True), ("$", True), (",", True),
                                    ("(", True), (")", True), ("&", True), ("@", True),
                                    ("!", True), (".", True), ("|", True), ("~", True),
                                    ("`", True),
                                    ("\\", False), ("'", False), ("[", False), ("]", False),
                                    ("/", False), ("?", False), ("*", False), ("\"", False),
                                    ("%", False), ("=", False), ("^", False)])

    unsupported_partition_values = set([partition_value for partition_value, supported in partition_values.items()
                                        if not supported])

    raw_data = [dict(id=str(uuid.uuid4()), part=partition_value) for partition_value in partition_values.keys()]

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    partition_configuration = [{'name': 'part', 'valueType': 'STRING', 'valueEL': '${record:attribute("part")}'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'part',
                                                          'headerAttributeExpression': "${record:value('/part')}"}]

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/part'])

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

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['errorRecordPolicy'] = 'ORIGINAL_RECORD'
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        error_records = wiretap.error_records
        assert len(unsupported_partition_values) == len(error_records)

        for error_record in error_records:
            assert error_record.field['part'] in unsupported_partition_values

        logger.info('Validating Supported Partition Characters for Table %s in Hive...', table_name)
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        hive_values = sorted(hive_values, key=itemgetter(1))
        expected_values = sorted([list(row.values()) for row in raw_data
                                  if partition_values[row['part']]], key=itemgetter(1))
        assert expected_values == hive_values
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('table_or_column', [True, False])
@pytest.mark.parametrize('special_character', ['#', '-', '$', '.'])
def test_special_characters_in_table_and_columns(sdc_builder, sdc_executor, cluster,
                                                 table_or_column, special_character):
    """Validate special characters for table and columns. The pipeline looks like:

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')
    # https://docs.cloudera.com/cdp/latest/data-migration/topics/cdp-data-migration-dbtable.html
    if isinstance(cluster, ClouderaManagerCluster) and cluster.version.startswith('cdh7') and special_character == '.':
        pytest.skip(f"CDH 7 ({cluster.version}) doesn't support dot in a table name")

    object_name_prefix_suffix = get_random_string(string.ascii_lowercase, 5)

    object_name = f'{object_name_prefix_suffix}{special_character}{object_name_prefix_suffix}'

    table_name = object_name if table_or_column else object_name_prefix_suffix

    col_name = object_name if not table_or_column else object_name_prefix_suffix

    raw_data = dict(id=str(uuid.uuid4()), table=table_name)
    raw_data[col_name] = str(uuid.uuid4())

    dev_raw_data_source_data = ''.join(json.dumps(raw_data))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'table',
                                                          'headerAttributeExpression': "${record:value('/table')}"}]

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/part'])

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="default",
                                 external_table=False,
                                 partition_configuration=[],
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name="${record:attribute('table')}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore

    pipeline = (pipeline_builder.build(title='Hive drift test - Table/Column Special Characters')
                .configure_for_environment(cluster))
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert 1 == len(wiretap.error_records)
        # TODO: TLKT-41 - Add support for reading reserved headers from RecordHeader to assert error code
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('keyword', ["table", "create", "date", "as", "year", "string", "default"])
def test_keywords_in_object_names(sdc_builder, sdc_executor, cluster, keyword):
    """Validate different keywords in table/database name. The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
        hive_metadata >> hadoop_fs
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = keyword
    db = keyword

    raw_data = dict(id=str(uuid.uuid4()), part=str(uuid.uuid4()))
    dev_raw_data_source_data = ''.join(json.dumps(raw_data))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'database',
                                                          'headerAttributeExpression': db},
                                                         {'attributeToSet': 'table',
                                                          'headerAttributeExpression': table_name},
                                                         {'attributeToSet': 'part',
                                                          'headerAttributeExpression': "${record:value('/part')}"}]

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/part'])

    partition_configuration = [{'name': 'part', 'valueType': 'STRING', 'valueEL': '${record:attribute("part")}'}]

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="${record:attribute('database')}",
                                 external_table=False,
                                 partition_configuration=partition_configuration,
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name="${record:attribute('table')}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - Keywords').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    hive_cursor.execute(f'CREATE DATABASE IF NOT EXISTS `{db}`')
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        logger.info('Validating table %s in Hive...', _get_qualified_table_name(db, table_name))
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(db, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        assert 1 == len(hive_values)
        assert hive_values[0] == list([v for v in raw_data.values()])
    finally:
        logger.info('Dropping table %s in Hive...', _get_qualified_table_name(db, table_name))
        hive_cursor.execute('DROP TABLE {0}'.format(_get_qualified_table_name(db, table_name)))
        if db != 'default':
            logger.info('Dropping Database %s in Hive...', db)
            hive_cursor.execute('DROP DATABASE IF EXISTS`{0}`'.format(db))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('location', ['', 'schemaFolder', '/tmp/absoluteLocation', "${str:concat('a', 'b')}"])
def test_hdfs_schema_serialization(sdc_builder, sdc_executor, cluster, location):
    """Validate schema location exists when STORED_AS_AVRO is unchecked. The pipeline looks like:

        dev_raw_data_source >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = dict(id=str(uuid.uuid4()), table=table_name)
    dev_raw_data_source_data = ''.join(json.dumps(raw_data))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="default",
                                 external_table=True,
                                 partition_configuration=[],
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name=table_name,
                                 table_path_template=f"/tmp/{table_name}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')
    hive_metastore.stored_as_avro = False
    if location:
        hive_metastore.schema_folder_location = location

    dev_raw_data_source >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = (pipeline_builder.build(title='Hive drift test - Serialization Location')
                .configure_for_environment(cluster))
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()

    if str.startswith(location, '/'):
        expected_location = location
    elif location == "${str:concat('a', 'b')}":
        expected_location = f'/tmp/{table_name}/ab'
    elif location:
        expected_location = f'/tmp/{table_name}/{location}'
    else:
        expected_location = f'/tmp/{table_name}/.schemas'

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        status = cluster.hdfs.client.status(expected_location)
        assert status is not None and status['type'] == 'DIRECTORY'
    finally:
        # Delete schema location
        logger.info('Deleting Schema Directory %s in Hadoop FS...', expected_location)
        cluster.hdfs.client.delete(expected_location, recursive=True)
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_decimal_values(sdc_builder, sdc_executor, cluster, keep_data):
    """Validate different decimal values. The pipeline looks like:

    dev_raw_data_source >> field_type_converter >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    valid_rows = [dict(id=1, number=12.12), dict(id=2, number=1.0), dict(id=3, number=12.0),
                  dict(id=4, number=0.1), dict(id=5, number=0.12), dict(id=6, number=12)]
    # incompatible scale, precision
    invalid_rows = [dict(id=7, number=0.123), dict(id=8, number=12345)]
    raw_data = valid_rows + invalid_rows
    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.conversion_method = 'BY_FIELD'
    field_type_converter.set_attributes(field_type_converter_configs=[{'fields': ['/number'],
                                                                       'targetType':'DECIMAL', }])

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="default",
                                 external_table=True,
                                 partition_configuration=[],
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name=table_name,
                                 table_path_template=f"/tmp/{table_name}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)
    if isinstance(cluster, ClouderaManagerCluster) and cluster.version.startswith('cdh7'):
        if cluster.kerberized_services:
            hadoop_fs.impersonation_user = "sdctest"
        else:
            hadoop_fs.impersonation_user = "root"

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_type_converter >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    create_table_command = ('CREATE EXTERNAL TABLE IF NOT EXISTS {0} (id int, number decimal(4, 2))'
                            ' STORED AS AVRO LOCATION "/tmp/{1}"').format(_get_qualified_table_name(None, table_name), table_name)
    hive_cursor.execute(create_table_command)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Validating table %s in Hive...', _get_qualified_table_name(None, table_name))
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        assert hive_values == [[Decimal(str(v)) if k == 'number' else v for k, v in row.items()]
                               for row in valid_rows]
        error_values = [[fld for k, fld in error_record.field.items()] for error_record in wiretap.error_records]
        assert error_values == [[Decimal(str(v)) if k == 'number' else v for k, v in row.items()]
                                for row in invalid_rows]
    finally:
        if not keep_data:
            logger.info('Dropping table %s in Hive...', table_name)
            hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_partial_input(sdc_builder, sdc_executor, cluster):
    """Validate partial inputs. The pipeline looks like:

        dev_raw_data_source >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = [dict(idx=0), dict(idx=1),
                dict(idx=2, id='id2'), dict(idx=3, id='id3'),
                dict(idx=4, name='name4'), dict(idx=5, name='id4'),
                dict(idx=6, value='value6'),  dict(idx=7, value='value7'),
                dict(idx=8, value='value8', id='id8'), dict(idx=9, value='value9', id='id9'),
                dict(idx=10, name='name10', id='id10'), dict(idx=11, name='name11', id='id11')]
    cols = ['idx', 'id', 'name', 'value']
    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

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

    dev_raw_data_source >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - Partial Input').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        logger.info('Validating table %s in Hive...', _get_qualified_table_name(None, table_name))
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        expected_data = [[r[col] if col in r else None for col in cols] for r in raw_data]
        assert sorted(hive_values) == sorted(expected_data)
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('external_table', [True, False])
def test_column_drift(sdc_builder, sdc_executor, cluster, external_table):
    """Validate Column Drift in inputs. The pipeline looks like:

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name_suffix = get_random_string(string.ascii_lowercase, 20)

    raw_data = [OrderedDict([('id', 0), ('table', f'column_rename_add{table_name_suffix}')]),
                OrderedDict([('id', 1), ('col1', 'col11'), ('table', f'column_rename_add{table_name_suffix}')]),
                OrderedDict([('id', 2), ('col2', 'col22'), ('table', f'column_rename_add{table_name_suffix}')]),
                OrderedDict([('id', 3), ('col1', 'col31'), ('middle', 'middle'),
                             ('col2', 'col32'), ('table', f'column_rename_add{table_name_suffix}')]),

                OrderedDict([('first', 0), ('second', 0), ('third', 0), ('table', f'reorder{table_name_suffix}')]),
                OrderedDict([('second', 1), ('third', 1), ('first', 1), ('table', f'reorder{table_name_suffix}')]),

                OrderedDict([('id', 4), ('removed', 'removed'), ('table', f'removed{table_name_suffix}')]),
                OrderedDict([('id', 5), ('table', f'removed{table_name_suffix}')]),

                OrderedDict([('id', 8), ('col', 'col_lower_case'), ('table', f'column_case{table_name_suffix}')]),
                OrderedDict([('id', 9), ('COL', 'col_upper_case'), ('table', f'column_case{table_name_suffix}')]),

                OrderedDict([('id', 10), ('col', 1), ('table', f'column_type_change{table_name_suffix}')]),
                OrderedDict([('id', 11), ('col', 'col'), ('table', f'column_type_change{table_name_suffix}')])]

    table_path_prefix = '/tmp/sdc/hive/warehouse/default/' if external_table else ''

    table_path_template = table_path_prefix + "${record:attribute('table')}"

    table_to_invalid_rows = {f'column_type_change{table_name_suffix}': [OrderedDict(
        [('id', 11), ('col', 'col'), ('table', f'column_type_change{table_name_suffix}')])]}

    table_to_raw_data = {k: [{col.lower(): val for col, val in v.items() if col != 'table'} for v in list(g)]
                         for k, g in groupby(sorted(raw_data, key=itemgetter('table')), key=itemgetter('table'))}

    table_to_cols = {k: OrderedDict({col.lower(): col.lower() for v in list(g)
                                     for col, val in v.items() if col != 'table'})
                     for k, g in groupby(sorted(raw_data, key=itemgetter('table')), key=itemgetter('table'))}

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'table',
                                                          'headerAttributeExpression': "${record:value('/table')}"}]

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/table'])

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression='default',
                                 external_table=external_table,
                                 partition_configuration=[],
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_path_template=table_path_template,
                                 table_name='${record:attribute("table")}')

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['errorRecordPolicy'] = 'ORIGINAL_RECORD'
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.error_records) == 1  # column_type_change
        for table_name in table_to_raw_data.keys():
            logger.info('Validating table %s in Hive...', _get_qualified_table_name(None, table_name))
            hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
            table_columns_and_type = _get_table_columns_and_type(hive_cursor, None, table_name)
            hive_table_columns = list(table_columns_and_type.keys())
            data_table_cols = table_to_cols[table_name]
            assert hive_table_columns == list(data_table_cols.keys())
            hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
            hive_values = sorted([list(row) for row in hive_cursor.fetchall()], key=itemgetter(0))
            invalid_rows = ([[v for k, v in row.items() if k != 'table'] for row in table_to_invalid_rows[table_name]]
                            if table_name in table_to_invalid_rows else [])
            expected_data = sorted([[data[col] if col in data else None for col in hive_table_columns]
                                    for data in table_to_raw_data[table_name]
                                    if list(data.values()) not in invalid_rows],
                                   key=itemgetter(0))
            assert hive_values == expected_data

        error_values = [[fld for k, fld in error_record.field.items()] for error_record in wiretap.error_records]
        assert error_values == [[v for k, v in row.items()]
                                for table_invalid_rows in table_to_invalid_rows.values()
                                for row in table_invalid_rows]
    finally:
        for table_name in table_to_raw_data.keys():
            logger.info('Dropping table %s in Hive...', table_name)
            hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))
            if external_table:
                logger.info('Deleting Hadoop FS directory %s ...', table_path_prefix + table_name)
                cluster.hdfs.client.delete(table_path_prefix + table_name, recursive=True)


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('data_format', ['AVRO', 'PARQUET'])
def test_unsupported_table_data_formats(sdc_builder, sdc_executor, cluster, data_format):
    """Validate Unsupported Data Formats. The pipeline looks like:

    dev_raw_data_source >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = dict(id=str(uuid.uuid4()), name=get_random_string(string.ascii_lowercase, 20))
    dev_raw_data_source_data = ''.join(json.dumps(raw_data))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format=data_format,
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
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore

    pipeline = (pipeline_builder.build(title='Hive drift test - Unsupported Table Data Format Test')
                .configure_for_environment(cluster))
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        hive_cursor.execute(
            f'CREATE TABLE `{table_name}` (id int, value string) partitioned by (dt String) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.error_records) == 1

        logger.info('Validating table %s in Hive...', _get_qualified_table_name(None, table_name))
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        assert len([list(row) for row in hive_cursor.fetchall()]) == 0
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_drift_multiple_open_partitions(sdc_builder, sdc_executor, cluster):
    """Validate Multiple open partitions for the table. The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = [dict(id=str(uuid.uuid4()), part='part1'),
                dict(id=str(uuid.uuid4()), part='part2'),
                dict(id=str(uuid.uuid4()), part='part3', new_col='new_col31'),
                dict(id=str(uuid.uuid4()), part='part4'),
                dict(id=str(uuid.uuid4()), part='part5'),
                dict(id=str(uuid.uuid4()), part='part6', new_col='new_col61'),
                dict(id=str(uuid.uuid4()), part='part2', new_col='new_col21'),
                dict(id=str(uuid.uuid4()), part='part6'),
                dict(id=str(uuid.uuid4()), part='part4', new_col='new_col41'),
                dict(id=str(uuid.uuid4()), part='part2', new_col='new_col22')]
    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'part',
                                                          'headerAttributeExpression': "${record:value('/part')}"}]

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/part'])

    partition_configuration = [{'name': 'part', 'valueType': 'STRING', 'valueEL': '${record:attribute("part")}'}]

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression='default',
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

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = (pipeline_builder.build(title='Hive drift test - Multiple open partitions')
                .configure_for_environment(cluster))
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        logger.info('Validating table %s in Hive...', table_name)
        hive_cursor.execute('RELOAD {0}'.format(table_name))
        table_columns = ['id', 'new_col', 'part']
        hive_cursor.execute('SELECT * from {0}'.format(table_name))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        assert (sorted(hive_values, key=itemgetter(0)) ==
                sorted([[r[col] if col in r else None for col in table_columns]
                        for r in raw_data], key=itemgetter(0)))
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE {0}'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_sub_partitions(sdc_builder, sdc_executor, cluster):
    """Validate Sub partitions. The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_type_convertor >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = [dict(id=1, name='abc', timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                dict(id=2, name='def', timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                dict(id=3, name='ghi', timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))]

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[{'attributeToSet': 'table_name',
                                                                       'headerAttributeExpression': table_name}])

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.conversion_method = 'BY_FIELD'
    field_type_converter.set_attributes(field_type_converter_configs=[{'fields': ['/timestamp'],
                                                                       'targetType':'DATETIME',
                                                                       'dateFormat':'YYYY_MM_DD_HH_MM_SS'}])

    partition_configuration = [{'name': 'year', 'valueType': 'STRING', 'valueEL': '${YYYY()}'},
                               {'name': 'month', 'valueType': 'STRING', 'valueEL': '${MM()}'},
                               {'name': 'day', 'valueType': 'STRING', 'valueEL': '${DD()}'},
                               {'name': 'hour', 'valueType': 'STRING', 'valueEL': '${hh()}'},
                               {'name': 'minute', 'valueType': 'STRING', 'valueEL': '${mm()}'},
                               {'name': 'seconds', 'valueType': 'STRING', 'valueEL': '${ss()}'}]

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="${record:attribute('db')}",
                                 external_table=False,
                                 partition_configuration=partition_configuration,
                                 decimal_scale_expression='5',
                                 decimal_precision_expression='10',
                                 table_name="${record:attribute('table_name')}",
                                 time_basis="${record:value('/timestamp')}")
    if Version(sdc_builder.version) >= Version('3.9.0'):
        # Convert Timestamps To String has to be set like below as it was not working with the set_attributes() method.
        # Probably it was not working due to having the stage defined in more than one lib that this has not been
        # confirmed. Additionally it has to be set only for SDC versions >= 3.9.0 as it is when this new property
        # is added.
        hive_metadata.configuration.update({"convertTimesToString": True})

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    dev_raw_data_source >> expression_evaluator >> field_type_converter >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build(title='Hive drift test - Cold Start').configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]

        def split_date_time_string(datetime_str):
            v = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            return [datetime_str, v.strftime('%Y'), v.strftime('%m'),
                    v.strftime('%d'), v.strftime('%H'), v.strftime('%M'),
                    v.strftime('%S')]

        raw_values = [list(chain.from_iterable([[v] if k != 'timestamp' else split_date_time_string(v)
                                                for k, v in row.items()])) for row in raw_data]
        assert sorted(hive_values) == sorted(raw_values)
    finally:
        logger.info('Dropping table %s in Hive...', _get_qualified_table_name(None, table_name))
        hive_cursor.execute('DROP TABLE {0}'.format(_get_qualified_table_name(None, table_name)))


@sdc_min_version('3.9.0')
@cluster('cdh')
def test_native_parquet_timestamps(sdc_builder, sdc_executor, cluster):
    """Validate native timestamps can be written in a Hive Parquet table. The pipeline looks like:

        dev_raw_data_source >> expression_evaluator >> field_type_convertor >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
        hadoop_fs >= mapreduce
    """
    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = [dict(id=1, name='abc', timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                dict(id=2, name='def', timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                dict(id=3, name='ghi', timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))]

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(header_attribute_expressions=[{'attributeToSet': 'table_name',
                                                                       'headerAttributeExpression': table_name}])

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.conversion_method = 'BY_FIELD'
    field_type_converter.set_attributes(field_type_converter_configs=[{'fields': ['/timestamp'],
                                                                       'targetType':'DATETIME',
                                                                       'dateFormat':'YYYY_MM_DD_HH_MM_SS'}])

    partition_configuration = [{'name': 'year', 'valueType': 'STRING', 'valueEL': '${YYYY()}'},
                               {'name': 'month', 'valueType': 'STRING', 'valueEL': '${MM()}'},
                               {'name': 'day', 'valueType': 'STRING', 'valueEL': '${DD()}'},
                               {'name': 'hour', 'valueType': 'STRING', 'valueEL': '${hh()}'},
                               {'name': 'minute', 'valueType': 'STRING', 'valueEL': '${mm()}'},
                               {'name': 'seconds', 'valueType': 'STRING', 'valueEL': '${ss()}'}]

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='PARQUET',
                                 database_expression="${record:attribute('db')}",
                                 external_table=True,
                                 partition_configuration=partition_configuration,
                                 decimal_scale_expression='5',
                                 decimal_precision_expression='10',
                                 table_name="${record:attribute('table_name')}",
                                 time_basis="${record:value('/timestamp')}",
                                 table_path_template=f"/tmp/{table_name}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True,
                             max_records_in_file=1)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')

    mapreduce = pipeline_builder.add_stage('MapReduce', type='executor')
    mapreduce.set_attributes(job_type='AVRO_PARQUET',
                             output_directory="${file:parentPath(file:parentPath(record:value('/filepath')))}")

    dev_raw_data_source >> expression_evaluator >> field_type_converter >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    hadoop_fs >= mapreduce

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # Need to wait for 45 seconds until mapreduce jobs are finished.
        time.sleep(120)
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_values = [list(row) for row in hive_cursor.fetchall()]

        def split_date_time_string(datetime_str):
            v = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            return [v, v.strftime('%Y'), v.strftime('%m'),
                    v.strftime('%d'), v.strftime('%H'), v.strftime('%M'),
                    v.strftime('%S')]

        raw_values = [list(chain.from_iterable([[v] if k != 'timestamp' else split_date_time_string(v)
                                                for k, v in row.items()])) for row in raw_data]
        assert sorted(hive_values) == sorted(raw_values)
    finally:
        logger.info('Dropping table %s in Hive...', _get_qualified_table_name(None, table_name))
        hive_cursor.execute('DROP TABLE {0}'.format(_get_qualified_table_name(None, table_name)))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_events(sdc_builder, sdc_executor, cluster):
    """Validate Events from hive_metadata. The pipeline looks like:

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    hive_metastore >= wiretap.destination
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)

    raw_data = [dict(id=str(uuid.uuid4()), name='abc', part='part1'),
                dict(id=str(uuid.uuid4()), name='def', col1='col1', part='part1'),
                dict(id=str(uuid.uuid4()), name='ghi', col1='col1', part='part2'),
                dict(id=str(uuid.uuid4()), name='jkl', col1='col1', col2='col2', part='part3')]

    cols = ['id', 'name', 'col1', 'col2', 'part']

    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'part',
                                                          'headerAttributeExpression': "${record:value('/part')}"}]

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/part'])

    partition_configuration = [{'name': 'part', 'valueType': 'STRING', 'valueEL': '${record:attribute("part")}'}]

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression='default',
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

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    hive_metastore >= wiretap.destination

    pipeline = (pipeline_builder.build(title='Hive drift test - Events Test')
                .configure_for_environment(cluster))
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()

    expected_event_type_values = [('new-table', dict(table=_get_qualified_table_name('default', table_name),
                                                     columns=OrderedDict(id='STRING', name='STRING'),
                                                     partitions=OrderedDict(part='STRING'))),
                                  ('new-partition', dict(table=_get_qualified_table_name('default', table_name),
                                                         partition=OrderedDict(part='part1'))),
                                  ('new-columns', dict(table=_get_qualified_table_name('default', table_name),
                                                       columns=OrderedDict(col1='STRING'))),
                                  ('new-partition', dict(table=_get_qualified_table_name('default', table_name),
                                                         partition=OrderedDict(part='part2'))),
                                  ('new-columns', dict(table=_get_qualified_table_name('default', table_name),
                                                       columns=OrderedDict(col2='STRING'))),
                                  ('new-partition', dict(table=_get_qualified_table_name('default', table_name),
                                                         partition=OrderedDict(part='part3')))]
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.output_records) == 6
        for event_idx in range(len(wiretap.output_records)):
            event_record = wiretap.output_records[event_idx]
            event_type, expected_event_values = expected_event_type_values[event_idx]
            assert event_type == event_record.header['values']['sdc.event.type']
            assert expected_event_values == event_record.field

        logger.info('Validating table %s in Hive...', _get_qualified_table_name(None, table_name))
        hive_cursor.execute('RELOAD {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_cursor.execute('SELECT * from {0}'.format(_get_qualified_table_name(None, table_name)))
        hive_values = sorted([list(row) for row in hive_cursor.fetchall()], key=itemgetter(0))
        expected_values = sorted([[row[col] if col in row else None for col in cols] for row in raw_data],
                                 key=itemgetter(0))
        assert hive_values == expected_values
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
@pytest.mark.parametrize('stop_on_query_failure', [True, False])
def test_hive_query_executor(sdc_builder, sdc_executor, cluster, stop_on_query_failure):
    """Validate Hive Query Executor. The pipeline looks like:

    dev_raw_data_source >> hive_query_executor >= wiretap.destination
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    raw_data = [dict(name='multiple_queries_all_success',
                     query1='select 11',
                     query2='select 12',
                     query3='select 13'),
                dict(name='multiple_queries_failure_in_middle',
                     query1='select 21',
                     query2='select 22 from invalid_table',
                     query3='select 23')]
    dev_raw_data_source_data = ''.join(json.dumps(d) for d in raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=dev_raw_data_source_data,
                                       stop_after_first_batch=True)

    hive_query_executor = pipeline_builder.add_stage('Hive Query', type='executor')
    hive_query_executor.sql_queries = ['${record:value("/query1")}',
                                       '${record:value("/query2")}',
                                       '${record:value("/query3")}']
    hive_query_executor.stop_on_query_failure = stop_on_query_failure
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> hive_query_executor >= wiretap.destination

    pipeline = pipeline_builder.build(title='Hive Query Executor Test') .configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    if not stop_on_query_failure:
        event_type_expected_query_values = [('successful-query', dict(query='select 11')),
                                            ('successful-query', dict(query='select 12')),
                                            ('successful-query', dict(query='select 13')),
                                            ('successful-query', dict(query='select 21')),
                                            ('failed-query', dict(query='select 22 from invalid_table')),
                                            ('successful-query', dict(query='select 23'))]
    else:
        event_type_expected_query_values = [('successful-query', dict(query='select 11')),
                                            ('successful-query', dict(query='select 12')),
                                            ('successful-query', dict(query='select 13')),
                                            ('successful-query', dict(query='select 21')),
                                            ('failed-query', dict([('query', 'select 22 from invalid_table'),
                                                                  ('unexecuted-queries', ['select 23'])]))]
    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    assert len(wiretap.output_records) == len(event_type_expected_query_values)
    for event_idx in range(len(wiretap.output_records)):
        event_record = wiretap.output_records[event_idx]
        expected_event_type, expected_event_values = event_type_expected_query_values[event_idx]
        assert expected_event_type == event_record.header['values']["sdc.event.type"]
        assert expected_event_values == wiretap.output_records[event_idx].field


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_hive_avro_schema_contains_only_columninfo(sdc_builder, sdc_executor, cluster):
    """Validate that the avro Schema in records contains strictly hive table column information
        and not partition information.

        Currently, we use the "DESC <tableName>" query to gather table information
        results in partition names appear twice in the resultset - one with column info and one with partiton info,
        so they are counted twice.
        This behavior is fixed as a part of SDC-13898

        dev_raw_data_source >> hive_metadata
        hive_metadata >> [hadoop_fs, wiretap.destination]
        hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
            and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)
    id, username = 'id', 'username'
    raw_data = json.dumps({id: 1, username: 'abc'})

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    partition_name = 'partition1'
    partition_configuration = [
        {'name': partition_name, 'valueType': 'STRING', 'valueEL': '${record:value("/username")}'}
    ]

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 partition_configuration=partition_configuration,
                                 external_table=True,
                                 table_name=table_name,
                                 table_path_template=f"/tmp/{table_name}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> hive_metadata
    hive_metadata >> [hadoop_fs, wiretap.destination]
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    hive_cursor = cluster.hive.client.cursor()

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1

        avro_schema = json.loads(wiretap.output_records[0].header['values']['avroSchema'])
        column_list = [field['name'] for field in avro_schema['fields']]

        assert id and username in column_list and partition_name not in column_list
    finally:
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE {}'.format(table_name))


@sdc_min_version('3.0.0.0')
@cluster('cdh', 'hdp')
def test_hdfs_avro_update_schema_stored_externally(sdc_builder, sdc_executor, cluster):
    """Validate that Hive schema stored as AVRO at external location is correctly updated,
    when there is a column update/addition to the Hive table
    The test populates/creates a table with 2 columns: 'name' and 'id'
    Then the pipeline is stopped, and a new field 'age' is added
    Verify that the Hive table is updated with new column and that a new AVRO schema file is generated and stored
    at external location

        dev_raw_data_source >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore
    """
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')

    table_name = get_random_string(string.ascii_lowercase, 20)
    avro_dir = f'/tmp/{get_random_string(string.ascii_lowercase, 20)}'
    raw_data = dict(id=1, name="testuser")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source', type='origin')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json.dumps(raw_data),
                                       stop_after_first_batch=True)

    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO',
                                 database_expression="default",
                                 external_table=True,
                                 partition_configuration=[],
                                 decimal_scale_expression='2',
                                 decimal_precision_expression='4',
                                 table_name=table_name,
                                 table_path_template=f"/tmp/{table_name}")

    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER',
                             data_format='AVRO',
                             directory_in_header=True,
                             use_roll_attribute=True)

    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')
    hive_metastore.set_attributes(stored_as_avro=False, schema_folder_location=avro_dir)

    dev_raw_data_source >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore

    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)
    hive_cursor = cluster.hive.client.cursor()
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        status = cluster.hdfs.client.status(avro_dir)
        assert status is not None and status['type'] == 'DIRECTORY'
        avro_dir_filelist = cluster.hdfs.client.list(avro_dir)
        assert len(avro_dir_filelist) == 1

        # Stop and restart the pipeline after adding another element in dev_raw_data_source to create an new column
        # Because of a bug in update_pipeline, we need to re-get a reference to the stage from pipeline,
        # before we can call update_pipeline
        dev_raw_data_source = pipeline.stages.get(label=dev_raw_data_source.label)
        raw_data['age'] = 25 # Add a new element to create a new column in table
        dev_raw_data_source.set_attributes(data_format='JSON',
                                           raw_data=json.dumps(raw_data),
                                           stop_after_first_batch=True)
        sdc_executor.update_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        avro_dir_filelist = cluster.hdfs.client.list(avro_dir)
        assert len(avro_dir_filelist) == 2
        table_column_list = list(_get_table_columns_and_type(hive_cursor, None, table_name).keys())
        assert 'age' in table_column_list
    finally:
        # Delete schema location
        logger.info('Deleting Schema Directory %s in Hadoop FS...', avro_dir)
        cluster.hdfs.client.delete(avro_dir, recursive=True)
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{0}`'.format(table_name))


def _get_qualified_table_name(db, table_name):
    return f'`{db}`.`{table_name}`' if db else f'`{table_name}`'


def _get_table_location(hive_cursor, db, table_name):
    hive_cursor.execute('DESC FORMATTED {0}'.format(_get_qualified_table_name(db, table_name)))
    rows = [row for row in hive_cursor.fetchall()]
    url_result = urlparse([r[1] for r in rows if 'Location:' in r[0]][0])
    return url_result.path


def _get_table_columns_and_type(hive_cursor, db, table_name):
    hive_cursor.execute(f'DESC {_get_qualified_table_name(db, table_name)}')
    return OrderedDict({col_name: col_type for col_name, col_type, comment in hive_cursor.fetchall()})


def _get_hive_warehouse_dir(hive_cursor):
    """Return the directory path used by default to store databases in Hive."""
    hive_cursor.execute("SET hive.metastore.warehouse.dir")
    return hive_cursor.fetchone()[0].split('=')[1]


def _get_hive_warehouse_external_dir(hive_cursor):
    """Return the directory path used by default to store external databases in Hive."""
    hive_cursor.execute("SET hive.metastore.warehouse.external.dir")
    return hive_cursor.fetchone()[0].split('=')[1]
