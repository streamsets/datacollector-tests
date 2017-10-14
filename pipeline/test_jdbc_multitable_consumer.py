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

import logging
import string

import pytest
import sqlalchemy

from testframework.environments.databases import oraclize_config_if_needed, upper_if_required
from testframework.markers import database, cluster
from testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@database
def test_jdbc_query_no_more_data(sdc_builder, sdc_executor, database):
    """Validate end-to-end case with stopping pipeline after it read all the data from database. The pipeline
    will look like:
        jdbc_query_consumer >> trash
                            >= pipeline_finished_executor
    """
    table_name = get_random_string(string.ascii_lowercase, 20) # lowercase for db compatibility (e.g. PostgreSQL)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
    sql_query = f'SELECT * FROM {table_name} WHERE col > ${{OFFSET}} order by col'
    jdbc_query_consumer.set_attributes(initial_offset='0', offset_column='col', sql_query=sql_query)
    trash = pipeline_builder.add_stage('Trash')
    pipeline_finished_executor = pipeline_builder.add_stage("Pipeline Finisher Executor")

    jdbc_query_consumer >> trash
    jdbc_query_consumer >= pipeline_finished_executor
    pipeline = pipeline_builder.build('JDBC no more data pipeline').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('col', sqlalchemy.Integer))
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'col':1}, {'col':2}, {'col':3}])

        sdc_executor.start_pipeline(pipeline)

        metrics = sdc_executor.pipeline_history(pipeline).latest.metrics
        assert metrics.counter("pipeline.batchCount.counter").count == 1
        assert metrics.counter("pipeline.batchInputRecords.counter").count == 3
        assert metrics.counter("pipeline.batchOutputRecords.counter").count == 4
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@cluster('cdh')
@database
# lowercase for db compatibility (e.g. PostgreSQL)
@pytest.mark.parametrize('table_name_characters', [string.ascii_lowercase, string.digits])
@pytest.mark.parametrize('table_name_length', [8, 20])
@pytest.mark.timeout(180)
def test_jdbc_multitable_consumer_to_hive(sdc_builder, sdc_executor, database, cluster,
                                          table_name_characters, table_name_length):
    """Validate an end to end case of reading Multi-tables from JDBC source and making sure they are
    written to Hadoop FS. We use Hive Metadata processor for drift synchronization. The pipeline looks like:

        jdbc_multitable_consumer >= pipeline_finisher_executor
        jdbc_multitable_consumer >> expression_evaluator >> field_remover >> hive_metadata
        hive_metadata >> hadoop_fs
        hive_metadata >> hive_metastore

    Note: Numeric fixture of the test fails till SDC-6766 is addressed.
    """
    # Generate two random strings to use when naming the DB tables at the source.
    src_table_suffix = get_random_string(string.ascii_lowercase, 6) # lowercase for db compatibility (e.g. PostgreSQL)
    random_table_name_1 = '{}_{}'.format(get_random_string(table_name_characters, table_name_length), src_table_suffix)
    random_table_name_2 = '{}_{}'.format(get_random_string(table_name_characters, table_name_length), src_table_suffix)

    # build the pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    table_config = oraclize_config_if_needed({'tablePattern': f'%{src_table_suffix}'}, database)
    jdbc_multitable_consumer.set_attributes(table_configuration=[table_config])
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.set_attributes(
        header_expressions=[{'attributeToSet': 'database',
                             'headerAttributeExpression': f'{database.database}'},
                            {'attributeToSet': 'dt',
                             'headerAttributeExpression': "${record:value('/dt')}"},
                            {'attributeToSet': 'table_name',
                             'headerAttributeExpression': "${record:attribute('jdbc.tables')}"}])
    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.fields = ["/dt"]
    hive_metadata = pipeline_builder.add_stage('Hive Metadata')
    hive_metadata.set_attributes(data_format='AVRO', database_expression="${record:attribute('database')}",
                                 decimal_precision_expression="${record:attribute(str:concat(str:concat('jdbc.', field:field()), '.precision'))}",
                                 decimal_scale_expression="${record:attribute(str:concat(str:concat('jdbc.', field:field()), '.scale'))}",
                                 table_name="${record:attribute('table_name')}")
    hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
    hadoop_fs.set_attributes(avro_schema_location='HEADER', data_format='AVRO', directory_in_header=True,
                             file_type='TEXT', files_prefix='sdc-${sdc:id()}', files_suffix='avro', max_file_size=0,
                             max_records_in_file=0, roll_attribute_name='roll', use_roll_attribute=True)
    hive_metastore = pipeline_builder.add_stage('Hive Metastore', type='destination')
    hive_metastore.set_attributes(stored_as_avro=True)
    pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')

    jdbc_multitable_consumer >= pipeline_finisher_executor
    jdbc_multitable_consumer >> expression_evaluator >> field_remover >> hive_metadata
    hive_metadata >> hadoop_fs
    hive_metadata >> hive_metastore
    pipeline = pipeline_builder.build(title='Multi-table consumer to Hive').configure_for_environment(cluster, database)
    sdc_executor.add_pipeline(pipeline)

    tables = []
    try:
        # create table and load data in the JDBC database
        for table_name in (random_table_name_1, random_table_name_2):
            logger.info('Creating table %s in %s database ...', table_name, database.type)
            table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                     sqlalchemy.Column('event_id', sqlalchemy.Integer, primary_key=True),
                                     sqlalchemy.Column('order_id', sqlalchemy.Integer),
                                     sqlalchemy.Column('event_type', sqlalchemy.String(32)),
                                     sqlalchemy.Column('dt', sqlalchemy.String(20)))
            table.create(database.engine)
            tables.append(table)
            rows = [
                {'event_id': 1, 'order_id': 123, 'event_type': 'SHIPPED', 'dt':'2017-07-13'},
                {'event_id': 2, 'order_id': 234, 'event_type': 'ARRIVED', 'dt':'2017-07-13'},
                {'event_id': 3, 'order_id': 345, 'event_type': 'READY', 'dt':'2017-07-13'}
            ]
            logger.info('Adding %s rows to %s of %s database ...', len(rows), table_name, database.type)
            connection = database.engine.connect()
            connection.execute(table.insert(), rows)

        # run the pipeline
        sdc_executor.start_pipeline(pipeline)

        # Check that the data shows up in Hive.
        hive_cursor = cluster.hive.client.cursor()
        for table in tables:
            table_name = upper_if_required(table.name, database)
            logger.info('Asserting table %s', table_name)
            hive_cursor.execute(f'SELECT * from `{table_name}`')
            hive_values = [list(row) for row in hive_cursor.fetchall()]
            raw_values = [list(row.values()) for row in rows]
            assert sorted(hive_values) == sorted(raw_values)
    finally:
        for table in tables:
            table_name = upper_if_required(table.name, database)
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            table.drop(database.engine)
            logger.info('Dropping table %s in Hive ...', table_name)
            hive_cursor.execute(f'DROP TABLE `{table_name}`')
