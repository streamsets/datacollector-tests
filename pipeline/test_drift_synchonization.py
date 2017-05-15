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
import string
import sqlalchemy

from testframework import environment, sdc, sdc_models
from testframework.markers import *
from testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Validate end-to-end case with stopping the pipeline and executing the map/reduce job after
# it read all the data from databases.
# This test case uses the query JDBC origin and the map/reduce executor
# to solve the hive synchonization solution in parquet data format
@database
def test_query_with_parquet(sdc_builder, sdc_executor, cluster, database):
    hive_cursor = cluster.hive.client.cursor()

    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(25)),
        sqlalchemy.Column('dt', sqlalchemy.String(20))
    )

    rows_in_database = [
        {'id':1, 'name': 'Ji Sun', 'dt':'2017-05-03'},
        {'id':2, 'name': 'Jarcec', 'dt':'2017-05-03'},
        {'id':3, 'name': 'Santhosh', 'dt':'2017-05-03'}
    ]

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), rows_in_database)

        logger.info('start pipeline builder')
        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')
        jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
        jdbc_query_consumer.configuration['isIncrementalMode'] = False
        jdbc_query_consumer.sql_query = 'SELECT * FROM {0};'.format(table_name)

        expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
        expression_evaluator.header_expressions = [{"attributeToSet":"dt", "headerAttributeExpression":"${record:value('/dt')}"}]

        field_remover = pipeline_builder.add_stage('Field Remover')
        field_remover.fields = ["/dt"]

        hive_metadata_processor = pipeline_builder.add_stage('Hive Metadata')
        hive_metadata_processor.database_expression = 'default'
        hive_metadata_processor.table_name = "${record:attribute('jdbc.tables')}"
        hive_metadata_processor.data_format = 'PARQUET'

        hadoop_fs = pipeline_builder.add_stage('Hadoop FS', type='destination')
        hadoop_fs.set_attributes(avro_schema_location='HEADER',
                                data_format='AVRO',
                                directory_in_header=True)

        hive_metastore_destination = pipeline_builder.add_stage('Hive Metastore', type='destination')

        mapreduce_executor = pipeline_builder.add_stage('MapReduce')
        mapreduce_executor.job_type= 'AVRO_PARQUET'
        mapreduce_executor.output_directory = "${file:parentPath(file:parentPath(record:value('/filepath')))}"

        pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')

        jdbc_query_consumer >= pipeline_finisher_executor
        jdbc_query_consumer >> expression_evaluator >> field_remover >> hive_metadata_processor
        hive_metadata_processor >> hadoop_fs
        hive_metadata_processor >> hive_metastore_destination
        hadoop_fs >= mapreduce_executor

        pipeline = pipeline_builder.build().configure_for_environment(cluster, database)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        hive_cursor.execute('RELOAD `{0}`'.format(table_name))
        hive_cursor.execute('SELECT * from `{0}`'.format(table_name))
        assert hive_cursor.fetchall() == [(row['id'], row['name'], row['dt']) for row in rows_in_database]
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute('DROP TABLE `{}`'.format(table_name))
