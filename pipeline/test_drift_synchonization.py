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

import logging
import string

import sqlalchemy
from streamsets.testframework.markers import database, cluster
from streamsets.testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        hive_cursor = cluster.hive.client.cursor()
        hive_cursor.execute('RELOAD `{0}`'.format(table_name))
        hive_cursor.execute('SELECT * from `{0}`'.format(table_name))
        hive_values = [list(row) for row in hive_cursor.fetchall()]
        raw_values = [list(row.values()) for row in rows_in_database]
        assert sorted(hive_values) == sorted(raw_values)
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
        logger.info('Dropping table %s in Hive...', table_name)
        hive_cursor.execute(f'DROP TABLE `{table_name}`')
