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

import json
import logging
import string
from time import sleep, time

import pytest
import sqlalchemy

from testframework.markers import database, sdc_min_version
from testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_NAME = 'dbo'


def setup_table(database, schema_name, table_name, sample_data):
    """Create table and insert the sample data into the table"""
    table = create_table(database, schema_name, table_name)
    connection = database.engine.connect()

    logger.info('Enabling CDC on %s.%s...', schema_name, table_name)
    connection.execute(f'exec sys.sp_cdc_enable_table @source_schema=\'{schema_name}\', '
                       f'@source_name=\'{table_name}\', @supports_net_changes=1, @role_name=NULL')

    logger.info('Adding %s rows into %s.%s...', len(sample_data), schema_name, table_name)
    connection.execute(table.insert(), sample_data)

    return table


def create_table(database, schema_name, table_name):
    """Create table with the folloiwng scheam: id int primary key, name varchar(25), dt datetime"""
    logger.info('Creating table %s.%s...', schema_name, table_name)
    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                             sqlalchemy.Column('name', sqlalchemy.String(25)),
                             sqlalchemy.Column('dt', sqlalchemy.String(25)),
                             schema=schema_name)

    table.create(database.engine)

    return table


def assert_table_replicated(database, sample_data, schema_name, table_name):
    """Assert the sample data matches with the desitnation table data wrote using JDBC Producer"""
    db_engine = database.engine

    target_table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                    autoload=True, autoload_with=db_engine,
                                    schema=schema_name)
    target_result = db_engine.execute(target_table.select().order_by(target_table.c['id']))
    target_result_list = target_result.fetchall()
    target_result.close()

    input_values = [tuple(data.values()) for data in sample_data]

    assert sorted(input_values) == sorted(target_result_list)


def setup_sample_data(no_of_records):
    """Generate the given number of sample data with 'id', 'name', and 'dt'"""
    rows_in_database = [{'id': counter, 'name': get_random_string(string.ascii_lowercase, 20), 'dt': '2017-05-03'}
                        for counter in range(0, no_of_records)]
    return rows_in_database


def wait_for_data_in_ct_table(ct_table_name, no_of_records, database=None, timeout_sec=50):
    """Wait for data is captured by CDC jobs in SQL Server
    (i.e, number of records in CT table is equal to the total number of records).
    """
    logger.info('Waiting for no_data_event to be updated in %s seconds ...', timeout_sec)
    start_waiting_time = time()
    stop_waiting_time = start_waiting_time + timeout_sec
    db_engine = database.engine

    while time() < stop_waiting_time:
        event_table = sqlalchemy.Table(ct_table_name, sqlalchemy.MetaData(), autoload=True,
                                       autoload_with=db_engine, schema='cdc')
        event_result = db_engine.execute(event_table.select())
        event_result_list = event_result.fetchall()
        event_result.close()

        if len(event_result_list) >= no_of_records:
            logger.info('%s of data is captured in CT Table %s', no_of_records, ct_table_name)
            return
        sleep(5)

    raise Exception('Timed out after %s seconds while waiting for captured data.', timeout_sec)


@database('sqlserver')
@pytest.mark.parametrize('no_of_threads', [1, 5])
@sdc_min_version('3.0.0.0')
def test_sql_server_cdc_with_cdc_schema_name(sdc_builder, sdc_executor, database, no_of_threads):
    """Test for SQL Server CDC origin stage when schema change is enables.
    We do so by capturing Insert Operation on CDC enabled table(s)
    using SQL Server CDC Origin and having a pipeline which reads that data using SQL Server CDC origin stage.
    The records in the pipeline will be stored in SQL Server table using JDBC Producer.
    While the pipeline is running the source table schema is changed by dropping or adding the columns,
    the dest table will be dropping or adding the columns respectively.
    Data is then asserted for what is captured at SQL Server Job and what we read in the pipeline.
    The pipeline looks like:
        sql_server_cdc_origin >= stream_selector
        stream_selector >> jdbc_query_executor
        sql_server_cdc_origin >> jdbc_producer
    """
    schema_name = DEFAULT_SCHEMA_NAME

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
    sql_server_cdc.set_attributes(allow_late_tables=True,
                                  enable_schema_changes_event=True,
                                  # when allow_late_tables = true, the pipeline runs one background thread
                                  # to spool the list of cdc tables
                                  max_pool_size=no_of_threads+1,
                                  min_idle_connections=no_of_threads+1,
                                  new_table_discovery_interval='${1 * SECONDS}',
                                  no_of_threads=no_of_threads)

    dest_table_name = get_random_string(string.ascii_uppercase, 9)

    dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

    jdbc_producer.set_attributes(default_operation='INSERT',
                                 field_to_column_mapping=[],
                                 schema_name=DEFAULT_SCHEMA_NAME,
                                 table_name_template=dest_table_name)

    stream_selector = pipeline_builder.add_stage('Stream Selector')

    # disable cdc table and enable it again which will start applying the changing schema
    schema_change_event_statement = ("EXEC sys.sp_cdc_disable_table "
                                     "@source_schema='${record:value(\"/source-table-schema-name\")}'"
                                     ",@source_name='${record:value(\"/source-table-name\")}'"
                                     ",@capture_instance='${record:value(\"/capture-instance-name\")}';\n"
                                     "EXEC sys.sp_cdc_enable_table "
                                     "@source_schema='${record:value(\"/source-table-schema-name\")}',"
                                     "@source_name='${record:value(\"/source-table-name\")}',"
                                     "@supports_net_changes=1,"
                                     "@role_name=NULL,"
                                     "@capture_instance='${record:value(\"/capture-instance-name\")}_2';")

    jdbc_query = pipeline_builder.add_stage('JDBC Query', type='executor')
    jdbc_query.set_attributes(sql_query=schema_change_event_statement)

    trash = pipeline_builder.add_stage('Trash')

    sql_server_cdc >= stream_selector
    stream_selector >> jdbc_query
    stream_selector >> trash
    sql_server_cdc >> jdbc_producer

    stream_selector.condition = [dict(outputLane=stream_selector.output_lanes[0],
                                      predicate="${record:attribute('sdc.event.type')=='schema-change'}"),
                                 dict(outputLane=stream_selector.output_lanes[1],
                                      predicate='default')]

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        tables = []
        no_of_records = 5
        rows_in_database = setup_sample_data(no_of_threads * no_of_records)

        for index in range(0, no_of_threads):
            table_name = get_random_string(string.ascii_lowercase, 20)
            # split the rows_in_database into no_of_records for each table
            # e.g. for no_of_records=5, the first table inserts rows_in_database[0:5]
            # and the secord table inserts rows_in_database[5:10]
            table = setup_table(database, DEFAULT_SCHEMA_NAME, table_name,
                                rows_in_database[(index*no_of_records): ((index+1)*no_of_records)])
            tables.append(table)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        ct_table_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, no_of_records, database)

        sdc_executor.start_pipeline(pipeline)

        assert_table_replicated(database, rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

        # add the new column to the last input table
        connection = database.engine.connect()
        logger.info('Adding the column new_column varchar(10) on %s.%s...', schema_name, table_name)
        connection.execute(f'ALTER TABLE {table_name} ADD new_column VARCHAR(10)')
        logger.info('Adding the column new_column varchar(10) on %s.%s...', schema_name, dest_table_name)
        connection.execute(f'ALTER TABLE {dest_table_name} ADD new_column VARCHAR(10)')

        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                                 sqlalchemy.Column('name', sqlalchemy.String(25)),
                                 sqlalchemy.Column('dt', sqlalchemy.String(25)),
                                 sqlalchemy.Column('new_column', sqlalchemy.String(10)),
                                 schema=schema_name)

        new_sample_data = [{'id': counter,
                            'name': get_random_string(string.ascii_lowercase, 20),
                            'dt': '2017-05-05',
                            'new_column': get_random_string(string.ascii_lowercase, 10)}
                           # start with the last counter of rows_in_data to the number of records
                           for counter in range(no_of_threads * no_of_records, (no_of_threads + 1) * no_of_records)]
        logger.info('Adding %s rows into %s.%s...', len(new_sample_data), schema_name, table_name)
        connection.execute(table.insert(), new_sample_data)

        # adjust sample data by adding new_columns: None and add new sample data to the list
        rows_in_database.extend(new_sample_data)
        # warning the schema change is not captured by JDBC Producer
        for data in rows_in_database:
            data.update(new_column=None)

        ct2_table_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}_2_CT'

        # wait for the compleltion of the next batch
        wait_for_data_in_ct_table(ct2_table_name, no_of_records, database)
        sdc_executor.stop_pipeline(pipeline)

        assert_table_replicated(database, rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)
    finally:
        logger.info('Dropping table %s in %s database...', table, database.type)
        tables.append(dest_table)
        for table in tables:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)
