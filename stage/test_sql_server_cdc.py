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
from time import sleep, time

import pytest
import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

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
@sdc_min_version('2.7.0.0')
def test_sql_server_cdc_no_more_data(sdc_builder, sdc_executor, database, no_of_threads):
    """Test for SQL Server CDC origin stage. We do so by capturing Insert Operation on CDC enabled table(s)
    using SQL Server CDC Origin and having a pipeline which reads that data using SQL Server CDC origin stage.
    The records in the pipeline will be stored in SQL Server table using JDBC Producer.
    Data is then asserted for what is captured at SQL Server Job and what we read in the pipeline.
    The pipeline looks like:
        sql_server_cdc_origin >= pipeline_finisher_executor
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
    sql_server_cdc.set_attributes(max_pool_size=no_of_threads,
                                  no_of_threads=no_of_threads)

    dest_table_name = get_random_string(string.ascii_uppercase, 9)

    dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

    jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                 table_name_template=dest_table_name,
                                 default_operation='INSERT',
                                 field_to_column_mapping=[])

    pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')

    sql_server_cdc >= pipeline_finisher_executor
    sql_server_cdc >> jdbc_producer
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

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert_table_replicated(database, rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        for table in tables:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        logger.info('Dropping table %s in %s database...', dest_table, database.type)
        dest_table.drop(database.engine)


@database('sqlserver')
@sdc_min_version('3.0.1.0')
def test_sql_server_cdc_with_specific_capture_instance_name(sdc_builder, sdc_executor, database):
    """Test for SQL Server CDC origin stage when capture instance is configured.
    We do so by capturing Insert Operation on CDC enabled table
    using SQL Server CDC Origin and having a pipeline which reads that data using SQL Server CDC origin stage.
    We setup no_of_tables of CDC tables in SQL Server and configured one specific capture instance name
    so that the records from one table will be stored in SQL Server table using JDBC Producer.
    Data is then asserted for what is captured at SQL Server Job and what we read in the pipeline.
    The pipeline looks like:
        sql_server_cdc_origin >= pipeline_finisher_executor
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        schema_name = DEFAULT_SCHEMA_NAME
        tables = []
        no_of_records = 5
        no_of_tables = 3
        target_table_index = 2
        rows_in_database = setup_sample_data(no_of_tables * no_of_records)

        # setup the tables first
        for index in range(0, no_of_tables):
            table_name = get_random_string(string.ascii_lowercase, 20)
            # split the rows_in_database into no_of_records for each table
            # e.g. for no_of_records=5, the first table inserts rows_in_database[0:5]
            # and the secord table inserts rows_in_database[5:10]
            table = setup_table(database, schema_name, table_name,
                                rows_in_database[(index*no_of_records): ((index+1)*no_of_records)])
            tables.append(table)

            if (index == target_table_index):
                capture_instance_name = f'{schema_name}_{table_name}'

        target_rows = rows_in_database[target_table_index * no_of_records: (target_table_index + 1) * no_of_records]

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configuration=[{'capture_instance': capture_instance_name}])

        dest_table_name = get_random_string(string.ascii_uppercase, 9)

        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)
        tables.append(dest_table)
        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name_template=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        pipeline_finisher_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')

        sql_server_cdc >= pipeline_finisher_executor
        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, no_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert_table_replicated(database, target_rows, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        for table in tables:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)
