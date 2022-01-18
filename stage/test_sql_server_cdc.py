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

import binascii
import logging
import string
from time import sleep, time

import pytest
import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_NAME = 'dbo'


def setup_table(connection, schema_name, table_name, sample_data=None, capture_instance_name=None):
    """Create table and insert the sample data into the table"""
    table = create_table(connection, schema_name, table_name)

    if capture_instance_name is None:
        capture_instance_name = f'{schema_name}_{table_name}'

    logger.info('Enabling CDC on %s.%s...', schema_name, table_name)
    connection.execute(f'exec sys.sp_cdc_enable_table @source_schema=\'{schema_name}\', '
                       f'@source_name=\'{table_name}\', '
                       f'@capture_instance=\'{capture_instance_name}\', '
                       f'@supports_net_changes=1, @role_name=NULL')

    if sample_data is not None:
        add_data_to_table(connection, table, sample_data)

    return table


def add_data_to_table(connection, table, sample_data):
    logger.info('Adding %s rows into %s...', len(sample_data), table)
    connection.execute(table.insert(), sample_data)


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
@sdc_min_version('3.0.1.0')
def test_sql_server_cdc_with_specific_capture_instance_name(sdc_builder, sdc_executor, database, no_of_threads):
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
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME
        tables = []
        table_configs = []
        no_of_records = 5
        target_table_index = 2
        rows_in_database = setup_sample_data(no_of_threads * no_of_records)

        # setup the tables first
        for index in range(0, no_of_threads):
            table_name = get_random_string(string.ascii_lowercase, 20)
            # split the rows_in_database into no_of_records for each table
            # e.g. for no_of_records=5, the first table inserts rows_in_database[0:5]
            # and the secord table inserts rows_in_database[5:10]
            table = setup_table(connection, schema_name, table_name,
                                rows_in_database[(index * no_of_records): ((index + 1) * no_of_records)])
            tables.append(table)
            table_configs.append({'capture_instance': f'{schema_name}_{table_name}'})

        rows_in_database[target_table_index * no_of_records: (target_table_index + 1) * no_of_records]

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(maximum_pool_size=no_of_threads,
                                      number_of_threads=no_of_threads,
                                      table_configs=table_configs)

        dest_table_name = get_random_string(string.ascii_uppercase, 9)

        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)
        tables.append(dest_table)
        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        for table_config in table_configs:
            ct_table_name = f'{table_config.get("capture_instance")}_CT'
            wait_for_data_in_ct_table(ct_table_name, no_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(no_of_records * no_of_threads)
        sdc_executor.stop_pipeline(pipeline)

        assert_table_replicated(database, rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        for table in tables:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
def test_sql_server_cdc_with_empty_initial_offset(sdc_builder, sdc_executor, database, use_table):
    """Test for SQL Server CDC origin stage with the empty initial offset (fetch all changes)
    on both use table config is true and false

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME
        no_of_records = 10
        rows_in_database = setup_sample_data(no_of_records)

        # create the table with the above sample data
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = setup_table(connection, schema_name, table_name, rows_in_database)

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name}],
                                      use_direct_table_query=use_table
                                      )

        # create the destination table
        dest_table_name = get_random_string(string.ascii_uppercase, 9)
        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, no_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        assert_table_replicated(database, rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if dest_table is not None:
            logger.info('Dropping table %s in %s database...', dest_table, database.type)
            dest_table.drop(database.engine)


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
def test_sql_server_cdc_with_nonempty_initial_offset(sdc_builder, sdc_executor, database, use_table):
    """Test for SQL Server CDC origin stage with non-empty initial offset (fetch the data from the given LSN)
    on both use table config is true and false

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME
        first_no_of_records = 3
        second_no_of_records = 8
        total_no_of_records = first_no_of_records + second_no_of_records
        rows_in_database = setup_sample_data(total_no_of_records)

        # create the table and insert the first half of the rows
        table_name = get_random_string(string.ascii_lowercase, 20)
        capture_instance_name = f'{schema_name}_{table_name}'
        table = setup_table(connection, schema_name, table_name, rows_in_database[0:first_no_of_records])
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, first_no_of_records, database)

        # insert the last half of the sample data
        add_data_to_table(connection, table, rows_in_database[first_no_of_records:total_no_of_records])
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        # get the current LSN
        currentLSN = binascii.hexlify(connection.execute('SELECT sys.fn_cdc_get_max_lsn() as currentLSN').scalar())

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name,
                                                      'initialOffset': currentLSN.decode("utf-8")}],
                                      use_direct_table_query=use_table
                                      )

        # create the destination table
        dest_table_name = get_random_string(string.ascii_uppercase, 9)
        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(second_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        assert_table_replicated(database, rows_in_database[first_no_of_records:total_no_of_records],
                                DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if dest_table is not None:
            logger.info('Dropping table %s in %s database...', dest_table, database.type)
            dest_table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_sql_server_cdc_insert_and_update(sdc_builder, sdc_executor, database, use_table):
    """Test for SQL Server CDC origin stage with insert and update ops

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME

        rows_in_database = setup_sample_data(1)

        # create the table and insert 1 row
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = setup_table(connection, schema_name, table_name, rows_in_database)

        # update the row
        updated_name = 'jisun'
        connection.execute(table.update()
                           .where(table.c.id == 0)
                           .values(name=updated_name))

        total_no_of_records = 3

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name}],
                                      use_direct_table_query=use_table,
                                      fetch_size=2
                                      )

        # create the destination table
        dest_table_name = get_random_string(string.ascii_uppercase, 9)
        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)
        expected_rows_in_database = [
            {'id': rows_in_database[0].get('id'), 'name': updated_name, 'dt': rows_in_database[0].get('dt')}]
        assert_table_replicated(database, expected_rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if dest_table is not None:
            logger.info('Dropping table %s in %s database...', dest_table, database.type)
            dest_table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_sql_server_cdc_insert_update_delete(sdc_builder, sdc_executor, database, use_table):
    """Test for SQL Server CDC origin stage with insert, update, and delete ops

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        schema_name = DEFAULT_SCHEMA_NAME

        rows_in_database = setup_sample_data(1)

        # create the table and insert 1 row
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = setup_table(connection, schema_name, table_name, rows_in_database)

        # update the row
        updated_name = 'jisun'
        connection.execute(table.update()
                           .where(table.c.id == 0)
                           .values(name=updated_name))

        # delete the rows
        connection.execute(table.delete())

        total_no_of_records = 4

        # get the capture_instance_name
        capture_instance_name = f'{schema_name}_{table_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(table_configs=[{'capture_instance': capture_instance_name}],
                                      use_direct_table_query=use_table,
                                      fetch_size=1
                                      )

        # create the destination table
        dest_table_name = get_random_string(string.ascii_uppercase, 9)
        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before starting the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)
        expected_rows_in_database = []
        assert_table_replicated(database, expected_rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if dest_table is not None:
            logger.info('Dropping table %s in %s database...', dest_table, database.type)
            dest_table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.6.0')
@pytest.mark.parametrize('use_table', [True, False])
@pytest.mark.timeout(180)
def test_sql_server_cdc_multiple_tables(sdc_builder, sdc_executor, database, use_table):
    """Test for SQL Server CDC origin stage with multiple transactions on multiple CDC tables (SDC-10926)

    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        no_of_tables = 3
        table_configs = []
        tables = []

        rows_in_database = setup_sample_data(3)

        for index in range(0, no_of_tables):
            # create the table and insert 1 row and update the row
            table_name = get_random_string(string.ascii_lowercase, 20)
            table = setup_table(connection, DEFAULT_SCHEMA_NAME, table_name, rows_in_database[index:index + 1])

            updated_name = 'jisun'
            connection.execute(table.update()
                               .values(name=updated_name))

            table_configs.append({'capture_instance': f'{DEFAULT_SCHEMA_NAME}_{table_name}'})
            tables.append(table)

        # update the row from the first table
        connection.execute(tables[0].update().values(name='sdc'))

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(fetch_size=1,
                                      table_configs=table_configs,
                                      use_direct_table_query=use_table
                                      )

        # create the destination table
        dest_table_name = get_random_string(string.ascii_uppercase, 9)
        dest_table = create_table(database, DEFAULT_SCHEMA_NAME, dest_table_name)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(schema_name=DEFAULT_SCHEMA_NAME,
                                     table_name=dest_table_name,
                                     default_operation='INSERT',
                                     field_to_column_mapping=[])

        sql_server_cdc >> jdbc_producer
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        total_no_of_records = 11

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)
        expected_rows_in_database = [{'id': 0, 'name': 'sdc', 'dt': '2017-05-03'},
                                     {'id': 1, 'name': 'jisun', 'dt': '2017-05-03'},
                                     {'id': 2, 'name': 'jisun', 'dt': '2017-05-03'}]
        assert_table_replicated(database, expected_rows_in_database, DEFAULT_SCHEMA_NAME, dest_table_name)

        sqlserver_cdc_pipeline_history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = sqlserver_cdc_pipeline_history.latest.metrics.counter(
            'pipeline.batchOutputRecords.counter').count
        assert msgs_sent_count == total_no_of_records
    finally:
        for index in range(0, no_of_tables):
            logger.info('Dropping table %s in %s database...', tables[index], database.type)
            tables[index].drop(database.engine)

        logger.info('Dropping table %s in %s database...', dest_table, database.type)
        dest_table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@pytest.mark.timeout(180)
def test_sql_server_cdc_source_table_in_record_header(sdc_builder, sdc_executor, database):
    """Test for SQL Server CDC origin stage puts the source table in a record header attribute,
        * jdbc.cdc.source_schema_name = <source schema>
        * jdbc.cdc.source_name = <source table>

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    table = None
    connection = None
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        total_no_of_records = 1

        rows_in_database = setup_sample_data(total_no_of_records)

        table_name = get_random_string(string.ascii_lowercase, 20)
        schema_name = get_random_string(string.ascii_lowercase, 3)
        capture_instance_name = get_random_string(string.ascii_lowercase, 20)

        # create schema & table
        connection.execute(f'CREATE SCHEMA {schema_name}')
        table = setup_table(connection, schema_name, table_name,
                            rows_in_database[0:1], capture_instance_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(fetch_size=1,
                                      max_batch_size_in_records=1,
                                      table_configs=[{'capture_instance': capture_instance_name}]
                                      )

        wiretap = pipeline_builder.add_wiretap()

        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # wait for data captured by cdc jobs in sql server before capturing the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records / 2, database)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        # assert all the data captured have the same raw_data
        for record in wiretap.output_records:
            assert record.field['id'] == rows_in_database[0].get('id')
            assert record.field['name'] == rows_in_database[0].get('name')
            assert record.field['dt'] == rows_in_database[0].get('dt')
            assert record.header['values']['jdbc.cdc.source_schema_name'] == schema_name
            assert record.header['values']['jdbc.cdc.source_name'] == table_name
    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if schema_name is not None:
            logger.info('Dropping schema %s in %s database...', schema_name, database.type)
            connection.execute(f'drop schema {schema_name}')

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.8.0')
@pytest.mark.timeout(180)
def test_sql_server_cdc_starting_without_operation_committed_offset(sdc_builder, sdc_executor, database):
    """Test for SQL Server CDC origin stage runningn on missing __$operation in the committed offset
        __$operation field was introduced after 3.8.0

    The pipeline looks like:
        sql_server_cdc_origin >> wiretap
    """
    table = None
    connection = None
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    try:
        connection = database.engine.connect()
        total_no_of_records = 1

        rows_in_database = setup_sample_data(total_no_of_records)

        table_name = get_random_string(string.ascii_lowercase, 20)
        # get the capture_instance_name
        capture_instance_name = f'{DEFAULT_SCHEMA_NAME}_{table_name}'

        # create schema & table
        table = setup_table(connection, DEFAULT_SCHEMA_NAME, table_name, rows_in_database)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        sql_server_cdc = pipeline_builder.add_stage('SQL Server CDC Client')
        sql_server_cdc.set_attributes(fetch_size=1,
                                      max_batch_size_in_records=1,
                                      table_configs=[{'capture_instance': capture_instance_name}])

        wiretap = pipeline_builder.add_wiretap()

        sql_server_cdc >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # We hard code offset to be pre-migration to multi-threaded origin and thus forcing the origin to upgrade it
        offset = {
            'version': 2,
            'offsets': {
                '$com.streamsets.pipeline.stage.origin.jdbc.CDC.sqlserver.SQLServerCDCSource.offset.version$': '1',
                f'tableName=cdc.{capture_instance_name}_CT;;;partitioned=false;;;partitionSequence=-1;;;partitionStartOffsets=;;;partitionMaxOffsets=;;;usingNonIncrementalLoad=false': '__$seqval=0::__$start_lsn=0'
            }
        }

        sdc_executor.api_client.update_pipeline_committed_offsets(pipeline.id, body=offset)

        # wait for data captured by cdc jobs in sql server before capturing the pipeline
        ct_table_name = f'{capture_instance_name}_CT'
        wait_for_data_in_ct_table(ct_table_name, total_no_of_records, database)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', total_no_of_records)
        sdc_executor.stop_pipeline(pipeline)

        # assert all the data captured have the same raw_data
        for record in wiretap.output_records:
            assert record.field['id'] == rows_in_database[0].get('id')
            assert record.field['name'] == rows_in_database[0].get('name')
            assert record.field['dt'] == rows_in_database[0].get('dt')
    finally:
        if table is not None:
            logger.info('Dropping table %s in %s database...', table, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@sdc_min_version('3.0.0.0')
@pytest.mark.timeout(180)
def test_schema_change(sdc_builder, sdc_executor, database, keep_data):
    """Test for SQL Server CDC origin stage when schema change is enabled.
    We do so by capturing Insert Operation on CDC enabled table(s)
    using SQL Server CDC Origin and having a pipeline which reads that data using SQL Server CDC origin stage.
    The records in the pipeline will be stored in SQL Server table using JDBC Producer.
    While the pipeline is running the source table schema is changed by dropping or adding the columns,
    the dest table will be dropping or adding the columns respectively.
    Data is then asserted for what is captured at SQL Server Job and what we read in the pipeline.
    The pipeline looks like:
        sql_server_cdc_origin >> jdbc_producer
    """
    num_of_tables = 2
    schema_name = DEFAULT_SCHEMA_NAME
    table_prefix = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('SQL Server CDC Client')
    origin.allow_late_tables = True
    origin.enable_schema_changes_event = True
    # when allow_late_tables = true, the pipeline runs one background thread
    # to spool the list of cdc tables
    origin.maximum_pool_size = num_of_tables + 1
    origin.minimum_idle_connections = num_of_tables + 1
    origin.new_table_discovery_interval = '${1 * SECONDS}'
    origin.table_configs = [{'capture_instance': f'dbo_{table_prefix}_%'}]
    origin.number_of_threads = num_of_tables

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    connection = database.engine.connect()
    try:
        global_index = 0
        tables = []

        for index in range(0, num_of_tables):
            table_name = f"{table_prefix}_{get_random_string(string.ascii_lowercase, 20)}"
            connection.execute(f"CREATE TABLE {schema_name}.{table_name}(id INT)")
            _enable_cdc(connection, schema_name, table_name)
            connection.execute(f"INSERT INTO {schema_name}.{table_name} VALUES({global_index})")
            global_index += 1

            tables.append(table_name)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_of_tables)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == num_of_tables
        records.sort(key=_sort_records)
        for i in range(0, num_of_tables):
            assert records[i].field['id'] == i

        # Add a new column to the tables
        for table_name in tables:
            logger.info('Adding the column new_column varchar(10) on %s.%s...', schema_name, table_name)
            connection.execute(f'ALTER TABLE {table_name} ADD new_column INT')

            _disable_cdc(connection, schema_name, table_name)
            _enable_cdc(connection, schema_name, table_name, f"{schema_name}_{table_name}_2")

        # Reset record accumulation
        wiretap.reset()

        # Insert new data to the tables
        for table_name in tables:
            logger.info("Inserting new row into %s", table_name)
            connection.execute(f"INSERT INTO {schema_name}.{table_name} VALUES({global_index}, {global_index})")
            global_index += 1

        logger.info("Waiting on pipeline to finish processing all records")
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_of_tables)
        sdc_executor.stop_pipeline(pipeline)

        # All data were read
        records = wiretap.output_records
        assert len(records) == num_of_tables
        records.sort(key=_sort_records)
        for i in range(0, num_of_tables):
            assert records[i].field['id'] == num_of_tables + i
            assert records[i].field['new_column'] == num_of_tables + i
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            for table in tables:
                logger.info('Dropping table %s in %s database...', table, database.type)
                connection.execute(f"DROP TABLE {schema_name}.{table}")


def _sort_records(entry):
    return entry.field['id'].value


def _enable_cdc(connection, schema_name, table_name, capture_instance=None):
    if capture_instance is None:
        capture_instance = f"{schema_name}_{table_name}"

    logger.info('Enabling CDC on %s.%s into table %s...', schema_name, table_name, capture_instance)
    connection.execute(f'EXEC sys.sp_cdc_enable_table '
                       f'@source_schema=N\'{schema_name}\', '
                       f'@source_name=N\'{table_name}\','
                       f'@role_name = NULL, '
                       f'@capture_instance={capture_instance}')

    _wait_until_is_tracked_by_cdc(connection, table_name, 1)


def _disable_cdc(connection, schema_name, table_name, capture_instance=None):
    if capture_instance is None:
        capture_instance = f"{schema_name}_{table_name}"

    logger.info('Disabling CDC on %s.%s from table %s...', schema_name, table_name, capture_instance)
    connection.execute(
        f'EXEC sys.sp_cdc_disable_table '
        f'@source_schema=N\'{schema_name}\', '
        f'@source_name=N\'{table_name}\','
        f'@capture_instance={capture_instance}')

    _wait_until_is_tracked_by_cdc(connection, table_name, 0)


def _wait_until_is_tracked_by_cdc(connection, table_name, is_tracked_by_cdc):
    while True:
        cursor = connection.execute(f"select name from sys.tables where is_tracked_by_cdc = {is_tracked_by_cdc} and name = '{table_name}'")
        if len(cursor.fetchall()) > 0:
            break
        else:
            logger.info(f'Waiting until CDC is enabled/disabled for the table {table_name}')
            sleep(1)
