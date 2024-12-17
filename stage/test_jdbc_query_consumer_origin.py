# Copyright 2020 StreamSets Inc.
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
import math
import string
import time

import pytest
import sqlalchemy
from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.environments.databases import (MySqlDatabase, OracleDatabase,
                                                             PostgreSqlDatabase, SQLServerDatabase, MariaDBDatabase)

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Eddie'},
    {'id': 4, 'name': 'Jarcec'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

DEFAULT_DB2_SCHEMA = 'DB2INST1'

# Copied from **next-tests** test_jdbc_stages.py
def _create_table(table_name, database, schema_name=None, quote=False):
    """Helper function to create a table with two columns: id (int, PK) and name (str).

    Args:
        table_name: (:obj:`str`) the name for the new table.
        database: a :obj:`streamsets.testframework.environment.Database` object.
        schema_name: (:obj:`str`, optional) when provided, create the new table in a specific schema; otherwise,
            the default schema for the engine’s database connection is used.

    Return:
        The new table as a sqlalchemy.Table object.

    """
    metadata = sqlalchemy.MetaData()

    if type(database) == SQLServerDatabase:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', sqlalchemy.String(32), quote=quote),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True,
                                                   autoincrement=False, quote=quote),
                                 schema=schema_name,
                                 quote=quote)
    else:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', sqlalchemy.String(32), quote=quote),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=quote),
                                 schema=schema_name,
                                 quote=quote)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


# SDC-14882: JDBC Query Consumer closing the connection after each batch
@database
@pytest.mark.parametrize('batch_size', [1, 3, 10])
def test_jdbc_consumer_non_incremental_mode(sdc_builder, sdc_executor, database, batch_size):
    """Ensure that the Query consumer works properly in non-incremental mode.

    We test the stage with different batch sizes, since the logic is different depending on the whole table
    fit in a batch or not (see e.g. SDC-14882 for an issue with the second case).

    Pipeline:  jdbc_consumer >> wiretap
               jdbc_consumer >= finisher

    """
    if database.type == 'Oracle':
        pytest.skip("This test depends on proper case for column names that Oracle auto-uppers.")

    num_records = 8
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('JDBC Query Consumer')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=batch_size)
    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> wiretap.destination
    origin >= finisher
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create and populate table
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('name', sqlalchemy.String(32)))
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), input_data)

        # Run the pipeline and check the stage consumed all the expected records. Repeat several times to
        # ensure non-incremental mode works as expected after restarting the pipeline.
        for i in range(3):
            wiretap.reset()
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            sdc_records = [record.field
                           for record in wiretap.output_records]
            assert sdc_records == input_data

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database('mysql')
def test_stored_procedure_mysql(sdc_builder, sdc_executor, database, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    procedure_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name} (
                id int
            )
        """)

        # Create table
        connection.execute(f"""
            INSERT INTO {table_name} VALUES (1)
        """)

        # Create stored procedure
        connection.execute(f"""
            CREATE PROCEDURE {procedure_name}()
            BEGIN
                SELECT * FROM {table_name};
            END;
        """)

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('JDBC Query Consumer')
        origin.incremental_mode = False
        origin.sql_query = f"CALL {procedure_name}()"

        wiretap = builder.add_wiretap()

        finisher = builder.add_stage("Pipeline Finisher Executor")
        finisher.preconditions = ['${record:eventType() == \'no-more-data\'}']

        origin >> wiretap.destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['id'] == 1

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            connection.execute(f"DROP PROCEDURE IF EXISTS {procedure_name}")


@database('postgresql')
def test_stored_procedure_postgresql(sdc_builder, sdc_executor, database, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    procedure_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name} (
                id int
            )
        """)

        # Create table
        connection.execute(f"""
            INSERT INTO {table_name} VALUES (1)
        """)

        # Create stored procedure
        connection.execute(f"""
            create or replace function {procedure_name}()
                returns table (id int)
                language plpgsql
            as $$
            begin
                return query
                    select * from {table_name};
            end;$$
        """)

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('JDBC Query Consumer')
        origin.incremental_mode = False
        origin.sql_query = f"SELECT * FROM {procedure_name}()"

        wiretap = builder.add_wiretap()

        finisher = builder.add_stage("Pipeline Finisher Executor")
        finisher.preconditions = ['${record:eventType() == \'no-more-data\'}']

        origin >> wiretap.destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['id'] == 1

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            connection.execute(f"DROP FUNCTION IF EXISTS {procedure_name}()")


@database('sqlserver')
def test_stored_procedure_sqlserver(sdc_builder, sdc_executor, database, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    procedure_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name} (
                id int
            )
        """)

        # Create table
        connection.execute(f"""
            INSERT INTO {table_name} VALUES (1)
        """)

        # Create stored procedure
        connection.execute(f"""
            CREATE PROCEDURE {procedure_name}
            AS
               SELECT * FROM {table_name}
            RETURN
        """)

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('JDBC Query Consumer')
        origin.incremental_mode = False
        origin.sql_query = f"EXEC {procedure_name}"

        wiretap = builder.add_wiretap()

        finisher = builder.add_stage("Pipeline Finisher Executor")
        finisher.preconditions = ['${record:eventType() == \'no-more-data\'}']

        origin >> wiretap.destination
        origin >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['id'] == 1

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            connection.execute(f"DROP PROCEDURE IF EXISTS {procedure_name}")

@sdc_min_version('5.0.0')
@database('oracle')
def test_jdbc_consumer_read_timestamp_with_local_timezone(sdc_builder, sdc_executor, database):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                hold_id int,
                creation_date TIMESTAMP WITH LOCAL TIME ZONE
            )
        """)

        sql_query = f"SELECT HOLD_ID, CURRENT_TIMESTAMP as KFK_INS_DTSZ FROM {table_name} WHERE " \
                    f"HOLD_ID > ${{OFFSET}} ORDER BY HOLD_ID"

        connection.execute(f"INSERT INTO {table_name} VALUES(0, CURRENT_TIMESTAMP)")
        connection.execute(f"INSERT INTO {table_name} VALUES(1, CURRENT_TIMESTAMP)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Query Consumer')
        origin.sql_query = sql_query
        origin.offset_column = 'HOLD_ID'
        origin.incremental_mode = True
        origin.on_unknown_type = 'STOP_PIPELINE'

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        record = wiretap.output_records
        assert len(record) == 1
        assert record[0].field['HOLD_ID'] == 1

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('5.5.0')
@database('mysql','oracle', 'postgresql', 'sqlserver', 'mariadb')
def test_jdbc_tables_header(sdc_builder, sdc_executor, database):
    """Tests that 'JDBC Query Consumer' populates the jdbc.tables in header.values correctly - COLLECTOR-1503.
    The case of multiple tables is covered by creating two identical tables and joining them on id.
    The jdbc.tables string format and content varies between JDBC drivers, and test assertions deal with each case.

    The pipeline looks like:
        jdbc_query_consumer >> wiretap
    """

    table_name1 = get_random_string(string.ascii_lowercase, 20)
    table_name2 = get_random_string(string.ascii_lowercase, 20)
    if database.type == 'Oracle':
        # When not quoted, Oracle automatically converts names to upper case. Quoting is inconsistent between
        # databases, so it is preferable to avoid it in SQL below. And to get a compatible result during creation,
        # we omit quotes here also.
        create_quotes_names = False
    else:
        create_quotes_names = True

    logger.info('Creating two identical tables in %s database...', database.type)
    table1 = _create_table(table_name1, database, quote=create_quotes_names)
    table2 = _create_table(table_name2, database, quote=create_quotes_names)

    connection = database.engine.connect()
    try:
        logger.info('Adding %s rows into each table...', len(ROWS_IN_DATABASE))
        connection.execute(table1.insert(), ROWS_IN_DATABASE)
        connection.execute(table2.insert(), ROWS_IN_DATABASE)

        builder = sdc_builder.get_pipeline_builder()

        sql_query = "SELECT t1.id, t2.name " \
                    f"FROM {table_name1} t1 " \
                    f"    JOIN {table_name2} t2 " \
                    "     ON t1.name = t2.name " \
                    "WHERE t1.id > ${OFFSET} " \
                    "ORDER BY t1.id"
        origin = builder.add_stage('JDBC Query Consumer')
        origin.sql_query = sql_query
        origin.offset_column = 'id'
        origin.incremental_mode = True
        origin.on_unknown_type = 'STOP_PIPELINE'

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)
        sdc_executor.stop_pipeline(pipeline)

        # Check jdbc.tables header.
        tables_header = wiretap.output_records[0].header['values']['jdbc.tables']
        logger.debug('%s="%s"', "header['values']['jdbc.tables']", tables_header)
        logger.debug('%s="%s"', "database.type", database.type)
        # According to documentation some JDBC drivers may not provide this information:
        # https://docs.streamsets.com/platform-datacollector/latest/datacollector/UserGuide/Origins/JDBCConsumer.html
        if database.type == 'Oracle':
            # Oracle does not seem to populate this field
            assert tables_header == ""
        elif database.type == 'SQLServer':
            # SQLServer does not seem to populate this field
            assert tables_header == ""
        else:
            # MySQL, PostgreSQL and MiriaDB all return source table names as a coma-delimited list.
            # Ordering of the list is not known for PostgreSQL and MiriaDB, but For MySQL it is predictably random.
            # The logic below asserts that both names are reported in any order (and case is ignored, though this
            # should not be necessary):
            tables_list = tables_header.split(',')
            tables_normalized_map = map(lambda x:x.lower(), tables_list)
            assert set(tables_normalized_map) == {table_name1, table_name2}

    finally:
        try:
            logger.info('Dropping table %s in %s database ...', table_name1, database.type)
            connection.execute(f"DROP TABLE {table_name1}")
            logger.info('Dropping table %s in %s database ...', table_name2, database.type)
            connection.execute(f"DROP TABLE {table_name2}")
        except Exception as ex:
            logger.warning('Error during cleanup', exc_info=ex)


# Test for COLLECTOR-962
@database('postgresql')
@pytest.mark.parametrize('limit', [4,5,6])
def test_jdbc_consumer_no_more_data_with_limit(sdc_builder, sdc_executor, database, limit):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create table
        connection.execute(f"""
                    CREATE TABLE {table_name}(
                        id int primary key,
                        data_column int NULL
                    )
                """)

        # Add some data to the table
        for i in range(1, 11):
            connection.execute(f"INSERT INTO {table_name} VALUES({i}, {i})")

        # To make sure all issues with limit and maxBatchSize are fixed, we will run the test a few times with a different
        # limit each time and check that all the data is read each time. The different scenarios to test are:
        #   limit < maxBatchSize
        #   limit = maxBatchSize
        #   limit > maxBatchSize
        sql_query = f'SELECT * FROM {table_name} WHERE id > ${{OFFSET}} ORDER BY id LIMIT {limit}'

        # Create pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        origin = pipeline_builder.add_stage('JDBC Query Consumer')
        origin.set_attributes(incremental_mode=True,
                              sql_query=sql_query,
                              offset_column='id',
                              max_batch_size_in_records=5)

        wiretap = pipeline_builder.add_wiretap()
        finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"],
                                on_record_error= 'DISCARD')

        origin >> wiretap.destination
        origin >= finisher
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        sdc_records = [record.field for record in wiretap.output_records]
        assert len(sdc_records) == 10
        for i in range(10):
            assert sdc_records[i]['id'] == i+1

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")


@database('postgresql')
@pytest.mark.parametrize('initial_offset', ['01/01/2023', '\'01/01/2023\'', '\"01/01/2023\"'])
@pytest.mark.parametrize('offset_var', ['${OFFSET}', '\'${OFFSET}\'', '\"${OFFSET}\"'])
@sdc_min_version('5.8.0')
def test_jdbc_incremental_offset_to_string(sdc_builder, sdc_executor, database, initial_offset, offset_var):
    """Check that the pipeline completes successfully using several combinations of quoted initial offset and
    quoted offset variable when convert timestamp to string is set to true.

    The pipeline looks like:
        jdbc_query_consumer >> trash
    """

    num_records = 10
    time_column = 'postgresql_time'
    table_name = get_random_string(string.ascii_lowercase)
    query = f'SELECT * FROM {table_name} WHERE {time_column} > {offset_var} ORDER BY {time_column}'
    connection = database.engine.connect()

    try:
        connection.execute(f'CREATE TABLE {table_name}'
                           f'(u_id INT PRIMARY KEY, name varchar(100), postgresql_time timestamp default current_timestamp)');
        for id in range(num_records):
            connection.execute(f'INSERT INTO {table_name} VALUES ({id}, \'{get_random_string()}\')')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer')
        jdbc_query_consumer.set_attributes(sql_query=query,
                                           initial_offset=initial_offset,
                                           offset_column=time_column,
                                           incremental_mode=True,
                                           query_interval=0,
                                           convert_timestamp_to_string=True)

        trash = pipeline_builder.add_stage('Trash')

        pipeline_finished_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
        pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        jdbc_query_consumer >> trash
        jdbc_query_consumer >= pipeline_finished_executor

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == num_records, 'Wrong number of records'

    finally:
        logger.info('Dropping tables in %s database...', database.type)
        connection.execute(f'DROP TABLE IF EXISTS {table_name}')


@database('postgresql', 'sqlserver', 'mariadb', 'mysql')
def test_jdbc_query_special_character_offset_column(sdc_builder, sdc_executor, database):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        if database.type in ['PostgreSQL', 'SQLServer']:
            connection.execute(f"""
                        CREATE TABLE {table_name} (
                            "id-1" int
                        )
                    """)
        else:
            connection.execute(f"""
                        CREATE TABLE {table_name} (
                            `id-1` int
                        )
                    """)

        # Add some data to the table
        for i in range(1, 11):
            connection.execute(f"INSERT INTO {table_name} VALUES({i})")

        sql_query = f'SELECT * FROM {table_name} WHERE id-1 > ${{OFFSET}} ORDER BY id-1'

        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('JDBC Query Consumer')
        origin.set_attributes(incremental_mode=True,
                              sql_query=sql_query,
                              offset_column='id-1',
                              max_batch_size_in_records=5)

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(ValidationError) as e:
            sdc_executor.validate_pipeline(pipeline)

        assert e is not None
        assert e.value.issues is not None
        assert e.value.issues['issueCount'] == 1
        exception_message = e.value.issues['stageIssues'][origin.instance_name][0]['message']
        assert f"JDBC_INIT_64 -" in exception_message

    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
