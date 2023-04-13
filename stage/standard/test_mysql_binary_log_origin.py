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
import random
import string

import pytest
import sqlalchemy

from streamsets.sdk.utils import Version
from streamsets.testframework.environments.databases import MySqlDatabase, MemSqlDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def preflight_check(database):
    if isinstance(database, MySqlDatabase) and not database.is_cdc_enabled:
        pytest.skip('Test only runs against MySQL with CDC enabled.')
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")


# https://dev.mysql.com/doc/refman/8.0/en/data-types.html
# The underlying library we're using doesn't work properly with unsigned types:
#   https://github.com/shyiko/mysql-binlog-connector-java#implementation-notes
#   They are always returned as signed numbers, meaning that -1 is a max value of the column
# The underlying library is also not working properly with binary types - assuming that they are strings
#   unless a config is set (but then all strings are binaries), ... Seems like some inherent limitation
#   inside MySQL's bin log.
#   https://github.com/shyiko/mysql-binlog-connector-java/issues/276
# No good support for the poly type either (point, linestring, polygon).
DATA_TYPES = [
    ('TINYINT', '-128', 'INTEGER', '-128'),
    ('TINYINT UNSIGNED', '255', 'INTEGER', '-1'),
    ('SMALLINT', '-32768', 'INTEGER', '-32768'),
    ('SMALLINT UNSIGNED', '65535', 'INTEGER', '-1'),
    ('MEDIUMINT', '-8388608', 'INTEGER', '-8388608'),
    ('MEDIUMINT UNSIGNED', '16777215', 'INTEGER', '-1'),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('INT UNSIGNED', '4294967295', 'INTEGER', '-1'),
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('BIGINT UNSIGNED', '18446744073709551615', 'LONG', '-1'),
    ('DECIMAL(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('FLOAT', '5.2', 'FLOAT', '5.2'),
    ('DOUBLE', '5.2', 'DOUBLE', '5.2'),
    ('BIT(8)', "b'1000001'", 'STRING', '{0, 6}'), # I have no clue what this means and where it's coming from
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIMESTAMP', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIME', "'5:00:00'", 'TIME', 18000000),
    ('YEAR', "'2019'", 'INTEGER', '2019'),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('BINARY(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARBINARY(5)', "'Hello'", 'STRING', 'Hello'),
    ('BLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ("ENUM('a', 'b')", "'a'", 'STRING', '1'),
    ("set('a', 'b', 'c')", "'a,c'", 'STRING', '5'),
#    ("POINT", "POINT(1, 1)", 'STRING', 'AAAAAAEBAAAAAAAAAAAA8D8AAAAAAADwPw=='),
#    ("LINESTRING", "LineString(Point(0,0), Point(10,10), Point(20,25), Point(50,60))", 'STRING', 'AAAAAAECAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkQAAAAAAAACRAAAAAAAAANEAAAAAAAAA5QAAAAAAAAElAAAAAAAAATkA='),
#    ("POLYGON", "Polygon(LineString(Point(0,0),Point(10,0),Point(10,10),Point(0,10),Point(0,0)),LineString(Point(5,5),Point(7,5),Point(7,7),Point(5,7),Point(5,5)))", 'STRING', 'AAAAAAEDAAAAAgAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAAAAUQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAFEAAAAAAAAAUQA=='),
    ("JSON", "'{\"a\":\"b\"}'", 'STRING', '{\"a\":\"b\"}'),
]


@database('mysql')
@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES, ids=[i[0] for i in DATA_TYPES])
def test_data_types(
        sdc_builder,
        sdc_executor,
        database,
        sql_type,
        insert_fragment,
        expected_type,
        expected_value,
        keep_data
):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if (binlog_row_metadata == 'FULL') and (sql_type == 'TEXT'):
            # The connector we are using converts Text types to Byte Arrays, but this is only used when the table is
            # loaded via event. This cannot be changed for tables loaded via catalog as this would affect the default
            # mapping of unsupported types and there would be more types that would not match
            expected_type = 'BYTE_ARRAY'
            expected_value = 'SGVsbG8='

    try:
        # Create Pipeline.
        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('MySQL Binary Log')
        origin.initial_offset = _get_initial_offset(database)
        origin.server_id = _get_server_id()
        origin.include_tables = database.database + '.' + table_name

        wiretap = builder.add_wiretap()
        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                data_column {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        # Run pipeline and verify output.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        # TLKT-177: Add ability for field to return raw value
        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.

        assert record.field['Data']['data_column'].type == expected_type
        assert null_record.field['Data']['data_column'].type == expected_type

        assert record.field['Data']['data_column']._data['value'] == expected_value
        assert null_record.field['Data']['data_column'] == None
    finally:
        if not keep_data:
            if connection is not None:
                logger.info('Dropping table %s in %s database ...', table_name, database.type)
                connection.execute(f"DROP TABLE IF EXISTS {table_name}")

            if connection is not None:
                connection.close()


# Rules: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
# Rules: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
OBJECT_NAMES = [
    ('keywords', 'schema_name', 'table', 'column'),
    (
        'lowercase',
        get_random_string(string.ascii_lowercase, 20),
        get_random_string(string.ascii_lowercase, 20),
        get_random_string(string.ascii_lowercase, 20)
    ), (
        'uppercase',
        get_random_string(string.ascii_uppercase, 20),
        get_random_string(string.ascii_uppercase, 20),
        get_random_string(string.ascii_uppercase, 20)
    ), (
        'mixedcase',
        get_random_string(string.ascii_letters, 20),
        get_random_string(string.ascii_letters, 20),
        get_random_string(string.ascii_letters, 20)
    ), (
        'max_table_name',
        get_random_string(string.ascii_letters, 64),
        get_random_string(string.ascii_letters, 64),
        get_random_string(string.ascii_letters, 20)
    ), (
        'max_column_name',
        get_random_string(string.ascii_letters, 20),
        get_random_string(string.ascii_letters, 20),
        get_random_string(string.ascii_letters, 64)
    ), (
        'numbers',
        get_random_string(string.ascii_letters, 5) + "0123456789",
        get_random_string(string.ascii_letters, 5) + "0123456789",
        get_random_string(string.ascii_letters, 5) + "0123456789"
    ), (
        'special',
        get_random_string(string.ascii_letters, 5) + "$_",
        get_random_string(string.ascii_letters, 5) + "$_",
        get_random_string(string.ascii_letters, 5) + "$_"
    ), (
        'dashed',
        get_random_string(string.ascii_letters, 5) + "-",
        get_random_string(string.ascii_letters, 5) + "-",
        get_random_string(string.ascii_letters, 5) + "-"
    )
]


@database('mysql')
@pytest.mark.parametrize('test_name,schema_name,table_name,offset_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names(
        sdc_builder,
        sdc_executor,
        database,
        test_name,
        schema_name,
        table_name,
        offset_name,
        keep_data
):
    if test_name == 'dashed' and Version(sdc_builder.version) < Version('5.6.0'):
        pytest.skip('Table and schema names with dashes were not correctly processed until SDC version 5.6.0')

    connection = database.engine.connect()

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('MySQL Binary Log')
    origin.initial_offset = _get_initial_offset(database)
    origin.server_id = _get_server_id()
    origin.include_tables = schema_name + '.' + table_name

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(offset_name, sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True,
        schema=schema_name
    )
    try:
        logger.info('Creating schema %s in %s database ...', schema_name, database.type)
        database.engine.execute(sqlalchemy.schema.CreateSchema(schema_name))

        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection.execute(table.insert(), [{offset_name: 1}])

        sdc_executor.add_pipeline(pipeline)

        # Run pipeline and verify output.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # Verify that we properly read that one record
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['Data'][offset_name] == 1
    finally:
        if not keep_data:
            logger.info(f'Dropping table {table_name} in {database.type} database...')
            connection.execute(f'DROP TABLE IF EXISTS `{table_name}`')

            logger.info(f'Dropping schema {schema_name} in {database.type} database')
            connection.execute(f'DROP SCHEMA IF EXISTS `{schema_name}`')


@database('mysql')
def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data):
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    max_batch_size = 1000
    batches = 50
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('MySQL Binary Log')
    origin.initial_offset = _get_initial_offset(database)
    origin.server_id = _get_server_id()
    origin.include_tables = database.database + '.' + table_name

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        logger.info('Inserting data into %s', table_name)
        connection.execute(table.insert(), [{'id': n} for n in range(1, max_batch_size * batches + 1)])

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(max_batch_size * batches)

        records = wiretap.output_records
        assert len(records) == max_batch_size * batches

        # Verify each record
        def sortFunc(entry):
            return entry.field['Data']['id'].value

        records.sort(key=sortFunc)

        expected_number = 1
        for record in records:
            assert record.field['Data']['id'] == expected_number
            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database('mysql')
def test_dataflow_events(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip('MySQL Binary Log Origin does not support events')


@database('mysql')
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("MySQL Origin doesn't deal with data formats")


@database('mysql')
def test_resume_offset(sdc_builder, sdc_executor, database, keep_data):
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    iterations = 3
    records_per_iteration = 10
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('MySQL Binary Log')
    origin.initial_offset = _get_initial_offset(database)
    origin.server_id = _get_server_id()
    origin.include_tables = database.database + '.' + table_name

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            logger.info('Inserting data into %s', table_name)
            connection.execute(table.insert(), [{'id': n} for n in range(iteration * records_per_iteration + 1,
                                                                         iteration * records_per_iteration + 1 + records_per_iteration)])

            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert record.field['Data']['id'].value == expected_number

                expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


def _get_server_id():
    server_id = str(random.randint(1, 2147483647))
    logger.info(f"Generated server id {server_id}")
    return server_id


def _get_initial_offset(database):
    """Return current position of the bin log that can be used for Initial Offset configuration."""
    connection = database.engine.connect()
    rs = None

    try:
        rs = connection.execute("SHOW MASTER STATUS")
        rows = [row for row in rs]

        assert len(rows) == 1
        offset = f"{rows[0][0]}:{rows[0][1]}"
        logger.info(f"Generated starting offset: {offset}")
        return offset
    finally:
        if rs is not None:
            rs.close()

        if connection is not None:
            connection.close()
