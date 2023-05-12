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
import string

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx2048m -Xms2048m'
    return hook


logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_NAME = 'dbo'


# https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017
# hiearchyid types not supported
# Geometry and geography not supported
DATA_TYPES_SQLSERVER = [
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2004-05-23T14:25:10'", 'DATETIME', 1085322310000),
    ('DATETIME2', "'2004-05-23T14:25:10'", 'DATETIME', 1085322310000),
    ('DATETIMEOFFSET', "'2004-05-23 14:25:10.3456 -08:00'", 'DEPENDS_ON_VERSION', 'depends_on_version'),
    ('SMALLDATETIME', "'2004-05-23T14:25:10'", 'DATETIME', 1085322300000),
    ('TIME', "'14:25:10'", 'TIME', 51910000),
    ('BIT', "1", 'BOOLEAN', True),
    ('DECIMAL(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('REAL', '5.20', 'FLOAT', '5.2'),
    ('FLOAT', '5.20', 'DOUBLE', '5.2'),
    ('TINYINT', '255', 'SHORT', 255),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('MONEY', '255.60', 'DECIMAL', '255.6000'),
    ('SMALLMONEY', '255.60', 'DECIMAL', '255.6000'),
    ('BINARY(5)', "CAST('Hello' AS BINARY(5))", 'BYTE_ARRAY', 'SGVsbG8='),
    ('VARBINARY(5)', "CAST('Hello' AS VARBINARY(5))", 'BYTE_ARRAY', 'SGVsbG8='),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NVARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ('NTEXT', "'Hello'", 'STRING', 'Hello'),
    ('IMAGE', "CAST('Hello' AS IMAGE)", 'BYTE_ARRAY', 'SGVsbG8='),
#    ('GEOGRAPHY',"geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326)", 'BYTE_ARRAY', '5hAAAAEUhxbZzvfTR0DXo3A9CpdewIcW2c7300dAy6FFtvOVXsA='), # Not supported
#    ('GEOMETRY',"geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0)", 'BYTE_ARRAY', 'AAAAAAEEAwAAAAAAAAAAAFlAAAAAAAAAWUAAAAAAAAA0QAAAAAAAgGZAAAAAAACAZkAAAAAAAIBmQAEAAAABAAAAAAEAAAD/////AAAAAAI='), # Not supported
    ('XML', "'<a></a>'", 'STRING', '<a/>')
]

UNSUPPORTED_DATA_TYPES_SQLSERVER_AS_PRIMARY_KEY = ["TEXT", "NTEXT", "IMAGE", "XML"]

@sdc_min_version('3.0.0.0')
@database('sqlserver')
@pytest.mark.parametrize('fetch_size', [0, 1])
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_SQLSERVER, ids=[i[0] for i in DATA_TYPES_SQLSERVER])
def test_data_types(
        sdc_builder,
        sdc_executor,
        database,
        sql_type,
        insert_fragment,
        expected_type,
        expected_value,
        keep_data,
        fetch_size
):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                data_column {sql_type} NULL
            )
        """)
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SQL Server CDC Client')
        origin.fetch_size = fetch_size
        origin.table_configs = [{'capture_instance': f"{DEFAULT_SCHEMA_NAME}_{table_name}"}]

        wiretap = builder.add_wiretap()

        # As a part of SDC-10125, DATETIMEOFFSET is natively supported in SDC, and is converted into ZONED_DATETIME
        if sql_type == 'DATETIMEOFFSET':
            if Version(sdc_executor.version) >= Version('3.14.0'):
                expected_type = 'ZONED_DATETIME'
                expected_value = '2004-05-23T14:25:10.3456-08:00'
            else:
                expected_type = 'STRING'
                expected_value = '2004-05-23 14:25:10.3456 -08:00'
                # This unknown_type_action setting is required, otherwise DATETIMEOFFSET tests for SDC < 3.14 will fail.
                origin.on_unknown_type = 'CONVERT_TO_STRING'

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 2

        record = records[0]
        null_record = records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        record_field_data_column = _get_data_field_from_record(record, 'data_column')
        null_record_field_data_column = _get_data_field_from_record(null_record, 'data_column')

        assert record_field_data_column.type == expected_type
        assert null_record_field_data_column.type == expected_type

        assert record_field_data_column._data['value'] == expected_value
        assert null_record_field_data_column == None
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")

        if connection is not None:
            connection.close()


@sdc_min_version('3.0.0.0')
@database('sqlserver')
@pytest.mark.parametrize('fetch_size', [0, 1])
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_SQLSERVER, ids=[i[0] for i in DATA_TYPES_SQLSERVER])
def test_data_types_as_primary_keys(
        sdc_builder,
        sdc_executor,
        database,
        sql_type,
        insert_fragment,
        expected_type,
        expected_value,
        keep_data,
        fetch_size
):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    if sql_type in UNSUPPORTED_DATA_TYPES_SQLSERVER_AS_PRIMARY_KEY:
        pytest.skip('Test only runs against valid SQL Server Primary Key.')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int,
                data_column {sql_type} primary key
            )
        """)
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        # connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('SQL Server CDC Client')
        origin.fetch_size = fetch_size
        origin.table_configs = [{'capture_instance': f"{DEFAULT_SCHEMA_NAME}_{table_name}"}]

        wiretap = builder.add_wiretap()

        # As a part of SDC-10125, DATETIMEOFFSET is natively supported in SDC, and is converted into ZONED_DATETIME
        if sql_type == 'DATETIMEOFFSET':
            if Version(sdc_executor.version) >= Version('3.14.0'):
                expected_type = 'ZONED_DATETIME'
                expected_value = '2004-05-23T14:25:10.3456-08:00'
            else:
                expected_type = 'STRING'
                expected_value = '2004-05-23 14:25:10.3456 -08:00'
                # This unknown_type_action setting is required, otherwise DATETIMEOFFSET tests for SDC < 3.14 will fail.
                origin.on_unknown_type = 'CONVERT_TO_STRING'

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1

        record = records[0]

        # Since we are controlling types, we want to check explicit values inside the record rather the python wrappers.
        # TLKT-177: Add ability for field to return raw value

        data_column = _get_data_field_from_record(record, 'data_column')
        assert data_column.type == expected_type
        assert data_column._data['value'] == expected_value
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")

        if connection is not None:
            connection.close()


# Rules: https://stackoverflow.com/questions/5808332/sql-server-maximum-character-length-of-object-names
# Rules:
OBJECT_NAMES_SQLSERVER = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    # Capture instance name is limited to 100, - 7 (dbo_ _CT)
    ('max_table_name', get_random_string(string.ascii_letters, 93), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 128)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>", get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>"),
]
@database('sqlserver')
@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES_SQLSERVER, ids=[i[0] for i in OBJECT_NAMES_SQLSERVER])
def test_object_names(
        sdc_builder,
        sdc_executor,
        database,
        test_name,
        table_name,
        offset_name,
        keep_data
):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    connection = database.engine.connect()
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('SQL Server CDC Client')
    origin.fetch_size = 1
    origin.table_configs = [{'capture_instance': f"{DEFAULT_SCHEMA_NAME}_{table_name}"}]

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(offset_name, sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(connection.engine)
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name)

        logger.info('Adding data into %s database ...', database.type)
        connection.execute(table.insert(), [{offset_name: 1}])

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1

        # SDC Will escape field names with certain characters, but not always...
        field_name = f'"{offset_name}"' if "$" in offset_name else offset_name
        assert _get_data_field_from_record(records[0], field_name) == 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
@pytest.mark.parametrize('number_of_threads', [1, 10])
def test_multiple_batches(sdc_builder, sdc_executor, database, number_of_threads, keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    connection = database.engine.connect()
    max_batch_size = 100
    batches = 5
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('SQL Server CDC Client')
    origin.fetch_size = 1
    origin.table_configs = [{'capture_instance': f"{DEFAULT_SCHEMA_NAME}_{table_name}"}]
    origin.max_batch_size_in_records = max_batch_size
    origin.number_of_threads = number_of_threads
    origin.maximum_pool_size = number_of_threads

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    finisher = builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    origin >= finisher

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(connection.engine)
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name)

        logger.info('Inserting data into %s', table_name)
        connection.execute(table.insert(), [{'id' : n} for n in range(1, max_batch_size * batches + 1)])

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == max_batch_size * batches
        records.sort(key=_sort_records)

        expected_number = 1
        for record in records:
            assert _get_data_field_from_record(record, 'id').value == expected_number
            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
def test_dataflow_events(sdc_builder, sdc_executor, database, keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    connection = database.engine.connect()
    table_prefix = get_random_string(string.ascii_lowercase, 20)
    table_a = '{}_a'.format(table_prefix)
    table_b = '{}_b'.format(table_prefix)
    table_a_ct = '{}_{}_CT'.format(DEFAULT_SCHEMA_NAME, table_a)
    table_b_ct = '{}_{}_CT'.format(DEFAULT_SCHEMA_NAME, table_b)

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('SQL Server CDC Client')
    source.fetch_size = 1
    source.table_configs = [
        {'capture_instance': f"{DEFAULT_SCHEMA_NAME}_{table_a}"},
        {'capture_instance': f"{DEFAULT_SCHEMA_NAME}_{table_b}"}
    ]

    trash = builder.add_stage('Trash')

    source >> trash

    wiretap = builder.add_wiretap()
    source >= wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    #  We need three tables for this test
    metadata = sqlalchemy.MetaData()
    a = sqlalchemy.Table(
        table_a,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )
    b = sqlalchemy.Table(
        table_b,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )

    try:
        logger.info('Creating tables %s and %s in %s database ...', table_a, table_b, database.type)
        a.create(connection.engine)
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_a)
        b.create(connection.engine)
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_b)

        logger.info('Inserting rows into %s and %s', table_a, table_b)
        connection.execute(a.insert(), {'id': 1})
        connection.execute(b.insert(), {'id': 1})

        # Start the pipeline
        status = sdc_executor.start_pipeline(pipeline)

        # Read two records, generate 4 events, 6 records
        status.wait_for_pipeline_output_records_count(6)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 4

        # First two events should be table-finished (for any order of the tables though)
        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[1].header.values['sdc.event.type'] == 'table-finished'
        table_set = set()
        table_set.add(records[0].field['table'])
        table_set.add(records[1].field['table'])
        assert table_a_ct in table_set
        assert table_b_ct in table_set

        # Then we should have schema done with all the tables
        assert records[2].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a_ct in records[2].field['tables']
        assert table_b_ct in records[2].field['tables']

        # Final event should be no more data
        assert records[3].header.values['sdc.event.type'] == 'no-more-data'

        wiretap.reset()

        # Second iteration - insert one new row
        logger.info('Inserting rows into %s', table_a)
        connection.execute(a.insert(), {'id': 2})

        # 1 record, 3 events more
        status.wait_for_pipeline_output_records_count(10)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 3

        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[0].field['table'] == table_a_ct

        assert records[1].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a_ct in records[1].field['tables']
        assert table_b_ct in records[1].field['tables']

        assert records[2].header.values['sdc.event.type'] == 'no-more-data'

        # Now let's stop the pipeline and start it again
        # SDC-10022: Multitable JDBC Origin with non-incremental table does not properly trigger 'no-more-data' event
        sdc_executor.stop_pipeline(pipeline)

        # Portable truncate
        wiretap.reset()

        # Start the pipeline and wait for it to read three records (3 events)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 4

        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[1].header.values['sdc.event.type'] == 'table-finished'
        table_set = set()
        table_set.add(records[0].field['table'])
        table_set.add(records[1].field['table'])
        assert table_a_ct in table_set
        assert table_b_ct in table_set

        assert records[2].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a_ct in records[2].field['tables']
        assert table_b_ct in records[2].field['tables']

        assert records[3].header.values['sdc.event.type'] == 'no-more-data'

        sdc_executor.stop_pipeline(pipeline)
    finally:
        if not keep_data:
            logger.info('Dropping tables %s and %s in %s database...', table_a, table_b, database.type)
            a.drop(database.engine)
            b.drop(database.engine)

        if connection is not None:
            connection.close()


@database('sqlserver')
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("SQL Server CDC Origin doesn't deal with data formats")


@database('sqlserver')
def test_resume_offset(sdc_builder, sdc_executor, database, keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against SQL Server with CDC enabled.')

    iterations = 3
    records_per_iteration = 10
    connection = database.engine.connect()
    table_name = get_random_string(string.ascii_lowercase, 20)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                             schema=DEFAULT_SCHEMA_NAME)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('SQL Server CDC Client')
    origin.fetch_size = 1
    origin.table_configs = [{'capture_instance': f"{DEFAULT_SCHEMA_NAME}_{table_name}"}]

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(connection.engine)
        _enable_cdc(connection, DEFAULT_SCHEMA_NAME, table_name)

        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()


            logger.info('Inserting data into %s', table_name)
            connection.execute(table.insert(), [{'id': n} for n in range(iteration * records_per_iteration + 1, iteration * records_per_iteration + 1 + records_per_iteration)])

            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert _get_data_field_from_record(record, 'id').value == expected_number
                expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)

        if connection is not None:
            connection.close()


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
                       f'@capture_instance=N\'{capture_instance}\'')


def _disable_cdc(connection, schema_name, table_name, capture_instance=None):
    if capture_instance is None:
        capture_instance = f"{schema_name}_{table_name}"

    logger.info('Disabling CDC on %s.%s from table %s...', schema_name, table_name, capture_instance)
    connection.execute(
        f'EXEC sys.sp_cdc_disable_table '
        f'@source_schema=N\'{schema_name}\', '
        f'@source_name=N\'{table_name}\','
        f'@capture_instance={capture_instance}')


def _get_data_field_from_record(record, field_name):
    return record.field[field_name]
