# Copyright 2022 StreamSets Inc.
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
import time

import pytest
import sqlalchemy
from streamsets.testframework.utils import get_random_string


@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx2048m -Xms2048m'
    return hook


logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.sdc_min_version('5.4.0'), pytest.mark.database('postgresql'), pytest.mark.skip]


# https://www.postgresql.org/docs/11/datatype.html
# Not testing 'serial' family explicitly as that is just an alias
# Not supporting tsvector tsquery as that doesn't seem fit for us
# bit(n) is not supported
# xml is not supported
# domain types (as a category are not supported)
# pg_lsn not supported
DATA_TYPES_POSTGRESQL = [
    ('smallint', '-32768', 'SHORT', -32768),
    ('integer', '2147483647', 'INTEGER', '2147483647'),
    ('bigint', '-9223372036854775808', 'LONG', '-9223372036854775808'),
    ('decimal(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('numeric(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('real', '5.20', 'FLOAT', '5.2'),
    ('double precision', '5.20', 'DOUBLE', '5.2'),
    ('money', '12.34', 'DOUBLE', '12.34'),
    ('char(5)', "'Hello'", 'STRING', 'Hello'),
    ('varchar(5)', "'Hello'", 'STRING', 'Hello'),
    ('text', "'Hello'", 'STRING', 'Hello'),
    ('bytea', "'\\xDEADBEEF'", 'BYTE_ARRAY', '3q2+7w=='),
    ('timestamp', "'2003-04-12 04:05:06'", 'DATETIME', 1050120306000),
    ('timestamp with time zone', "'2003-04-12 04:05:06 America/New_York'", 'DATETIME', 1050134706000),
    # For PostgreSQL, we don't create ZONED_DATETIME
    ('date', "'2019-01-01'", 'DATE', 1546300800000),
    ('time', "'5:00:00'", 'TIME', 18000000),
    ('time with time zone', "'04:05:06-08:00'", 'TIME', 43506000),
    ('interval', "INTERVAL '1' YEAR", 'STRING', '1 years 0 mons 0 days 0 hours 0 mins 0.0 secs'),
    ('boolean', "true", 'BOOLEAN', True),
    ('ai', "'sad'", 'STRING', 'sad'),
    ('point', "'(1, 1)'", 'STRING', '(1.0,1.0)'),
    ('line', "'{1, 1, 1}'", 'STRING', '{1.0,1.0,1.0}'),
    ('lseg', "'((1,1)(2,2))'", 'STRING', '[(1.0,1.0),(2.0,2.0)]'),
    ('box', "'(1,1)(2,2)'", 'STRING', '(2.0,2.0),(1.0,1.0)'),
    ('path', "'((1,1),(2,2))'", 'STRING', '((1.0,1.0),(2.0,2.0))'),
    ('polygon', "'((1,1),(2,2))'", 'STRING', '((1.0,1.0),(2.0,2.0))'),
    ('circle', "'<(1,1),5>'", 'STRING', '<(1.0,1.0),5.0>'),
    ('inet', "'127.0.0.1/16'", 'STRING', '127.0.0.1/16'),
    ('cidr', "'127.0.0.0/16'", 'STRING', '127.0.0.0/16'),
    ('macaddr', "'08:00:2b:01:02:03'", 'STRING', '08:00:2b:01:02:03'),
#    ('macaddr8', "'08:00:2b:01:02:03'", 'STRING', '08:00:2b:ff:fe:01:02:03'), # Not supported
#    ('bit(8)', "b'10101010'", 'BYTE_ARRAY', '08:00:2b:ff:fe:01:02:03'), # Doesn't work at all today
    ('bit varying(3)', "b'101'", 'STRING', '101'),
    ('uuid', "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", 'STRING', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
#    ('xml', "'<foo>bar</foo>'", 'STRING', ''), # Doesn't work properly today
    ("json", "'{\"a\":\"b\"}'", 'STRING', '{"a":"b"}'),
    ("jsonb", "'{\"a\":\"b\"}'", 'STRING', '{"a": "b"}'),
    ("integer[3][3]", "'{{1,2,3},{4,5,6},{7,8,9}}'", 'STRING', '{{1,2,3},{4,5,6},{7,8,9}}'),
    ("ct", "ROW(1, 2)", 'STRING', '(1,2)'),
    ("int4range", "'[1,2)'", 'STRING', '[1,2)'),
    ("int8range", "'[1,2)'", 'STRING', '[1,2)'),
    ("numrange", "'[1,2)'", 'STRING', '[1,2)'),
    ("tsrange", "'[2010-01-01 14:30, 2010-01-01 15:30)'", 'STRING', '["2010-01-01 14:30:00","2010-01-01 15:30:00")'),
    ("tstzrange", "'[2010-01-01 14:30 America/New_York, 2010-01-01 15:30 America/New_York)'", 'STRING',
     '["2010-01-01 19:30:00+00","2010-01-01 20:30:00+00")'),
    ("daterange", "'[2010-01-01, 2010-01-02)'", 'STRING', '[2010-01-01,2010-01-02)'),
]
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_POSTGRESQL, ids=[i[0] for i in DATA_TYPES_POSTGRESQL])
def test_data_types_postgresql(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create enum type conditionally
        connection.execute(f"""
            DO
            $$
            BEGIN
              IF NOT EXISTS (SELECT * FROM pg_type typ
                                      INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
                                      WHERE nsp.nspname = current_schema() AND typ.typname = 'ai') THEN
                CREATE TYPE ai AS ENUM ('sad', 'ok', 'happy');
              END IF;
            END;
            $$
            LANGUAGE plpgsql;
        """)

        # Create enum complex type conditionally
        connection.execute(f"""
            DO
            $$
            BEGIN
              IF NOT EXISTS (SELECT * FROM pg_type typ
                                      INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
                                      WHERE nsp.nspname = current_schema() AND typ.typname = 'ct') THEN
                CREATE TYPE ct AS (a int, b int);
              END IF;
            END;
            $$
            LANGUAGE plpgsql;
        """)

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

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('PostgreSQL Multitable Consumer')
        origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
        origin.on_unknown_type = 'CONVERT_TO_STRING'
        origin.ssl_mode = 'DISABLED'

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


# Rules: https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html
OBJECT_NAMES_POSTGRESQL = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 63), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 63)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "$_", get_random_string(string.ascii_letters, 5) + "$_"),
]


@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES_POSTGRESQL, ids=[i[0] for i in OBJECT_NAMES_POSTGRESQL])
def test_object_names_postgresql(sdc_builder, sdc_executor, database, test_name, table_name, offset_name):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('PostgreSQL Multitable Consumer')
    origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
    origin.max_batch_size_in_records = 10

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'
    origin.ssl_mode = 'DISABLED'

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(offset_name, sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{offset_name: 1}])

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        # We want to run for a few seconds to see if any errors show up (like that did in previous versions)
        time.sleep(10)
        sdc_executor.stop_pipeline(pipeline)

        # There should be no errors reported
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.PostgreSQLMultitableConsumer_01.errorRecords.counter').count == 0
        assert history.latest.metrics.counter('stage.PostgreSQLMultitableConsumer_01.stageErrors.counter').count == 0

        # And verify that we properly read that one record
        assert len(wiretap.output_records) == 1
        # SDC Will escape field names with certain characters, but not always...
        if "$" in offset_name:
            assert wiretap.output_records[0].field[f'"{offset_name}"'] == 1
        else:
            assert wiretap.output_records[0].field[offset_name] == 1
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@pytest.mark.parametrize('number_of_threads', [1, 10])
@pytest.mark.parametrize('processing_mode', ['DISABLED', 'BEST_EFFORT', 'REQUIRED'])
def test_multiple_batches(sdc_builder, sdc_executor, database, number_of_threads, processing_mode, keep_data):
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

    origin = builder.add_stage('PostgreSQL Multitable Consumer')
    origin.table_configs=[{
        "tablePattern": f'%{table_name}%',
        'partitioningMode': processing_mode,
        'partitionSize': str(2 * max_batch_size)
    }]
    origin.max_batch_size_in_records = max_batch_size
    origin.number_of_threads = number_of_threads
    origin.maximum_pool_size = number_of_threads
    origin.ssl_mode = 'DISABLED'

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    finisher = builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    origin >= finisher

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        logger.info('Inserting data into %s', table_name)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'id' : n} for n in range(1, max_batch_size * batches + 1)])

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == max_batch_size * batches

        # Verify each record
        def sortFunc(entry):
            return entry.field['id'].value

        records.sort(key=sortFunc)

        expected_number = 1
        for record in records:
            assert record.field['id'] == expected_number
            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


def test_dataflow_events(sdc_builder, sdc_executor, database, keep_data):
    table_prefix = get_random_string(string.ascii_lowercase, 20)
    table_a = '{}_a'.format(table_prefix)
    table_b = '{}_b'.format(table_prefix)

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('PostgreSQL Multitable Consumer')
    source.transaction_isolation = 'TRANSACTION_READ_COMMITTED'
    source.table_configs = [{
        'tablePattern': f'{table_prefix}%',
        "enableNonIncremental": True,
    }]
    source.ssl_mode = 'DISABLED'

    trash = builder.add_stage('Trash')

    source >> trash

    wiretap = builder.add_wiretap()
    source >= wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    source.table_configs[0]["tablePattern"] = f'{table_prefix}%'
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
        a.create(database.engine)
        b.create(database.engine)

        logger.info('Inserting rows into %s and %s', table_a, table_b)
        connection = database.engine.connect()
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
        assert table_a in table_set
        assert table_b in table_set

        # Then we should have schema done with all the tables
        assert records[2].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a in records[2].field['tables']
        assert table_b in records[2].field['tables']

        # Final event should be no more data
        assert records[3].header.values['sdc.event.type'] == 'no-more-data'

        wiretap.reset()

        # Second iteration - insert one new row
        logger.info('Inserting rows into %s', table_a)
        connection = database.engine.connect()
        connection.execute(a.insert(), {'id': 2})

        # 1 record, 3 events more
        status.wait_for_pipeline_output_records_count(10)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 3

        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[0].field['table'] == table_a

        assert records[1].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a in records[1].field['tables']
        assert table_b in records[1].field['tables']

        assert records[2].header.values['sdc.event.type'] == 'no-more-data'

        # Now let's stop the pipeline and start it again
        # SDC-10022: Multitable PostgreSQL Origin with non-incremental table does not properly trigger 'no-more-data' event
        sdc_executor.stop_pipeline(pipeline)

        # Portable truncate
        wiretap.reset()

        # Start the pipeline and wait for it to read three records (3 events)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 3

        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[0].field['table'] == table_a

        assert records[1].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a in records[1].field['tables']
        assert table_b in records[1].field['tables']

        assert records[2].header.values['sdc.event.type'] == 'no-more-data'
    finally:
        sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            logger.info('Dropping tables %s and %s in %s database...', table_a, table_b, database.type)
            a.drop(database.engine)
            b.drop(database.engine)


def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("PostgreSQL MultiTable Origin doesn't deal with data formats")


def test_resume_offset(sdc_builder, sdc_executor, database, keep_data):
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

    origin = builder.add_stage('PostgreSQL Multitable Consumer')
    origin.transaction_isolation = 'TRANSACTION_READ_COMMITTED'
    origin.table_configs = [{'tablePattern': f'{table_name}%'}]
    origin.ssl_mode = 'DISABLED'

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            logger.info('Inserting data into %s', table_name)
            connection = database.engine.connect()
            connection.execute(table.insert(), [{'id': n} for n in range(iteration * records_per_iteration + 1, iteration * records_per_iteration + 1 + records_per_iteration)])

            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert record.field['id'].value == expected_number

                expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
