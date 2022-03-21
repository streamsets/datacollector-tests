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

import pytest
import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_NAME = 'public'
POLL_INTERVAL = "${1 * SECONDS}"

# https://help.qlik.com/en-US/replicate/November2021/Content/Replicate/Main/Aurora%20PostgreSQL/aws_postgresql_data_types_source.htm
DATA_TYPES_AURORA = [
    ('INT', '-2147483648', 'INTEGER', -2147483648),
    ('SMALLINT', '-32768', 'SMALLINT', -32768),
    ('BIGINT', '-9223372036854775807', 'BIGINT', -9223372036854775807),
    ('NUMERIC(5,2)', '5.20', 'NUMERIC(5,2)', 5.2),
    ('DECIMAL(5,2)', '5.20', 'NUMERIC(5,2)', 5.2),
    ('REAL', '5.20', 'REAL', 5.2),
    ('DOUBLE PRECISION', '5.20', 'DOUBLE PRECISION', 5.2),
    ('MONEY', '255.60', 'MONEY', '$255.60'),
    ('CHAR(5)', "'Hello'", 'CHARACTER(5)', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'CHARACTER VARYING(5)', 'Hello'),
    ('NCHAR(5)', "'Hello'", 'CHARACTER(5)', 'Hello'),
    ('TEXT', "'Hello'", 'TEXT', 'Hello'),
    ('TIMESTAMP', "'2019-01-01'", 'TIMESTAMP WITHOUT TIME ZONE', '2019-01-01 00:00:00'),
    ('DATE', "'2019-01-01'", 'DATE', '2019-01-01'),
    ('TIME', "'14:25:10'", 'TIME WITHOUT TIME ZONE', '14:25:10'),
    ('INTERVAL', "'10 SECOND'", 'INTERVAL', '00:00:10'),
    ('BOOLEAN', "'TRUE'", 'BOOLEAN', True),
    ('XML', "'<a></a>'", 'XML', '<a></a>'),
]
@sdc_min_version('5.0.0')
@database('postgresqlaurora')
@pytest.mark.parametrize('data_type, insert_fragment, expected_type, expected_value', DATA_TYPES_AURORA,
                         ids=[i[0] for i in DATA_TYPES_AURORA])
def test_data_types(sdc_builder, sdc_executor, database, data_type, insert_fragment, expected_type, expected_value,
                    keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    origin.set_attributes(remove_replication_slot_on_close=True,
                          max_batch_size_in_records=1,
                          poll_interval=POLL_INTERVAL,
                          replication_slot=replication_slot_name
                          )

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                data_column {data_type} NULL
            )
        """)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 2

        record = records[0].field['change'][0]
        null_record = records[1].field['change'][0]
        index_of_data_type = record.get('columnnames').index('data_column')

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.

        assert record.get('columntypes')[index_of_data_type] == expected_type.lower()
        assert null_record.get('columntypes')[index_of_data_type] == expected_type.lower()

        assert record.get('columnvalues')[index_of_data_type].value == expected_value
        assert null_record.get('columnvalues')[index_of_data_type].value is None

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            database.deactivate_and_drop_replication_slot(replication_slot_name)
            connection.execute(f'DROP TABLE {table_name}')
        connection.close()


@sdc_min_version('5.0.0')
@database('postgresqlaurora')
@pytest.mark.parametrize('data_type, insert_fragment, expected_type, expected_value', DATA_TYPES_AURORA,
                         ids=[i[0] for i in DATA_TYPES_AURORA])
def test_data_types_as_primary_keys(sdc_builder, sdc_executor, database, data_type, insert_fragment, expected_type,
                                    expected_value, keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    # XML data type can't be a primary key
    if data_type == 'XML':
        pytest.skip('XML not supported as primary key.')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    origin.set_attributes(remove_replication_slot_on_close=True,
                          max_batch_size_in_records=1,
                          poll_interval=POLL_INTERVAL,
                          replication_slot=replication_slot_name
                          )

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    try:
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int,
                data_column {data_type} primary key
            )
        """)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1

        record = records[0].field['change'][0]
        index_of_data_type = record.get('columnnames').index('data_column')

        assert record.get('columntypes')[index_of_data_type] == expected_type.lower()
        assert record.get('columnvalues')[index_of_data_type].value == expected_value

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            database.deactivate_and_drop_replication_slot(replication_slot_name)
            connection.execute(f'DROP TABLE {table_name}')
        connection.close()


SERIAL_DATA_TYPES_AURORA = [
    ('SERIAL', 'DEFAULT', 'INTEGER', 3),
    ('BIGSERIAL', 'DEFAULT', 'BIGINT', 3),
    ('SMALLSERIAL', 'DEFAULT', 'SMALLINT', 3)
]
@sdc_min_version('5.0.0')
@database('postgresqlaurora')
@pytest.mark.parametrize('data_type, insert_fragment, expected_type, expected_value', SERIAL_DATA_TYPES_AURORA,
                         ids=[i[0] for i in SERIAL_DATA_TYPES_AURORA])
def test_data_types_as_primary_keys_serial_numeric(sdc_builder, sdc_executor, database, data_type, insert_fragment,
                                                   expected_type, expected_value, keep_data):
    raw_data = ['The Hobbit', 'Tolkien', 'The Community']
    table_name = get_random_string(string.ascii_lowercase, 20)
    sequence_name = get_random_string(string.ascii_lowercase, 5)
    connection = database.engine.connect()

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    origin.set_attributes(remove_replication_slot_on_close=True,
                          max_batch_size_in_records=1,
                          poll_interval=POLL_INTERVAL,
                          replication_slot=replication_slot_name
                          )

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id {data_type} primary key,
                name VARCHAR(20)
            )
        """)

        # Create sequence
        connection.execute(f"CREATE SEQUENCE {sequence_name} start 1 increment 1")

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # And insert a row with actual value
        data = ', '.join([f"(nextval(\'{sequence_name}\'), \'{d}\')" for d in raw_data])
        query = f"INSERT INTO {table_name} (id, name) VALUES {data};"
        connection.execute(query)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records[0].field['change']
        assert len(records) == expected_value

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        index_of_primary_key = records[0].get('columnnames').index('id')
        index_of_name = records[0].get('columnnames').index('name')

        expected_id = 1
        for i in range(len(records)):
            record = records[i]

            # Assert the type and value of the primary key column 'id'
            assert record.get('columntypes')[index_of_primary_key] == expected_type.lower()
            assert record.get('columnvalues')[index_of_primary_key].value == expected_id
            expected_id = expected_id + 1

            # Assert the value of the column 'name'
            assert record.get('columnvalues')[index_of_name] == raw_data[i]

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            database.deactivate_and_drop_replication_slot(replication_slot_name)
            connection.execute(f'DROP TABLE {table_name}')
        connection.close()


# Rule: In PostgreSQL, identifiers — table names, column names, etc. — are limited to a maximum length of 63 bytes
# (https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)
OBJECT_NAMES_POSTGRES = [
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 63), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 63)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "!@#$^&*()_+=-?<>", get_random_string(string.ascii_letters, 5) + "!@#$^&*()_+=-?<>"),  # '%' not supported
]
@sdc_min_version('5.0.0')
@database('postgresqlaurora')
@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES_POSTGRES,
                         ids=[i[0] for i in OBJECT_NAMES_POSTGRES])
def test_object_names(sdc_builder, sdc_executor, database, test_name, table_name, offset_name, keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    connection = database.engine.connect()
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    origin.set_attributes(remove_replication_slot_on_close=True,
                          max_batch_size_in_records=1,
                          poll_interval=POLL_INTERVAL,
                          replication_slot=replication_slot_name
                          )

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    # For make PostgreSQL Aurora case-sensitive, the objects names should be between double-quotes
    table_name_quotes = '"' + table_name + '"'
    offset_name_quotes = '"' + offset_name + '"'

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        connection.execute(f"""
            CREATE TABLE {table_name_quotes} ( 
                {offset_name_quotes} int primary key
            )
        """)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        logger.info('Adding data into %s database ...', database.type)
        connection.execute(f"INSERT INTO {table_name_quotes} VALUES(1)")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1

        record = records[0].field['change'][0]

        # SDC Will escape field names with certain characters, but not always...
        if "$" in offset_name:
            assert record.get('columnnames')[0] == f'{offset_name}'
        else:
            assert record.get('columnnames')[0] == offset_name
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            database.deactivate_and_drop_replication_slot(replication_slot_name)
            connection.execute(f'DROP TABLE {table_name_quotes}')
        connection.close()


@sdc_min_version('5.0.0')
@database('postgresqlaurora')
def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    connection = database.engine.connect()
    max_batch_size = 50
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

    origin = builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    origin.set_attributes(remove_replication_slot_on_close=True,
                          max_batch_size_in_records=max_batch_size,
                          poll_interval=POLL_INTERVAL,
                          replication_slot=replication_slot_name
                          )

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(connection.engine)

        sdc_executor.start_pipeline(pipeline)

        logger.info('Inserting data into %s', table_name)
        for n in range(1, max_batch_size * batches + 1):
            connection.execute(table.insert(), {'id': n})

        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', max_batch_size * batches + 1)
        sdc_executor.stop_pipeline(pipeline)

        records_changes = wiretap.output_records
        assert len(records_changes) == max_batch_size * batches
        records = [r.field['change'][0] for r in records_changes]

        # Get the id's from the records
        index_of_id = records[0].get('columnnames').index('id')
        id_records = [r.get('columnvalues')[index_of_id] for r in records]

        expected_number = 1
        for id_record in id_records:
            assert id_record == expected_number
            expected_number = expected_number + 1
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            database.deactivate_and_drop_replication_slot(replication_slot_name)
            connection.execute(f'DROP TABLE {table_name}')
        connection.close()


@sdc_min_version('5.0.0')
@database('postgresqlaurora')
def test_dataflow_events(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("No events supported in PostgreSQL CDC Aurora origin at this time.")


@sdc_min_version('5.0.0')
@database('postgresqlaurora')
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("PostgreSQL CDC Aurora Client doesn't deal with data formats")


@sdc_min_version('5.0.0')
@database('postgresqlaurora')
def test_resume_offset(sdc_builder, sdc_executor, database, keep_data):
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    iterations = 3
    records_per_iteration = 10
    connection = database.engine.connect()
    table_name = get_random_string(string.ascii_lowercase, 20)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                             schema=DEFAULT_SCHEMA_NAME)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    origin.set_attributes(remove_replication_slot_on_close=True,
                          max_batch_size_in_records=1,
                          poll_interval=POLL_INTERVAL,
                          replication_slot=replication_slot_name
                          )

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(connection.engine)

        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            sdc_executor.start_pipeline(pipeline)

            logger.info('Inserting data into %s', table_name)
            for n in range(iteration * records_per_iteration + 1,
                           iteration * records_per_iteration + 1 + records_per_iteration):
                connection.execute(table.insert(), {'id': n})

            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1

            for r in records:
                record = r.field['change'][0]
                assert record.get('columnvalues')[record.get('columnnames').index('id')].value == expected_number
                expected_number = expected_number + 1
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            database.deactivate_and_drop_replication_slot(replication_slot_name)
            connection.execute(f'DROP TABLE {table_name}')
        connection.close()
