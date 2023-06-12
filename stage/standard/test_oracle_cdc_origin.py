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
from datetime import datetime
from time import sleep

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

SHORT_WAIT_TIME = 0
LONG_WAIT_TIME = 2000
SESSION_WAIT_TIME_MIN_VERSION = "5.3.0"

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module', autouse=True)
def create_logminer_dictionary(database):
    """Fixture to ensure there will exist a LogMiner dictionary before running the Oracle CDC tests"""
    connection = database.engine.connect()
    query = ("  select thread#, "
             "         sequence# "
             "    from v$archived_log "
             "   where dictionary_end = 'YES' "
             "     and status = 'A' "
             "order by first_change# desc")
    result = connection.execute(query).first()
    if result is not None:
        thread, sequence = result
        query = (f"select * "
                 f"  from v$archived_log"
                 f" where thread# = {thread} "
                 f"   and sequence# = (select max(sequence#) "
                 f"                      from v$archived_log"
                 f"                     where thread# = {thread} "
                 f"                       and sequence# <= {sequence} "
                 f"                       and dictionary_begin = 'YES')")
        result = connection.execute(query).first()
        if result is not None:
            logger.info('LogMiner dictionary found. No needs to create one.')
            return

    logger.info('No LogMiner dictionary found. Creating one...')
    connection.execute('begin dbms_logmnr_d.build(options => dbms_logmnr_d.store_in_redo_logs); end;')
    logger.info('LogMiner dictionary ready.')


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
# https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1821
# We don't support UriType (requires difficult workaround in JDBC)
# We don't support timezone types
# We don't suppport float/double
# And general LOB things (clob, blob, long, nclob)
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', [
    ('number', '1', 'DECIMAL', '1'),
    ('char(2)', "'AB'", 'STRING', 'AB'),
    ('varchar(4)', "'ABCD'", 'STRING', 'ABCD'),
    ('varchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
    ('nchar(3)', "'NCH'", 'STRING', 'NCH'),
    ('nvarchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
    # ('binary_float', '1.0', 'FLOAT', '1.0'),
    # ('binary_double', '2.0', 'DOUBLE', '2.0'),
    ('date', "TO_DATE('1998-1-1 6:22:33', 'YYYY-MM-DD HH24:MI:SS')", 'DATETIME', 883635753000),
    ('timestamp', "TIMESTAMP'1998-1-2 6:00:00'", 'DATETIME', 883720800000),
    #  ('timestamp with time zone', "TIMESTAMP'1998-1-3 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-03T06:00:00-05:00'),
    ('timestamp with time zone', "null", 'ZONED_DATETIME', None),
    # ('timestamp with local time zone', "TIMESTAMP'1998-1-4 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-04T07:00:00Z'),
    ('timestamp with local time zone', "null", 'ZONED_DATETIME', None),
    #  ('long', "'LONG'", 'STRING', 'LONG'),
    ('blob', "utl_raw.cast_to_raw('BLOB')", 'BYTE_ARRAY', 'QkxPQg=='),
    ('clob', "'CLOB'", 'STRING', 'CLOB'),
    # ('nclob', "'NCLOB'", 'STRING', 'NCLOB'),
    # ('XMLType', "xmltype('<a></a>')", 'STRING', '<a></a>')
])
def test_data_types(sdc_builder, sdc_executor, database, buffer_location, sql_type, insert_fragment, expected_type, expected_value):
    """Test all feasible Oracle types in the CDC origin."""

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id number primary key,
                data_column {sql_type} NULL
            )
        """)

        builder = sdc_builder.get_pipeline_builder()
        origin = _get_oracle_cdc_client_origin(connection=connection,
                                               database=database,
                                               sdc_builder=sdc_builder,
                                               pipeline_builder=builder,
                                               buffer_locally=True,
                                               buffer_location=buffer_location,
                                               src_table_name=table_name,
                                               enable_blob_and_clob_columns_processing=True)
        wiretap = builder.add_wiretap()
        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # And insert a row with actual value
        txn = connection.begin()
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")
        txn.commit()
        _wait_until_time(_get_current_oracle_time(connection=connection))

        sdc_pipeline_cmd = sdc_executor.start_pipeline(pipeline)
        sdc_pipeline_cmd.wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline=pipeline)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['DATA_COLUMN'].type == expected_type
        assert null_record.field['DATA_COLUMN'].type == expected_type

        assert record.field['DATA_COLUMN']._data['value'] == expected_value
        assert null_record.field['DATA_COLUMN'] == None

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('table_name', [
    'TABLE',  # reserved words (must be quoted)
    '92TABLE',  # begin with numeric characters (must be quoted)
    'MY#$#TABLE_',  # allowed symbols (even for unquoted names)
    'myTABLE_upperANDlowerCaSEs',  # case sensitiveness (only when quoted)
    'TABLE_7D510B56T_219B_4DAB_AFAB',  # max length for a table name (30 bytes)
    'EVEN THIS & THAT!',  # allowed identifier when quoted
])
def test_object_names_tables(sdc_builder, sdc_executor, database, keep_data, buffer_location, table_name):
    """Test allowed names for Oracle tables"""
    
    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')
            
    # Reference: https://docs.oracle.com/database/121/SQLRF/sql_elements008.htm#SQLRF51129
    num_records = 10
    connection = database.engine.connect()

    try:
        logger.info('Creating table %s', table_name)
        connection.execute(f'CREATE TABLE "{table_name}" (ID NUMBER PRIMARY KEY)')
        initial_scn = _get_last_scn(connection)

        logger.info('Building pipeline')
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_name,
                                                   initial_change='SCN',
                                                   start_scn=initial_scn,
                                                   case_sensitive_names=True)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Inserting data into %s', table_name)
        input_values = list(range(num_records))
        for val in input_values:
            connection.execute(f'INSERT INTO "{table_name}" VALUES ({val})')

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(num_records)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = num_records
        wiretap_output_records = wiretap.output_records
        while len(wiretap_output_records) != wiretap_output_records_control_length and \
                wiretap_output_records_retries < wiretap_output_records_max_retries:
            wiretap_output_records_retries = wiretap_output_records_retries + 1
            logger.info(
                f'wiretap says it has {wiretap_output_records_control_length} records, but it actually has {len(wiretap_output_records)} records')
            logger.info(
                f'waiting {wiretap_output_records_max_wait} seconds ({wiretap_output_records_retries} out of {wiretap_output_records_max_retries} retry)')
            sleep(wiretap_output_records_max_wait)
            wiretap_output_records = wiretap.output_records

        assert len(wiretap_output_records) == wiretap_output_records_control_length

        output_values = [rec.field['ID'].value for rec in wiretap.output_records]

        assert len(output_values) == wiretap_output_records_control_length

        assert all(input_value in output_values for input_value in input_values)

    finally:
        try:
            sdc_executor.stop_pipeline(pipeline)
        except:
            pass

        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f'DROP TABLE "{table_name}"')

@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('column_name', [
    'VARCHAR2',  # reserved words (must be quoted)
    '92COLUMN',  # begin with numeric characters (must be quoted)
    'MY#$#COLUMN_',  # allowed symbols (even for unquoted names)
    'myCOLUMN_upperANDlowerCaSEs',  # case sensitiveness (only when quoted)
    'COLUMN_7D510B56_219B_4DAB_AFAB',  # max length for a table name (30 bytes)
    'EVEN THIS & THAT!',  # allowed identifier when quoted
])
def test_object_names_columns(sdc_builder, sdc_executor, database, keep_data, buffer_location, column_name):
    """Test allowed names for table columns"""
    
    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')
            
    # Reference: https://docs.oracle.com/database/121/SQLRF/sql_elements008.htm#SQLRF51129
    num_records = 10
    table_name = f'STF_{get_random_string(string.ascii_uppercase)}'
    connection = database.engine.connect()

    try:
        logger.info('Creating table %s', table_name)
        connection.execute(f'CREATE TABLE {table_name} ("{column_name}" NUMBER PRIMARY KEY)')
        initial_scn = _get_last_scn(connection)

        logger.info('Building pipeline')
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_name,
                                                   initial_change='SCN',
                                                   start_scn=initial_scn,
                                                   logminer_session_window='${2 * MINUTES}',
                                                   maximum_transaction_length='${1 * MINUTES}',
                                                   case_sensitive_names=True)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Inserting data into %s', table_name)
        input_values = list(range(num_records))
        for val in input_values:
            connection.execute(f'INSERT INTO {table_name} VALUES ({val})')

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(num_records)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = num_records
        wiretap_output_records = wiretap.output_records
        while len(wiretap_output_records) != wiretap_output_records_control_length and \
                wiretap_output_records_retries < wiretap_output_records_max_retries:
            wiretap_output_records_retries = wiretap_output_records_retries + 1
            logger.info(f'wiretap says it has {wiretap_output_records_control_length} records, but it actually has {len(wiretap_output_records)} records')
            logger.info(f'waiting {wiretap_output_records_max_wait} seconds ({wiretap_output_records_retries} out of {wiretap_output_records_max_retries} retry)')
            sleep(wiretap_output_records_max_wait)
            wiretap_output_records = wiretap.output_records

        assert len(wiretap_output_records) == wiretap_output_records_control_length

        sorted_records = sorted(wiretap.output_records,
                         key=lambda record: (record.header.values["oracle.cdc.scn"],
                                             record.header.values['oracle.cdc.sequence.internal']))

        assert len(sorted_records) == wiretap_output_records_control_length

        output_values = [record.field[column_name].value for record in sorted_records]

        assert len(output_values) == wiretap_output_records_control_length

        assert all(input_value in output_values for input_value in input_values)

    finally:
        try:
            sdc_executor.stop_pipeline(pipeline)
        except:
            pass

        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f'DROP TABLE {table_name}')


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data, buffer_location):
    
    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')
            
    max_batch_size = 1000
    batches = 10
    table_name = f'STF_{get_random_string(string.ascii_lowercase)}'
    connection = database.engine.connect()

    try:
        logger.info('Creating table %s', table_name)
        connection.execute(f'CREATE TABLE {table_name} (ID NUMBER PRIMARY KEY)')
        initial_scn = _get_last_scn(connection)

        logger.info('Building pipeline')
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_name,
                                                   initial_change='SCN',
                                                   start_scn=initial_scn,
                                                   dictionary_source='DICT_FROM_REDO_LOGS',
                                                   batch_size=max_batch_size)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination

        # Adding a finisher to stop the pipeline. We do so by monitoring the truncation of the table, which
        # will be forced after the insertions.
        finisher = builder.add_stage("Pipeline Finisher Executor")
        finisher.stage_record_preconditions = ["${record:eventType() == 'TRUNCATE'}"]
        oracle_cdc >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Inserting data into %s', table_name)
        input_values = list(range(max_batch_size * batches))

        transaction = connection.begin()
        for val in input_values:
            connection.execute(f"INSERT INTO {table_name} VALUES ({val})")
        transaction.commit()
        connection.execute(f'TRUNCATE TABLE {table_name}')

        sdc_executor.start_pipeline(pipeline).wait_for_finished(timeout_sec=420)

        output_values = [rec.field['ID'].value for rec in wiretap.output_records]

        output_values.sort(key=float)

        assert input_values == output_values

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_dataflow_events(sdc_builder, sdc_executor, database, buffer_location):
    """Test Oracle CDC event generation.

    We create two tables (sports and cities) and add additional columns. Then we truncate and drop the cities
    table and check all the events are correctly generated by the origin. Since snapshots require capturing
    some records, we use the sports table insertions as a means to indirectly capture all the events.

    NOTE: we drop the cities table *after* the pipeline startup as it is required to be tracked. Tables that
    are dropped before the pipeline initialization are not tracked by the Oracle CDC origin.

    Pipeline: oracle_cdc >> trash

    """
    
    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')
            
    table_prefix = f'STF_{get_random_string(string.ascii_uppercase)}'
    table_pattern = f'{table_prefix}%'
    cities_table = f'{table_prefix}_CITY'
    sports_table = f'{table_prefix}_SPORT'
    connection = database.engine.connect()

    # Data will be inserted as a mean to gather the event records generated before the data insertion.
    sports_data = [(1, 'Kelly Slater', 'Surf'),
                   (2, 'Steve Caballero', 'Skateboard'),
                   (3, 'Andre Botha', 'Bodyboard')]

    # Event info is: table name, event type, schema.
    expected_events = [(cities_table, 'STARTUP', {'ID': 'NUMERIC', 'CITY': 'VARCHAR',
                                                  'COUNTRY': 'VARCHAR', 'POPULATION': 'NUMERIC'}),
                       (sports_table, 'STARTUP', {'ID': 'NUMERIC', 'PLAYER': 'VARCHAR', 'SPORT': 'VARCHAR'}),
                       (cities_table, 'CREATE', {}),
                       (cities_table, 'ALTER', {}),
                       (sports_table, 'CREATE', {}),
                       (sports_table, 'ALTER', {}),
                       (sports_table, 'TRUNCATE', {}),
                       (cities_table, 'DROP', {})]

    try:
        start_scn = _get_last_scn(connection)
        logger.info('Initial SCN will be %s', start_scn)

        connection.execute(f'CREATE TABLE {cities_table} (ID NUMBER PRIMARY KEY, CITY VARCHAR2(50))')
        connection.execute(f'ALTER TABLE {cities_table} ADD (COUNTRY VARCHAR2(50), POPULATION NUMBER)')

        connection.execute(f'CREATE TABLE {sports_table} (ID NUMBER PRIMARY KEY, PLAYER VARCHAR2(50))')
        connection.execute(f'ALTER TABLE {sports_table} ADD (SPORT VARCHAR2(50))')

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   batch_size=6,
                                                   batch_wait_time_in_secs=5,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_pattern,
                                                   initial_change='SCN',
                                                   start_scn=start_scn,
                                                   dictionary_source='DICT_FROM_REDO_LOGS')
        trash = builder.add_stage('Trash')
        wiretap = builder.add_wiretap()
        oracle_cdc >> trash
        oracle_cdc >= wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline, drop cities table and insert some records into sports table to capture the snapshot
        # with all the events. We use the PURGE clause in the DROP statement to avoid sending the table to the
        # recycle bin, as it would create spurious ALTER events.
        sdc_pipeline_cmd = sdc_executor.start_pipeline(pipeline)

        for identifier, name, sport in sports_data:
            connection.execute(f"INSERT INTO {sports_table} VALUES({identifier}, '{name}', '{sport}')")

        connection.execute(f'TRUNCATE TABLE {sports_table}')
        connection.execute(f'DROP TABLE {cities_table} PURGE')

        for identifier, name, sport in sports_data:
            connection.execute(f"INSERT INTO {sports_table} VALUES({identifier}, '{name}', '{sport}')")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 6, timeout_sec=600)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = (len(2 * sports_data) + 2)
        wiretap_output_records = wiretap.output_records
        while len(wiretap_output_records) != wiretap_output_records_control_length and \
                wiretap_output_records_retries < wiretap_output_records_max_retries:
            wiretap_output_records_retries = wiretap_output_records_retries + 1
            logger.info(
                f'wiretap says it has {wiretap_output_records_control_length} records, but it actually has {len(wiretap_output_records)} records')
            logger.info(
                f'waiting {wiretap_output_records_max_wait} seconds ({wiretap_output_records_retries} out of {wiretap_output_records_max_retries} retry)')
            sleep(wiretap_output_records_max_wait)
            wiretap_output_records = wiretap.output_records

        sdc_events = [(event.header.values['oracle.cdc.table'],
                       event.header.values['sdc.event.type'],
                       event.field)
                      for event in wiretap_output_records]

        for record in wiretap_output_records:
            logger.info(f'Read record (data): {record}')

        for _event in sdc_events:
            logger.info(f'Read record (event): {_event}')

        assert len(wiretap_output_records) == wiretap_output_records_control_length

        assert len(sdc_events) == len(expected_events)

        for _event in sdc_events:
            assert _event in expected_events[0:len(expected_events)], f'Missing event: {_event}'

    finally:
        try:
            sdc_executor.stop_pipeline(pipeline)
        except:
            pass

        logger.info('Dropping table %s in %s database ...', sports_table, database.type)
        connection.execute(f'DROP TABLE {sports_table}')


@database('oracle')
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("Oracle CDC Origin doesn't deal with data formats")


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_resume_offset(sdc_builder, sdc_executor, database, keep_data, buffer_location):
    
    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')
            
    iterations = 3
    records_per_iteration = 10
    table_name = f'STF_{get_random_string(string.ascii_lowercase)}'
    connection = database.engine.connect()

    try:
        logger.info('Creating table %s', table_name)
        connection.execute(f'CREATE TABLE {table_name} (ID NUMBER PRIMARY KEY)')
        initial_scn = _get_last_scn(connection)

        logger.info('Building pipeline')
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_name,
                                                   initial_change='SCN',
                                                   start_scn=initial_scn,
                                                   logminer_session_window='${2 * MINUTES}',
                                                   maximum_transaction_length='${1 * MINUTES}')
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        for iteration in range(iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            logger.info('Inserting data into %s', table_name)
            input_values = list(range(iteration * records_per_iteration + 1,
                                      iteration * records_per_iteration + 1 + records_per_iteration))
            for val in input_values:
                connection.execute(f"INSERT INTO {table_name} VALUES ({val})")

            sdc_executor.start_pipeline(pipeline)
            sdc_executor.wait_for_pipeline_metric(
                pipeline, 'input_record_count', records_per_iteration, timeout_sec=3600)

            sdc_executor.stop_pipeline(pipeline, Force=False)

            sleep(30)

            wiretap_output_records_max_retries = 12
            wiretap_output_records_max_wait = 10
            wiretap_output_records_retries = 0
            wiretap_output_records_control_length = records_per_iteration
            wiretap_output_records = wiretap.output_records
            while len(wiretap_output_records) != wiretap_output_records_control_length and \
                    wiretap_output_records_retries < wiretap_output_records_max_retries:
                wiretap_output_records_retries = wiretap_output_records_retries + 1
                logger.info(
                    f'wiretap says it has {wiretap_output_records_control_length} records, but it actually has {len(wiretap_output_records)} records')
                logger.info(
                    f'waiting {wiretap_output_records_max_wait} seconds ({wiretap_output_records_retries} out of {wiretap_output_records_max_retries} retry)')
                sleep(wiretap_output_records_max_wait)
                wiretap_output_records = wiretap.output_records

            assert len(wiretap_output_records) == wiretap_output_records_control_length

            sorted_records = sorted(wiretap_output_records,
                                    key=lambda record: (record.header.values["oracle.cdc.scn"],
                                                        record.header.values['oracle.cdc.sequence.internal']))

            assert len(sorted_records) == wiretap_output_records_control_length

            output_values = [rec.field['ID'].value for rec in sorted_records]
            assert input_values == output_values

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


def _get_oracle_cdc_client_origin(connection,
                                  database,
                                  sdc_builder,
                                  pipeline_builder,
                                  buffer_locally,
                                  buffer_location,
                                  src_table_name=None,
                                  batch_size=10,
                                  **kwargs):
    kwargs.setdefault('dictionary_source', 'DICT_FROM_ONLINE_CATALOG')
    kwargs.setdefault('logminer_session_window', '${2 * MINUTES}')
    kwargs.setdefault('db_time_zone', 'UTC')
    kwargs.setdefault('maximum_transaction_length', '${1 * MINUTES}')
    kwargs.setdefault('initial_change', 'DATE')

    if Version(sdc_builder.version) >= Version(SESSION_WAIT_TIME_MIN_VERSION):
        kwargs.setdefault("time_after_session_window_start_in_ms", SHORT_WAIT_TIME)
        kwargs.setdefault("time_between_session_windows_in_ms", SHORT_WAIT_TIME)

    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.16.0'):
        # In versions < 3.16 the user has to define a maximum time to look back for a valid dictionary. From
        # 3.16 onward this is not required anymore. By default avoid to set an specific duration and use all
        # the redo logs instead.
        kwargs.setdefault('duration_of_directory_extraction', -1)

    if src_table_name is not None:
        if Version(sdc_builder.version) >= Version('3.1.0.0'):
            tables = [{'schema': database.username.upper(), 'table': src_table_name, 'excludePattern': ''}]
            kwargs.setdefault('tables', tables)
        else:
            kwargs.setdefault('schema_name', database.username.upper())
            kwargs.setdefault('tables', [src_table_name])
    elif 'tables' not in kwargs.keys():
        raise Exception("Either 'tables' or 'src_table_name' must be passed as argument.")

    start = _get_current_oracle_time(connection=connection)
    kwargs.setdefault('start_date', start.strftime('%d-%m-%Y %H:%M:%S'))

    # The time at the oracle db and the node executing the test may not have the exact same time.
    # So wait until this node reaches that time (including the timezone offset),
    # otherwise validation will fail because the origin thinks the
    # start time is in the future.
    _wait_until_time(time=start)
    logger.info('Start Date is %s', kwargs['start_date'])

    oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')

    return oracle_cdc_client.set_attributes(buffer_changes_locally=buffer_locally,
                                            buffer_location=buffer_location,
                                            max_batch_size_in_records=batch_size,
                                            **kwargs)


def _get_current_oracle_time(connection):
    """Queries current time in Oracle database or raises an Exception if anything wrong happened."""
    try:
        return connection.execute(sqlalchemy.sql.text('SELECT SYSDATE FROM DUAL')).fetchall()[0][0]
    except:
        raise Exception('Error retrieving SYSDATE from Oracle database.')


def _get_last_scn(connection):
    """Obtains last SCN from the database or raises an Exception if anything wrong happened."""
    try:
        return str(connection.execute('SELECT CURRENT_SCN FROM V$DATABASE').first()[0])
    except:
        raise Exception('Error retrieving last SCN from Oracle database.')


def _wait_until_time(time):
    current_time = datetime.utcnow()
    if current_time < time:
        sleep((time - current_time).total_seconds() + 1)
