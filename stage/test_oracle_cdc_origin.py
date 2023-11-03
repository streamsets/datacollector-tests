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
import pytest
import sqlalchemy
import string
import uuid

from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime, timedelta
from pytest import fixture
from random import randint
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError
from time import sleep

from streamsets.sdk.exceptions import StartError, RunningError, ValidationError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

from stage.utils.utils_oracle import FETCH_PARAMETERS, FEAT_VER_FETCH_STRATEGY


logger = logging.getLogger(__name__)

# SQL Parser processor was renamed in SDC-10697, so we need to reference it by name.
SQL_PARSER_STAGE_NAME = 'com_streamsets_pipeline_stage_processor_parser_sql_SqlParserDProcessor'

PRIMARY_KEY = 'ID'
OTHER_COLUMN = 'NAME'
BATCH_SIZE = 10  # Max limit imposed
Operations = namedtuple('Operations', ['rows', 'cdc_op_types', 'sdc_op_types', 'change_count'])
LAST_NON_MULTITENANT_ORACLE_VERSION = 11

SHORT_WAIT_TIME = 0
LONG_WAIT_TIME = 2000
SESSION_WAIT_TIME_MIN_VERSION = "5.3.0"
PEG_LOB_SUPPORT_VERSION = Version("5.5.0")

EMPTY_BLOB = b"EMPTY_BLOB()"
EMPTY_BLOB_STRING = "EMPTY_BLOB()"
EMPTY_CLOB = "EMPTY_CLOB()"


# pylint: disable=pointless-statement, too-many-locals


def set_session_wait_times(sdc_builder, oracle_cdc_client_stage, wait_time=SHORT_WAIT_TIME):
    if Version(sdc_builder.version) >= Version(SESSION_WAIT_TIME_MIN_VERSION):
        oracle_cdc_client_stage.set_attributes(
            time_after_session_window_start_in_ms=wait_time,
            time_between_session_windows_in_ms=wait_time,
        )


@pytest.fixture(scope='module', autouse=True)
def create_logminer_dictionary(database):
    """Fixture to ensure there will exist a LogMiner dictionary before running the Oracle CDC tests"""
    connection = database.engine.connect()
    query = ("SELECT THREAD#, SEQUENCE# FROM V$ARCHIVED_LOG WHERE DICTIONARY_END = 'YES' "
             "AND STATUS = 'A' ORDER BY FIRST_CHANGE# DESC")
    result = connection.execute(query).first()
    if result is not None:
        thread, seq = result
        query = (f"SELECT * FROM V$ARCHIVED_LOG WHERE THREAD# = {thread} AND SEQUENCE# = ("
                 f"SELECT MAX(SEQUENCE#) FROM V$ARCHIVED_LOG WHERE THREAD# = {thread} "
                 f"AND SEQUENCE# <= {seq} AND DICTIONARY_BEGIN = 'YES')")
        result = connection.execute(query).first()
        if result is not None:
            logger.info('LogMiner dictionary found. No needs to create one.')
            return

    logger.info('No LogMiner dictionary found. Creating one...')
    connection.execute('BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;')
    logger.info('LogMiner dictionary ready.')


@sdc_min_version('3.6.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_decimal_attributes(sdc_builder, sdc_executor, database, buffer_location):
    """Validates that Field attributes for decimal types will get properly generated
    Runs oracle_cdc_client >> wiretap.destination
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    pipeline = None
    table = None

    try:
        src_table_name = get_random_string(string.ascii_uppercase, 9)
        logger.info('Using table pattern %s', src_table_name)

        connection = database.engine.connect()
        table = sqlalchemy.Table(src_table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column(OTHER_COLUMN, sqlalchemy.Numeric(20, 2)))
        table.create(db_engine)
        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=True,
                                                          buffer_location=buffer_location,
                                                          src_table_name=src_table_name)
        wiretap = pipeline_builder.add_wiretap()

        lines = [
            f"INSERT INTO {src_table_name} VALUES (1, 10.2)",
        ]
        txn = connection.begin()
        for line in lines:
            transaction_text = text(line)
            connection.execute(transaction_text)
        txn.commit()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC: Decimal Attributes').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        # assert all the data captured have the same raw_data
        wiretap_output_records = wiretap.output_records
        assert len(wiretap_output_records) == 1
        attributes = wiretap_output_records[0].get_field_attributes(f'/{OTHER_COLUMN}')
        assert '20' == attributes['precision']
        assert '2' == attributes['scale']

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@sdc_min_version('3.9.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('parse_sql', [True, False])
def test_date_type_conversions(sdc_builder, sdc_executor, database, buffer_location, parse_sql):
    """Check that Oracle CDC Origin / SQL Parser Processor convert Oracle types DATE and TIMESTAMP to SDC DATETIME type.

    We create a table with a DATE and a TIMESTAMP columns and insert two rows by using different forms of TO_DATE and
    TO_TIMESTAMP invocations. Then check the corresponding values in the records generated by Oracle CDC Origin are
    DATETIME type and equal to those stored in the database.

    Pipeline: depending on the `parse_sql` value,
      True)  oracle_cdc_client >> wiretap.destination
      False) oracle_cdc_client >> sql_parser >> wiretap.destination

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    # Create table in database.
    table_name = 'STF_{}'.format(get_random_string(string.ascii_uppercase, 9))
    logger.info('Using table pattern %s', table_name)

    connection = database.engine.connect()
    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('COL_DATE', sqlalchemy.DATE),
                             sqlalchemy.Column('COL_TIMESTAMP', sqlalchemy.TIMESTAMP))
    table.create(database.engine)

    # Create pipeline.
    pipeline_builder = sdc_builder.get_pipeline_builder()
    oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                      database=database,
                                                      sdc_builder=sdc_builder,
                                                      pipeline_builder=pipeline_builder,
                                                      buffer_locally=True,
                                                      buffer_location=buffer_location,
                                                      src_table_name=table_name)
    oracle_cdc_client.parse_sql_query = parse_sql
    wiretap = pipeline_builder.add_wiretap()
    if parse_sql:
        oracle_cdc_client >> wiretap.destination
    else:
        sql_parser = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
        sql_parser.set_attributes(sql_field='/sql',
                                  target_field='/',
                                  resolve_schema_from_db=True,
                                  db_time_zone='UTC')
        oracle_cdc_client >> sql_parser >> wiretap.destination

    pipeline = pipeline_builder.build('Oracle CDC: Date conversion').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        ts = datetime.now()
        date_str = ts.strftime('%Y-%m-%d %H-%M-%S')
        timestamp_str = ts.strftime('%Y-%m-%d %H-%M-%S.%f')
        day_str = ts.strftime('%Y-%m-%d')

        # Insert rows into table. For the second row we will check that creating DATEs/TIMESTAMPs values without
        # specifying a date results also on DATETIME values in SDC.
        lines = [f"INSERT INTO {table_name} VALUES (1, TO_DATE('{date_str}', 'YYYY-MM-DD HH24:MI:SS'), "
                 f"TO_TIMESTAMP('{timestamp_str}', 'YYYY-MM-DD HH24:MI:SS.FF'))",
                 f"INSERT INTO {table_name} VALUES (2, TO_DATE('{day_str}', 'YYYY-MM-DD'), "
                 f"TO_TIMESTAMP('{day_str}', 'YYYY-MM-DD'))"]

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time).
        _wait_until_time(_get_current_oracle_time(connection=connection))

        sdc_executor.start_pipeline(pipeline)

        # All the data is inserted after the pipeline is running.
        txn = connection.begin()
        for line in lines:
            connection.execute(line)
        txn.commit()
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
        sdc_executor.stop_pipeline(pipeline=pipeline)

        # Assert all the data captured have the same raw_data.
        records = sorted(wiretap.output_records,
                         key=lambda rec: rec.field['ID'].value)

        assert len(records) == 2

        # Check first row.
        assert records[0].field['COL_DATE'].type == 'DATETIME'
        assert records[0].field['COL_DATE'].value == datetime.strptime(date_str, '%Y-%m-%d %H-%M-%S')
        # Oracle TIMESTAMP values have microseconds precision. SDC DATETIME fields have only milliseconds precision, but
        # use a 'nanoSeconds' attribute to extend the precision. We need to combine both values to compare with Oracle
        # TIMESTAMP.
        assert records[0].field['COL_TIMESTAMP'].type == 'DATETIME'
        us = timedelta(microseconds=int(records[0].get_field_attributes('COL_TIMESTAMP')['nanoSeconds']) // 1000)
        assert records[0].field['COL_TIMESTAMP'].value + us == datetime.strptime(timestamp_str, '%Y-%m-%d %H-%M-%S.%f')

        # Check second row.
        assert records[1].field['COL_DATE'].type == 'DATETIME'
        assert records[1].field['COL_DATE'].value == datetime.strptime(day_str, '%Y-%m-%d')
        assert records[1].field['COL_TIMESTAMP'].type == 'DATETIME'
        assert records[1].field['COL_TIMESTAMP'].value == datetime.strptime(day_str, '%Y-%m-%d')

    finally:
        logger.info('Dropping table %s....', table_name)
        table.drop(database.engine)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('use_pattern', [True, False])
def test_oracle_cdc_client_basic(
    sdc_builder,
    sdc_executor,
    database,
    buffer_locally,
    buffer_location,
    use_pattern,
    row_count=3,
    fetch_strategy=None,
    fetch_overflow=None,
):
    """Basic test that reads inserts/updates/deletes to an Oracle table,
    and validates that they are read in the same order.
    Runs oracle_cdc_client >> wiretap
    """
    assert row_count > 0, "row_count must be greater than 0"

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    pipeline = None
    table = None

    try:
        src_table_name = get_random_string(string.ascii_uppercase, 9)

        # If use_pattern is True, run the test if and only if sdc_builder >= 3.1.0.0
        if use_pattern:
            if Version(sdc_builder.version) >= Version('3.1.0.0'):
                src_table_pattern = _get_table_pattern(src_table_name)
            else:
                pytest.skip('Skipping test as SDC Builder version < 3.1.0.0')
        else:
            src_table_pattern = src_table_name

        connection = database.engine.connect()
        table = _setup_table(database=database,
                             table_name=src_table_name)

        logger.info('Using table pattern %s', src_table_pattern)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=buffer_locally,
                                                          buffer_location=buffer_location,
                                                          src_table_name=src_table_pattern)
        if Version(sdc_builder.version) >= FEAT_VER_FETCH_STRATEGY and fetch_strategy is not None:
            oracle_cdc_client.set_attributes(fetch_strategy=fetch_strategy)
            if fetch_strategy == "MEMORY_OVERFLOW_DISK" and fetch_overflow is not None:
                oracle_cdc_client.set_attributes(fetch_overflow=fetch_overflow)

        inserts = _insert(connection=connection, table=table, count=row_count)

        rows = inserts.rows
        cdc_op_types = inserts.cdc_op_types
        sdc_op_types = inserts.sdc_op_types
        change_count = inserts.change_count

        updates = _update(connection=connection, table=table, count=row_count)

        rows += updates.rows
        cdc_op_types += updates.cdc_op_types
        sdc_op_types += updates.sdc_op_types
        change_count += updates.change_count

        deletes = _delete(connection=connection, table=table, count=row_count)

        # deletes should have the last state of the row, so it would be the what comes from the updates.
        rows += updates.rows
        cdc_op_types += deletes.cdc_op_types
        sdc_op_types += deletes.sdc_op_types
        change_count += deletes.change_count

        logger.info('Expected number of records is %s.', change_count)

        wiretap = pipeline_builder.add_wiretap()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', change_count)

        sleep(10)  # If this test becomes flaky try increasing the sleep time, it used to be 30

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = change_count
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
                                             int(record.header.values["oracle.cdc.sequence.internal"])))

        assert len(sorted_records) == wiretap_output_records_control_length

        row_index = 0
        op_index = 0
        # assert all the data captured have the same raw_data
        for record in sorted_records:
            assert row_index == int(record.field['ID'].value)
            assert rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index]
            row_index = (row_index + 1) % row_count
            op_index += 1

        assert op_index == change_count

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('max_connection_lifetime', [True, False])
def test_oracle_cdc_client_bulk(
    sdc_builder, sdc_executor, database, buffer_locally, buffer_location, max_connection_lifetime, fetch_strategy=None, fetch_overflow=None
):
    """Test that reads inserts/updates/deletes to an Oracle table with bulk sentences,
    and verifies that no record is lost .
    Runs oracle_cdc_client >> wiretap
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    if Version(sdc_builder.version) < Version('5.8.0') and max_connection_lifetime:
        pytest.skip('Setting the max connection lifetime to 0 is not supported in this SDC version')

    source_table = None
    target_table = None

    pipeline = None

    try:

        database_connection = database.engine.connect()

        source_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        source_table.create(database.engine)
        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)
        number_of_rows = 100

        database_transaction = database_connection.begin()
        for id in range(0, number_of_rows):
            table_id = id
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            sentence = f'insert into {source_table} values ({table_id}, {table_name}, {table_surname}, {table_country}, {table_city})'
            sql = text(sentence)
            database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'insert into {target_table_name} select * from {source_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'update {target_table_name} set CITY = COUNTRY'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'delete from {target_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
        oracle_cdc_client.set_attributes(dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         tables=[{"schema": database.username.upper(), "table": target_table_name, "excludePattern": ""}],
                                         buffer_changes_locally=buffer_locally,
                                         buffer_location=buffer_location,
                                         logminer_session_window="${10 * MINUTES}",
                                         maximum_transaction_length="${2 * MINUTES}",
                                         db_time_zone="UTC",
                                         max_batch_size_in_records=1,
                                         initial_change="SCN",
                                         start_scn=database_last_scn)
        if Version(sdc_builder.version) >= FEAT_VER_FETCH_STRATEGY and fetch_strategy is not None:
            oracle_cdc_client.set_attributes(fetch_strategy=fetch_strategy)
            if fetch_strategy == "MEMORY_OVERFLOW_DISK" and fetch_overflow is not None:
                oracle_cdc_client.set_attributes(fetch_overflow=fetch_overflow)
        if Version(sdc_builder.version) >= Version('5.8.0') and max_connection_lifetime:
            oracle_cdc_client.set_attributes(max_connection_lifetime_in_seconds=0)

        set_session_wait_times(sdc_builder, oracle_cdc_client)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * number_of_rows)

        q_insert = sum(1 for record in wiretap.output_records if record.header.values["oracle.cdc.operation"] == 'INSERT')
        q_update = sum(1 for record in wiretap.output_records if record.header.values["oracle.cdc.operation"] == 'UPDATE')
        q_delete = sum(1 for record in wiretap.output_records if record.header.values["oracle.cdc.operation"] == 'DELETE')

        logger.info(f'Total INSERTs {q_insert}')
        logger.info(f'Total UPDATEs {q_update}')
        logger.info(f'Total DELETEs {q_delete}')

        assert q_insert == number_of_rows
        assert q_update == number_of_rows
        assert q_delete == number_of_rows

        for record in wiretap.output_records:
            logger.info(f'{record.header.values["oracle.cdc.operation"]} - {record.header.values["sdc.operation.type"]} - {record}')

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@sdc_min_version('3.22.3')
@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_oracle_cdc_client_headers(sdc_builder,
                                   sdc_executor,
                                   database,
                                   buffer_locally,
                                   buffer_location):
    """
    Test to check all headers are present in the output records.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    source_table = None
    target_table = None

    pipeline = None

    try:

        database_connection = database.engine.connect()

        source_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        source_table.create(database.engine)
        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)
        number_of_rows = 100

        database_transaction = database_connection.begin()
        for id in range(0, number_of_rows):
            table_id = id
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            sentence = f'insert into {source_table} values ({table_id}, {table_name}, {table_surname}, {table_country}, {table_city})'
            sql = text(sentence)
            database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'insert into {target_table_name} select * from {source_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'update {target_table_name} set CITY = COUNTRY'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'delete from {target_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
        oracle_cdc_client.set_attributes(dictionary_source='DICT_FROM_ONLINE_CATALOG',
                                         tables=[{'schema': database.username.upper(), 'table': target_table_name, 'excludePattern': ''}],
                                         buffer_changes_locally=buffer_locally,
                                         buffer_location=buffer_location,
                                         logminer_session_window='${10 * MINUTES}',
                                         maximum_transaction_length='${2 * MINUTES}',
                                         db_time_zone='UTC',
                                         max_batch_size_in_records=1,
                                         initial_change='SCN',
                                         start_scn=database_last_scn,
                                         send_redo_query_in_headers=True)
        set_session_wait_times(sdc_builder, oracle_cdc_client)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * number_of_rows)

        for record in wiretap.output_records:
            assert {record.header.values["oracle.cdc.scn"]} is not None
            assert {record.header.values["oracle.cdc.user"]} is not None
            assert {record.header.values["oracle.cdc.timestamp"]} is not None
            assert {record.header.values["oracle.cdc.table"]} is not None
            assert {record.header.values["SEQ"]} is not None
            assert {record.header.values["oracle.cdc.xid"]} is not None
            assert {record.header.values["oracle.cdc.RS_ID"]} is not None
            assert {record.header.values["oracle.cdc.SSN"]} is not None
            assert {record.header.values["schema"]} is not None
            assert {record.header.values["rollback"]} is not None
            assert {record.header.values["oracle.cdc.rowId"]} is not None
            assert {record.header.values["oracle.cdc.redoValue"]} is not None
            assert {record.header.values["oracle.cdc.undoValue"]} is not None
            assert {record.header.values["oracle.cdc.precisionTimestamp"]} is not None
            assert {record.header.values["oracle.cdc.query"]} is not None
            assert {record.header.values["oracle.cdc.sequence.oracle"]} is not None
            assert {record.header.values["oracle.cdc.sequence.internal"]} is not None

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@sdc_min_version('3.22.3')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_oracle_cdc_client_sequence(sdc_builder,
                                    sdc_executor,
                                    database,
                                    buffer_location):
    """
    Test that checks that the internal sequence is generated properly.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    source_table = None
    target_table = None

    pipeline = None

    try:

        database_connection = database.engine.connect()

        source_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        source_table.create(database.engine)
        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)
        number_of_rows = 100

        database_transaction = database_connection.begin()
        for id in range(0, number_of_rows):
            table_id = id
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            sentence = f'insert into {source_table} values ({table_id}, {table_name}, {table_surname}, {table_country}, {table_city})'
            sql = text(sentence)
            database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'insert into {target_table_name} select * from {source_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'update {target_table_name} set CITY = COUNTRY'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'delete from {target_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         tables=[{"schema": database.username.upper(), "table": target_table_name, "excludePattern": ""}],
                                         buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         logminer_session_window="${10 * MINUTES}",
                                         maximum_transaction_length="${2 * MINUTES}",
                                         db_time_zone="UTC",
                                         max_batch_size_in_records=1,
                                         initial_change="SCN",
                                         start_scn=database_last_scn)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * number_of_rows)

        sequence = 0
        operation_types = {'INSERT', 'UPDATE', 'DELETE'}
        operations_sequences = {}
        for operation_type in operation_types:
            operations_sequences[operation_type] = {}
        operation = ''
        for record in wiretap.output_records:
            record_operation = record.header.values["oracle.cdc.operation"]
            if record_operation != operation:
                operation = record_operation
                sequence = 0
            record_sequence = int(record.header.values["oracle.cdc.sequence.internal"])
            record_string_sequence = record.header.values["oracle.cdc.sequence.internal"]
            logger.info(f'{record_operation} - {record_sequence}')
            operations_sequences[record_operation][record_string_sequence] = True
            # assert record_sequence == sequence
            sequence = sequence + 1
        operation_types = {'INSERT', 'UPDATE', 'DELETE'}
        for operation_type in operation_types:
            for operation_sequence in range(100):
                operation_string_sequence = f'{operation_sequence}'
                assert operations_sequences[operation_type][operation_string_sequence] != None, \
                    f'{operation_type} - {operation_string_sequence} sequence not found and requiered'

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@sdc_min_version('3.22.3')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('case_sensitive', [True, False])
@pytest.mark.parametrize('pseudocolumns_in_header', [True, False])
def test_sql_parser_pseudocolumns(sdc_builder,
                                  sdc_executor,
                                  database,
                                  buffer_location,
                                  case_sensitive,
                                  pseudocolumns_in_header):
    """
    Check pseudocolumns processing.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    source_table = None
    target_table = None

    pipeline = None

    try:

        database_connection = database.engine.connect()

        source_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('Id', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Name', sqlalchemy.String(32)),
                                        sqlalchemy.Column('Surname', sqlalchemy.String(64)),
                                        sqlalchemy.Column('Country', sqlalchemy.String(2)),
                                        sqlalchemy.Column('City', sqlalchemy.String(3)))
        source_table.create(database.engine)
        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('Id', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Name', sqlalchemy.String(32)),
                                        sqlalchemy.Column('Surname', sqlalchemy.String(64)),
                                        sqlalchemy.Column('Country', sqlalchemy.String(2)),
                                        sqlalchemy.Column('City', sqlalchemy.String(3)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)
        number_of_rows = 1

        database_transaction = database_connection.begin()
        for id in range(0, number_of_rows):
            table_id = id
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            sentence = f'insert into {source_table} values ({table_id}, {table_name}, {table_surname}, {table_country}, {table_city})'
            sql = text(sentence)
            database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'insert into {target_table_name} select * from {source_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'update {target_table_name} set "City"  = "Country"'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'delete from {target_table_name}'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         tables=[{"schema": database.username.upper(), "table": target_table_name, "excludePattern": ""}],
                                         buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         logminer_session_window="${10 * MINUTES}",
                                         maximum_transaction_length="${2 * MINUTES}",
                                         db_time_zone="UTC",
                                         max_batch_size_in_records=1,
                                         initial_change="SCN",
                                         start_scn=database_last_scn,
                                         parse_sql_query=True,
                                         case_sensitive_names=case_sensitive,
                                        pseudocolumns_in_header=pseudocolumns_in_header)
        set_session_wait_times(sdc_builder, oracle_cdc_client)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * number_of_rows)

        for record in wiretap.output_records:
            record_operation = record.header.values["oracle.cdc.operation"]
            if record_operation == 'UPDATE':
                if case_sensitive:
                    assert ('Id' in record.field)
                    assert ('Name' in record.field)
                    assert ('Surname' in record.field)
                    assert ('Country' in record.field)
                    assert ('City' in record.field)
                    assert not ('ID' in record.field)
                    assert not ('NAME' in record.field)
                    assert not ('SURNAME' in record.field)
                    assert not ('COUNTRY' in record.field)
                    assert not ('CITY' in record.field)
                else:
                    assert ('ID' in record.field)
                    assert ('NAME' in record.field)
                    assert ('SURNAME' in record.field)
                    assert ('COUNTRY' in record.field)
                    assert ('CITY' in record.field)
                    assert not ('Id' in record.field)
                    assert not ('Name' in record.field)
                    assert not ('Surname' in record.field)
                    assert not ('Country' in record.field)
                    assert not ('City' in record.field)
                assert ('oracle.cdc.rowId' in record.header.values)
                if pseudocolumns_in_header:
                    assert ('oracle.cdc.oracle.pseudocolumn.ROWID' in record.header.values)
                else:
                    assert not ('oracle.cdc.oracle.pseudocolumn.ROWID' in record.header.values)

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@database('oracle')
def test_oracle_cdc_client_preview_and_run(sdc_builder, sdc_executor, database, buffer_locally, buffer_location):
    """Basic test that reads inserts first via preview and then run and preview again returning records
        Perform update/deletes and see run correctly picks up from where it left off and do a preview
        again to see preview still returns the inserts, updates and deletes all
        Runs oracle_cdc_client >> wiretap
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    table = None
    src_table_name = get_random_string(string.ascii_uppercase, 9)

    try:
        connection = database.engine.connect()
        table = _setup_table(database=database,
                             table_name=src_table_name)

        _dump_dictionary_to_log(connection)
        start_scn = _get_last_scn(connection)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=buffer_locally,
                                                          buffer_location=buffer_location,
                                                          logminer_session_window="${2 * MINUTES}",
                                                          maximum_transaction_length="${1 * MINUTES}",
                                                          src_table_name=src_table_name,
                                                          initial_change="SCN",
                                                          start_scn=start_scn,
                                                          batch_wait_time_in_secs=30)
        set_session_wait_times(sdc_builder, oracle_cdc_client)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        inserts = _insert(connection=connection, table=table)
        rows = inserts.rows
        cdc_op_types = inserts.cdc_op_types
        sdc_op_types = inserts.sdc_op_types
        change_count = inserts.change_count

        # Preview: 01
        preview = sdc_executor.run_pipeline_preview(pipeline,
                                                    batches=len(rows),
                                                    batch_size=1,
                                                    timeout=300000).preview
        assert preview is not None, 'Got no preview instance in preview mode 1'
        assert preview.issues.issues_count == 0, 'Unexpected issues in preview mode 1'
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches
                           if len(batch[oracle_cdc_client.instance_name].output) == 1]
        assert len(preview_records) == len(rows), 'Wrong number of records in preview mode 1'

        row_index = 0
        op_index = 0

        sorted_records = sorted(preview_records,
                         key=lambda record: (record.header.values["oracle.cdc.scn"],
                                             record.header.values['oracle.cdc.sequence.internal']))
        for record in sorted_records:
            logger.info(f'Preview record in round 1: {record} - '
                        f'{record.header.values["oracle.cdc.scn"]} - '
                        f'{record.header.values["oracle.cdc.sequence.internal"]}')
        for record in sorted_records:
            assert row_index == int(record.field['ID'].value), 'Wrong row index in preview mode 1'
            assert rows[op_index]['NAME'] == record.field['NAME'].value, 'Wrong NAME in preview mode 1'
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index], 'Wrong operation type in preview mode 1'
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index], 'Wrong operation in preview mode 1'
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == change_count, 'Unmatched number of changes in preview mode 1'

        wiretap.reset()

        # Run: 01
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(rows), timeout_sec=300)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = len(rows)
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

        row_index = 0
        op_index = 0

        sorted_records = sorted(wiretap.output_records,
                         key=lambda record: (record.header.values["oracle.cdc.scn"],
                                             record.header.values['oracle.cdc.sequence.internal']))

        assert len(sorted_records) == wiretap_output_records_control_length

        for record in sorted_records:
            logger.info(f'Run record in round 1: {record} - '
                        f'{record.header.values["oracle.cdc.scn"]} - '
                        f'{record.header.values["oracle.cdc.sequence.internal"]}')
        for record in sorted_records:
            assert row_index == int(record.field['ID'].value), 'Wrong ID in run mode 1'
            assert rows[op_index]['NAME'] == record.field['NAME'].value, 'Wrong NAME in run mode 1'
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index], 'Wrong operation type in run mode 1'
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index], 'Wrong operation in run mode 1'
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == change_count, 'Unmatched number of changes in run mode 1'

        sdc_executor.stop_pipeline(pipeline, force=False)
        wiretap.reset()

        # Preview: 02
        preview = sdc_executor.run_pipeline_preview(pipeline,
                                                    batches=len(rows),
                                                    batch_size=1,
                                                    timeout=300000).preview
        assert preview is not None, 'Got no preview instance in preview mode 2'
        assert preview.issues.issues_count == 0, 'Unexpected issues in preview mode 2'
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches]
        assert len(preview_records) == len(rows),'Wrong number of records in preview mode 2'

        row_index = 0
        op_index = 0

        sorted_records = sorted(preview_records,
                         key=lambda record: (record.header.values["oracle.cdc.scn"],
                                             record.header.values['oracle.cdc.sequence.internal']))
        for record in sorted_records:
            logger.info(f'Preview record in round 2: {record} - '
                        f'{record.header.values["oracle.cdc.scn"]} - '
                        f'{record.header.values["oracle.cdc.sequence.internal"]}')
        for record in sorted_records:
            assert row_index == int(record.field['ID'].value), 'Wrong ID in preview mode 2'
            assert rows[op_index]['NAME'] == record.field['NAME'].value, 'Wrong NAME in preview mode 2'
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index], 'Wrong operation type in preview mode 2'
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index], 'Wrong operation in preview mode 2'
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == change_count, 'Unmatched number of changes in preview mode 2'

        wiretap.reset()

        updates = _update(connection=connection, table=table)
        new_rows = updates.rows
        new_cdc_op_types = updates.cdc_op_types
        new_sdc_op_types = updates.sdc_op_types
        new_change_count = updates.change_count

        deletes = _delete(connection=connection, table=table)
        new_rows += updates.rows
        new_cdc_op_types += deletes.cdc_op_types
        new_sdc_op_types += deletes.sdc_op_types
        new_change_count += deletes.change_count

        merged_rows = rows + new_rows
        merged_sdc_op_types = sdc_op_types + new_sdc_op_types
        merged_cdc_op_types = cdc_op_types + new_cdc_op_types
        merged_change_count = change_count + new_change_count

        # Preview 03
        preview_command = sdc_executor.run_pipeline_preview(pipeline,
                                                            batches=len(merged_rows),
                                                            batch_size=1,
                                                            timeout=300000)
        preview = preview_command.preview
        assert preview is not None, 'Got no preview instance in preview mode 3'
        assert preview.issues.issues_count == 0, 'Unexpected issues in preview mode 3'
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches]
        assert len(preview_records) == len(merged_rows), 'Wrong number of records in preview mode 3'

        row_index = 0
        op_index = 0

        sorted_records = sorted(preview_records,
                         key=lambda record: (record.header.values["oracle.cdc.scn"],
                                             record.header.values['oracle.cdc.sequence.internal']))
        for record in sorted_records:
            logger.info(f'Preview record in round 3: {record} - '
                        f'{record.header.values["oracle.cdc.scn"]} - '
                        f'{record.header.values["oracle.cdc.sequence.internal"]}')
        for record in sorted_records:
            assert row_index == int(record.field['ID'].value), 'Wrong ID in preview mode 3'
            assert merged_rows[op_index]['NAME'] == record.field['NAME'].value, 'Wrong NAME in preview mode 3'
            assert int(record.header.values['sdc.operation.type']) == merged_sdc_op_types[op_index], 'Wrong operation type in preview mode 3'
            assert record.header.values['oracle.cdc.operation'] == merged_cdc_op_types[op_index], 'Wrong operation in preview mode 3'
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == merged_change_count, 'Unmatched number of changes in preview mode 3'

        wiretap.reset()

        # Run 02
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(new_rows), timeout_sec=300)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = len(new_rows)
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

        row_index = 0
        op_index = 0

        sorted_records = sorted(wiretap.output_records,
                         key=lambda record: (record.header.values["oracle.cdc.scn"],
                                             record.header.values['oracle.cdc.sequence.internal']))

        assert len(sorted_records) == wiretap_output_records_control_length

        for record in sorted_records:
            logger.info(f'Run record in round 2: {record} - '
                        f'{record.header.values["oracle.cdc.scn"]} - '
                        f'{record.header.values["oracle.cdc.sequence.internal"]}')
        for record in sorted_records:
            assert row_index == int(record.field['ID'].value), 'Wrong ID in run mode 2'
            assert new_rows[op_index]['NAME'] == record.field['NAME'].value, 'Wrong NAME in run mode 2'
            assert int(record.header.values['sdc.operation.type']) == new_sdc_op_types[op_index], 'Wrong operation type in run mode 2'
            assert record.header.values['oracle.cdc.operation'] == new_cdc_op_types[op_index], 'Wrong operation in run mode 2'
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == new_change_count, 'Unmatched number of changes in run mode 2'

        sdc_executor.stop_pipeline(pipeline, force=False)
        wiretap.reset()

        # Preview 04
        preview_command = sdc_executor.run_pipeline_preview(pipeline,
                                                            batches=len(merged_rows),
                                                            batch_size=1,
                                                            timeout=300000)
        preview = preview_command.preview
        wiretap.reset()

        assert preview is not None, 'Got no preview instance in preview mode 4'
        assert preview.issues.issues_count == 0, 'Unexpected issues in preview mode 4'
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches]
        assert len(preview_records) == len(merged_rows), 'Wrong number of records in preview mode 4'

        row_index = 0
        op_index = 0

        sorted_records = sorted(preview_records,
                         key=lambda record: (record.header.values["oracle.cdc.scn"],
                                             record.header.values['oracle.cdc.sequence.internal']))
        for record in sorted_records:
            logger.info(f'Preview record in round 4: {record} - '
                        f'{record.header.values["oracle.cdc.scn"]} - '
                        f'{record.header.values["oracle.cdc.sequence.internal"]}')
        for record in sorted_records:
            assert row_index == int(record.field['ID'].value), 'Wrong ID in preview mode 4'
            assert merged_rows[op_index]['NAME'] == record.field['NAME'].value, 'Wrong NAME in preview mode 4'
            assert int(record.header.values['sdc.operation.type']) == merged_sdc_op_types[op_index], 'Wrong operation type in preview mode 4'
            assert record.header.values['oracle.cdc.operation'] == merged_cdc_op_types[op_index], 'Wrong operation in preview mode 4'
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == merged_change_count, 'Unmatched number of changes in preview mode 4'

        wiretap.reset()

    finally:
        try:
            sdc_executor.stop_pipeline(pipeline, force=True)
        except:
            pass

        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@sdc_min_version('3.5.1')
@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('use_pattern', [False])
def test_oracle_cdc_client_stop_pipeline_when_no_archived_logs(sdc_builder,
                                                               sdc_executor,
                                                               database,
                                                               buffer_locally,
                                                               buffer_location,
                                                               use_pattern):
    """
    Test for SDC-8418.  Pipeline should stop with RUN ERROR when there is no archived log files.
    Runs oracle_cdc_client >> trash
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    src_table_name = get_random_string(string.ascii_uppercase, 9)

    try:
        connection = database.engine.connect()
        table = _setup_table(database=database, table_name=src_table_name)

        logger.info('Using table pattern: %s', src_table_name)
        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
        # Obviously past time so there is no archived redo logs for this.
        start_date = '30-09-2017 10:10:10'
        tables = [{'schema': database.username.upper(), 'table': src_table_name, 'excludePattern': ''}]

        oracle_cdc_client.set_attributes(buffer_changes_locally=buffer_locally,
                                         buffer_location=buffer_location,
                                         db_time_zone="UTC",
                                         dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         initial_change="DATE",
                                         logminer_session_window="${10 * MINUTES}",
                                         max_batch_size_in_records=BATCH_SIZE,
                                         maximum_transaction_length="${1 * MINUTES}",
                                         start_date=start_date,
                                         tables=tables)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        trash = pipeline_builder.add_stage('Trash')
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        pipeline.configuration["shouldRetry"] = False
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # 330 seconds is what takes one pipeline to fail when init goes fine but then everything fails
        # in a "normal" wal. (1 + 2) 3 tries for produce, having each one 6 tries (5 + 1) to start the
        # generator thread, having each one 6 (5 + 1) tries to start a LogMiner session. Please check
        # enum  OracleCDCConstants.RetryPolicy to check the current retry policies values.
        sleep_time = 330
        logger.info(f'Waiting {sleep_time} seconds before stopping the pipeline')
        sleep(sleep_time)

        # Pipeline should stop with StageExcception
        with pytest.raises(Exception):
            sdc_executor.stop_pipeline(pipeline)

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert 'RUN_ERROR' == status
    finally:
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('use_pattern', [True, False])
def test_oracle_cdc_client_string_null_values(sdc_builder, sdc_executor, database, buffer_locally, buffer_location, use_pattern):
    """Basic test that tests for SDC-8340. This test ensures that Strings with value 'NULL'/'null' is treated correctly,
    and null is not returned.
    Runs oracle_cdc_client >> wiretap
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    pipeline = None
    table = None

    try:
        src_table_name = get_random_string(string.ascii_uppercase, 9)

        # If use_pattern is True, run the test if and only if sdc_builder >= 3.1.0.0
        if use_pattern:
            if Version(sdc_builder.version) >= Version('3.1.0.0'):
                src_table_pattern = _get_table_pattern(src_table_name)
            else:
                pytest.skip('Skipping test as SDC Builder version < 3.1.0.0')
        else:
            src_table_pattern = src_table_name

        connection = database.engine.connect()
        table = _setup_table(database=database,
                             table_name=src_table_name,
                             create_primary_key=False)

        logger.info('Using table pattern %s', src_table_pattern)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=buffer_locally,
                                                          buffer_location=buffer_location,
                                                          src_table_name=src_table_pattern)
        set_session_wait_times(sdc_builder, oracle_cdc_client, LONG_WAIT_TIME)
        rows = [{'ID': 100, 'NAME': 'NULL'},
                {'ID': None, 'NAME': 'Whose Name?'},
                {'ID': 123, 'NAME': None},
                {'ID': None, 'NAME': None}]
        txn = connection.begin()

        connection.execute(table.insert(), rows)

        try:
            def update_table_where_id(tbl_row):
                connection.execute(table.update().where(table.c.ID == tbl_row['ID']).values(NAME=tbl_row['NAME']))

            # using ID is None causes an invalid SQL statement to be created since "is" is evaluated right away.
            row = {'ID': None, 'NAME': 'New Name'}
            update_table_where_id(row)
            # The above statement will update 2 rows, so the change generates 2 records.
            rows += [row for _ in range(0, 2)]

            row = {'ID': 100, 'NAME': None}
            update_table_where_id(row)
            rows.append(row)

            row = {'ID': 123, 'NAME': 'NULL'}
            update_table_where_id(row)
            rows.append(row)

            row = {'ID': None, 'NAME': 'New Name'}
            connection.execute(table.update().where(table.c.NAME == row['NAME']).values(ID=row['ID']))
            rows += [row for _ in range(0, 2)]

            txn.commit()
        except:
            txn.rollback()
            raise

        wiretap = pipeline_builder.add_wiretap()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(rows))

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = len(rows)
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

        # assert all the data captured have the same raw_data
        output = wiretap.output_records

        assert len(output) == wiretap_output_records_control_length

        for i, record in enumerate(output):
            # In update records, values with NULLs in the row are not returned
            if 'ID' in record.field:
                id_val = record.field['ID'].value
                assert rows[i]['ID'] == None if id_val is None else int(id_val)
            assert rows[i]['NAME'] == record.field['NAME']

        assert len(output) == len(rows)
    finally:
        if pipeline is not None:
            try:
                sdc_executor.stop_pipeline(pipeline=pipeline,
                                           force=True)
            except:
                pass

        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_long_sql_statements(sdc_builder, sdc_executor, database, buffer_location):
    """Test Oracle CDC correctly handles long SQL statements.

    When querying LogMiner content (i.e. V$LOGMNR_CONTENT view), any database operation whose SQL statement is
    longer than 4000 bytes is split into several records. In that case, Oracle CDC must join the partial SQL
    statements to build the corresponding SDC record.

    This test creates several SQL insertions longer than 4000 bytes and checks Oracle CDC correctly generates
    the corresponding records, with no data loss.

    Pipeline: oracle_cdc >> wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = f'STF_{get_random_string(string.ascii_uppercase)}'
    connection = database.engine.connect()

    # We will use VARCHAR values with 4000 bytes (the max size allowed for this data type). Thus the resulting
    # SQL insert statements will be longer than 4000 bytes.
    max_size = 4000  # max size in bytes for a VARCHAR
    input_data = [{'ID': i, 'NAME': get_random_string(string.ascii_lowercase, max_size)} for i in range(5)]

    try:
        start_scn = _get_last_scn(connection)
        # Create table and populate with long values.
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('ID', sqlalchemy.Integer),
                                 sqlalchemy.Column('NAME', sqlalchemy.types.VARCHAR(length=max_size)))
        table.create(database.engine)
        for rec in input_data:
            connection.execute(table.insert(rec))

        # Build and start the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_name,
                                                   batch_size=1,
                                                   initial_change='SCN',
                                                   start_scn=start_scn,
                                                   parse_sql_query=True)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(input_data))
        sdc_executor.stop_pipeline(pipeline=pipeline)

        # Check there is no data loss.
        sdc_records = [record.field for record in wiretap.output_records]
        assert set([tuple(el.items()) for el in sdc_records]) == set([tuple(el.items()) for el in input_data]), \
            f'Generated records and gathered records do not match: {input_data} @@@ {sdc_records}'

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_overlapping_transactions(sdc_builder, sdc_executor, database, buffer_locally, buffer_location):
    """Tests SDC-8359. The basic premise of the test:
    - Start a transaction, and insert some data
    - Wait for 1 second so timestamp of next transaction is different
    - Start a 2nd transaction, insert data and commit
    - Start pipeline
    - Get the output, make sure the 2nd txn's data is read
    - Stop pipeline
    - Commit transaction 1
    - Start pipeline, get the output
    - Must contain all data from transaction 1
    (Pre-8359, this would fail when buffer_locally=true with 2nd pipeline run timing out, since no data is read)
    Runs oracle_cdc_client >> wiretap
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    pipeline = None
    table = None

    try:
        src_table_name = get_random_string(string.ascii_uppercase, 9)

        connection = database.engine.connect()
        connection2 = database.engine.connect()
        table = _setup_table(database=database,
                             table_name=src_table_name,
                             create_primary_key=False)

        logger.info('Using table name %s', src_table_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=buffer_locally,
                                                          buffer_location=buffer_location,
                                                          src_table_name=src_table_name)

        # Start transaction
        long_txn = connection2.begin()

        # Insert data, don't commit
        rows_c2 = [{'ID': 100, 'NAME': 'TEST_LONG_TXN'} for _ in range(0, 10)]
        connection2.execute(table.insert(), rows_c2)

        # Ensure timestamp changes
        sleep(5)

        # Insert data into txn 2, and commit immediately
        rows_c1 = [{'ID': 200, 'NAME': 'TEST_SHORT_TXN'} for _ in range(0, 10)]
        connection.execute(table.insert(), rows_c1)

        wiretap = pipeline_builder.add_wiretap()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(rows_c1))
        sdc_executor.stop_pipeline(pipeline=pipeline)

        def compare_output(output_records, rows):
            assert len(output_records) == len(rows)
            for i, output_record in enumerate(output_records):
                assert output_record.field['ID'] == rows[i]['ID']
                assert output_record.field['NAME'] == rows[i]['NAME']

        # assert all the data captured have the same as rows_c1
        output = wiretap.output_records
        compare_output(output, rows_c1)

        # Now commit the older transaction, which has overlapped over the second one
        long_txn.commit()

        wiretap.reset()

        # Pre-3.1.0.0, this times out
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(rows_c2))
        sdc_executor.stop_pipeline(pipeline=pipeline)

        # assert all the data captured have the same raw_data
        output = wiretap.output_records
        compare_output(output, rows_c2)

    finally:
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('use_pattern', [True, False])
def test_oracle_cdc_to_jdbc_producer(sdc_builder, sdc_executor, database, buffer_locally, buffer_location, use_pattern):

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    pipeline = None
    src_table = None
    dest_table = None

    try:
        src_table_name = get_random_string(string.ascii_uppercase, 9)
        # If use_pattern is True, run the test if and only if sdc_builder >= 3.1.0.0
        if use_pattern:
            if Version(sdc_builder.version) >= Version('3.1.0.0'):
                src_table_pattern = _get_table_pattern(src_table_name)
            else:
                pytest.skip('Skipping test as SDC Builder version < 3.1.0.0')
        else:
            src_table_pattern = src_table_name

        connection = database.engine.connect()
        src_table = _setup_table(database, src_table_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        logger.info('Using table pattern %s', src_table_pattern)
        batch_size = 10

        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=buffer_locally,
                                                          buffer_location=buffer_location,
                                                          src_table_name=src_table_pattern,
                                                          batch_size=batch_size)

        dest_table_name = get_random_string(string.ascii_uppercase, 9)

        dest_table = _setup_table(database, dest_table_name)
        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(table_name=dest_table_name,
                                     default_operation='INSERT',
                                     # A framework bug creates a 1-element array, so remove the entry
                                     field_to_column_mapping=[])

        oracle_cdc_client >> jdbc_producer

        pipeline = pipeline_builder.build('Oracle CDC Client to JDBC Producer').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        inserts = _insert(connection=connection, table=src_table, count=batch_size).rows

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)

        assert [tuple(row.values()) for row in inserts] == _select_from_table(db_engine=db_engine, dest_table=dest_table)

        updates = _update(connection=connection, table=src_table, count=batch_size).rows
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 20)

        assert [tuple(row.values()) for row in updates] == _select_from_table(db_engine=db_engine, dest_table=dest_table)

        _delete(connection=connection, table=src_table, count=batch_size)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 30)

        assert len(_select_from_table(db_engine=db_engine, dest_table=dest_table)) == 0

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if src_table is not None:
            src_table.drop(db_engine)
        if dest_table is not None:
            dest_table.drop(db_engine)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('use_pattern', [True, False])
def test_rollback_to_savepoint(
    sdc_builder,
    sdc_executor,
    database,
    buffer_locally,
    buffer_location,
    use_pattern,
    fetch_strategy=None,
    fetch_overflow=None,
):
    """Test that writes some data, then creates a save point, writes some more data and then rolls back to savepoint,
    and validates that only the data that is before the save point and after the rollback is read
    Runs oracle_cdc_client >> wiretap
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    db_engine = database.engine
    pipeline = None
    table = None

    try:
        src_table_name = get_random_string(string.ascii_uppercase, 9)

        # If use_pattern is True, run the test if and only if sdc_builder >= 3.1.0.0
        if use_pattern:
            if Version(sdc_builder.version) >= Version('3.1.0.0'):
                src_table_pattern = _get_table_pattern(src_table_name)
            else:
                pytest.skip('Skipping test as SDC Builder version < 3.1.0.0')
        else:
            src_table_pattern = src_table_name

        connection = database.engine.connect()

        _dump_dictionary_to_log(connection)

        start_scn = _get_last_scn(connection)

        table = _setup_table(database=database,
                             table_name=src_table_name)

        logger.info('Using table pattern %s', src_table_pattern)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=buffer_locally,
                                                          buffer_location=buffer_location,
                                                          logminer_session_window='${2 * MINUTES}',
                                                          maximum_transaction_length='${1 * MINUTES}',
                                                          src_table_name=src_table_pattern,
                                                          initial_change='SCN',
                                                          start_scn=start_scn)
        if Version(sdc_builder.version) >= FEAT_VER_FETCH_STRATEGY and fetch_strategy is not None:
            oracle_cdc_client.set_attributes(fetch_strategy=fetch_strategy)
            if fetch_strategy == "MEMORY_OVERFLOW_DISK" and fetch_overflow is not None:
                oracle_cdc_client.set_attributes(fetch_overflow=fetch_overflow)

        wiretap = pipeline_builder.add_wiretap()
        lines = [
            f"INSERT INTO {src_table_name} VALUES (1, 'MORDOR')",
            f"INSERT INTO {src_table_name} VALUES (2, 'GONDOR')",
            f"UPDATE {src_table_name} SET {OTHER_COLUMN} = 'MINAS MORGUL' WHERE {PRIMARY_KEY} = 1",
            'SAVEPOINT stf_test_savepoint',
            f"INSERT INTO {src_table_name} VALUES(3, 'ROHAN')",
            f"UPDATE {src_table_name} SET {OTHER_COLUMN} = 'SHIRE' WHERE {PRIMARY_KEY} = 1",
            f"DELETE FROM {src_table_name} WHERE {PRIMARY_KEY} = 1",
            'ROLLBACK TO stf_test_savepoint',
            f"UPDATE {src_table_name} SET {OTHER_COLUMN} = 'HOBBITON' WHERE {PRIMARY_KEY} = 2",
            f"INSERT INTO {src_table_name} VALUES (3, 'GONDOR')",
            'COMMIT'
        ]
        txn = connection.begin()
        for line in lines:
            transaction_text = text(line)
            connection.execute(transaction_text)
        txn.commit()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 5)

        assert len(wiretap.output_records) == 5

        # assert all the data captured have the same raw_data
        output_records = sorted(wiretap.output_records,
                                key=lambda record: (record.header.values["oracle.cdc.scn"],
                                                    record.header.values['oracle.cdc.sequence.internal']))

        for record in wiretap.output_records:
            logger.info(f'Record :: '
                        f'{record.header.values["oracle.cdc.scn"]} - '                        
                        f'{record.header.values["oracle.cdc.sequence.internal"]} - '
                        f'{record.header.values["oracle.cdc.operation"]} - '
                        f'{record.header.values["sdc.operation.type"]} - '
                        f'{record.field[PRIMARY_KEY]}'
                        f'{record.field[OTHER_COLUMN]}'
                        f'{record}')

        assert len(output_records) == 5
        assert output_records[0].field[PRIMARY_KEY] == 1
        assert output_records[0].field[OTHER_COLUMN] == 'MORDOR'
        assert output_records[0].header.values['sdc.operation.type'] == '1'
        assert output_records[1].field[PRIMARY_KEY] == 2
        assert output_records[1].field[OTHER_COLUMN] == 'GONDOR'
        assert output_records[1].header.values['sdc.operation.type'] == '1'
        assert output_records[2].field[PRIMARY_KEY] == 1
        assert output_records[2].field[OTHER_COLUMN] == 'MINAS MORGUL'
        assert output_records[2].header.values['sdc.operation.type'] == '3'
        assert output_records[3].field[PRIMARY_KEY] == 2
        assert output_records[3].field[OTHER_COLUMN] == 'HOBBITON'
        assert output_records[3].header.values['sdc.operation.type'] == '3'
        assert output_records[4].field[PRIMARY_KEY] == 3
        assert output_records[4].field[OTHER_COLUMN] == 'GONDOR'
        assert output_records[4].header.values['sdc.operation.type'] == '1'

    finally:
        if pipeline is not None:
            try:
                sdc_executor.stop_pipeline(pipeline=pipeline,
                                           force=True)
            except:
                pass

        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@sdc_min_version('3.6.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('sql_type, insert_fragment, expected_values', [
    ('binary_float', '1.0', ['1.0E+000']),
    ('binary_double', '1.0', ['1.0E+000']),
    ('timestamp with time zone', "TIMESTAMP'1998-1-3 6:00:00-5:00'", ['1998-01-03T06:00:00-05:00']),
    ('timestamp with local time zone', "TIMESTAMP'1998-1-4 6:00:00-5:00'", ['1998-01-04T11:00:00Z[UTC]']),
    ('long', "'VALUE EXAMPLE'", ['VALUE EXAMPLE']),
    ('nclob', "'VALUE EXAMPLE'", ['EMPTY_CLOB()', 'VALUE EXAMPLE']),
    # ('XMLType', "xmltype('<a></a>')", ['<a></a>'])  # Not fully supported by LogMiner
])
def test_unsupported_types_send_to_pipeline(sdc_builder,
                                            sdc_executor,
                                            database,
                                            buffer_location,
                                            sql_type,
                                            insert_fragment,
                                            expected_values):
    """Test Oracle types not supported by the CDC origin when they are sent to pipeline.

    The test creates a table containing an unsupported type and check the record is properly generated with
    the unsuported type value returned as a raw string field, according to the configuration
    (unsupported_field_type = 'SEND_TO_PIPELINE', add_unsupported_fields_to_records = True).

    NOTE: parameter 'expected_values' is a list because for some data types Oracle generates two CDC records
    per row insertion (e.g. BLOB type). The first CDC record is a SQL insertion with a void value
    (e.g. EMPTY_BLOB()), the second one is a SQL update with the actual value (e.g. HEXTORAW('424c4f42')).

    NOTE: COLLECTOR-1215 added support for BLOB and CLOB as long as the "Enable BLOB and CLOB columns processing"
    configuration is enabled. This test keeps it disabled so the behaviour is the same as before adding support.

    More info about the supported Oracle data types:
    - https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm

    Pipeline: oracle_cdc >> wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    expected_output = [{'ID': 1, 'DATA_COLUMN': v} for v in expected_values]

    try:
        # Create table and populate with a record.
        connection.execute(f'CREATE TABLE {table_name} (ID NUMBER PRIMARY KEY, DATA_COLUMN {sql_type} NULL)')
        start_scn = _get_last_scn(connection)
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   initial_change='SCN',
                                                   start_scn=start_scn,
                                                   src_table_name=table_name,
                                                   unsupported_field_type='SEND_TO_PIPELINE',
                                                   add_unsupported_fields_to_records=True)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Capture the output and check the record is correctly generated.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(expected_output))
        sdc_executor.stop_pipeline(pipeline=pipeline)

        actual_output = [record.field
                         for record in wiretap.output_records]
        assert actual_output == expected_output

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.6.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('action', ['TO_ERROR', 'DISCARD'])
def test_unsupported_types_other_actions(sdc_builder, sdc_executor, database, buffer_location, action):
    """Test Oracle types not supported by the CDC origin when the action to take is other than SEND_TO_PIPELINE
    (which has a specific test).

    The test creates a table1 containing an unsupported type, and a table2 with only supported types; then
    inserts a record in both tables and check output and error streams. In particular, table1 record is
    expected to appear in the output stream and the table2 record only must appear in the error stream when
    'TO_ERROR' is the configured action.

    More info about the supported Oracle data types:
    - https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm

    Pipeline: oracle_cdc >> wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    connection = database.engine.connect()
    base_name = get_random_string(string.ascii_lowercase, 20)
    table_name1 = f'{base_name}_table1'
    table_name2 = f'{base_name}_table2'
    input_value1 = "'AAAAAAAABAAAA3VAAA'"
    input_value2 = 'just plain text'
    expected_output = [{'ID': 1, 'DATA_COLUMN': input_value2}]
    if action == 'TO_ERROR':
        expected_error_output = [{'ID': 1, 'DATA_COLUMN': "AAAAAAAABAAAA3VAAA"}]
    else:
        # When action is 'DISCARD' the stage ignores CDC records for tables with unsupported types.
        expected_error_output = []

    try:
        # Create tables and insert one record into each table.
        connection.execute(f'CREATE TABLE {table_name1} (ID NUMBER PRIMARY KEY, DATA_COLUMN ROWID NULL)')
        connection.execute(f'CREATE TABLE {table_name2} (ID NUMBER PRIMARY KEY, DATA_COLUMN VARCHAR(50) NULL)')
        start_scn = _get_last_scn(connection)
        connection.execute(f"INSERT INTO {table_name1} VALUES (1, {input_value1})")
        connection.execute(f"INSERT INTO {table_name2} VALUES (1, '{input_value2}')")

        # Build pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   initial_change='SCN',
                                                   start_scn=start_scn,
                                                   src_table_name=f'{base_name}%',
                                                   unsupported_field_type=action,
                                                   add_unsupported_fields_to_records=True)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Capture and check the output.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(expected_output))

        sdc_executor.stop_pipeline(pipeline=pipeline)

        actual_output = [record.field for record in wiretap.output_records]
        actual_error_output = [record.field for record in wiretap.error_records]
        assert actual_output == expected_output
        assert actual_error_output == expected_error_output

    finally:
        logger.info('Dropping tables %s and %s in %s database ...', table_name1, table_name2, database.type)
        connection.execute(f"DROP TABLE {table_name1}")
        connection.execute(f"DROP TABLE {table_name2}")


@sdc_min_version('3.19.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('peg_parser', [True, False])
def test_unsupported_types_adt(sdc_builder, sdc_executor, database, buffer_location, peg_parser):
    """ This tests OracleCDC origin when the table has an ADT column.
    This is behaving different according to Oracle version. In Oracle 11 the redo
    logs for tables that contain ADT columns are unsupported, that means the OracleCDC stage
    will ignore all redo logs from that table because the Log Miner will return a 255 operation code
    (Unsupported). On Oracle 19 this will work but throws an exception if we use the default parser. The exception
    is because the default parser does not support the alias for tables.
    The exception has been done on SDC-15822 and is checked on this test.
    In case the peg parser is used on Oracle 19 the redo logs are well processed by the OracleCDC origin
    and must produce an output record.

    The pipeline looks like that:
        OracleCDC >> Wiretap
        OracleCDC >= Pipeline Finisher (finishes on TRUNCATE event)

    For Oracle >= v12
        Using Peg Parser:
            Should works fine, produces n output records.
        Not using the Peg Parser:
            Should throw an exception that contains the JDBC-94 error.
    For Oracle 11
        Not supported by LogMiner so no output neither exception is expected, no matter if
        peg parser is enabled or not.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    connection = database.engine.connect()
    db_version = _get_oracle_db_version(connection)
    table_name = get_random_string(string.ascii_uppercase, 10)
    # We create a table that contains an ADT type column that has a collection type, in this case an VARRAY
    type_array_create = f'CREATE TYPE ARRAY_{table_name} AS VARRAY (1048576) of NUMBER'
    type_create = f'CREATE TYPE TPE_{table_name} AS OBJECT (EX_ARRAY ARRAY_{table_name}, EX_VARCHAR VARCHAR2(60 BYTE))'
    table_create = f'CREATE TABLE {table_name} (ID_EX NUMBER NOT NULL ENABLE, ADT_EX TPE_{table_name}, ' \
        f'NAME_EX VARCHAR2(60 BYTE), CONSTRAINT PK_{table_name}_EX PRIMARY KEY (ID_EX))'
    names_insert = ["INDURAIN","PANTANI","ULRICH"]
    num_records = len(names_insert);

    try:
        connection.execute(type_array_create)
        connection.execute(type_create)
        connection.execute(table_create)
        for i in range(num_records):
            connection.execute(f'INSERT INTO {table_name} (ID_EX, ADT_EX, NAME_EX) '
                               f"VALUES ({i},NULL,'{names_insert[i]}')")

        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   initial_change='LATEST',
                                                   src_table_name=f'{table_name}%',
                                                   unsupported_field_type='TO_ERROR',
                                                   add_unsupported_fields_to_records=True,
                                                   parse_sql_query=True,
                                                   use_peg_parser_in_beta=peg_parser,
                                                   on_record_error='STOP_PIPELINE',
                                                   dictionary_source='DICT_FROM_REDO_LOGS')

        wiretap = builder.add_wiretap()

        finisher = builder.add_stage("Pipeline Finisher Executor")
        finisher.on_record_error = 'DISCARD'
        finisher.stage_record_preconditions = ["${record:eventType() == 'TRUNCATE'}"]

        oracle_cdc >> wiretap.destination
        oracle_cdc >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        status = sdc_executor.start_pipeline(pipeline);
        connection.execute(f"UPDATE {table_name} SET name_ex = 'TONI'");

        if db_version[0] >= 12:
            # Version 12 LogMiner has support for Redo Logs that contain ADT columns
            if peg_parser:
                # With peg parser we expect n output records with the update we did on the table
                sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_records, timeout_sec=300)
                sdc_executor.stop_pipeline(pipeline)
                for i in range(num_records):
                    assert wiretap.output_records[i].field['ID_EX'] == i
                    assert wiretap.output_records[i].field['NAME_EX'] == 'TONI'
            else:
                # With default parser throws the exception SDC-15822"""
                with pytest.raises(RunningError) as exception_info:
                    status.wait_for_status('RUN_ERROR', timeout_sec=300)
                assert 'JDBC_405 - ' in f'{exception_info.value}'
                assert 'UPDATE' in f'{exception_info.value}'
        else:
            # Version 11 LogMiner does not support Redo Logs for ADT columns, we check we have no output records
            connection.execute(f"TRUNCATE TABLE {table_name}");
            status.wait_for_status('FINISHED', timeout_sec=300)
            assert len(wiretap.output_records) == 0

    finally:
        logger.info('Dropping types %s and %s and table %s ...',
                    f'ARRAY_{table_name}', f'TPE_{table_name}', f'TABLE {table_name}')
        connection.execute(f'DROP TABLE {table_name}')
        connection.execute(f'DROP TYPE TPE_{table_name}')
        connection.execute(f'DROP TYPE ARRAY_{table_name}')


@sdc_min_version('3.20.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('peg_parser', [True, False])
def test_unsupported_types_empty_redo_log(sdc_builder, sdc_executor, database, buffer_location, peg_parser):
    """ This tests OracleCDC origin when the table has an ADT column and an update
    is executed on the ADT column. The expected behaviour would be to have an unsupported
    operation type from the LOG MINER but instead of this we get an UPDATE operation and an empty
    SQL Redo log from the LOG MINER on ORACLE 12c. See more details on SDC-16136.
    We treat this as an unsupported column type and the user can choose how to manage this by
    using the unsupported_field_type config.

    In this test we will check if the implemented exception at SDC-16136 is thrown when we have an
    empty REDO LOG. We will check it for both parsers: the default one and the PEG parser.

    Oracle CDC >> Wiretap
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    connection = database.engine.connect()
    db_version = _get_oracle_db_version(connection)

    if db_version[0] != 12:
        pytest.skip('To run this test Oracle DB version must be 12.')

    table_name = get_random_string(string.ascii_uppercase, 10)
    # We create a table that contains an ADT type column that has a collection type, in this case an VARRAY
    type_array_create = f'CREATE TYPE ARRAY_{table_name} AS VARRAY (1048576) of NUMBER'
    type_create = f'CREATE TYPE TPE_{table_name} AS OBJECT (EX_ARRAY ARRAY_{table_name}, EX_VARCHAR VARCHAR2(60 BYTE))'
    table_create = f'CREATE TABLE {table_name} (ID_EX NUMBER NOT NULL ENABLE, ADT_EX TPE_{table_name}, ' \
        f'NAME_EX VARCHAR2(60 BYTE), CONSTRAINT PK_{table_name}_EX PRIMARY KEY (ID_EX))'
    names_insert = ["INDURAIN", "PANTANI", "ULRICH"]
    num_records = len(names_insert)

    try:
        connection.execute(type_array_create)
        connection.execute(type_create)
        connection.execute(table_create)
        for i in range(num_records):
            connection.execute(f'INSERT INTO {table_name} (ID_EX, ADT_EX, NAME_EX) '
                               f"VALUES ({i},NULL,'{names_insert[i]}')")

        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   initial_change='LATEST',
                                                   src_table_name=table_name,
                                                   unsupported_field_type='TO_ERROR',
                                                   add_unsupported_fields_to_records=True,
                                                   parse_sql_query=True,
                                                   send_redo_query_in_headers=True,
                                                   use_peg_parser_in_beta=peg_parser,
                                                   on_record_error='TO_ERROR',
                                                   dictionary_source='DICT_FROM_ONLINE_CATALOG')

        wiretap = builder.add_wiretap()

        oracle_cdc >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline);
        connection.execute(f"UPDATE {table_name} set ADT_EX = TPE_{table_name}("f"ARRAY_{table_name}(null),"
                           f"'EXAMPLE')")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_records, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)
        for i in range(num_records):
            assert wiretap.error_records[i].header['errorCode'] == 'JDBC_85'
            assert 'LogMiner returned empty SQL Redo statement' in wiretap.error_records[i].header['errorMessage']

    finally:
        logger.info('Dropping types %s and %s and table %s ...',
                    f'ARRAY_{table_name}', f'TPE_{table_name}', f'TABLE {table_name}')
        connection.execute(f'DROP TABLE {table_name}')
        connection.execute(f'DROP TYPE TPE_{table_name}')
        connection.execute(f'DROP TYPE ARRAY_{table_name}')


@sdc_min_version('3.21.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_empty_redo_log_record_is_ignored(sdc_builder, sdc_executor, database, buffer_location):
    """ This tests OracleCDC origin when the table has an ADT column and an update
    is executed on the ADT column and the unsupported types is set to 'Send to Pipeline'.
    This tests that when the empty redo log record is ignored on the pipeline.

    To test the empty redo log record is ignored we will first run the update on the ADT column
    and later we will run an update on another column of the same table. The first update should not
    create records but the last update should create the redo logs records. So after executing the last
    update we will check the output records and we will ensure we only have the records from the last update
    and not the ones from the first update. That will check the first update redo logs records have been ignored.

    Oracle CDC >> Wiretap
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    connection = database.engine.connect()
    db_version = _get_oracle_db_version(connection)

    if db_version[0] != 12:
        pytest.skip('To run this test Oracle DB version must be 12.')

    table_name = get_random_string(string.ascii_uppercase, 10)
    # We create a table that contains an ADT type column that has a collection type, in this case an VARRAY
    type_array_create = f'CREATE TYPE ARRAY_{table_name} AS VARRAY (1048576) of NUMBER'
    type_create = f'CREATE TYPE TPE_{table_name} AS OBJECT (EX_ARRAY ARRAY_{table_name}, EX_VARCHAR VARCHAR2(60 BYTE))'
    table_create = f'CREATE TABLE {table_name} (ID_EX NUMBER NOT NULL ENABLE, ADT_EX TPE_{table_name}, ' \
        f'NAME_EX VARCHAR2(60 BYTE), CONSTRAINT PK_{table_name}_EX PRIMARY KEY (ID_EX))'
    names_insert = ["INDURAIN", "PANTANI", "ULRICH"]
    num_records = len(names_insert)

    try:
        connection.execute(type_array_create)
        connection.execute(type_create)
        connection.execute(table_create)
        for i in range(num_records):
            connection.execute(f'INSERT INTO {table_name} (ID_EX, ADT_EX, NAME_EX) '
                               f"VALUES ({i},NULL,'{names_insert[i]}')")

        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   initial_change='LATEST',
                                                   src_table_name=table_name,
                                                   unsupported_field_type='SEND_TO_PIPELINE',
                                                   add_unsupported_fields_to_records=True,
                                                   parse_sql_query=True,
                                                   send_redo_query_in_headers=True,
                                                   use_peg_parser_in_beta=True,
                                                   on_record_error='TO_ERROR',
                                                   dictionary_source='DICT_FROM_ONLINE_CATALOG')

        wiretap = builder.add_wiretap()

        finisher = builder.add_stage("Pipeline Finisher Executor")
        finisher.on_record_error = 'DISCARD'
        finisher.stage_record_preconditions = ["${record:eventType() == 'TRUNCATE'}"]

        oracle_cdc >> wiretap.destination
        oracle_cdc >= finisher

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline);

        connection.execute(f"UPDATE {table_name} set ADT_EX = TPE_{table_name}("f"ARRAY_{table_name}(null),"
                           f"'EXAMPLE')")
        connection.execute(f"UPDATE {table_name} SET NAME_EX = 'TONI'");

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_records)

        sleep(10)

        assert len(wiretap.output_records) == num_records
        sdc_executor.stop_pipeline(pipeline)

        for i in range(num_records):
            assert wiretap.output_records[i].field['ID_EX'] == i
            assert wiretap.output_records[i].field['NAME_EX'] == 'TONI'

    finally:
        logger.info('Dropping types %s and %s and table %s ...',
                    f'ARRAY_{table_name}', f'TPE_{table_name}', f'TABLE {table_name}')
        connection.execute(f'DROP TABLE {table_name}')
        connection.execute(f'DROP TYPE TPE_{table_name}')
        connection.execute(f'DROP TYPE ARRAY_{table_name}')


@sdc_min_version('3.0.0.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_event_startup(sdc_builder, sdc_executor, database, buffer_location):
    """Verify that we create at least one event - STARTUP - in the origin. We don't check all types as that requires
       additional configuration on the shared database server (redo logs dictionary source).
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        connection.execute(f"CREATE TABLE {table_name}(id number primary key)")
        start_scn = _get_last_scn(connection)

        builder = sdc_builder.get_pipeline_builder()
        origin = _get_oracle_cdc_client_origin(connection=connection,
                                               database=database,
                                               sdc_builder=sdc_builder,
                                               pipeline_builder=builder,
                                               initial_change='SCN',
                                               start_scn=start_scn,
                                               buffer_locally=True,
                                               buffer_location=buffer_location,
                                               src_table_name=table_name)
        trash = builder.add_stage('Trash')
        identity = builder.add_stage('Dev Identity')
        event_wiretap = builder.add_wiretap()
        origin >> trash
        origin >= identity
        identity >> event_wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Insert one row
        txn = connection.begin()
        connection.execute(f"INSERT INTO {table_name} VALUES(1)")
        txn.commit()

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline=pipeline)

        assert len(event_wiretap.output_records) == 1
        assert event_wiretap.output_records[0].header.values['sdc.event.type'] == 'STARTUP'

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.1.0.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_oracle_cdc_exclusion_pattern(sdc_builder, sdc_executor, database, buffer_location):
    """Test Oracle CDC table exclusion patterns.

    The test configures an Oracle CDC stage to consume tables matching a defined name pattern, but excluding
    any table among them matching the exclusion name pattern (see `table_config` variable for this
    configuration). Then two tables are created and populated, one of them with a name that should be excluded
    according to the configuration. The Oracle CDC output (records and events) is examined to validate this
    behavior.

    Pipeline: oracle_cdc >> wiretap
              oracle_cdc >= wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_prefix = f'STF_{get_random_string(string.ascii_uppercase)}'
    sports_table1 = f'{table_prefix}_SPORT1'
    sports_table2 = f'{table_prefix}_SPORT2'
    table_config = dict(schema=database.username.upper(),
                        table=f'{table_prefix}%',
                        excludePattern='.*_SPORT1')
    connection = database.engine.connect()

    sports_data1 = [(1, 'Kelly Slater', 'Surf'),
                    (2, 'Steve Caballero', 'Skateboard'),
                    (3, 'Andre Botha', 'Bodyboard')]

    sports_data2 = [(4, 'Magnus Carlsen', 'Chess'),
                    (5, 'Xin Xu', 'Table tennis'),
                    (6, 'Michael van Gerwen', 'Darts')]

    # Event info is: table name, event type, schema.
    expected_events = [(sports_table2, 'STARTUP', {'ID': 'NUMERIC', 'PLAYER': 'VARCHAR', 'SPORT': 'VARCHAR'}),
                       (sports_table2, 'CREATE', {})]

    try:
        start_scn = _get_last_scn(connection)
        logger.info('Initial SCN will be %s', start_scn)

        # Create and populate the tables.
        connection.execute(f'CREATE TABLE {sports_table1} '
                           '(ID NUMBER PRIMARY KEY, PLAYER VARCHAR2(50), SPORT VARCHAR2(50))')
        connection.execute(f'CREATE TABLE {sports_table2} '
                           '(ID NUMBER PRIMARY KEY, PLAYER VARCHAR2(50), SPORT VARCHAR2(50))')

        for id, name, sport in sports_data1:
            connection.execute(f"INSERT INTO {sports_table1} VALUES({id}, '{name}', '{sport}')")
        for id, name, sport in sports_data2:
            connection.execute(f"INSERT INTO {sports_table2} VALUES({id}, '{name}', '{sport}')")

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   batch_size=1,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   dictionary_source='DICT_FROM_REDO_LOGS',
                                                   tables=[table_config],
                                                   initial_change='SCN',
                                                   start_scn=start_scn)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        wiretap_evts = builder.add_wiretap()
        oracle_cdc >= wiretap_evts.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and check only cities data is consumed by Oracle CDC origin.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(sports_data2), timeout_sec=380)
        sdc_executor.stop_pipeline(pipeline)

        records = sorted(wiretap.output_records,
                         key=lambda rec: rec.field['ID'].value)
        sdc_records = [(record.field['ID'], record.field['PLAYER'], record.field['SPORT'])
                       for record in records]

        assert sdc_records == sports_data2

        sdc_events = [(event.header.values['oracle.cdc.table'],
                       event.header.values['sdc.event.type'],
                       event.field)
                      for event in wiretap_evts.output_records]
        assert sdc_events == expected_events

    finally:
        logger.info('Dropping tables %s and %s ...', sports_table1, sports_table2)
        connection.execute(f'DROP TABLE {sports_table1}')
        connection.execute(f'DROP TABLE {sports_table2}')


@database('oracle')
@sdc_min_version('5.0.0')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('case_sensitive', [True, False])
@pytest.mark.parametrize('scenario', [{'pattern': '%',            'records':  5},
                                      {'pattern': '/%',           'records':  0},
                                      {'pattern': '_',            'records':  0},
                                      {'pattern': '/_',           'records':  0},
                                      {'pattern': 'ten%uki',      'records':  5},
                                      {'pattern': 'ten/%uki',     'records':  0},
                                      {'pattern': 'ten___uki',    'records': 10},
                                      {'pattern': 'ten/_/_/_uki', 'records':  0},
                                      {'pattern': 'ten_/__uki',   'records':  0},
                                      {'pattern': 'ten/__/_uki',  'records': 10},
                                      {'pattern': 'ten/_x/_uki',  'records': 10}])
def test_oracle_cdc_inclusion_pattern(sdc_builder, sdc_executor, database, buffer_location, case_sensitive, scenario):
    """Test Oracle CDC table inclusion patterns (considering escaped characters).
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    logger.info(f'Running test mode: {case_sensitive} - {scenario["pattern"]} - {scenario["records"]}')

    total_records = 10

    table_prefix = f'STF{get_random_string(string.ascii_lowercase, 16)}$'

    pattern = f'{table_prefix}{scenario["pattern"]}'
    records = scenario["records"]

    check_table_name = f'{table_prefix}tenuki'
    mined_table_name = f'{table_prefix}ten_x_uki'

    try:

        connection = database.engine.connect()

        start_scn = _get_last_scn(connection)
        logger.info(f'Initial SCN is {start_scn}')

        schema_db = database.username.upper()
        check_table_name_db = f'"{check_table_name}"' if case_sensitive else check_table_name.upper()
        mined_table_name_db = f'"{mined_table_name}"' if case_sensitive else mined_table_name.upper()

        logger.info(f'Creating temporary check table: {check_table_name_db}')
        connection.execute(f'create table {check_table_name_db} (id number, name varchar2(32))')

        logger.info(f'Creating temporary mined table: {mined_table_name_db}')
        connection.execute(f'create table {mined_table_name_db} (id number, name varchar2(32))')

        transaction = connection.begin()
        for i in range(0, total_records - records):
            connection.execute(f"insert into {check_table_name_db} values({i}, '{get_random_string(string.ascii_lowercase, 32)}')")

        for i in range(0, records):
            connection.execute(f"insert into {mined_table_name_db} values({i}, '{get_random_string(string.ascii_lowercase, 32)}')")
        transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         tables=[
                                             {"schema": schema_db, "table": check_table_name, "excludePattern": ""},
                                             {"schema": schema_db, "table": pattern, "excludePattern": ""},
                                         ],
                                         case_sensitive_names=case_sensitive,
                                         buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         logminer_session_window="${2 * MINUTES}",
                                         maximum_transaction_length="${1 * MINUTES}",
                                         db_time_zone="UTC",
                                         max_batch_size_in_records=total_records,
                                         initial_change="SCN",
                                         start_scn=start_scn)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Origin Offset Testing Pipeline').configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', total_records, timeout_sec=300)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = total_records
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

        check_table_name_token = check_table_name if case_sensitive else check_table_name.upper()
        mined_table_name_token = mined_table_name if case_sensitive else mined_table_name.upper()

        q_check = sum(1 for record in sorted_records if record.header.values["oracle.cdc.table"] == check_table_name_token)
        q_mined = sum(1 for record in sorted_records if record.header.values["oracle.cdc.table"] == mined_table_name_token)

        logger.info(f'Total for table {check_table_name}: {q_check}')
        logger.info(f'Total for table {mined_table_name}: {q_mined}')

        assert q_check == total_records - records
        assert q_mined == records

    finally:

        try:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        except:
            pass

        try:
            logger.info(f'Dropping temporary check table: {check_table_name_db}')
            connection.execute(f'drop table {check_table_name_db}')
        except:
            pass

        try:
            logger.info(f'Dropping temporary mined table: {mined_table_name_db}')
            connection.execute(f'drop table {mined_table_name_db}')
        except:
            pass


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_oracle_cdc_mining_new_table(sdc_builder, sdc_executor, database, buffer_location):
    """Test Oracle CDC can track new tables created after the pipeline initialization.

    Besides to validate the origin consumes the records inserted in the new table, the test also validates the
    corresponding DDL events are also created (CREATE and TRUNCATE events).

    Pipeline: oracle_cdc >> wiretap
              oracle_cdc >= wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_prefix = f'STF_{get_random_string(string.ascii_uppercase)}'
    table_pattern = f'{table_prefix}%'
    sports_table = f'{table_prefix}_SPORTS'
    connection = database.engine.connect()

    start_scn = _get_last_scn(connection)

    sports_data1 = [(1, 'Kelly Slater', 'Surf'),
                    (2, 'Steve Caballero', 'Skateboard'),
                    (3, 'Andre Botha', 'Bodyboard')]

    sports_data2 = [(4, 'Magnus Carlsen', 'Chess'),
                    (5, 'Xin Xu', 'Table tennis'),
                    (6, 'Michael van Gerwen', 'Darts')]

    # Event info is: table name, event type, schema.
    expected_events = [(sports_table, 'CREATE', {'ID': 'NUMERIC', 'PLAYER': 'VARCHAR', 'SPORT': 'VARCHAR'}),
                       (sports_table, 'TRUNCATE', {})]

    try:
        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   batch_size=1,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_pattern,
                                                   initial_change="SCN",
                                                   start_scn=start_scn,
                                                   dictionary_source="DICT_FROM_REDO_LOGS")
        set_session_wait_times(sdc_builder, oracle_cdc, LONG_WAIT_TIME)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        wiretap_evts = builder.add_wiretap()
        oracle_cdc >= wiretap_evts.destination
        pipeline = builder.build().configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        # Start pipeline, create table and populate
        sdc_executor.start_pipeline(pipeline)

        connection.execute(f'CREATE TABLE {sports_table} (ID NUMBER PRIMARY KEY, '
                           'PLAYER VARCHAR2(50), SPORT VARCHAR2(50))')
        for id, name, sport in sports_data1:
            connection.execute(f"INSERT INTO {sports_table} VALUES({id}, '{name}', '{sport}')")

        connection.execute(f'TRUNCATE TABLE {sports_table}')
        for id, name, sport in sports_data2:
            connection.execute(f"INSERT INTO {sports_table} VALUES({id}, '{name}', '{sport}')")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(sports_data1 + sports_data2),
                                              timeout_sec=380)
        sdc_executor.stop_pipeline(pipeline)

        sdc_events = [(event.header.values['oracle.cdc.table'],
                       event.header.values['sdc.event.type'],
                       event.field)
                      for event in wiretap_evts.output_records]
        assert sdc_events == expected_events

        records = sorted(wiretap.output_records,
                         key=lambda rec: rec.field['ID'].value)
        sdc_records = [(record.field['ID'], record.field['PLAYER'], record.field['SPORT'])
                       for record in records]

        assert sdc_records == sports_data1 + sports_data2

    finally:
        logger.info('Dropping table %s in %s database ...', sports_table, database.type)
        connection.execute(f'DROP TABLE {sports_table}')


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_oracle_cdc_ignores_dropped_table(sdc_builder, sdc_executor, database, buffer_location):
    """Test that tables dropped before being chached by Oracle CDC origin are correctly ingored.

    There are three cases regarding the tables potentially tracked by the stage:
    - The table have been created before pipeline initialization and is still present.
    - The table is created after the pipeline initialization.
    - The table have been created and removed before the pipeline initialization.

    Since the stage needs the table schema to create CDC records for a table, it cannot handle the last
    case. This test check that Oracle CDC correctly ignores removed tables and that no spurious events are
    generated for them. To do so, we create two tables, insert records and drop one of them; then check that
    only the remaining table generated CDC records and events.

    Pipeline: oracle_cdc >> wiretap
              oracle_cdc >= wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_prefix = f'STF_{get_random_string(string.ascii_uppercase)}'
    table_pattern = f'{table_prefix}%'
    sports_table = f'{table_prefix}_SPORTS'
    cities_table = f'{table_prefix}_CITY'
    connection = database.engine.connect()

    sports_data = [(1, 'Kelly Slater', 'Surf'),
                   (2, 'Steve Caballero', 'Skateboard'),
                   (3, 'Andre Botha', 'Bodyboard')]

    cities_data = [(1, 'Glasgow', 'Scotland'),
                   (2, 'Cork', 'Ireland'),
                   (3, 'Lisbon', 'Portugal')]

    # Event info is: table name, event type, schema.
    expected_events = [(sports_table, 'STARTUP', {'ID': 'NUMERIC', 'PLAYER': 'VARCHAR', 'SPORT': 'VARCHAR'}),
                       (sports_table, 'CREATE', {})]

    try:
        start_scn = _get_last_scn(connection)

        connection.execute(f'CREATE TABLE {cities_table} (ID NUMBER PRIMARY KEY, CITY VARCHAR2(50), '
                           'COUNTRY VARCHAR2(50))')
        for id, city, country in cities_data:
            connection.execute(f"INSERT INTO {cities_table} VALUES({id}, '{city}', '{country}')")
        connection.execute(f'DROP TABLE {cities_table}')

        connection.execute(f'CREATE TABLE {sports_table} (ID NUMBER PRIMARY KEY, '
                           'PLAYER VARCHAR2(50), SPORT VARCHAR2(50))')
        for id, name, sport in sports_data:
            connection.execute(f"INSERT INTO {sports_table} VALUES({id}, '{name}', '{sport}')")

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   batch_size=1,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_pattern,
                                                   initial_change='SCN',
                                                   start_scn=start_scn,
                                                   dictionary_source='DICT_FROM_REDO_LOGS')
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        wiretap_evts = builder.add_wiretap()
        oracle_cdc >= wiretap_evts.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline, create table and populate
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(sports_data), timeout_sec=380)
        sdc_executor.stop_pipeline(pipeline)

        sdc_events = [(event.header.values['oracle.cdc.table'],
                       event.header.values['sdc.event.type'],
                       event.field)
                      for event in wiretap_evts.output_records]
        assert sdc_events == expected_events

        records = sorted(wiretap.output_records,
                         key=lambda rec: rec.field['ID'].value)
        sdc_records = [(record.field['ID'], record.field['PLAYER'], record.field['SPORT'])
                       for record in records]

        assert sdc_records == sports_data

    finally:
        logger.info('Dropping table %s in %s database ...', sports_table, database.type)
        connection.execute(f'DROP TABLE {sports_table}')


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('initial_change', ['DATE', 'SCN', 'LATEST'])
def test_initial_change(sdc_builder, sdc_executor, database, buffer_location, initial_change):
    """Test Initial Change config.

    The test performs 3 database transactions T1, T2 and T3, and the pipeline is started at a time in between
    transactions T2 and T3. We expect the following outcome:
    - When initial_change is 'DATE' or 'SCN', the test configures the Oracle CDC stage to read from
      transaction T2, and consequently checks that the consumed data correspond to T2+T3.
    - When initial_change is 'LATEST', the expected outcome is to read only data from T3.

    Pipeline: oracle_cdc >> wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = f'STF_{get_random_string(string.ascii_lowercase)}'
    connection = database.engine.connect()
    test_options = {'initial_change': initial_change}
    txn1_data = [1, 2]
    txn2_data = [3, 4, 5]
    txn3_data = [6, 7]
    expected_data = txn3_data if initial_change == 'LATEST' else txn2_data + txn3_data

    try:
        # Create table and commit the first transaction.
        connection.execute(f'CREATE TABLE {table_name} (id number primary key)')
        txn1 = connection.begin()
        for i in txn1_data:
            connection.execute(f'INSERT INTO {table_name} VALUES({i})')
        txn1.commit()
        sleep(2)  # Seems to be necessary to avoid race conditions between commiting the first transaction and
                  # retrieving the start date/scn.

        # For initial_change 'DATE' or 'SCN', we configure as 'initial change' a point in between the first
        # and second transaction. That means that changes in the first transaction should be ignored by Oracle
        # CDC stage.
        if initial_change == 'DATE':
            current_time = _get_current_oracle_time(connection=connection)
            test_options['start_date'] = current_time.strftime('%d-%m-%Y %H:%M:%S')
        elif initial_change == 'SCN':
            test_options['start_scn'] = _get_last_scn(connection)

        # Commit a second transaction.
        txn2 = connection.begin()
        for i in txn2_data:
            connection.execute(f'INSERT INTO {table_name} VALUES({i})')
        txn2.commit()
        sleep(2)

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_name,
                                                   batch_size=1,
                                                   **test_options)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and commit a third transaction after that.
        status = sdc_executor.start_pipeline(pipeline)
        txn3 = connection.begin()
        for i in txn3_data:
            connection.execute(f'INSERT INTO {table_name} VALUES({i})')
        txn3.commit()

        # Check the data consumed by the pipeline is the expected one.
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(expected_data),
                                              timeout_sec=380)
        consumed_data = [rec.field['ID'].value for rec in wiretap.output_records]
        sdc_executor.stop_pipeline(pipeline=pipeline)
        assert sorted(consumed_data) == expected_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


@sdc_min_version('3.14.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_dictionary_extraction(sdc_builder, sdc_executor, database, buffer_location):
    """Test extraction of the LogMiner dictionary from the existing redo logs.

    This test configures an Oracle CDC stage with 'DICT_FROM_REDO_LOGS' as dictionary source, builds the
    dictionary, and check that the Oracle CDC is able to use the fresh dictionary to recover the data inserted
    in the database before the pipeline is started.

    Pipeline: oracle_cdc >> wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = f'STF_{get_random_string(string.ascii_lowercase)}'
    connection = database.engine.connect()
    input_data = [1, 2, 3, 4, 5]

    try:
        # Create table and dictionary
        connection.execute(f'CREATE TABLE {table_name} (ID NUMBER PRIMARY KEY)')
        logger.info('Creating LogMiner dictionary...')
        connection.execute('BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;')
        logger.info('LogMiner dictionary ready.')

        # Populate table with data
        start_scn = _get_last_scn(connection)  # It will be the starting point to mine redo logs.
        txn = connection.begin()
        for i in input_data:
            connection.execute(f'INSERT INTO {table_name} VALUES({i})')
        txn.commit()

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_params = dict(connection=connection,
                                 database=database,
                                 sdc_builder=sdc_builder,
                                 pipeline_builder=builder,
                                 batch_size=1,
                                 buffer_locally=True,
                                 buffer_location=buffer_location,
                                 src_table_name=table_name,
                                 initial_change='SCN',
                                 start_scn=start_scn,
                                 dictionary_source='DICT_FROM_REDO_LOGS')

        if Version(sdc_builder.version) < Version('3.16.0'):
            # In versions < 3.16 the user has to define a maximum time to look back for a valid
            # dictionary. From 3.16 onward this is not required anymore.
            oracle_cdc_params['duration_of_directory_extraction'] = '${2 * MINUTES}'

        oracle_cdc = _get_oracle_cdc_client_origin(**oracle_cdc_params)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and check the data consumed by the pipeline is the expected one.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(input_data), timeout_sec=380)
        sdc_executor.stop_pipeline(pipeline)
        consumed_data = [rec.field['ID'].value for rec in wiretap.output_records]
        assert sorted(consumed_data) == input_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('dictionary_source', ['DICT_FROM_REDO_LOGS', 'DICT_FROM_ONLINE_CATALOG'])
def test_logminer_session_switch(sdc_builder, sdc_executor, database, buffer_location, dictionary_source):
    """Test LogMiner session switches.

    In particular we want to check the stage is able to open subsequent LogMiner session windows after the
    initial one, especially when using DICT_FROM_ONLINE_CATALOG, which involves keeping the LogMiner
    dictionary between sessions.

    To ensure Oracle CDC actually switches between LogMiner sessions, we perform two database transactions
    which are separated in time by more than the configured LogMiner session window, and configure Oracle CDC
    to consume both transactions.

    Pipeline: oracle_cdc >> wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = f'STF_{get_random_string(string.ascii_uppercase)}'
    connection = database.engine.connect()

    if dictionary_source == 'DICT_FROM_REDO_LOGS':
        _dump_dictionary_to_log(connection)

    txn1_data = [1, 2, 3, 4, 5]
    txn2_data = [11, 12, 13, 14, 15]
    session_window_secs = 10

    try:
        # Create and populate table. Use 2 transactions that will be separated in time by more than the
        # configured LogMiner session window. This will force they must be consumed in different LogMiner
        # sessions.
        logger.info('Create and populate table %s...', table_name)
        connection.execute(f'CREATE TABLE {table_name} (ID NUMBER PRIMARY KEY)')
        start_date = _get_current_oracle_time(connection=connection).strftime('%d-%m-%Y %H:%M:%S')

        txn1 = connection.begin()
        for i in txn1_data:
            connection.execute(f'INSERT INTO {table_name} VALUES({i})')
        txn1.commit()
        sleep(session_window_secs * 2)

        txn2 = connection.begin()
        for i in txn2_data:
            connection.execute(f'INSERT INTO {table_name} VALUES({i})')
        txn2.commit()

        # Build the pipeline.
        builder = sdc_builder.get_pipeline_builder()
        oracle_cdc = _get_oracle_cdc_client_origin(connection=connection,
                                                   database=database,
                                                   sdc_builder=sdc_builder,
                                                   pipeline_builder=builder,
                                                   batch_size=1,
                                                   buffer_locally=True,
                                                   buffer_location=buffer_location,
                                                   src_table_name=table_name,
                                                   initial_change='DATE',
                                                   start_date=start_date,
                                                   dictionary_source=dictionary_source,
                                                   maximum_transaction_length=session_window_secs - 1,
                                                   logminer_session_window=session_window_secs)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline, populate table with data 1 and wait for the pipeline to consume it.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(txn1_data+txn2_data), timeout_sec=380)
        sdc_executor.stop_pipeline(pipeline)
        consumed_data = [record.field['ID'].value for record in wiretap.output_records]
        assert sorted(consumed_data) == txn1_data + txn2_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


@sdc_min_version('3.19.0')
@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('dictionary_source', ['DICT_FROM_REDO_LOGS', 'DICT_FROM_ONLINE_CATALOG'])
def test_disable_continuous_mine(sdc_builder,
                                 sdc_executor,
                                 database,
                                 keep_data,
                                 buffer_locally,
                                 buffer_location,
                                 dictionary_source):
    """Simple test to check "Disable Continuous Mine" stage option.

    The test just inserts a few records in the test table and check they are successfully consumed by Oracle
    CDC. This is tested with different configurations which affect how LogMiner sessions are set.

    Pipeline: oracle_cdc >> wiretap

    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table_name = f'STF_{get_random_string(string.ascii_uppercase)}'
    num_records = 10
    connection = database.engine.connect()

    _dump_dictionary_to_log(connection)

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
                                                   buffer_locally=buffer_locally,
                                                   buffer_location=buffer_location,
                                                   logminer_session_window='${2 * MINUTES}',
                                                   maximum_transaction_length='${1 * MINUTES}',
                                                   src_table_name=table_name,
                                                   initial_change='SCN',
                                                   start_scn=initial_scn,
                                                   dictionary_source=dictionary_source,
                                                   disable_continuous_mine=True)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        logger.info('Inserting data into %s', table_name)
        input_values = list(range(num_records))
        for val in input_values:
            connection.execute(f'INSERT INTO {table_name} VALUES ({val})')

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_records, timeout_sec=360)
        sdc_executor.stop_pipeline(pipeline)

        records = sorted(wiretap.output_records,
                         key=lambda rec: rec.field['ID'].value)
        output_values = [record.field['ID'].value for record in records]

        assert input_values == output_values

    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f'DROP TABLE {table_name}')


def _setup_table(database, table_name, create_primary_key=True):
    db_engine = database.engine
    logger.info('Creating source table %s in %s database ...', table_name, database.type)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=create_primary_key),
                             sqlalchemy.Column(OTHER_COLUMN, sqlalchemy.String(20)))
    table.create(db_engine)
    return table


@sdc_min_version('4.2.0')
@database('oracle')
@pytest.mark.parametrize('permission_to_test', ['select_on_views', 'select_on_table', 'role_execute_catalog',
                                                'privilege_alter_session', 'privilege_select_any_transaction',
                                                'privilege_select_any_table', 'privilege_logmining',
                                                'privilege_set_container'])
@pytest.mark.parametrize('has_permission', [True, False])
def test_user_configuration_checks(sdc_builder,
                                   sdc_executor,
                                   database,
                                   permission_to_test,
                                   has_permission):
    """Simple test to check the user configuration permissions.

    The test creates a user guest/guest that lacks the input permission if has_permission is False. If the user does
    not have enough privileges for the current database version the pipeline outputs an error when starting. We compare
    the produced error with the expected one. The test finishes removing the guest user to reset the database state.

    Pipeline: oracle_cdc >> wiretap

    """

    pytest.skip(f"Skipping test as current implementation is not 100% reliable and has been disabled")

    engine = database.engine
    connection = engine.connect()

    guest_username = f"C##_GUEST_{get_random_string(string.ascii_uppercase, 16)}"
    guest_password = f"C##_guest_{get_random_string(string.ascii_uppercase, 16)}"
    src_table_name = get_random_string(string.ascii_uppercase, 9)

    # Skip tests not relevant for the current Oracle DB version
    db_version = _get_oracle_db_version(connection)[0]
    logger.info(f"Oracle DB version is {db_version}")
    db_multitenant = _is_db_multitenant(connection)
    logger.info(f"Oracle DB is multitenant") if db_multitenant else logger.info(f"Oracle DB is not multitenant")
    if permission_to_test == 'privilege_select_any_transaction' and db_version > LAST_NON_MULTITENANT_ORACLE_VERSION:
        pytest.skip(f"Skipping test as Oracle DB version > {LAST_NON_MULTITENANT_ORACLE_VERSION}")
    if permission_to_test == 'privilege_select_any_table' and db_version > LAST_NON_MULTITENANT_ORACLE_VERSION:
        pytest.skip(f"Skipping test as Oracle DB version > {LAST_NON_MULTITENANT_ORACLE_VERSION}")
    if permission_to_test == 'privilege_logmining' and db_version <= LAST_NON_MULTITENANT_ORACLE_VERSION:
        pytest.skip(f"Skipping test as Oracle DB version <= {LAST_NON_MULTITENANT_ORACLE_VERSION}")
    if permission_to_test == 'privilege_set_container' and not db_multitenant:
        pytest.skip(f"Skipping test as Oracle DB is not multitenant")

    try:
        # Create source table
        logger.info("Using source table name %s", src_table_name)
        table = sqlalchemy.Table(src_table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column(PRIMARY_KEY,
                                                   sqlalchemy.Integer,
                                                   primary_key=True),
                                 sqlalchemy.Column(OTHER_COLUMN,
                                                   sqlalchemy.Numeric(20, 2)))
        table.create(engine)

        # Define pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=True,
                                                          src_table_name=src_table_name)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build().configure_for_environment(database)
        pipeline.configuration["shouldRetry"] = False

        # Create guest user
        logger.info(f"Creating user {guest_username} in database...")
        connection.execute(f"CREATE USER {guest_username} IDENTIFIED BY {guest_password}")
        connection.execute(f"GRANT CONNECT TO {guest_username}")

        # Give permissions to guest user and select expected error message
        expected_error_message = ""
        if permission_to_test == 'select_on_views' and not has_permission:
            expected_error_message = "JDBC_613 - Not enough permissions to access the following tables:"
        else:
            connection.execute(f"GRANT SELECT_CATALOG_ROLE TO {guest_username}")
            logger.info(f"Given permission SELECT_CATALOG_ROLE to {guest_username}")
        if permission_to_test == 'select_on_table' and not has_permission:
            expected_error_message = "JDBC_613 - Not enough permissions to access the following tables:"
        else:
            connection.execute(f"GRANT SELECT ON {src_table_name} TO {guest_username}")
            logger.info(f"Given permission SELECT ON {src_table_name} to {guest_username}")
        if permission_to_test == 'role_execute_catalog' and not has_permission:
            expected_error_message = "JDBC_618 - User does not have the role EXECUTE_CATALOG_ROLE"
        else:
            connection.execute(f"GRANT EXECUTE_CATALOG_ROLE TO {guest_username}")
            logger.info(f"Given permission EXECUTE_CATALOG_ROLE to {guest_username}")
        if permission_to_test == 'privilege_alter_session' and not has_permission:
            expected_error_message = "JDBC_619 - User does not have the alter session privilege"
        else:
            connection.execute(f"GRANT ALTER SESSION TO {guest_username}")
            logger.info(f"Given permission ALTER SESSION to {guest_username}")
        if permission_to_test == 'privilege_select_any_transaction' and not has_permission:
            expected_error_message = "JDBC_623 - User does not have the select any transaction privilege"
        elif db_version <= LAST_NON_MULTITENANT_ORACLE_VERSION:
            connection.execute(f"GRANT SELECT ANY TRANSACTION TO {guest_username}")
            logger.info(f"Given permission SELECT ANY TRANSACTION to {guest_username}")
        if permission_to_test == 'privilege_select_any_table' and not has_permission:
            expected_error_message = "JDBC_624 - User does not have the select any table privilege"
        elif db_version <= LAST_NON_MULTITENANT_ORACLE_VERSION:
            connection.execute(f"GRANT SELECT ANY TABLE TO {guest_username}")
            logger.info(f"Given permission SELECT ANY TABLE to {guest_username}")
        if permission_to_test == 'privilege_logmining' and not has_permission:
            expected_error_message = "JDBC_621 - User does not have the logmining privilege"
        elif db_version > LAST_NON_MULTITENANT_ORACLE_VERSION:
            connection.execute(f"GRANT LOGMINING TO {guest_username}")
            logger.info(f"Given permission LOGMINING to {guest_username}")
        if permission_to_test == 'privilege_set_container' and not has_permission:
            expected_error_message = "JDBC_622 - User does not have the set container privilege"
        elif db_multitenant:
            connection.execute(f"GRANT SET CONTAINER TO {guest_username}")
            logger.info(f"Given permission SET CONTAINER to {guest_username}")
        logger.info(f"Expected error message is: {expected_error_message}")

        # Assign the guest user to the pipeline
        oracle_cdc_client.set_attributes(username=guest_username,
                                         password=guest_password)

        # Create the pipeline
        sdc_executor.add_pipeline(pipeline)

        # Execute the pipeline
        if has_permission:
            # It should execute and stop normally
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
            status = sdc_executor.get_pipeline_status(pipeline).response.json().get("status")
            logger.info(f"Status after stopping the pipeline when no error should occur: {status}")
            assert "STOPPED" == status
        else:
            # It should stop with StageException
            with pytest.raises(Exception):
                sdc_executor.start_pipeline(pipeline)
                sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
            status = sdc_executor.get_pipeline_status(pipeline).response.json().get("status")
            message = sdc_executor.get_pipeline_status(pipeline).response.json().get("message")
            logger.info(f"Status after stopping the pipeline when starting error should occur: {status}")
            logger.info(f"Message after stopping the pipeline when starting error should occur: {message}")
            assert "START_ERROR" == status
            assert expected_error_message in message, f"Expected error message is not in message, expected error" \
                                                      f"message is: {expected_error_message} and message is: {message}"

    finally:
        # Stop pipeline
        try:
            if sdc_executor.get_pipeline_status(pipeline).response.json().get("status") == "RUNNING":
                sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        finally:
            pass

        # Drop guest user
        for i in range(10):
            user_sessions = connection.execute(f"select SID, SERIAL#, STATUS from V$SESSION where USERNAME='{guest_username}'").fetchall()
            if len(user_sessions) > 0:
                logger.info(f'Guest user {guest_username} still in use ({len(user_sessions)}). Waiting 5 seconds... {i}')
                sleep(5)
            else:
                logger.info(f'Guest user {guest_username} not in use... {i}')
                break
        user_sessions = connection.execute(f"select SID, SERIAL#, STATUS from V$SESSION where USERNAME='{guest_username}'").fetchall()
        for user_session in user_sessions:
            logger.info(f"Killing guest user session {user_session}")
            connection.execute(f"alter system kill session '{user_session[0]},{user_session[1]}' immediate")
        logger.info(f"Dropping guest user {guest_username} in database...")
        connection.execute(f"drop user {guest_username}")

        # Drop source table
        logger.info("Dropping source table table %s", src_table_name)
        table.drop(engine)


@pytest.mark.skip('Skipping for now. It takes too much time that makes the environment to timeout.')
@sdc_min_version('4.0.0')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@database('oracle')
def test_oracle_cdc_offset_chain(sdc_builder,
                                 sdc_executor,
                                 database,
                                 buffer_locally,
                                 buffer_location):
    """
    Test to check that offset between pipeline re-starts is tracked properly, especially focusing on JSON-based offsets.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    source_table = None
    target_table = None

    pipeline = None

    try:

        database_connection = database.engine.connect()

        source_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('IDENTIFIER', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        source_table.create(database.engine)
        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('IDENTIFIER', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         tables=[{"schema": database.username.upper(), "table": target_table_name, "excludePattern": ""}],
                                         buffer_changes_locally=buffer_locally,
                                         buffer_location=buffer_location,
                                         logminer_session_window="${5 * MINUTES}",
                                         maximum_transaction_length="${0 * SECONDS}",
                                         db_time_zone="UTC",
                                         max_batch_size_in_records=1,
                                         initial_change="SCN",
                                         start_scn=database_last_scn,
                                         send_redo_query_in_headers=True)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Origin Offset Testing Pipeline').configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        number_of_runs = 10
        number_of_rows = 100
        global_identifier = 0

        for runs in range(0, number_of_runs):
            database_transaction = database_connection.begin()
            for identifier in range(global_identifier, global_identifier + number_of_rows):
                table_identifier = identifier
                table_name = "'" + str(uuid.uuid4())[:32] + "'"
                table_surname = "'" + str(uuid.uuid4())[:64] + "'"
                table_country = "'" + str(uuid.uuid4())[:2] + "'"
                table_city = "'" + str(uuid.uuid4())[:3] + "'"
                sentence = f'insert into {source_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city})'
                sql = text(sentence)
                database_connection.execute(sql)
            global_identifier = identifier + 1
            database_transaction.commit()

            sentence = f'select count(*) from  {source_table}'
            sql = text(sentence)
            q = database_connection.execute(sql).fetchone()
            logger.info(f'(1) Total in source table: {q}')

            database_transaction = database_connection.begin()
            sentence = f'insert into {target_table_name} select * from {source_table_name}'
            sql = text(sentence)
            database_connection.execute(sql)
            database_transaction.commit()

            sentence = f'select count(*) from  {target_table}'
            sql = text(sentence)
            q = database_connection.execute(sql).fetchone()
            logger.info(f'(2) Total in target table: {q}')

            database_transaction = database_connection.begin()
            sentence = f'update {target_table_name} set CITY = COUNTRY'
            sql = text(sentence)
            database_connection.execute(sql)
            database_transaction.commit()

            sentence = f'select count(*) from  {target_table}'
            sql = text(sentence)
            q = database_connection.execute(sql).fetchone()
            logger.info(f'(3) Total in target table: {q}')

            database_transaction = database_connection.begin()
            sentence = f'delete from {target_table_name}'
            sql = text(sentence)
            database_connection.execute(sql)
            database_transaction.commit()

            sentence = f'select count(*) from  {target_table}'
            sql = text(sentence)
            q = database_connection.execute(sql).fetchone()
            logger.info(f'(4) Total in target table: {q}')

            database_transaction = database_connection.begin()
            sentence = f'delete from {source_table_name}'
            sql = text(sentence)
            database_connection.execute(sql)
            database_transaction.commit()

            sentence = f'select count(*) from  {source_table}'
            sql = text(sentence)
            q = database_connection.execute(sql).fetchone()
            logger.info(f'(5) Total in source table: {q}')

            sdc_executor.start_pipeline(pipeline)
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * number_of_rows)

            q_insert = sum(1 for record in wiretap.output_records if record.header.values["oracle.cdc.operation"] == 'INSERT')
            q_update = sum(1 for record in wiretap.output_records if record.header.values["oracle.cdc.operation"] == 'UPDATE')
            q_delete = sum(1 for record in wiretap.output_records if record.header.values["oracle.cdc.operation"] == 'DELETE')

            logger.info(f'Total INSERT\'s {q_insert}')
            logger.info(f'Total UPDATE\'s {q_update}')
            logger.info(f'Total DELETE\'s {q_delete}')

            for record in wiretap.output_records:
                logger.info(f'Run: {runs} :: '
                            f'{record.header.values["oracle.cdc.sequence.internal"]} - '
                            f'{record.header.values["oracle.cdc.operation"]} - '
                            f'{record.header.values["sdc.operation.type"]} - '
                            f'{record}')

            assert q_insert == number_of_rows
            assert q_update == number_of_rows
            assert q_delete == number_of_rows

            sdc_executor.stop_pipeline(pipeline=pipeline, force=False)

            wiretap.reset()

            logger.info('Pipeline stopped')

    finally:

        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@sdc_min_version('4.2.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_oracle_cdc_offset_and_nested_transactions(sdc_builder,
                                                   sdc_executor,
                                                   database,
                                                   buffer_location):
    """
    Test to check that "nested" transactions are recovered properly if after processing the "enclosing" transaction, but
    before processing the "enclosed" transaction, and stopping the pipeline, then the "enclosed" transaction is
    "recovering" when the pipeline starts again.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    target_table = None

    pipeline = None

    maximum_transaction_length = 40
    logminer_session_window = 60

    try:

        database_connection_enclosing = database.engine.connect()
        database_connection_enclosed = database.engine.connect()

        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('IDENTIFIER', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(32)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)),
                                        sqlalchemy.Column('ORIGIN', sqlalchemy.String(16)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection_enclosing)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(initial_change="SCN",
                                         start_scn=database_last_scn,
                                         maximum_transaction_length=f"${{{maximum_transaction_length} * SECONDS}}",
                                         logminer_session_window=f"${{{logminer_session_window} * SECONDS}}",
                                         dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         disable_continuous_mine=True,
                                         buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         discard_old_uncommitted_transactions=False,
                                         parsing_thread_pool_size=1,
                                         parse_sql_query=True,
                                         use_peg_parser=True,
                                         pseudocolumns_in_header=True,
                                         send_redo_query_in_headers=True,
                                         max_batch_size_in_records=64,
                                         tables=[{"schema": database.username.upper(), "table": target_table_name, "excludePattern": ""}],
                                         db_time_zone="UTC")
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Origin Offset Testing Pipeline').configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        number_of_rows_enclosing = 5
        number_of_rows_enclosed = 10
        global_identifier = 0

        sleep(5)
        database_transaction_enclosing = database_connection_enclosing.begin()
        sleep(20)
        for identifier in range(global_identifier, global_identifier + number_of_rows_enclosing):
            table_identifier = identifier
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            table_origin = "'enclosing'"
            sentence = f'insert into {target_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_origin})'
            sql = text(sentence)
            database_connection_enclosing.execute(sql)
        global_identifier = identifier + 1

        sleep(40)

        database_transaction_enclosed = database_connection_enclosed.begin()
        sleep(5)
        for identifier in range(global_identifier, global_identifier + number_of_rows_enclosed):
            table_identifier = identifier
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            table_origin = "'enclosed'"
            sentence = f'insert into {target_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_origin})'
            sql = text(sentence)
            database_connection_enclosed.execute(sql)
        global_identifier = identifier + 1
        sleep(10)
        database_transaction_enclosed.commit()

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', number_of_rows_enclosed, timeout_sec=300)

        q_insert = sum(1 for record in wiretap.output_records if record.header.values["oracle.cdc.operation"] == 'INSERT')
        logger.info(f'Total INSERT\'s {q_insert}')
        for record in wiretap.output_records:
            logger.info(f'{record.header.values["oracle.cdc.sequence.internal"]} - '
                        f'{record.header.values["oracle.cdc.operation"]} - '
                        f'{record.header.values["sdc.operation.type"]} - '
                        f'{record}')
        assert q_insert == number_of_rows_enclosed

        sdc_executor.stop_pipeline(pipeline=pipeline, force=False)

        sleep(5)

        for identifier in range(global_identifier, global_identifier + number_of_rows_enclosing):
            table_identifier = identifier
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            table_origin = "'enclosing'"
            sentence = f'insert into {target_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_origin})'
            sql = text(sentence)
            database_connection_enclosing.execute(sql)
        global_identifier = identifier + 1
        sleep(10)
        for identifier in range(global_identifier, global_identifier + number_of_rows_enclosing):
            table_identifier = identifier
            table_name = "'" + str(uuid.uuid4())[:32] + "'"
            table_surname = "'" + str(uuid.uuid4())[:64] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            table_origin = "'enclosing'"
            sentence = f'insert into {target_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_origin})'
            sql = text(sentence)
            database_connection_enclosing.execute(sql)
        global_identifier = identifier + 1
        sleep(5)
        database_transaction_enclosing.commit()

        wiretap.reset()

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * number_of_rows_enclosing, timeout_sec=300)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = 3 * number_of_rows_enclosing
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

        q_insert = sum(1 for record in sorted_records if record.header.values["oracle.cdc.operation"] == 'INSERT')
        logger.info(f'Total INSERT\'s {q_insert}')
        for record in sorted_records:
            logger.info(f'{record.header.values["oracle.cdc.sequence.internal"]} - '
                        f'{record.header.values["oracle.cdc.operation"]} - '
                        f'{record.header.values["sdc.operation.type"]} - '
                        f'{record}')
        assert q_insert == 3 * number_of_rows_enclosing

        sdc_executor.stop_pipeline(pipeline=pipeline, force=False)

        logger.info('Pipeline stopped')

    finally:

        if target_table is not None:
            try:
                target_table.drop(database.engine)
            except DatabaseError as error:
                logger.warning(f"Failed to drop table '{target_table_name}': {error}")


@pytest.mark.skip('Skipping for now. It takes too much time that makes the environment to timeout.')
@sdc_min_version('5.1.0')
@database('oracle')
def test_oracle_cdc_offset_commit_only(sdc_builder, sdc_executor, database):
    """
    Test offset management when using COMMIT ONLY option LogMiner.
    """

    target_table = None

    pipeline = None

    maximum_transaction_length = 300
    logminer_session_window = 600

    try:

        database_connection_enclosing = database.engine.connect()
        database_connection_enclosed = database.engine.connect()

        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('IDENTIFIER', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('NAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                        sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
                                        sqlalchemy.Column('CITY', sqlalchemy.String(3)),
                                        sqlalchemy.Column('ORIGIN', sqlalchemy.String(16)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection_enclosing)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(initial_change="SCN",
                                         start_scn=database_last_scn,
                                         maximum_transaction_length=f"${{{maximum_transaction_length} * SECONDS}}",
                                         logminer_session_window=f"${{{logminer_session_window} * SECONDS}}",
                                         dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         disable_continuous_mine=True,
                                         buffer_changes_locally=False,
                                         discard_old_uncommitted_transactions=False,
                                         parsing_thread_pool_size=1,
                                         parse_sql_query=True,
                                         use_peg_parser=True,
                                         pseudocolumns_in_header=True,
                                         send_redo_query_in_headers=True,
                                         max_batch_size_in_records=1,
                                         tables=[{"schema": database.username.upper(), "table": target_table_name, "excludePattern": ""}],
                                         db_time_zone="UTC")
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Origin Offset Testing Pipeline').configure_for_environment(database)
        pipeline.rate_limit = 1

        sdc_executor.add_pipeline(pipeline)

        number_of_rows_enclosing = 30
        number_of_rows_enclosed = 30

        initial_number_of_records = 5

        global_identifier = 1

        sleep(2)

        database_transaction_enclosing = database_connection_enclosing.begin()
        sleep(1)
        for identifier in range(global_identifier, global_identifier + number_of_rows_enclosing // 2):
            table_identifier = identifier
            table_name = "'OUT.start - " + str(uuid.uuid4())[:16] + "'"
            table_surname = "'" + str(uuid.uuid4())[:32] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            table_origin = "'enclosing'"
            sentence = f'insert into {target_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_origin})'
            sql = text(sentence)
            database_connection_enclosing.execute(sql)
            sleep(1)
        global_identifier = identifier + 1
        sleep(1)

        database_transaction_enclosed = database_connection_enclosed.begin()
        sleep(1)
        for identifier in range(global_identifier, global_identifier + number_of_rows_enclosed):
            table_identifier = identifier
            table_name = "'IN.closed - " + str(uuid.uuid4())[:16] + "'"
            table_surname = "'" + str(uuid.uuid4())[:32] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            table_origin = "'enclosed'"
            sentence = f'insert into {target_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_origin})'
            sql = text(sentence)
            database_connection_enclosed.execute(sql)
            sleep(1)
        global_identifier = identifier + 1
        sleep(1)
        database_transaction_enclosed.commit()

        sleep(2)

        pipeline_execution = sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count',
                                              initial_number_of_records,
                                              timeout_sec=300)
        try:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        except:
            pass

        sleep(30)

        q_operations_01 = len(wiretap.output_records)
        logger.info(f'Total operations in first run: {q_operations_01} - {initial_number_of_records}')

        sleep(5)

        sleep(10)
        for identifier in range(global_identifier, global_identifier + number_of_rows_enclosing // 2):
            table_identifier = identifier
            table_name = "'OUT.end - " + str(uuid.uuid4())[:16] + "'"
            table_surname = "'" + str(uuid.uuid4())[:32] + "'"
            table_country = "'" + str(uuid.uuid4())[:2] + "'"
            table_city = "'" + str(uuid.uuid4())[:3] + "'"
            table_origin = "'enclosing'"
            sentence = f'insert into {target_table} values ({table_identifier}, {table_name}, {table_surname}, {table_country}, {table_city}, {table_origin})'
            sql = text(sentence)
            database_connection_enclosing.execute(sql)
            sleep(1)
        global_identifier = identifier + 1
        sleep(10)
        database_transaction_enclosing.commit()

        sleep(5)

        wiretap.reset()

        pipeline_execution = sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline,
                                              'input_record_count',
                                              number_of_rows_enclosed + number_of_rows_enclosing - q_operations_01,
                                              timeout_sec=300)
        try:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=False)
        except:
            pass

        sleep(30)

        q_operations_02 = len(wiretap.output_records)
        logger.info(f'Total operations in second run: {q_operations_02} - {number_of_rows_enclosed + number_of_rows_enclosing - q_operations_01}')

        assert q_operations_01 + q_operations_02 == number_of_rows_enclosed + number_of_rows_enclosing

    finally:

        if target_table is not None:
            target_table.drop(database.engine)


@sdc_min_version('5.0.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
@pytest.mark.parametrize('use_peg_parser', [False, True])
def test_oracle_cdc_client_primary_keys_headers(sdc_builder,
                                                sdc_executor,
                                                database,
                                                buffer_location,
                                                use_peg_parser):
    """
    Test to check all headers for primary keys are present in the output records.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    pipeline = None

    try:

        database_connection = database.engine.connect()

        table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('TYPE', sqlalchemy.String(64), primary_key=True),
                                 sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('NAME', sqlalchemy.String(64)),
                                 sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
                                 sqlalchemy.Column('ADDRESS', sqlalchemy.String(64)))
        table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)

        column_type = "'" + "Hobbit" + "'"
        column_id = 1
        column_name = "'" + "Bilbo" + "'"
        column_surname = "'" + "Baggins" + "'"

        column_address = "'" + "Bag End 0" + "'"
        database_transaction = database_connection.begin()
        sentence = f"insert into {table} " \
                   f"values ({column_type}, {column_id}, {column_name}, {column_surname}, {column_address})"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 1" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Fallohide' where TYPE = 'Hobbit' and ID = 1"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 2" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, ID = 2 where TYPE = 'Fallohide' and ID = 1"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 3" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Hobbit - Fallohide', ID = 3 where TYPE = 'Fallohide' and ID = 2"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 4" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, TYPE = 'Hobbit, Fallohide' where TYPE = 'Hobbit - Fallohide'"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 5" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address}, ID = 4 where ID = 3"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        column_address = "'" + "Bag End 6" + "'"
        database_transaction = database_connection.begin()
        sentence = f"update {table_name} set ADDRESS = {column_address} where ID = 4"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f"delete from {table_name}"
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         tables=[{"schema": database.username.upper(), "table": table_name, "excludePattern": ""}],
                                         buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         logminer_session_window="${2 * MINUTES}",
                                         maximum_transaction_length="${1 * MINUTES}",
                                         db_time_zone="UTC",
                                         max_batch_size_in_records=8,
                                         initial_change="SCN",
                                         start_scn=database_last_scn,
                                         send_redo_query_in_headers=True,
                                         disable_continuous_mine=True,
                                         use_peg_parser=use_peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 8)

        sleep(30)

        wiretap_output_records_max_retries = 12
        wiretap_output_records_max_wait = 10
        wiretap_output_records_retries = 0
        wiretap_output_records_control_length = 8
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

        assert len(wiretap_output_records) == 8

        for record in wiretap_output_records:
            assert "schema" in record.header.values
            assert "oracle.cdc.table" in record.header.values
            assert "oracle.cdc.operation" in record.header.values
            assert "sdc.operation.type" in record.header.values
            assert "oracle.cdc.redoValue" in record.header.values
            assert "oracle.cdc.undoValue" in record.header.values
            assert "oracle.cdc.query" in record.header.values
            assert {record.header.values["schema"]} is not None
            assert {record.header.values["oracle.cdc.table"]} is not None
            assert {record.header.values["oracle.cdc.operation"]} is not None
            assert {record.header.values["sdc.operation.type"]} is not None
            assert {record.header.values["oracle.cdc.redoValue"]} is not None
            assert {record.header.values["oracle.cdc.undoValue"]} is not None
            assert {record.header.values["oracle.cdc.query"]} is not None
            if record.header.values["oracle.cdc.operation"] == 'UPDATE':

                assert "jdbc.primaryKey.before.TYPE" in record.header.values
                assert "jdbc.primaryKey.before.ID" in record.header.values
                assert "jdbc.primaryKey.after.TYPE" in record.header.values
                assert "jdbc.primaryKey.after.ID" in record.header.values

                assert record.header.values["jdbc.primaryKey.before.TYPE"] is not None
                assert record.header.values["jdbc.primaryKey.before.ID"] is not None
                assert record.header.values["jdbc.primaryKey.after.TYPE"] is not None
                assert record.header.values["jdbc.primaryKey.after.ID"] is not None

                column_address = record.field['ADDRESS'].value

                if column_address == 'Bag End 1':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "1"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "1"
                elif column_address == 'Bag End 2':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "1"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "2"
                elif column_address == 'Bag End 3':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "2"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit - Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "3"
                elif column_address == 'Bag End 4':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit - Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "3"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "3"
                elif column_address == 'Bag End 5':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "3"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "4"
                elif column_address == 'Bag End 6':
                    assert record.header.values["jdbc.primaryKey.before.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.before.ID"] == "4"
                    assert record.header.values["jdbc.primaryKey.after.TYPE"] == "Hobbit, Fallohide"
                    assert record.header.values["jdbc.primaryKey.after.ID"] == "4"

            else:

                assert "jdbc.primaryKey.before.TYPE" not in record.header.values
                assert "jdbc.primaryKey.before.ID" not in record.header.values
                assert "jdbc.primaryKey.after.TYPE" not in record.header.values
                assert "jdbc.primaryKey.after.ID" not in record.header.values

            logger.info(f"schema..............: {record.header.values['schema']}")
            logger.info(f"oracle.cdc.table....: {record.header.values['oracle.cdc.table']}")
            logger.info(f"oracle.cdc.operation: {record.header.values['oracle.cdc.operation']}")
            logger.info(f"sdc.operation.type..: {record.header.values['sdc.operation.type']}")
            logger.info(f"oracle.cdc.redoValue: {record.header.values['oracle.cdc.redoValue']}")
            logger.info(f"oracle.cdc.undoValue: {record.header.values['oracle.cdc.undoValue']}")
            logger.info(f"oracle.cdc.query....: {record.header.values['oracle.cdc.query']}")
            logger.info(f".....................")
            if record.header.values["oracle.cdc.operation"] == 'UPDATE':
                logger.info(f"column - address.................: {record.field['ADDRESS'].value}")
                logger.info(f"jdbc.primaryKey.before.TYPE: {record.header.values['jdbc.primaryKey.before.TYPE']}")
                logger.info(f"jdbc.primaryKey.before.ID..: {record.header.values['jdbc.primaryKey.before.ID']}")
                logger.info(f"jdbc.primaryKey.after.TYPE.: {record.header.values['jdbc.primaryKey.after.TYPE']}")
                logger.info(f"jdbc.primaryKey.after.ID...: {record.header.values['jdbc.primaryKey.after.ID']}")
                logger.info(f"----------------------------------")

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            table.drop(database.engine)


@sdc_min_version('5.0.0')
@database('oracle')
@pytest.mark.parametrize('buffer_location', ['IN_MEMORY', 'ON_DISK'])
def test_oracle_cdc_client_primary_keys_metadata_headers(sdc_builder,
                                                        sdc_executor,
                                                        database,
                                                        buffer_location):
    """
    Test to check all headers for primary keys metadata are present in the output records.
    """

    # These versions contain a bug (COLLECTOR-987) that makes buffering on disk fail.
    if buffer_location == 'ON_DISK':
        if Version('4.1.0') <= Version(sdc_builder.version) < Version('5.0.0'):
            pytest.skip('Local buffering on disk will fail in this SDC version')

    table = None
    pipeline = None

    try:

        database_connection = database.engine.connect()

        database_last_scn = _get_last_scn(database_connection)

        table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating source table %s in %s database ...', table_name, database.type)

        trasaction = database_connection.begin()
        database_connection.execute(f"""create table {table_name}
                                                     (my_binary_double binary_double,
                                                      my_binary_float  binary_float,
                                                      my_char_byte     char(32 byte),
                                                      my_char_char     char(64 char),
                                                      my_date          date,
                                                      my_float         number(6),
                                                      my_interval_year interval year(6) to month,
                                                      my_interval_day  interval day(9) to second(7),
                                                      my_nvarchar2     nvarchar2(512),
                                                      my_number        number(4, 8),
                                                      my_raw           raw(1024),
                                                      my_timestamp     timestamp(9),
                                                      my_varchar_byte  varchar(32 byte),
                                                      my_varchar_char  varchar(64 char),
                                                      my_varchar2_byte varchar2(128 byte),
                                                      my_varchar2_char varchar2(256 char),
                                                      primary key (my_binary_double,
                                                                   my_binary_float,
                                                                   my_char_byte,
                                                                   my_char_char,
                                                                   my_date,
                                                                   my_float,
                                                                   my_interval_year,
                                                                   my_interval_day,
                                                                   my_nvarchar2,
                                                                   my_number,
                                                                   my_raw,
                                                                   my_timestamp,
                                                                   my_varchar_byte,
                                                                   my_varchar_char,
                                                                   my_varchar2_byte,
                                                                   my_varchar2_char))""")
        database_connection.execute(f"""insert into {table_name}
                                                    (my_binary_double,
                                                     my_binary_float,
                                                     my_char_byte,
                                                     my_char_char,
                                                     my_date,
                                                     my_float,
                                                     my_interval_year,
                                                     my_interval_day,
                                                     my_nvarchar2,
                                                     my_number,
                                                     my_raw,
                                                     my_timestamp,
                                                     my_varchar_byte,
                                                     my_varchar_char,
                                                     my_varchar2_byte,
                                                     my_varchar2_char)
                                              values (0,
                                                      0,
                                                      ' ',
                                                      ' ',
                                                      sysdate,
                                                      0,
                                                      interval '1-1' year to month,
                                                      interval '1 1:1:1.1' day to second,
                                                      ' ',
                                                      0,
                                                      '0000',
                                                      current_timestamp,
                                                      ' ',
                                                      ' ',
                                                      ' ',
                                                      ' ')""")
        trasaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(dictionary_source="DICT_FROM_ONLINE_CATALOG",
                                         tables=[{"schema": database.username.upper(), "table": table_name, "excludePattern": ""}],
                                         buffer_changes_locally=True,
                                         buffer_location=buffer_location,
                                         logminer_session_window="${5 * MINUTES}",
                                         maximum_transaction_length="${2 * MINUTES}",
                                         db_time_zone="UTC",
                                         max_batch_size_in_records=8,
                                         initial_change="SCN",
                                         start_scn=database_last_scn,
                                         send_redo_query_in_headers=True,
                                         unsupported_field_type="SEND_TO_PIPELINE",
                                         add_unsupported_fields_to_records=True)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=300)

        assert len(wiretap.output_records) == 1

        for record in wiretap.output_records:
            assert "jdbc.primaryKeySpecification" in record.header.values
            assert {record.header.values["jdbc.primaryKeySpecification"]} is not None

            primary_key_specification_json = json.dumps(json.loads(record.header.values["jdbc.primaryKeySpecification"]), sort_keys=True)

            primary_key_specification_expected = \
                '''{"MY_CHAR_CHAR":     {"type": 1,    "datatype": "CHAR",      "size": 64,   "precision": 64,  "scale": 0, "signed": true, "currency": false},
                    "MY_RAW":           {"type": -3,   "datatype": "VARBINARY", "size": 1024, "precision": 0,   "scale": 0, "signed": true, "currency": false},
                    "MY_NUMBER":        {"type": 2,    "datatype": "NUMERIC",   "size": 6,    "precision": 4,   "scale": 8, "signed": true, "currency": true},
                    "MY_VARCHAR_BYTE":  {"type": 12,   "datatype": "VARCHAR",   "size": 32,   "precision": 32,  "scale": 0, "signed": true, "currency": false},
                    "MY_INTERVAL_YEAR": {"type": -103, "datatype": "UNKNOWN",   "size": 5,    "precision": 6,   "scale": 0, "signed": true, "currency": false},
                    "MY_VARCHAR2_CHAR": {"type": 12,   "datatype": "VARCHAR",   "size": 256,  "precision": 256, "scale": 0, "signed": true, "currency": false},
                    "MY_INTERVAL_DAY":  {"type": -104, "datatype": "UNKNOWN",   "size": 11,   "precision": 9,   "scale": 7, "signed": true, "currency": false},
                    "MY_TIMESTAMP":     {"type": 93,   "datatype": "TIMESTAMP", "size": 11,   "precision": 0,   "scale": 9, "signed": true, "currency": false},
                    "MY_BINARY_FLOAT":  {"type": 100,  "datatype": "UNKNOWN",   "size": 4,    "precision": 0,   "scale": 0, "signed": true, "currency": false},
                    "MY_CHAR_BYTE":     {"type": 1,    "datatype": "CHAR",      "size": 32,   "precision": 32,  "scale": 0, "signed": true, "currency": false},
                    "MY_VARCHAR_CHAR":  {"type": 12,   "datatype": "VARCHAR",   "size": 64,   "precision": 64,  "scale": 0, "signed": true, "currency": false},
                    "MY_BINARY_DOUBLE": {"type": 101,  "datatype": "UNKNOWN",   "size": 8,    "precision": 0,   "scale": 0, "signed": true, "currency": false},
                    "MY_NVARCHAR2":     {"type": -9,   "datatype": "NVARCHAR",  "size": 512,  "precision": 512, "scale": 0, "signed": true, "currency": false},
                    "MY_FLOAT":         {"type": 2,    "datatype": "NUMERIC",   "size": 7,    "precision": 6,   "scale": 0, "signed": true, "currency": true},
                    "MY_DATE":          {"type": 93,   "datatype": "TIMESTAMP", "size": 7,    "precision": 0,   "scale": 0, "signed": true, "currency": false},
                    "MY_VARCHAR2_BYTE": {"type": 12,   "datatype": "VARCHAR",   "size": 128,  "precision": 128, "scale": 0, "signed": true, "currency": false}}'''
            primary_key_specification_expected_json = json.dumps(json.loads(primary_key_specification_expected), sort_keys=True)

            assert primary_key_specification_json == primary_key_specification_expected_json

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            database_connection.execute(f"drop table {table_name}")


@sdc_min_version('5.0.0')
@database('oracle')
def test_oracle_cdc_client_read_timestamp_with_local_timezone(sdc_builder, sdc_executor, database):
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


@sdc_min_version('5.1.0')
@database('oracle')
@pytest.mark.parametrize('add_unsupported_fields_to_records', [True, False])
@pytest.mark.parametrize('case_sensitive', [True, False])
@pytest.mark.parametrize('use_peg_parser', [True, False])
@pytest.mark.parametrize('pseudocolumns_in_header', [True, False])
@pytest.mark.parametrize('include_nulls', [True, False])
def test_oracle_cdc_client_sorted_columns(sdc_builder,
                                          sdc_executor,
                                          database,
                                          add_unsupported_fields_to_records,
                                          case_sensitive,
                                          use_peg_parser,
                                          pseudocolumns_in_header,
                                          include_nulls):
    """
    Check that Oracle CDC Client produces columns in the database order.
    """

    try:

        test_pattern = f'{add_unsupported_fields_to_records} - '\
                       f'{case_sensitive} - '\
                       f'{use_peg_parser} - '\
                       f'{pseudocolumns_in_header} - '\
                       f'{include_nulls}'

        logger.info(f'Running test: {test_pattern}')

        source_table = None
        target_table = None

        pipeline = None

        database_connection = database.engine.connect()

        if case_sensitive:
            source_table_name = f'{get_random_string(string.ascii_uppercase, 8)}{get_random_string(string.ascii_lowercase, 8)}'
        else:
            source_table_name = f'{get_random_string(string.ascii_uppercase, 16)}'
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(source_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('A00', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Col00', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col01', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col02', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col03', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col04', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col05', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col06', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col07', sqlalchemy.String(8)))
        source_table.create(database.engine)

        if case_sensitive:
            target_table_name = f'{get_random_string(string.ascii_uppercase, 8)}{get_random_string(string.ascii_lowercase, 8)}'
        else:
            target_table_name = f'{get_random_string(string.ascii_uppercase, 16)}'
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(target_table_name, sqlalchemy.MetaData(),
                                        sqlalchemy.Column('A00', sqlalchemy.Integer, primary_key=True),
                                        sqlalchemy.Column('Col00', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col01', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col02', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col03', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col04', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col05', sqlalchemy.String(8)),
                                        sqlalchemy.Column('Col06', sqlalchemy.BLOB),
                                        sqlalchemy.Column('Col07', sqlalchemy.String(8)))
        target_table.create(database.engine)

        database_last_scn = _get_last_scn(database_connection)
        number_of_rows = 8

        database_transaction = database_connection.begin()
        for id in range(0, number_of_rows):
            table_benull = "''"
            table_beblob = "utl_raw.cast_to_raw('" + get_random_string(string.ascii_uppercase, 128) + "')"

            table_a00 = id
            table_col00 = table_beblob
            table_col01 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col02 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col03 = table_benull
            table_col04 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col05 = "'" + get_random_string(string.ascii_uppercase, 8) + "'"
            table_col06 = table_beblob
            table_col07 = "'sdc'"
            sentence = f'insert into "{source_table}" ' \
                       f'values (' \
                       f'{table_a00}, ' \
                       f'{table_col00}, ' \
                       f'{table_col01}, ' \
                       f'{table_col02}, ' \
                       f'{table_col03}, ' \
                       f'{table_col04}, ' \
                       f'{table_col05}, ' \
                       f'{table_col06},' \
                       f'{table_col07} ' \
                       f')'
            sql = text(sentence)
            database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'insert into "{target_table_name}" select * from "{source_table_name}"'
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'update "{target_table_name}" set "Col03" = \'SDC\''
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        database_transaction = database_connection.begin()
        sentence = f'delete from "{target_table_name}" where "Col03" = \'SDC\''
        sql = text(sentence)
        database_connection.execute(sql)
        database_transaction.commit()

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
        oracle_cdc_client.set_attributes(dictionary_source='DICT_FROM_ONLINE_CATALOG',
                                         tables=[{'schema': database.username.upper(),
                                                  'table': target_table_name,
                                                  'excludePattern': ''}],
                                         buffer_changes_locally=True,
                                         logminer_session_window='${10 * MINUTES}',
                                         maximum_transaction_length='${2 * MINUTES}',
                                         db_time_zone='UTC',
                                         max_batch_size_in_records=1,
                                         initial_change='SCN',
                                         start_scn=database_last_scn,
                                         case_sensitive_names=case_sensitive,
                                         include_nulls=include_nulls,
                                         parse_sql_query=True,
                                         use_peg_parser=use_peg_parser,
                                         unsupported_field_type='SEND_TO_PIPELINE',
                                         add_unsupported_fields_to_records=add_unsupported_fields_to_records,
                                         pseudocolumns_in_header=pseudocolumns_in_header,
                                         send_redo_query_in_headers=True)
        set_session_wait_times(sdc_builder, oracle_cdc_client)


        wiretap = pipeline_builder.add_wiretap()

        oracle_cdc_client >> wiretap.destination

        pipeline_name = f'{test_pattern} - {get_random_string(string.ascii_letters, 8)}'
        pipeline_title = f'Oracle CDC Client Pipeline: {pipeline_name}'
        pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 4 * number_of_rows, timeout_sec=300)

        assert len(wiretap.output_records) == 4 * number_of_rows

        for record in wiretap.output_records:
            last_column = ''
            logger.info(f'{record.field} - {record.header}')
            columns = record.field.keys()
            # Some columns might be missing due to stage configuration
            assert len(columns) >= 6
            for column in columns:
                assert column > last_column
                last_column = column

    finally:

        logger.info(f'Finished test: {test_pattern}')

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=False)

        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@sdc_min_version('5.1.0')
@database('oracle')
@pytest.mark.parametrize('test_case, setup_actions, tear_down_actions, expected_error, skip', [
    ('Buffer directory does not exist.', [], [], 'JDBC_643', False),
    ('Buffer directory does not have read permissions.', [f'mkdir -m 222 %s'], [f'chmod 700 %s', f'rm -fr %s'],
     'JDBC_644', True),
    ('Buffer directory does not have write permissions.', [f'mkdir -m 444 %s'], [f'rm -fr %s'], 'JDBC_645', True),
    ('Buffer directory is used correctly.', [f'mkdir -m 755 %s'], [], None, False)])
def test_buffer_directory(sdc_builder, sdc_executor, database, test_case, setup_actions, tear_down_actions,
                          expected_error, skip):
    """
        Verify that the buffer directory is used.
    """

    if skip:
        pytest.skip("This test is disabled because the user running the test always has permission to r/w the folder")

    buffer_directory = "/tmp/sdc-%s" % get_random_string(string.ascii_lowercase, 10)
    for setup_action in setup_actions:
        logger.info('Creating buffer directory %s', buffer_directory)
        sdc_executor.execute_shell(setup_action % buffer_directory)

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        start_scn = _get_last_scn(connection)
        oracle_origin = _get_oracle_cdc_client_origin(connection=connection,
                                                      database=database,
                                                      sdc_builder=sdc_builder,
                                                      pipeline_builder=pipeline_builder,
                                                      initial_change='SCN',
                                                      start_scn=start_scn,
                                                      buffer_locally=True,
                                                      buffer_location='ON_DISK',
                                                      src_table_name=table_name,
                                                      buffer_directory=buffer_directory)
        trash = pipeline_builder.add_stage('Trash')
        pipeline = pipeline_builder.build(test_case).configure_for_environment(database)
        oracle_origin >> trash
        sdc_executor.add_pipeline(pipeline)

        if expected_error:
            with pytest.raises(StartError) as e:
                sdc_executor.start_pipeline(pipeline)
            assert e.value.message.startswith(expected_error)
        else:
            pipeline_buffer_directory = f'{buffer_directory}/{sdc_executor.id}/{pipeline.id}'

            sdc_executor.start_pipeline(pipeline)
            # assert that buffer directory is used by Oracle CDC.
            assert int(sdc_executor.execute_shell(f'ls {pipeline_buffer_directory} | wc -l').stdout) > 0
            sdc_executor.stop_pipeline(pipeline=pipeline).wait_for_stopped()
            # make sure the directory gets cleaned after pipeline stops.
            assert int(sdc_executor.execute_shell(f'ls {pipeline_buffer_directory} | wc -l').stdout) == 0

    finally:
        for tear_down_action in tear_down_actions:
            logger.info('Deleting buffer directory %s', buffer_directory)
            sdc_executor.execute_shell(tear_down_action % buffer_directory)


@fixture(scope="module")  # Cache the fixture to avoid reading/writing the BLOB each time the fixture is called
def blob_file_specs(sdc_builder, sdc_executor, database):
    """Retrieve data about a binary file in the filesystem, namely path, filename, content and size.
    The 'ls' executable has been chosen as it will be available in most testing environments.
    The pipeline is the following:
        oracle_cdc_client >> wiretap"""
    id_column_name = "IDCOL"
    size_column_name = "SIZECOL"
    blob_column_name = "BLOBCOL"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path = "/usr/bin/"
    file_name = "ls"
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)

    logger.info(f"Retrieving data for binary file {dir_path}{dir_name}")

    with ExitStack() as on_exit:
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(size_column_name, sqlalchemy.Integer),
            sqlalchemy.Column(blob_column_name, sqlalchemy.BLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        try:
            txn = connection.begin()
            connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        lobFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        b_lob BLOB;
                    BEGIN
                        f_lob := bfilename(dir, lobFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key},  DBMS_LOB.GETLENGTH(f_lob), EMPTY_BLOB())
                        RETURNING {blob_column_name} INTO b_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        length, content = connection.execute(
            f"SELECT {size_column_name}, {blob_column_name} FROM {source_table_name}"
        ).fetchall()[0]

    return dir_path, file_name, length, content


@fixture(scope="module")  # Cache the fixture to avoid reading/writing the CLOB each time the fixture is called
def clob_file_specs(sdc_builder, sdc_executor, database):
    """Retrieve data about a text executable file in the filesystem, namely path, filename, content and size.
    The 'gpg-zip' executable script has been chosen as it should be available in most testing environments.

    IMPORTANT: if this test fails, it is probably due to the gpg-zip file not being available in the testing
    environment. Change the file_name variable to point to another text file that is big enough for
    oracle to split the LOB_WRITE into multiple records.

    The pipeline is the following:
        oracle_cdc_client >> wiretap"""
    id_column_name = "IDCOL"
    clob_column_name = "CLOBCOL"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path = "/usr/bin/"
    file_name = "gpg-zip"
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)

    logger.info(f"Retrieving data for binary file {dir_path}{dir_name}")

    with ExitStack() as on_exit:
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(clob_column_name, sqlalchemy.CLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        try:
            txn = connection.begin()
            connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
            load_parameters = "c_lob, f_lob, DBMS_LOB.LOBMAXSIZE, v_dest_offset, v_src_offset,"\
                              " DBMS_LOB.DEFAULT_CSID, v_lang_context, v_warning"
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        lobFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        c_lob CLOB;
                        v_dest_offset NUMBER := 1;
                        v_src_offset NUMBER := 1;
                        v_warning NUMBER;
                        v_lang_context NUMBER := DBMS_LOB.DEFAULT_LANG_CTX;
                    BEGIN
                        f_lob := bfilename(dir, lobFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key}, EMPTY_CLOB())
                        RETURNING {clob_column_name} INTO c_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        DBMS_LOB.LOADCLOBFROMFILE({load_parameters});
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        content = connection.execute(
            f"SELECT {clob_column_name} FROM {source_table_name}"
        ).fetchall()[0][0]

    return dir_path, file_name, len(content), content


@sdc_min_version("5.2.0")
@database("oracle")
def test_enabling_lobs_without_local_buffering(sdc_builder, sdc_executor, database):
    """Create a pipeline with LOB support enabled and local buffering disabled and
    check that the correct configuration error is raised."""

    source_table_name = get_random_string(string.ascii_uppercase, 16)

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column("IDCOL", sqlalchemy.Integer, primary_key=True),
        )
        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=False,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=False
        )
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        trash = pipeline_builder.add_stage('Trash')
        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)

        sdc_executor.add_pipeline(pipeline)

        with pytest.raises(ValidationError) as validation_error:
            sdc_executor.validate_pipeline(pipeline)
        logger.warning(validation_error)
        assert validation_error is not None
        assert validation_error.value.issues is not None
        assert validation_error.value.issues['issueCount'] == 1
        assert validation_error.value.issues['stageIssues']["OracleCDCClient_01"][0]['configName'] == "oracleCDCConfigBean.lobsEnabled"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("lob_type", ["BLOB", "CLOB"])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_lob_insert(sdc_builder, sdc_executor, database, peg_parser, lob_type, buffer_location):
    """Do a simple LOB insert and check that a single record is outputted and said record contains
    the inserted value. The pipeline is the following:
        oracle_cdc_client >> wiretap
    """

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    lob_column_name = "LOBCOL"
    test_values = {
        "BLOB": {
            "insert_value": "utl_raw.cast_to_raw('Hello there')",
            "expected": b"Hello there",
            "column_type": sqlalchemy.BLOB,
        },
        "CLOB": {"insert_value": f"TO_CLOB('Hello there')", "expected": "Hello there", "column_type": sqlalchemy.CLOB},
    }

    test_value = test_values[lob_type]

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column("IDCOL", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(lob_column_name, test_value["column_type"]),
        )
        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"INSERT INTO {source_table_name} VALUES ({primary_key}, {test_value['insert_value']})")
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 1, timeout_sec=120)

        assert len(wiretap.output_records) == 1, f"Expected a single record, got {len(wiretap.output_records)}"
        output_lob = wiretap.output_records[0].field[lob_column_name]
        assert output_lob == test_value["expected"], f"Mismatched LOB values {output_lob} and {test_value['expected']}"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("lob_types", [["BLOB", "BLOB"], ["BLOB", "CLOB"], ["CLOB", "BLOB"], ["CLOB", "CLOB"]])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_lob_insert_multicolumn(sdc_builder, sdc_executor, database, peg_parser, lob_types, buffer_location):
    """Do simple LOB inserts to multiple columns  and check that a single record is outputted
    and said record contains the inserted values. The pipeline is the following:
        oracle_cdc_client >> wiretap
    """

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    test_values = {
        "BLOB": {
            "insert_value": "utl_raw.cast_to_raw('Hello there')",
            "expected": b"Hello there",
            "column_type": sqlalchemy.BLOB,
        },
        "CLOB": {"insert_value": f"TO_CLOB('Hello there')", "expected": "Hello there", "column_type": sqlalchemy.CLOB},
    }

    test_value_A = test_values[lob_types[0]]
    test_value_B = test_values[lob_types[1]]

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column("IDCOL", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("LOBCOLA", test_value_A["column_type"]),
            sqlalchemy.Column("LOBCOLB", test_value_B["column_type"]),
        )
        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key}, {test_value_A['insert_value']}, {test_value_B['insert_value']})"
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 1, timeout_sec=120)

        assert len(wiretap.output_records) == 1, f"Expected a single record, got {len(wiretap.output_records)}"
        lob_a = wiretap.output_records[0].field["LOBCOLA"]
        assert lob_a == test_value_A["expected"], "Mismatched LOB A value"
        lob_b = wiretap.output_records[0].field["LOBCOLB"]
        assert lob_b == test_value_B["expected"], "Mismatched LOB B value"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("lob_type", ["BLOB", "CLOB"])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
@pytest.mark.parametrize("disable_continuous_mine", [True, False])
def test_lob_insert_mixed(
    sdc_builder, sdc_executor, database, peg_parser, lob_type, buffer_location, disable_continuous_mine
):
    """Mixed the LOB insert with other operations to ensure LOBs do not affect other records.
    The pipeline is the following:
        oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    test_values = {
        "BLOB": {
            "column_type": sqlalchemy.BLOB,
            "items": [
                {"insert_value": "utl_raw.cast_to_raw('Hello there')", "expected": b"Hello there"},
                {
                    "insert_value": "utl_raw.cast_to_raw('ereht olleH')",
                    "expected": b"ereht olleH",
                    "column_type": sqlalchemy.BLOB,
                },
            ],
        },
        "CLOB": {
            "column_type": sqlalchemy.CLOB,
            "items": [
                {"insert_value": f"TO_CLOB('Hello there')", "expected": "Hello there"},
                {"insert_value": f"TO_CLOB('ereht olleH')", "expected": "Hello there"},
            ],
        },
    }

    test_value = test_values[lob_type]

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column("IDCOL", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("LOBCOL", test_value["column_type"]),
            sqlalchemy.Column("STRCOL", sqlalchemy.String(20)),
        )
        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=disable_continuous_mine,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key}, {test_value['items'][0]['insert_value']}, 'General Kenobi')"
            )
            connection.execute(
                f"UPDATE {source_table_name} SET STRCOL = 'You are a bold one' WHERE IDCOL = {primary_key}"
            )
            connection.execute(f"DELETE FROM {source_table_name} WHERE IDCOL = {primary_key}")
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key + 1}, {test_value['items'][1]['insert_value']}, 'iboneK lareneG')"
            )
            connection.execute(
                f"UPDATE {source_table_name} SET STRCOL = 'eno dlob a era uoY' WHERE IDCOL = {primary_key + 1}"
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 4, timeout_sec=120)

        expected_blob_values = [item["expected"] for item in test_value["items"]]
        lob_values = {record.field["LOBCOL"] for record in wiretap.output_records if "LOBCOL" in record.field.keys()}

        assert len(wiretap.output_records) == 5, f"Expected 5 records, got {len(wiretap.output_records)}"
        assert len(lob_values) == 2, f"Expected two non-empty LOB values, got {len(lob_values)}"
        assert all(expected_blob in lob_values for expected_blob in expected_blob_values), "Missing expected LOB values"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
@pytest.mark.parametrize("disable_continuous_mine", [True, False])
def test_lob_consecutive_writes(
    sdc_builder, sdc_executor, database, peg_parser, blob_file_specs, buffer_location, disable_continuous_mine
):
    """Write several (n) times to a BLOB before committing and check that n records are outputted.
    The main goal is to test that the RsIdSsnSql implementation is correct.
    The pipeline is the following:
        oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    blob_column_name = "BLOBCOL"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, ls_file_length, ls_file_content = blob_file_specs
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    n = 3

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(blob_column_name, sqlalchemy.BLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=disable_continuous_mine,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        b_lob BLOB;
                    BEGIN
                        f_lob := bfilename(dir, imgFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key}, EMPTY_BLOB())
                        RETURNING {blob_column_name} INTO b_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        {'DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));' * n}
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 2, timeout_sec=120)

        assert len(wiretap.output_records) == 1 + n, f"Expected {1 + n} records, got {len(wiretap.output_records)}"
        blob_values = [record.field[blob_column_name].value for record in wiretap.output_records]
        assert blob_values.count(EMPTY_BLOB) == 1, "Missing or incorrect empty blob"
        assert blob_values.count(ls_file_content) == n, "Missing or incorrect LOB values"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
@pytest.mark.parametrize("disable_continuous_mine", [True, False])
def test_blob_write(
    sdc_builder, sdc_executor, database, peg_parser, buffer_location, blob_file_specs, disable_continuous_mine
):
    """Wrote a BLOB from a binary file and check the contents. The pipeline is the following:
    oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    blob_column_name = "BLOBCOL"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, ls_file_length, ls_file_content = blob_file_specs
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(blob_column_name, sqlalchemy.BLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=disable_continuous_mine,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location,
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        b_lob BLOB;
                    BEGIN
                        f_lob := bfilename(dir, imgFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key}, EMPTY_BLOB())
                        RETURNING {blob_column_name} INTO b_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 2, timeout_sec=120)

        assert len(wiretap.output_records) == 2, f"Expected 2 records, got {len(wiretap.output_records)}"
        blob_values = [record.field[blob_column_name].value for record in wiretap.output_records]
        assert blob_values.count(EMPTY_BLOB) == 1, "Missing or incorrect empty blob"
        assert blob_values.count(ls_file_content) == 1, "Missing or incorrect LOB value"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
@pytest.mark.parametrize("disable_continuous_mine", [True, False])
def test_blob_write_mixed(
    sdc_builder, sdc_executor, database, peg_parser, blob_file_specs, buffer_location, disable_continuous_mine
):
    """Mix other operations with BLOB loads from the filesystem to ensure other operations are not affected.
    The pipeline is the following:
        oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    blob_column_name = "BLOBCOL"
    string_column_name = "STRCOL"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, ls_file_length, ls_file_content = blob_file_specs
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(blob_column_name, sqlalchemy.BLOB),
            sqlalchemy.Column(string_column_name, sqlalchemy.String(20)),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=100,
            initial_change="LATEST",
            disable_continuous_mine=disable_continuous_mine,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        b_lob BLOB;
                    BEGIN
                        f_lob := bfilename(dir, imgFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key}, EMPTY_BLOB(), 'Hello there')
                        RETURNING {blob_column_name} INTO b_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            connection.execute(
                f"UPDATE {source_table_name} SET {string_column_name} = 'You are a bold one' WHERE IDCOL = {primary_key}"
            )
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        b_lob BLOB;
                    BEGIN
                        f_lob := bfilename(dir, imgFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key+1}, EMPTY_BLOB(), 'ereht olleH')
                        RETURNING {blob_column_name} INTO b_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            connection.execute(f"DELETE FROM {source_table_name} WHERE IDCOL = {primary_key}")
            connection.execute(
                f"UPDATE {source_table_name} SET {string_column_name} = 'eno dlob a era uoY' WHERE IDCOL = {primary_key + 1}"
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 7, timeout_sec=120)

        assert len(wiretap.output_records) == 7
        blob_values = [
            record.field[blob_column_name]
            for record in wiretap.output_records
            if blob_column_name in record.field.keys()
        ]
        # There must be four BLOBs
        assert len(blob_values) == 4
        # Two of them are empty BLOBs
        assert blob_values.count(EMPTY_BLOB) == 2, "Unexpected number of empty BLOBs"
        # Two of them are the content of the LS file
        assert blob_values.count(ls_file_content) == 2, "Unexpected number of BLOBs"
        # Check that every other value is present
        expected_strings = {"Hello there": 2, "You are a bold one": 2, "ereht olleH": 2, "eno dlob a era uoY": 1}
        string_values = [record.field[string_column_name] for record in wiretap.output_records]
        for value, expected_count in expected_strings.items():
            count = string_values.count(value)
            assert count == expected_count, f"Expected {expected_count} occurrences of {value}, got {count}"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("max_lob_size", [0, 128, 4096])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_blob_max_size(sdc_builder, sdc_executor, database, peg_parser, max_lob_size, blob_file_specs, buffer_location):
    """Set different maximum lob sizes and verify that BLOBs are split into the appropriate records.
    The pipeline is the following:
        oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")
    
    with database.engine.connect() as conn:
        if _get_oracle_db_version(conn) == 12:
            pytest.skip("Skipping test for Oracle 12 RAC")

    id_column_name = "IDCOL"
    blob_column_name = "BLOBCOL"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, ls_file_length, ls_file_content = blob_file_specs
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)

    expected_value_big = ls_file_content
    small_insert_value = "Did you ever hear the tragedy of Darth Plagueis The Wise? I thought not."
    expected_value_small = b"Did you ever hear the tragedy of Darth Plagueis The Wise? I thought not."

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(blob_column_name, sqlalchemy.BLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=True,
            maximum_lob_size=max_lob_size,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key}, utl_raw.cast_to_raw('{small_insert_value}'))"
            )
            connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        b_lob BLOB;
                    BEGIN
                        f_lob := bfilename(dir, imgFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key+1}, EMPTY_BLOB())
                        RETURNING {blob_column_name} INTO b_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(
            pipeline, "input_record_count", 1 if max_lob_size == 0 else 2, timeout_sec=120
        )

        blob_values = []
        null_values = []
        for record in wiretap.output_records:
            blob = record.field[blob_column_name].value
            if blob:
                blob_values.append(blob)
            else:
                null_values.append(blob)

        # Max LOB size is disabled or greater than any of the values
        if max_lob_size == 0 or max_lob_size >= len(expected_value_big):
            assert len(null_values) == 0
            assert expected_value_big in blob_values
            assert expected_value_small in blob_values
        # Max LOB size is greater than small value but lesser than big value
        elif len(expected_value_small) <= max_lob_size < len(expected_value_big):
            assert len(null_values) == 1
            assert expected_value_small in blob_values
        # Max LOB size is lesser than any value
        else:
            # Max LOB size is greater than empty blob value: "EMPTY_BLOB()"
            if max_lob_size >= len(EMPTY_BLOB):
                assert len(null_values) == 2
                assert len(blob_values) == 0
            # Max LOB size is lesser than empty blob value: "EMPTY_BLOB()"
            else:
                assert len(null_values) == 3
                assert len(blob_values) == 0


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("method", ["RAW", "FROM_FILE"])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
@pytest.mark.parametrize("disable_continuous_mine", [True, False])
def test_clob_write(
    sdc_builder, sdc_executor, database, peg_parser, method, clob_file_specs, buffer_location, disable_continuous_mine
):
    """Write a CLOB of enough size that it will be internally split into multiple LOB_WRITE operations
    and ensure a single record is produced. The file must be bigger than 1022 characters or it will not be
    split by Oracle. The pipeline is the following:
        oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    clob_column_name = "CLOBCOL"
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    n = 2900
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, content_size, content = clob_file_specs

    # Single quotation marks will be duplicated by the QL
    expected_value = "A" * n + "B" * n if method == "RAW" else content.replace("'", "''")

    # FROM_FILE method test will temporarily be run for Oracle versions <12
    connection = None
    try:
        connection = database.engine.connect()
        db_version = _get_oracle_db_version(connection)[0]
        if db_version < 12 and method == "FROM_FILE":
            pytest.skip("Required CLOB is not available in the containers of Oracle versions <12")
    except Exception as exception:
        pytest.fail(f"Failed to check DB version: {exception}")
    finally:
        if connection is not None:
            connection.close()

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(clob_column_name, sqlalchemy.CLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=disable_continuous_mine,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location,
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            if method == "RAW":
                connection.execute(
                    f"INSERT INTO {source_table_name} VALUES ({primary_key}, TO_CLOB('{'A' * n}') || '{'B' * n}')"
                )
            else:
                connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
                load_parameters = "c_lob, f_lob, DBMS_LOB.LOBMAXSIZE, v_dest_offset, v_src_offset," \
                                  " DBMS_LOB.DEFAULT_CSID, v_lang_context, v_warning"
                connection.execute(
                    f"""DECLARE
                            dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                            imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                            f_lob BFILE;
                            c_lob CLOB;
                            v_dest_offset NUMBER := 1;
                            v_src_offset NUMBER := 1;
                            v_warning NUMBER;
                            v_lang_context NUMBER := DBMS_LOB.DEFAULT_LANG_CTX;
                        BEGIN
                            f_lob := bfilename(dir, imgFile);
                            INSERT INTO {source_table_name} VALUES ({primary_key}, EMPTY_CLOB())
                            RETURNING {clob_column_name} INTO c_lob;

                            DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                            DBMS_LOB.LOADCLOBFROMFILE({load_parameters});
                            COMMIT;
                            DBMS_LOB.FILECLOSE(f_lob);
                        END;"""
                )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 2, timeout_sec=120)

        assert len(wiretap.output_records) == 2, f"Expected 2 records, got {len(wiretap.output_records)}"
        clob_values = [record.field[clob_column_name].value for record in wiretap.output_records]
        assert clob_values.count(EMPTY_CLOB) == 1, "Missing or incorrect empty blob"
        assert clob_values.count(expected_value) == 1, "Missing or incorrect LOB value"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
@pytest.mark.parametrize("disable_continuous_mine", [True, False])
def test_clob_write_mixed(
    sdc_builder, sdc_executor, database, peg_parser, blob_file_specs, buffer_location, disable_continuous_mine
):
    """Mix big CLOB writes with other instruction to verify they are not affected by each other.
    The pipeline is the following:
        oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    clob_column_name = "CLOBCOL"
    string_column_name = "STRCOL"
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    n = 2900
    expected_value = "A" * n + "B" * n

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(clob_column_name, sqlalchemy.CLOB),
            sqlalchemy.Column(string_column_name, sqlalchemy.String(20)),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=100,
            initial_change="LATEST",
            disable_continuous_mine=disable_continuous_mine,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key}, TO_CLOB('{'A' * n}') || '{'B' * n}', 'Hello there')"
            )
            connection.execute(
                f"UPDATE {source_table_name} SET {string_column_name} = 'You are a bold one' WHERE IDCOL = {primary_key}"
            )
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key+1}, TO_CLOB('{'A' * n}') || '{'B' * n}', 'ereht olleH')"
            )
            connection.execute(f"DELETE FROM {source_table_name} WHERE IDCOL = {primary_key}")
            connection.execute(
                f"UPDATE {source_table_name} SET {string_column_name} = 'eno dlob a era uoY' WHERE IDCOL = {primary_key + 1}"
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 7, timeout_sec=120)

        assert len(wiretap.output_records) == 7
        clob_values = [
            record.field[clob_column_name]
            for record in wiretap.output_records
            if clob_column_name in record.field.keys()
        ]
        # There must be four BLOBs
        assert len(clob_values) == 4
        # Two of them are empty BLOBs
        assert clob_values.count(EMPTY_CLOB) == 2, "Unexpected number of empty BLOBs"
        # Two of them are the content of the LS file
        assert clob_values.count(expected_value) == 2, "Unexpected number of CLOBs"
        # Check that every other value is present
        expected_strings = {"Hello there": 2, "You are a bold one": 2, "ereht olleH": 2, "eno dlob a era uoY": 1}
        string_values = [record.field[string_column_name] for record in wiretap.output_records]
        for value, expected_count in expected_strings.items():
            count = string_values.count(value)
            assert count == expected_count, f"Expected {expected_count} occurrences of {value}, got {count}"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("max_lob_size", [0, 16, 64, 4096])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_clob_max_size(sdc_builder, sdc_executor, database, peg_parser, max_lob_size, buffer_location):
    """Set different maximum lob sizes and verify that CLOBs are split into the appropriate records.
    The pipeline is the following:
        oracle_cdc_client >> wiretap"""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    clob_column_name = "CLOBCOL"
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    many = 2900  # There is a limit of 4000 characters per string
    expected_value_big = "A" * many + "B" * many
    few = 32
    expected_value_small = "A" * few

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(clob_column_name, sqlalchemy.CLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=True,
            maximum_lob_size=max_lob_size,
            buffer_changes_locally=True,
            buffer_location=buffer_location
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"INSERT INTO {source_table_name} VALUES ({primary_key}, TO_CLOB('{'A' * few}'))")
            # This big CLOB will be split into an initial empty INSERT followed by several LOB_WRITEs.
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key+1}, TO_CLOB('{'A' * many}') || '{'B' * many}')"
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 3, timeout_sec=120)

        clob_values = []
        empty_clob_values = []
        null_values = []
        for record in wiretap.output_records:
            clob = record.field[clob_column_name].value
            if clob is None:
                null_values.append(clob)
            elif clob == EMPTY_CLOB:
                empty_clob_values.append(clob)
            else:
                clob_values.append(clob)

        if max_lob_size >= len(EMPTY_CLOB) or max_lob_size == 0:
            assert len(empty_clob_values) == 1

        if max_lob_size == 0 or max_lob_size >= len(expected_value_big):
            assert len(null_values) == 0
            assert expected_value_big in clob_values
            assert expected_value_small in clob_values
        elif len(expected_value_small) <= max_lob_size < len(expected_value_big):
            assert len(null_values) == 1
            assert expected_value_small in clob_values
        else:
            if max_lob_size >= len(EMPTY_CLOB):
                assert len(null_values) == 2
                assert len(clob_values) == 0
            else:
                assert len(null_values) == 3
                assert len(clob_values) == 0
                assert len(empty_clob_values) == 0


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("add_unsupported_fields_to_records", [True, False])
@pytest.mark.parametrize("lob_type", ["BLOB", "CLOB"])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_lob_disabled_lob_insert(
    sdc_builder, sdc_executor, database, peg_parser, add_unsupported_fields_to_records, lob_type, buffer_location
):
    """Do a simple LOB insert and check that a single record is outputted and said record contains
    the inserted value. The pipeline is the following:
        oracle_cdc_client >> wiretap

    SDC 5.2.0 added support for BLOB/CLOB data types, but still gave the option to treat them as unsupported types.
    This test also checks that that is no longer the case."""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    lob_column_name = "LOBCOL"
    test_values = {
        "BLOB": {
            "insert_value": "utl_raw.cast_to_raw('Hello there')",
            "expected": "HEXTORAW('48656c6c6f207468657265')",
            "column_type": sqlalchemy.BLOB,
        },
        "CLOB": {"insert_value": f"TO_CLOB('Hello there')", "expected": "Hello there", "column_type": sqlalchemy.CLOB},
    }

    test_value = test_values[lob_type]

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column("IDCOL", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(lob_column_name, test_value["column_type"]),
        )
        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=False,
            buffer_changes_locally=True,
            buffer_location=buffer_location,
            add_unsupported_fields_to_records=add_unsupported_fields_to_records,
            unsupported_field_type="SEND_TO_PIPELINE"
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"INSERT INTO {source_table_name} VALUES ({primary_key}, {test_value['insert_value']})")
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 2, timeout_sec=120)

        logger.warning([record for record in wiretap.output_records])

        output_records = [record for record in wiretap.output_records]
        assert len(output_records) == 2, f"Expected 2 records, got {len(output_records)}"
        assert all(lob_column_name not in record.field for record in output_records)


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("add_unsupported_fields_to_records", [True, False])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_lob_disabled_blob_write(
    sdc_builder, sdc_executor, database, peg_parser, blob_file_specs, add_unsupported_fields_to_records, buffer_location
):
    """Write a LOB from a file while LOB support is disabled and ensure no LOB records are produced.
    The pipeline is the following:
        oracle_cdc_client >> wiretap

    SDC 5.2.0 added support for BLOB/CLOB data types, but still gave the option to treat them as unsupported types.
    This test also checks that that is no longer the case."""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    blob_column_name = "BLOBCOL"
    dir_name = get_random_string(string.ascii_uppercase, 16)
    dir_path, file_name, ls_file_length, ls_file_content = blob_file_specs
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(blob_column_name, sqlalchemy.BLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=100,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=False,
            buffer_changes_locally=True,
            buffer_location=buffer_location,
            add_unsupported_fields_to_records=add_unsupported_fields_to_records,
            unsupported_field_type="SEND_TO_PIPELINE"
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"CREATE OR REPLACE DIRECTORY {dir_name} AS '{dir_path}'")
            connection.execute(
                f"""DECLARE
                        dir VARCHAR2({len(dir_name)}) := '{dir_name}';
                        imgFile VARCHAR2({len(file_name)}) := '{file_name}';
                        f_lob BFILE;
                        b_lob BLOB;
                    BEGIN
                        f_lob := bfilename(dir, imgFile);
                        INSERT INTO {source_table_name} VALUES ({primary_key}, EMPTY_BLOB())
                        RETURNING {blob_column_name} INTO b_lob;

                        DBMS_LOB.FILEOPEN(f_lob,  DBMS_LOB.FILE_READONLY);
                        DBMS_LOB.LOADFROMFILE(b_lob, f_lob, DBMS_LOB.GETLENGTH(f_lob));
                        COMMIT;
                        DBMS_LOB.FILECLOSE(f_lob);
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 1, timeout_sec=120)

        assert len(wiretap.output_records) == 1, "LOB records were produced with LOB support disabled"
        assert blob_column_name not in wiretap.output_records[0].field, "Didn't expect to find LOB field"


@sdc_min_version("5.2.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("add_unsupported_fields_to_records", [True, False])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_lob_disabled_clob_write(
    sdc_builder, sdc_executor, database, peg_parser, add_unsupported_fields_to_records, buffer_location
):
    """Write a big CLOB while LOB support is disabled an ensure no CLOB records are produced.
    The pipeline is the following:
        oracle_cdc_client >> wiretap

    SDC 5.2.0 added support for BLOB/CLOB data types, but still gave the option to treat them as unsupported types.
    This test also checks that that is no longer the case."""

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    id_column_name = "IDCOL"
    clob_column_name = "CLOBCOL"
    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    n = 2900

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(clob_column_name, sqlalchemy.CLOB),
        )

        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=False,
            buffer_changes_locally=True,
            buffer_location=buffer_location,
            add_unsupported_fields_to_records=add_unsupported_fields_to_records,
            unsupported_field_type="SEND_TO_PIPELINE"
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)
        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(
                f"INSERT INTO {source_table_name} VALUES ({primary_key}, TO_CLOB('{'A' * n}') || '{'B' * n}')"
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 1, timeout_sec=120)

        assert len(wiretap.output_records) == 1, "LOB records were produced with LOB support disabled"
        assert clob_column_name not in wiretap.output_records[0].field, "Didn't expect to find LOB field"


@sdc_min_version("5.3.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("lob_type", ["BLOB", "CLOB"])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_lob_trim(sdc_builder, sdc_executor, database, peg_parser, lob_type, buffer_location):
    """WRITE and TRIM a LOB and verify the TRIM record headers contain the expected information.
    The pipeline is the following:
        oracle_cdc_client >> wiretap
    """

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    id_column_name = "IDCOL"
    lob_column_name = "LOBCOL"
    test_values = {
        "BLOB": {
            "insert_value": "utl_raw.cast_to_raw('Did you ever hear the tragedy of Darth Plagueis The Wise?')",
            "expected": b"Did you ever hear the tragedy of Darth Plagueis The Wise?",
            "column_type": sqlalchemy.BLOB,
        },
        "CLOB": {
            "insert_value": f"TO_CLOB('Did you ever hear the tragedy of Darth Plagueis The Wise?')",
            "expected": "Did you ever hear the tragedy of Darth Plagueis The Wise?",
            "column_type": sqlalchemy.CLOB,
        },
    }

    trim_length = 10
    test_value = test_values[lob_type]

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(lob_column_name, test_value["column_type"]),
        )
        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location,
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)

        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"INSERT INTO {source_table_name} VALUES ({primary_key}, {test_value['insert_value']})")
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        try:
            logger.info("Triming test values ...")
            txn = connection.begin()
            connection.execute(
                f"""DECLARE
                        loc_b {lob_type};
                    BEGIN
                        SELECT "{lob_column_name}" INTO loc_b FROM {source_table_name} WHERE {id_column_name} = {primary_key} FOR UPDATE;
                        dbms_lob.trim(loc_b, {trim_length});
                        COMMIT;
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 2, timeout_sec=120)

        assert len(wiretap.output_records) == 2, f"Expected 2 records, got {len(wiretap.output_records)}"
        for record in wiretap.output_records:
            logger.warning(record.header["values"])
        trim_records = [
            record for record in wiretap.output_records if "oracle.cdc.operation.lobLength" in record.header["values"]
        ]
        assert len(trim_records) == 1, f"Expected 1 trim record, got {len(trim_records)}"
        headers = trim_records[0].header["values"]
        assert headers["oracle.cdc.operation.lobColumnName"] == lob_column_name
        assert headers["oracle.cdc.operation.lobLength"] == f"{trim_length}"
        assert headers["oracle.cdc.operation.lobOperationName"] == "TRIM"


@sdc_min_version("5.3.0")
@database("oracle")
@pytest.mark.parametrize("peg_parser", [True, False])
@pytest.mark.parametrize("lob_type", ["BLOB", "CLOB"])
@pytest.mark.parametrize("buffer_location", ["IN_MEMORY", "ON_DISK"])
def test_lob_erase(sdc_builder, sdc_executor, database, peg_parser, lob_type, buffer_location):
    """WRITE and ERASE a LOB and verify the ERASE record headers contain the expected information.
    The pipeline is the following:
        oracle_cdc_client >> wiretap
    """

    sdc_version = Version(sdc_builder.version)
    if peg_parser and sdc_version < PEG_LOB_SUPPORT_VERSION:
        pytest.skip(f"BLOB parsing with the PEG parser is not supported for SDC v{PEG_LOB_SUPPORT_VERSION}")

    source_table_name = get_random_string(string.ascii_uppercase, 16)
    primary_key = randint(10000, 100000)
    id_column_name = "IDCOL"
    lob_column_name = "LOBCOL"
    test_values = {
        "BLOB": {
            "insert_value": "utl_raw.cast_to_raw('Did you ever hear the tragedy of Darth Plagueis The Wise?')",
            "expected": b"Did you ever hear the tragedy of Darth Plagueis The Wise?",
            "column_type": sqlalchemy.BLOB,
        },
        "CLOB": {
            "insert_value": f"TO_CLOB('Did you ever hear the tragedy of Darth Plagueis The Wise?')",
            "expected": "Did you ever hear the tragedy of Darth Plagueis The Wise?",
            "column_type": sqlalchemy.CLOB,
        },
    }

    erase_offset = 1
    erase_length = 10
    test_value = test_values[lob_type]

    with ExitStack() as on_exit:
        logger.info("Creating source table %s in %s database ...", source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column(id_column_name, sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(lob_column_name, test_value["column_type"]),
        )
        source_table.create(database.engine)
        on_exit.callback(source_table.drop, database.engine)

        connection = database.engine.connect()

        pipeline_builder = sdc_builder.get_pipeline_builder()
        oracle_cdc_client = pipeline_builder.add_stage("Oracle CDC Client")
        oracle_cdc_client.set_attributes(
            dictionary_source="DICT_FROM_ONLINE_CATALOG",
            tables=[{"schema": database.username.upper(), "table": source_table_name, "excludePattern": ""}],
            logminer_session_window="${2 * MINUTES}",
            maximum_transaction_length="${1 * MINUTES}",
            db_time_zone="UTC",
            max_batch_size_in_records=1,
            initial_change="LATEST",
            disable_continuous_mine=True,
            enable_blob_and_clob_columns_processing=True,
            buffer_changes_locally=True,
            buffer_location=buffer_location,
        )
        if sdc_version >= PEG_LOB_SUPPORT_VERSION:
            oracle_cdc_client.set_attributes(use_peg_parser=peg_parser)
        set_session_wait_times(sdc_builder, oracle_cdc_client)

        _wait_until_time(_get_current_oracle_time(connection=connection))

        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build("Oracle CDC Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        on_exit.callback(sdc_executor.stop_pipeline, pipeline)

        try:
            logger.info("Inserting test values ...")
            txn = connection.begin()
            connection.execute(f"INSERT INTO {source_table_name} VALUES ({primary_key}, {test_value['insert_value']})")
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        try:
            logger.info("Triming test values ...")
            txn = connection.begin()
            connection.execute(
                f"""DECLARE
                        loc_b {lob_type};
                        e_len NUMBER := {erase_length};
                    BEGIN
                        SELECT "{lob_column_name}" INTO loc_b FROM {source_table_name} WHERE {id_column_name} = {primary_key} FOR UPDATE;
                        dbms_lob.erase(loc_b, e_len, {erase_offset});
                        COMMIT;
                    END;"""
            )
            txn.commit()
        except:
            logger.error("Failed to insert values. Rolling back ...")
            txn.rollback()
            raise

        sdc_executor.wait_for_pipeline_metric(pipeline, "input_record_count", 2, timeout_sec=120)

        assert len(wiretap.output_records) == 2, f"Expected 2 records, got {len(wiretap.output_records)}"
        for record in wiretap.output_records:
            logger.warning(record.header["values"])
        erase_records = [
            record for record in wiretap.output_records if "oracle.cdc.operation.lobLength" in record.header["values"]
        ]
        assert len(erase_records) == 1, f"Expected 1 erase record, got {len(erase_records)}"
        headers = erase_records[0].header["values"]
        assert headers["oracle.cdc.operation.lobColumnName"] == lob_column_name
        assert headers["oracle.cdc.operation.lobLength"] == f"{erase_length}"
        assert headers["oracle.cdc.operation.lobOffset"] == f"{erase_offset}"
        assert headers["oracle.cdc.operation.lobOperationName"] == "ERASE"


@database("oracle")
@pytest.mark.parametrize(
    "buffer_location, buffer_locally", [["IN_MEMORY", True], ["ON_DISK", True], ["IN_MEMORY", False]]
)
@pytest.mark.parametrize("fetch_strategy, fetch_overflow", FETCH_PARAMETERS)
def test_fetch_strategy(
    sdc_builder, sdc_executor, database, buffer_location, buffer_locally, fetch_strategy, fetch_overflow
):
    """Run some existing tests with different fetch strategies and ensure they are unaffected"""

    if Version(sdc_builder.version) < FEAT_VER_FETCH_STRATEGY:
        pytest.skip("Fetch strategies are unsupported in this version")

    logger.info("Run 'test_oracle_cdc_client_basic'")
    test_oracle_cdc_client_basic(
        sdc_builder,
        sdc_executor,
        database,
        buffer_locally,
        buffer_location,
        False,
        fetch_strategy=fetch_strategy,
        fetch_overflow=fetch_overflow,
        row_count=300,
    )

    logger.info("Run 'test_rollback_to_savepoint'")
    test_rollback_to_savepoint(
        sdc_builder,
        sdc_executor,
        database,
        buffer_locally,
        buffer_location,
        False,
        fetch_strategy=fetch_strategy,
        fetch_overflow=fetch_overflow,
    )

    # Commented out to speed up the test
    # logger.info("Run 'test_oracle_cdc_client_bulk'")
    # test_oracle_cdc_client_bulk(
    #     sdc_builder,
    #     sdc_executor,
    #     database,
    #     buffer_locally,
    #     buffer_location,
    #     fetch_strategy=fetch_strategy,
    #     fetch_overflow=fetch_overflow,
    # )


def _get_oracle_cdc_client_origin(connection,
                                  database,
                                  sdc_builder,
                                  pipeline_builder,
                                  buffer_locally,
                                  buffer_location,
                                  src_table_name=None,
                                  batch_size=BATCH_SIZE,
                                  **kwargs):
    kwargs.setdefault("dictionary_source", "DICT_FROM_ONLINE_CATALOG")
    kwargs.setdefault("logminer_session_window", "${10 * MINUTES}")
    kwargs.setdefault("db_time_zone", "UTC")
    kwargs.setdefault("maximum_transaction_length", "${1 * MINUTES}")
    kwargs.setdefault("initial_change", "DATE")

    if Version(sdc_builder.version) >= Version(SESSION_WAIT_TIME_MIN_VERSION):
        kwargs.setdefault("time_after_session_window_start_in_ms", SHORT_WAIT_TIME)
        kwargs.setdefault("time_between_session_windows_in_ms", SHORT_WAIT_TIME)

    if Version("3.14.0") <= Version(sdc_builder.version) < Version("3.16.0"):
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


def _get_oracle_db_version(connection):
    """Returns an array of numbers where the first position is the major version"""
    db_version = connection.execute("SELECT version FROM product_component_version").fetchall()[0][0]
    str_version_list = db_version.split(".")
    version_list = [int(i) for i in str_version_list]
    return version_list


def _is_db_multitenant(connection):
    """Returns a boolean indicating whether the database is multitenant"""
    db_version = _get_oracle_db_version(connection)[0]
    if db_version > LAST_NON_MULTITENANT_ORACLE_VERSION:
        cdb_list = connection.execute("SELECT CDB FROM V$DATABASE").fetchall()
        if len(cdb_list) > 0 and len(cdb_list[0]) > 0 and cdb_list[0][0] == "YES":
            return True
    return False


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


def _get_table_pattern(src_table_name):
    return f'{src_table_name[:-2]}%'


def _insert(connection, table, count=3):
    rows = [{'ID': i, 'NAME': get_random_string(string.ascii_uppercase, 10)} for i in range(count)]
    sdc_op_types = [1 for i in range(count)]
    cdc_op_types = ['INSERT' for i in range(count)]

    connection.execute(table.insert(), rows)
    return Operations(rows=rows,
                      cdc_op_types=cdc_op_types,
                      sdc_op_types=sdc_op_types,
                      change_count=count)


def _update(connection, table, count=3):
    rows = []
    txn = connection.begin()
    try:
        for i in range(count):
            rows.append({'ID': i, 'NAME': get_random_string(string.ascii_uppercase, 6)})
            connection.execute(table.update().where(table.c.ID == i).values(NAME=rows[i]['NAME']))
        txn.commit()
    except:
        txn.rollback()
        raise

    sdc_op_types = [3 for i in range(count)]
    cdc_op_types = ['UPDATE' for i in range(count)]

    return Operations(rows=rows,
                      cdc_op_types=cdc_op_types,
                      sdc_op_types=sdc_op_types,
                      change_count=count)


def _delete(connection, table, count=3):
    txn = connection.begin()
    try:
        for i in range(count):
            connection.execute(table.delete().where(table.c.ID == i))
        txn.commit()
    except:
        txn.rollback()
        raise

    sdc_op_types = [2 for i in range(count)]
    cdc_op_types = ['DELETE' for i in range(count)]

    return Operations(rows=[],
                      cdc_op_types=cdc_op_types,
                      sdc_op_types=sdc_op_types,
                      change_count=count)


def _select_from_table(db_engine, dest_table):
    target_result = db_engine.execute(dest_table.select().order_by(dest_table.c[PRIMARY_KEY]))
    target_result_list = target_result.fetchall()
    target_result.close()
    return target_result_list

def _dump_dictionary_to_log(connection):
    """Make a dump of dictionary to redolog for better performance"""
    logger.info('Dumping dictionary to redolog started...')
    connection.execute('begin dbms_logmnr_d.build(options => dbms_logmnr_d.store_in_redo_logs); end;')
    connection.execute('alter system archive log current')
    logger.info('Dumping dictionary to redolog finished...')
