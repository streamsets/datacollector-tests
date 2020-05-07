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

import logging
import string
from collections import namedtuple
from datetime import datetime, timedelta
from time import sleep

import pytest
import sqlalchemy
from sqlalchemy import text
from streamsets.sdk import sdc_api
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# SQL Parser processor was renamed in SDC-10697, so we need to reference it by name.
SQL_PARSER_STAGE_NAME = 'com_streamsets_pipeline_stage_processor_parser_sql_SqlParserDProcessor'

PRIMARY_KEY = 'ID'
OTHER_COLUMN = 'NAME'
BATCH_SIZE = 10  # Max limit imposed by SDC for snapshots
Operations = namedtuple('Operations', ['rows', 'cdc_op_types', 'sdc_op_types', 'change_count'])


# pylint: disable=pointless-statement, too-many-locals


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
def test_decimal_attributes(sdc_builder, sdc_executor, database):
    """Validates that Field attributes for decimal types will get properly generated
    Runs oracle_cdc_client >> trash
    """
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
                                                          src_table_name=src_table_name)
        trash = pipeline_builder.add_stage('Trash')

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

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC: Decimal Attributes').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot
        # assert all the data captured have the same raw_data
        output_records = snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output
        assert len(output_records) == 1
        attributes = output_records[0].get_field_attributes(f'/{OTHER_COLUMN}')
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
@pytest.mark.parametrize('parse_sql', [True, False])
def test_date_type_conversions(sdc_builder, sdc_executor, database, parse_sql):
    """Check that Oracle CDC Origin / SQL Parser Processor convert Oracle types DATE and TIMESTAMP to SDC DATETIME type.

    We create a table with a DATE and a TIMESTAMP columns and insert two rows by using different forms of TO_DATE and
    TO_TIMESTAMP invocations. Then check the corresponding values in the records generated by Oracle CDC Origin are
    DATETIME type and equal to those stored in the database.

    Pipeline: depending on the `parse_sql` value,
      True)  oracle_cdc_client >> trash
      False) oracle_cdc_client >> sql_parser >> trash

    """
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
                                                      src_table_name=table_name)
    oracle_cdc_client.parse_sql_query = parse_sql

    if parse_sql:
        trash = pipeline_builder.add_stage('Trash')
        oracle_cdc_client >> trash
        instance_name = oracle_cdc_client.instance_name
    else:
        sql_parser = pipeline_builder.add_stage(name=SQL_PARSER_STAGE_NAME)
        sql_parser.set_attributes(sql_field='/sql',
                                  target_field='/',
                                  resolve_schema_from_db=True,
                                  db_time_zone='UTC')
        trash = pipeline_builder.add_stage('Trash')
        oracle_cdc_client >> sql_parser >> trash
        instance_name = sql_parser.instance_name

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
        txn = connection.begin()
        for line in lines:
            connection.execute(line)
        txn.commit()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time).
        _wait_until_time(_get_current_oracle_time(connection=connection))

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        # Assert all the data captured have the same raw_data.
        records = sorted(snapshot.snapshot_batches[0][instance_name].output,
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
@pytest.mark.parametrize('use_pattern', [True, False])
def test_oracle_cdc_client_basic(sdc_builder, sdc_executor, database, buffer_locally, use_pattern):
    """Basic test that reads inserts/updates/deletes to an Oracle table,
    and validates that they are read in the same order.
    Runs oracle_cdc_client >> trash
    """
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
                                                          src_table_name=src_table_pattern)

        inserts = _insert(connection=connection, table=table)

        rows = inserts.rows
        cdc_op_types = inserts.cdc_op_types
        sdc_op_types = inserts.sdc_op_types
        change_count = inserts.change_count

        updates = _update(connection=connection, table=table)

        rows += updates.rows
        cdc_op_types += updates.cdc_op_types
        sdc_op_types += updates.sdc_op_types
        change_count += updates.change_count

        deletes = _delete(connection=connection, table=table)

        # deletes should have the last state of the row, so it would be the what comes from the updates.
        rows += updates.rows
        cdc_op_types += deletes.cdc_op_types
        sdc_op_types += deletes.sdc_op_types
        change_count += deletes.change_count

        logger.info('Expected number of records is %s.', change_count)

        trash = pipeline_builder.add_stage('Trash')

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot

        row_index = 0
        op_index = 0
        # assert all the data captured have the same raw_data
        for record in snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output:
            assert row_index == int(record.field['ID'].value)
            assert rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header['values']['sdc.operation.type']) == sdc_op_types[op_index]
            assert record.header['values']['oracle.cdc.operation'] == cdc_op_types[op_index]
            row_index = (row_index + 1) % 3
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
@sdc_min_version('3.5.1')
@pytest.mark.parametrize('buffer_locally', [True])
@pytest.mark.parametrize('use_pattern', [False])
def test_oracle_cdc_client_stop_pipeline_when_no_archived_logs(sdc_builder, sdc_executor, database, buffer_locally, use_pattern):
    """
    Test for SDC-8418.  Pipeline should stop with RUN ERROR when there is no archived log files.
    Runs oracle_cdc_client >> trash
    """
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
                                         db_time_zone='UTC',
                                         dictionary_source='DICT_FROM_ONLINE_CATALOG',
                                         initial_change='DATE',
                                         logminer_session_window='${10 * MINUTES}',
                                         max_batch_size_in_records=BATCH_SIZE,
                                         maximum_transaction_length='${1 * MINUTES}',
                                         start_date=start_date,
                                         tables=tables)

        trash = pipeline_builder.add_stage('Trash')
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        pipeline.configuration["shouldRetry"] = False
        sdc_executor.add_pipeline(pipeline)

        # Pipeline should stop with StageExcception
        with pytest.raises(Exception):
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert 'RUN_ERROR' == status
    finally:
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('use_pattern', [True, False])
def test_oracle_cdc_client_string_null_values(sdc_builder, sdc_executor, database, buffer_locally, use_pattern):
    """Basic test that tests for SDC-8340. This test ensures that Strings with value 'NULL'/'null' is treated correctly,
    and null is not returned.
    Runs oracle_cdc_client >> trash
    """
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
                                                          src_table_name=src_table_pattern)
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

        trash = pipeline_builder.add_stage('Trash')

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot

        # assert all the data captured have the same raw_data
        output = snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output
        for i, record in enumerate(output):
            # In update records, values with NULLs in the row are not returned
            if 'ID' in record.field:
                id_val = record.field['ID'].value
                assert rows[i]['ID'] == None if id_val is None else int(id_val)
            assert rows[i]['NAME'] == record.field['NAME']

        assert len(output) == len(rows)
    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True])
def test_overlapping_transactions(sdc_builder, sdc_executor, database, buffer_locally):
    """Tests SDC-8359. The basic premise of the test:
    - Start a transaction, and insert some data
    - Wait for 1 second so timestamp of next transaction is different
    - Start a 2nd transaction, insert data and commit
    - Start pipeline
    - Get snapshot, make sure the 2nd txn's data is read
    - Stop pipeline
    - Commit transaction 1
    - Start pipeline, get snapshot
    - Must contain all data from transaction 1
    (Pre-8359, this would fail when buffer_locally=true with 2nd snapshot timing out, since no data is read)
    Runs oracle_cdc_client >> trash
    """

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

        # Start pipeline, get snapshot
        trash = pipeline_builder.add_stage('Trash')

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot
        sdc_executor.stop_pipeline(pipeline=pipeline,
                                   force=True)

        def compare_output(output_records, rows):
            assert len(output_records) == len(rows)
            for i, output_record in enumerate(output_records):
                assert output_record.field['ID'] == rows[i]['ID']
                assert output_record.field['NAME'] == rows[i]['NAME']

        # assert all the data captured have the same as rows_c1
        output = snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output
        compare_output(output, rows_c1)

        # Now commit the older transaction, which has overlapped over the second one
        long_txn.commit()

        # Pre-3.1.0.0, this times out
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot
        sdc_executor.stop_pipeline(pipeline=pipeline,
                                   force=True)
        # assert all the data captured have the same raw_data
        output = snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output
        compare_output(output, rows_c2)

    finally:
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('use_pattern', [True, False])
def test_oracle_cdc_to_jdbc_producer(sdc_builder, sdc_executor, database, buffer_locally, use_pattern):
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

        start_pipeline_cmd = sdc_executor.start_pipeline(pipeline)
        start_pipeline_cmd.wait_for_pipeline_batch_count(1)

        assert [tuple(row.values()) for row in inserts] == _select_from_table(db_engine=db_engine, dest_table=dest_table)

        updates = _update(connection=connection, table=src_table, count=batch_size).rows
        start_pipeline_cmd.wait_for_pipeline_batch_count(2)

        assert [tuple(row.values()) for row in updates] == _select_from_table(db_engine=db_engine, dest_table=dest_table)

        _delete(connection=connection, table=src_table, count=batch_size)
        start_pipeline_cmd.wait_for_pipeline_batch_count(3)

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
@pytest.mark.parametrize('use_pattern', [True, False])
def test_rollback_to_savepoint(sdc_builder, sdc_executor, database, buffer_locally, use_pattern):
    """Test that writes some data, then creates a save point, writes some more data and then rolls back to savepoint,
    and validates that only the data that is before the save point and after the rollback is read
    Runs oracle_cdc_client >> trash
    """
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
                                                          src_table_name=src_table_pattern)
        trash = pipeline_builder.add_stage('Trash')
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

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot
        # assert all the data captured have the same raw_data
        output_records = snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output
        assert len(output_records) == 5
        assert output_records[0].field[PRIMARY_KEY] == 1
        assert output_records[0].field[OTHER_COLUMN] == 'MORDOR'
        assert output_records[0].header['values']['sdc.operation.type'] == '1'
        assert output_records[1].field[PRIMARY_KEY] == 2
        assert output_records[1].field[OTHER_COLUMN] == 'GONDOR'
        assert output_records[1].header['values']['sdc.operation.type'] == '1'
        assert output_records[2].field[PRIMARY_KEY] == 1
        assert output_records[2].field[OTHER_COLUMN] == 'MINAS MORGUL'
        assert output_records[2].header['values']['sdc.operation.type'] == '3'
        assert output_records[3].field[PRIMARY_KEY] == 2
        assert output_records[3].field[OTHER_COLUMN] == 'HOBBITON'
        assert output_records[3].header['values']['sdc.operation.type'] == '3'
        assert output_records[4].field[PRIMARY_KEY] == 3
        assert output_records[4].field[OTHER_COLUMN] == 'GONDOR'
        assert output_records[4].header['values']['sdc.operation.type'] == '1'

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@sdc_min_version('3.0.0.0')
@database('oracle')
# https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1821
# We don't support UriType (requires difficult workaround in JDBC)
# We don't support timezone types
# We don't suppport float/double
# And general LOB things (clob, blob, long, nclob)
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', [
    ('number','1', 'DECIMAL', '1'),
    ('char(2)', "'AB'", 'STRING', 'AB'),
    ('varchar(4)', "'ABCD'", 'STRING', 'ABCD'),
    ('varchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
    ('nchar(3)',"'NCH'", 'STRING', 'NCH'),
    ('nvarchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
#    ('binary_float', '1.0', 'FLOAT', '1.0'),
#    ('binary_double', '2.0', 'DOUBLE', '2.0'),
    ('date', "TO_DATE('1998-1-1 6:22:33', 'YYYY-MM-DD HH24:MI:SS')", 'DATETIME', 883635753000),
    ('timestamp', "TIMESTAMP'1998-1-2 6:00:00'", 'DATETIME', 883720800000),
#    ('timestamp with time zone', "TIMESTAMP'1998-1-3 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-03T06:00:00-05:00'),
#    ('timestamp with local time zone', "TIMESTAMP'1998-1-4 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-04T07:00:00Z'),
#    ('long', "'LONG'", 'STRING', 'LONG'),
#    ('blob', "utl_raw.cast_to_raw('BLOB')", 'BYTE_ARRAY', 'QkxPQg=='),
#    ('clob', "'CLOB'", 'STRING', 'CLOB'),
#    ('nclob', "'NCLOB'", 'STRING', 'NCLOB'),
#    ('XMLType', "xmltype('<a></a>')", 'STRING', '<a></a>')
])
def test_all_types(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value):
    """Test all feasible Oracle types in the CDC origin."""
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
                                               src_table_name=table_name)
        trash = builder.add_stage('Trash')
        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # And insert a row with actual value
        txn = connection.begin()
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")
        txn.commit()
        _wait_until_time(_get_current_oracle_time(connection=connection))

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

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


@sdc_min_version('3.0.0.0')
@database('oracle')
def test_event_startup(sdc_builder, sdc_executor, database):
    """Verify that we create at least one event - STARTUP - in the origin. We don't check all types as that requires
       additional configuration on the shared database server (redo logs dictionary source).
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        builder = sdc_builder.get_pipeline_builder()
        origin = _get_oracle_cdc_client_origin(connection=connection,
                                               database=database,
                                               sdc_builder=sdc_builder,
                                               pipeline_builder=builder,
                                               buffer_locally=True,
                                               src_table_name=table_name)
        trash = builder.add_stage('Trash')
        # TLKT-248: Add ability to directly read events from snapshots
        identity = builder.add_stage('Dev Identity')
        event_trash = builder.add_stage('Trash')
        origin >> trash
        origin >= identity
        identity >> event_trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Create table & insert one row
        connection.execute(f"""CREATE TABLE {table_name}(id number primary key)""")
        txn = connection.begin()
        connection.execute(f"INSERT INTO {table_name} VALUES(1)")
        txn.commit()
        _wait_until_time(_get_current_oracle_time(connection=connection))

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        assert len(snapshot[identity].output) == 1
        assert snapshot[identity].output[0].header['values']['sdc.event.type'] == 'STARTUP'

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@database('oracle')
@pytest.mark.parametrize('initial_change', ['DATE', 'SCN', 'LATEST'])
def test_initial_change(sdc_builder, sdc_executor, database, initial_change):
    """Test Initial Change config.

    The test performs 3 database transactions T1, T2 and T3, and the pipeline is started at a time in between
    transactions T2 and T3. We expect the following outcome:
    - When initial_change is 'DATE' or 'SCN', the test configures the Oracle CDC stage to read from
      transaction T2, and consequently checks that the consumed data correspond to T2+T3.
    - When initial_change is 'LATEST', the expected outcome is to read only data from T3.

    Pipeline: oracle_cdc >> trash

    """
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
                                                   src_table_name=table_name,
                                                   batch_size=1,
                                                   **test_options)
        trash = builder.add_stage('Trash')
        oracle_cdc >> trash
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and commit a third transaction after that.
        snapshot_cmd = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True, wait=False,
                                                     batches=len(expected_data), batch_size=1)
        txn3 = connection.begin()
        for i in txn3_data:
            connection.execute(f'INSERT INTO {table_name} VALUES({i})')
        txn3.commit()

        # Check the data consumed by the pipeline is the expected one.
        snapshot = snapshot_cmd.wait_for_finished().snapshot
        sdc_records = [snapshot.snapshot_batches[i][oracle_cdc.instance_name].output[0]
                       for i in range(len(snapshot))]
        consumed_data = [rec.field['ID'].value for rec in sdc_records]
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        assert sorted(consumed_data) == expected_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


@sdc_min_version('3.14.0')
@database('oracle')
def test_dictionary_extraction(sdc_builder, sdc_executor, database):
    """Test extraction of the LogMiner dictionary from the existing redo logs.

    This test configures an Oracle CDC stage with 'DICT_FROM_REDO_LOGS' as dictionary source, builds the
    dictionary, and check that the Oracle CDC is able to use the fresh dictionary to recover the data inserted
    in the database before the pipeline is started.

    Pipeline: oracle_cdc >> trash

    """
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
                                 src_table_name=table_name,
                                 initial_change='SCN',
                                 start_scn=start_scn,
                                 dictionary_source='DICT_FROM_REDO_LOGS')

        if Version(sdc_builder.version) < Version('3.16.0'):
            # In versions < 3.16 the user has to define a maximum time to look back for a valid
            # dictionary. From 3.16 onward this is not required anymore.
            oracle_cdc_params['duration_of_directory_extraction'] = '${2 * MINUTES}'

        oracle_cdc = _get_oracle_cdc_client_origin(**oracle_cdc_params)
        trash = builder.add_stage('Trash')
        oracle_cdc >> trash
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and check the data consumed by the pipeline is the expected one.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, timeout_sec=120,
                                                 batches=len(input_data), batch_size=1).snapshot
        sdc_executor.stop_pipeline(pipeline, force=True)
        sdc_records = [snapshot.snapshot_batches[i][oracle_cdc.instance_name].output[0]
                       for i in range(len(snapshot))]
        consumed_data = [rec.field['ID'].value for rec in sdc_records]
        assert sorted(consumed_data) == input_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


@database('oracle')
@pytest.mark.parametrize('dictionary_source', ['DICT_FROM_REDO_LOGS', 'DICT_FROM_ONLINE_CATALOG'])
def test_logminer_session_switch(sdc_builder, sdc_executor, database, dictionary_source):
    """Test LogMiner session switches.

    In particular we want to check the stage is able to open subsequent LogMiner session windows after the
    initial one, especially when using DICT_FROM_ONLINE_CATALOG, which involves keeping the LogMiner
    dictionary between sessions.

    To ensure Oracle CDC actually switches between LogMiner sessions, we perform two database transactions
    which are separated in time by more than the configured LogMiner session window, and configure Oracle CDC
    to consume both transactions.

    Pipeline: oracle_cdc >> trash

    """
    table_name = f'STF_{get_random_string(string.ascii_uppercase)}'
    connection = database.engine.connect()
    txn1_data = [1, 2, 3, 4, 5]
    txn2_data = [11, 12, 13, 14, 15]
    session_window_secs = 10

    try:
        # Create and populate table. Use 2 transactions that will be separated in time by more than the
        # configured LogMiner session window. This will force they must be consumed in different LogMiner
        # sessions.
        logger.info('Create and populate table %s...', table_name)
        connection.execute(f'CREATE TABLE {table_name} (ID NUMBER PRIMARY KEY)')
        sleep(3)  # Avoiding Oracle CDC to create a DDL event for the previous CREATE TABLE until TLKT-467 be
                  # solved.
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
                                                   src_table_name=table_name,
                                                   initial_change='DATE',
                                                   start_date=start_date,
                                                   dictionary_source=dictionary_source,
                                                   maximum_transaction_length=session_window_secs - 1,
                                                   logminer_session_window=session_window_secs)
        trash = builder.add_stage('Trash')
        oracle_cdc >> trash
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline, populate table with data 1 and wait for the pipeline to consume it.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=True, timeout_sec=240,
                                                 batches=len(txn1_data + txn2_data), batch_size=1).snapshot
        sdc_executor.stop_pipeline(pipeline, force=True)
        sdc_records = [snapshot.snapshot_batches[i][oracle_cdc.instance_name].output[0]
                       for i in range(len(snapshot))]
        consumed_data = [rec.field['ID'].value for rec in sdc_records]
        assert sorted(consumed_data) == txn1_data + txn2_data

    finally:
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


def _get_oracle_cdc_client_origin(connection, database, sdc_builder, pipeline_builder, buffer_locally,
                                  src_table_name, batch_size=BATCH_SIZE, **kwargs):
    kwargs.setdefault('dictionary_source', 'DICT_FROM_ONLINE_CATALOG')
    kwargs.setdefault('logminer_session_window', '${10 * MINUTES}')
    kwargs.setdefault('db_time_zone', 'UTC')
    kwargs.setdefault('maximum_transaction_length', '${1 * MINUTES}')
    kwargs.setdefault('initial_change', 'DATE')
    if Version('3.14.0') <= Version(sdc_builder.version) < Version('3.16.0'):
        # In versions < 3.16 the user has to define a maximum time to look back for a valid dictionary. From
        # 3.16 onward this is not required anymore. By default avoid to set an specific duration and use all
        # the redo logs instead.
        kwargs.setdefault('duration_of_directory_extraction', -1)

    start = _get_current_oracle_time(connection=connection)
    kwargs.setdefault('start_date', start.strftime('%d-%m-%Y %H:%M:%S'))

    # The time at the oracle db and the node executing the test may not have the exact same time.
    # So wait until this node reaches that time (including the timezone offset),
    # otherwise validation will fail because the origin thinks the
    # start time is in the future.
    _wait_until_time(time=start)
    logger.info('Start Date is %s', kwargs['start_date'])

    oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')

    if Version(sdc_builder.version) >= Version('3.1.0.0'):
        tables = [{'schema': database.username.upper(), 'table': src_table_name, 'excludePattern': ''}]
    else:
        oracle_cdc_client.set_attributes(schema_name=database.username.upper())
        tables = [src_table_name]

    return oracle_cdc_client.set_attributes(buffer_changes_locally=buffer_locally,
                                            max_batch_size_in_records=batch_size,
                                            tables=tables,
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
