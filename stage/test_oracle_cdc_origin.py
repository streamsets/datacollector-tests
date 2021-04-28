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
import uuid

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
BATCH_SIZE = 10  # Max limit imposed
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
    Runs oracle_cdc_client >> wiretap.destination
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

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        # assert all the data captured have the same raw_data
        assert len(wiretap.output_records) == 1
        attributes = wiretap.output_records[0].get_field_attributes(f'/{OTHER_COLUMN}')
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
      True)  oracle_cdc_client >> wiretap.destination
      False) oracle_cdc_client >> sql_parser >> wiretap.destination

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
        txn = connection.begin()
        for line in lines:
            connection.execute(line)
        txn.commit()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time).
        _wait_until_time(_get_current_oracle_time(connection=connection))

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(2)
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

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
@pytest.mark.parametrize('use_pattern', [True, False])
def test_oracle_cdc_client_basic(sdc_builder, sdc_executor, database, buffer_locally, use_pattern):
    """Basic test that reads inserts/updates/deletes to an Oracle table,
    and validates that they are read in the same order.
    Runs oracle_cdc_client >> wiretap
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

        wiretap = pipeline_builder.add_wiretap()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(change_count)

        row_index = 0
        op_index = 0
        # assert all the data captured have the same raw_data
        for record in wiretap.output_records:
            assert row_index == int(record.field['ID'].value)
            assert rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index]
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


@pytest.mark.parametrize('buffer_locally', [True, False])
@database('oracle')
def test_oracle_cdc_client_bulk(sdc_builder, sdc_executor, database, buffer_locally):
    """Test that reads inserts/updates/deletes to an Oracle table with bulk sentences,
    and verifies that no record is lost .
    Runs oracle_cdc_client >> wiretap
    """

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
                                         logminer_session_window='${10 * MINUTES}',
                                         maximum_transaction_length='${2 * MINUTES}',
                                         db_time_zone='UTC',
                                         max_batch_size_in_records=1,
                                         initial_change='SCN',
                                         start_scn=database_last_scn)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3 * number_of_rows)

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


@sdc_min_version('4.0.0')
@pytest.mark.parametrize('buffer_locally', [True, False])
@database('oracle')
def test_oracle_cdc_headers(sdc_builder, sdc_executor, database, buffer_locally):
    """
    Test to check all headers arepresent in the output records.
    """

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
                                         logminer_session_window='${10 * MINUTES}',
                                         maximum_transaction_length='${2 * MINUTES}',
                                         db_time_zone='UTC',
                                         max_batch_size_in_records=1,
                                         initial_change='SCN',
                                         start_scn=database_last_scn,
                                         send_redo_query_in_headers=True)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3 * number_of_rows)

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

    finally:

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@pytest.mark.parametrize('buffer_locally', [True, False])
@database('oracle')
def test_oracle_cdc_client_preview_and_run(sdc_builder, sdc_executor, database, buffer_locally):
    """Basic test that reads inserts first via preview and then run and preview again returning records
        Perform update/deletes and see run correctly picks up from where it left off and do a preview
        again to see preview still returns the inserts, updates and deletes all
        Runs oracle_cdc_client >> wiretap
    """
    db_engine = database.engine
    table = None
    src_table_name = get_random_string(string.ascii_uppercase, 9)

    try:
        connection = database.engine.connect()
        table = _setup_table(database=database,
                             table_name=src_table_name)
        start_scn = _get_last_scn(connection)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = _get_oracle_cdc_client_origin(connection=connection,
                                                          database=database,
                                                          sdc_builder=sdc_builder,
                                                          pipeline_builder=pipeline_builder,
                                                          buffer_locally=buffer_locally,
                                                          logminer_session_window='${4 * MINUTES}',
                                                          src_table_name=src_table_name,
                                                          initial_change='SCN',
                                                          start_scn=start_scn)
        wiretap = pipeline_builder.add_wiretap()
        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Insert 3 records
        inserts = _insert(connection=connection, table=table)

        rows = inserts.rows
        cdc_op_types = inserts.cdc_op_types
        sdc_op_types = inserts.sdc_op_types
        change_count = inserts.change_count

        # Preview should return 3 records
        preview = sdc_executor.run_pipeline_preview(pipeline, batches=len(rows), batch_size=1, timeout=30000).preview
        assert preview is not None
        assert preview.issues.issues_count == 0
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches]
        assert len(preview_records) == len(rows)

        logger.debug('Count {}'.format(len(rows)))
        row_index = 0
        op_index = 0

        for record in preview_records:
            assert row_index == int(record.field['ID'].value)
            assert rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index]
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == change_count

        # Run pipeline and capture the output, we should see 3 inserts
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(rows))

        row_index = 0
        op_index = 0
        # assert all the data captured have the same raw_data
        for record in wiretap.output_records:
            assert row_index == int(record.field['ID'].value)
            assert rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index]
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == change_count

        sdc_executor.stop_pipeline(pipeline, force=True)

        # Do preview again and make sure preview still returns the 3 inserts
        preview = sdc_executor.run_pipeline_preview(pipeline, batches=len(rows), batch_size=1, timeout=30000).preview
        assert preview is not None
        assert preview.issues.issues_count == 0
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches]
        assert len(preview_records) == len(rows)

        row_index = 0
        op_index = 0

        for record in preview_records:
            assert row_index == int(record.field['ID'].value)
            assert rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == cdc_op_types[op_index]
            row_index = (row_index + 1) % 3
            op_index += 1
        assert op_index == change_count

        # Do more transactions (3 updates and 3 deletes)
        updates = _update(connection=connection, table=table)

        new_rows = updates.rows
        new_cdc_op_types = updates.cdc_op_types
        new_sdc_op_types = updates.sdc_op_types
        new_change_count = updates.change_count

        deletes = _delete(connection=connection, table=table)

        # deletes should have the last state of the row, so it would be the what comes from the updates.
        new_rows += updates.rows
        new_cdc_op_types += deletes.cdc_op_types
        new_sdc_op_types += deletes.sdc_op_types
        new_change_count += deletes.change_count

        merged_rows = rows + new_rows
        merged_sdc_op_types = sdc_op_types + new_sdc_op_types
        merged_cdc_op_types = cdc_op_types + new_cdc_op_types
        merged_change_count = change_count + new_change_count

        # Make sure preview return all 9 records
        preview_command = sdc_executor.run_pipeline_preview(pipeline,
                                                            batches=len(merged_rows),
                                                            batch_size=1,
                                                            timeout=30000)
        preview = preview_command.preview

        assert preview is not None
        assert preview.issues.issues_count == 0
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches]
        assert len(preview_records) == len(merged_rows)

        row_index = 0
        op_index = 0

        for record in preview_records:
            assert row_index == int(record.field['ID'].value)
            assert merged_rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == merged_sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == merged_cdc_op_types[op_index]
            row_index = (row_index + 1) % 3
            op_index += 1

        assert op_index == merged_change_count

        wiretap.reset()

        # If we run the pipeline and capture the output, we should see only the updates and deletes
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(new_rows))

        row_index = 0
        op_index = 0
        # assert all the data captured have the same raw_data
        for record in wiretap.output_records:
            assert row_index == int(record.field['ID'].value)
            assert new_rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == new_sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == new_cdc_op_types[op_index]
            row_index = (row_index + 1) % 3
            op_index += 1

        assert op_index == new_change_count
        sdc_executor.stop_pipeline(pipeline, force=True)

        # Make sure preview still return all 9 records
        preview_command = sdc_executor.run_pipeline_preview(pipeline,
                                                            batches=len(merged_rows),
                                                            batch_size=1,
                                                            timeout=30000)
        preview = preview_command.preview

        assert preview is not None
        assert preview.issues.issues_count == 0
        preview_records = [batch[oracle_cdc_client.instance_name].output[0] for batch in preview.preview_batches]
        assert len(preview_records) == len(merged_rows)

        row_index = 0
        op_index = 0

        for record in preview_records:
            assert row_index == int(record.field['ID'].value)
            assert merged_rows[op_index]['NAME'] == record.field['NAME'].value
            assert int(record.header.values['sdc.operation.type']) == merged_sdc_op_types[op_index]
            assert record.header.values['oracle.cdc.operation'] == merged_cdc_op_types[op_index]
            row_index = (row_index + 1) % 3
            op_index += 1

        assert op_index == merged_change_count

    finally:
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
    Runs oracle_cdc_client >> wiretap
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

        wiretap = pipeline_builder.add_wiretap()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(rows))


        # assert all the data captured have the same raw_data
        output = wiretap.output_records
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
def test_long_sql_statements(sdc_builder, sdc_executor, database):
    """Test Oracle CDC correctly handles long SQL statements.

    When querying LogMiner content (i.e. V$LOGMNR_CONTENT view), any database operation whose SQL statement is
    longer than 4000 bytes is split into several records. In that case, Oracle CDC must join the partial SQL
    statements to build the corresponding SDC record.

    This test creates several SQL insertions longer than 4000 bytes and checks Oracle CDC correctly generates
    the corresponding records, with no data loss.

    Pipeline: oracle_cdc >> wiretap

    """
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
                                                   src_table_name=table_name,
                                                   batch_size=1,
                                                   initial_change='SCN',
                                                   start_scn=start_scn,
                                                   parse_sql_query=True)
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(input_data))
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        # Check there is no data loss.
        sdc_records = [record.field for record in wiretap.output_records]
        assert sdc_records == input_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        table.drop(database.engine)


@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True])
def test_overlapping_transactions(sdc_builder, sdc_executor, database, buffer_locally):
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

        wiretap = pipeline_builder.add_wiretap()

        # Why do we need to wait?
        # The time at the DB might differ from here. If the DB is behind, we are ok, and we will get all the data.
        # If the DB is ahead, the batch end time the origin may not be after all the changes were written to the DB.
        # So we wait until the time here is past the time at which all data was written out to the DB (current time)
        _wait_until_time(_get_current_oracle_time(connection=connection))

        oracle_cdc_client >> wiretap.destination
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(rows_c1))
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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(rows_c2))
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        # assert all the data captured have the same raw_data
        output = wiretap.output_records
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
    Runs oracle_cdc_client >> wiretap
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

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(5)

        # assert all the data captured have the same raw_data
        output_records = wiretap.output_records
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
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if table is not None:
            table.drop(db_engine)
            logger.info('Table: %s dropped.', src_table_name)


@sdc_min_version('3.6.0')
@database('oracle')
@pytest.mark.parametrize('sql_type, insert_fragment, expected_values', [
    ('binary_float', '1.0', ['1.0E+000']),
    ('binary_double', '1.0', ['1.0E+000']),
    ('timestamp with time zone', "TIMESTAMP'1998-1-3 6:00:00-5:00'", ['1998-01-03T06:00:00-05:00']),
    ('timestamp with local time zone', "TIMESTAMP'1998-1-4 6:00:00-5:00'", ['1998-01-04T11:00:00Z[UTC]']),
    ('long', "'VALUE EXAMPLE'", ['VALUE EXAMPLE']),
    ('blob', "utl_raw.cast_to_raw('BLOB')", ['EMPTY_BLOB()', "HEXTORAW('424c4f42')"]),
    ('clob', "'VALUE EXAMPLE'", ['EMPTY_CLOB()', 'VALUE EXAMPLE']),
    ('nclob', "'VALUE EXAMPLE'", ['EMPTY_CLOB()', 'VALUE EXAMPLE']),
    # ('XMLType', "xmltype('<a></a>')", ['<a></a>'])  # Not fully supported by LogMiner
])
def test_unsupported_types_send_to_pipeline(sdc_builder, sdc_executor, database, sql_type, insert_fragment,
                                            expected_values):
    """Test Oracle types not supported by the CDC origin when they are sent to pipeline.

    The test creates a table containing an unsupported type and check the record is properly generated with
    the unsuported type value returned as a raw string field, according to the configuration
    (unsupported_field_type = 'SEND_TO_PIPELINE', add_unsupported_fields_to_records = True).

    NOTE: parameter 'expected_values' is a list because for some data types Oracle generates two CDC records
    per row insertion (e.g. BLOB type). The first CDC record is a SQL insertion with a void value
    (e.g. EMPTY_BLOB()), the second one is a SQL update with the actual value (e.g. HEXTORAW('424c4f42')).

    More info about the supported Oracle data types:
    - https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm

    Pipeline: oracle_cdc >> wiretap

    """
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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(expected_output))
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        actual_output = [record.field
                         for record in wiretap.output_records]
        assert actual_output == expected_output

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.6.0')
@database('oracle')
@pytest.mark.parametrize('action', ['TO_ERROR', 'DISCARD'])
def test_unsupported_types_other_actions(sdc_builder, sdc_executor, database, action):
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
    connection = database.engine.connect()
    base_name = get_random_string(string.ascii_lowercase, 20)
    table_name1 = f'{base_name}_table1'
    table_name2 = f'{base_name}_table2'
    input_value1 = "utl_raw.cast_to_raw('BLOB')"
    input_value2 = 'just plain text'
    expected_output = [{'ID': 1, 'DATA_COLUMN': input_value2}]
    if action == 'TO_ERROR':
        # For BLOB insertions, Oracle logs two CDC records: a SQL insertion with an empty blob, then a SQL
        # update over the inserted row with the actual value.
        expected_error_output = [{'ID': 1, 'DATA_COLUMN': 'EMPTY_BLOB()'},
                                 {'ID': 1, 'DATA_COLUMN': "HEXTORAW('424c4f42')"}]
    else:
        # When action is 'DISCARD' the stage ignores CDC records for tables with unsupported types.
        expected_error_output = []

    try:
        # Create tables and insert one record into each table.
        connection.execute(f'CREATE TABLE {table_name1} (ID NUMBER PRIMARY KEY, DATA_COLUMN BLOB NULL)')
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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(expected_output))

        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

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
@pytest.mark.parametrize('peg_parser', [True, False])
def test_unsupported_types_adt(sdc_builder, sdc_executor, database, peg_parser):
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
                status.wait_for_pipeline_output_records_count(num_records)
                sdc_executor.stop_pipeline(pipeline)
                for i in range(num_records):
                    assert wiretap.output_records[i].field['ID_EX'] == i
                    assert wiretap.output_records[i].field['NAME_EX'] == 'TONI'
            else:
                # With default parser throws the exception SDC-15822"""
                with pytest.raises(sdc_api.RunningError) as exception_info:
                    status.wait_for_status('RUN_ERROR', timeout_sec=300)
                assert 'JDBC_93 - ' in f'{exception_info.value}'
                assert 'UPDATE' in f'{exception_info.value}'
        else:
            # Version 11 LogMiner does not support Redo Logs for ADT columns, we check we have no output records
            connection.execute(f"TRUNCATE TABLE {table_name}");
            status.wait_for_status('FINISHED')
            assert len(wiretap.output_records) == 0

    finally:
        logger.info('Dropping types %s and %s and table %s ...',
                    f'ARRAY_{table_name}', f'TPE_{table_name}', f'TABLE {table_name}')
        connection.execute(f'DROP TABLE {table_name}')
        connection.execute(f'DROP TYPE TPE_{table_name}')
        connection.execute(f'DROP TYPE ARRAY_{table_name}')


@sdc_min_version('3.20.0')
@database('oracle')
@pytest.mark.parametrize('peg_parser', [True, False])
def test_unsupported_types_empty_redo_log(sdc_builder, sdc_executor, database, peg_parser):
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
        status = sdc_executor.start_pipeline(pipeline);
        connection.execute(f"UPDATE {table_name} set ADT_EX = TPE_{table_name}("f"ARRAY_{table_name}(null),"
                           f"'EXAMPLE')")

        status.wait_for_pipeline_error_records_count(num_records)
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
def test_empty_redo_log_record_is_ignored(sdc_builder, sdc_executor, database):
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
        status = sdc_executor.start_pipeline(pipeline);

        connection.execute(f"UPDATE {table_name} set ADT_EX = TPE_{table_name}("f"ARRAY_{table_name}(null),"
                           f"'EXAMPLE')")
        connection.execute(f"UPDATE {table_name} SET NAME_EX = 'TONI'");

        status.wait_for_pipeline_output_records_count(num_records)

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
def test_event_startup(sdc_builder, sdc_executor, database):
    """Verify that we create at least one event - STARTUP - in the origin. We don't check all types as that requires
       additional configuration on the shared database server (redo logs dictionary source).
    """
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

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        assert len(event_wiretap.output_records) == 1
        assert event_wiretap.output_records[0].header.values['sdc.event.type'] == 'STARTUP'

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f"DROP TABLE {table_name}")


@sdc_min_version('3.1.0.0')
@database('oracle')
def test_oracle_cdc_exclusion_pattern(sdc_builder, sdc_executor, database):
    """Test Oracle CDC table exclusion patterns.

    The test configures an Oracle CDC stage to consume tables matching a defined name pattern, but excluding
    any table among them matching the exclusion name pattern (see `table_config` variable for this
    configuration). Then two tables are created and populated, one of them with a name that should be excluded
    according to the configuration. The Oracle CDC output (records and events) is examined to validate this
    behavior.

    Pipeline: oracle_cdc >> wiretap
              oracle_cdc >= wiretap

    """
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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(sports_data2))
        sdc_executor.stop_pipeline(pipeline, force=True)

        sdc_records = [(record.field['ID'], record.field['PLAYER'], record.field['SPORT'])
                       for record in wiretap.output_records]

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
def test_oracle_cdc_mining_new_table(sdc_builder, sdc_executor, database):
    """Test Oracle CDC can track new tables created after the pipeline initialization.

    Besides to validate the origin consumes the records inserted in the new table, the test also validates the
    corresponding DDL events are also created (CREATE and TRUNCATE events).

    Pipeline: oracle_cdc >> wiretap
              oracle_cdc >= wiretap

    """
    table_prefix = f'STF_{get_random_string(string.ascii_uppercase)}'
    table_pattern = f'{table_prefix}%'
    sports_table = f'{table_prefix}_SPORTS'
    connection = database.engine.connect()

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
                                                   src_table_name=table_pattern,
                                                   initial_change='LATEST',
                                                   dictionary_source='DICT_FROM_REDO_LOGS')
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        wiretap_evts = builder.add_wiretap()
        oracle_cdc >= wiretap_evts.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline, create table and populate
        status = sdc_executor.start_pipeline(pipeline)

        connection.execute(f'CREATE TABLE {sports_table} (ID NUMBER PRIMARY KEY, '
                           'PLAYER VARCHAR2(50), SPORT VARCHAR2(50))')
        for id, name, sport in sports_data1:
            connection.execute(f"INSERT INTO {sports_table} VALUES({id}, '{name}', '{sport}')")

        connection.execute(f'TRUNCATE TABLE {sports_table}')
        for id, name, sport in sports_data2:
            connection.execute(f"INSERT INTO {sports_table} VALUES({id}, '{name}', '{sport}')")

        status.wait_for_pipeline_output_records_count(len(sports_data1+sports_data2))
        sdc_executor.stop_pipeline(pipeline, force=True)

        sdc_events = [(event.header.values['oracle.cdc.table'],
                       event.header.values['sdc.event.type'],
                       event.field)
                      for event in wiretap_evts.output_records]
        assert sdc_events == expected_events

        sdc_records = [(record.field['ID'], record.field['PLAYER'], record.field['SPORT'])
                       for record in wiretap.output_records]
        assert sdc_records == sports_data1 + sports_data2

    finally:
        logger.info('Dropping table %s in %s database ...', sports_table, database.type)
        connection.execute(f'DROP TABLE {sports_table}')


@database('oracle')
def test_oracle_cdc_ignores_dropped_table(sdc_builder, sdc_executor, database):
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
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(sports_data))
        sdc_executor.stop_pipeline(pipeline, force=True)

        sdc_events = [(event.header.values['oracle.cdc.table'],
                       event.header.values['sdc.event.type'],
                       event.field)
                      for event in wiretap_evts.output_records]
        assert sdc_events == expected_events

        sdc_records = [(record.field['ID'], record.field['PLAYER'], record.field['SPORT'])
                       for record in wiretap.output_records]
        assert sdc_records == sports_data

    finally:
        logger.info('Dropping table %s in %s database ...', sports_table, database.type)
        connection.execute(f'DROP TABLE {sports_table}')


@database('oracle')
@pytest.mark.parametrize('initial_change', ['DATE', 'SCN', 'LATEST'])
def test_initial_change(sdc_builder, sdc_executor, database, initial_change):
    """Test Initial Change config.

    The test performs 3 database transactions T1, T2 and T3, and the pipeline is started at a time in between
    transactions T2 and T3. We expect the following outcome:
    - When initial_change is 'DATE' or 'SCN', the test configures the Oracle CDC stage to read from
      transaction T2, and consequently checks that the consumed data correspond to T2+T3.
    - When initial_change is 'LATEST', the expected outcome is to read only data from T3.

    Pipeline: oracle_cdc >> wiretap

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
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(expected_data))
        consumed_data = [rec.field['ID'].value for rec in wiretap.output_records]
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

    Pipeline: oracle_cdc >> wiretap

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
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and check the data consumed by the pipeline is the expected one.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(input_data))
        sdc_executor.stop_pipeline(pipeline, force=True)
        consumed_data = [rec.field['ID'].value for rec in wiretap.output_records]
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

    Pipeline: oracle_cdc >> wiretap

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
        wiretap = builder.add_wiretap()
        oracle_cdc >> wiretap.destination
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline, populate table with data 1 and wait for the pipeline to consume it.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(txn1_data+txn2_data), timeout_sec=380)
        sdc_executor.stop_pipeline(pipeline, force=True)
        consumed_data = [record.field['ID'].value for record in wiretap.output_records]
        assert sorted(consumed_data) == txn1_data + txn2_data

    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')


@sdc_min_version('3.19.0')
@database('oracle')
@pytest.mark.parametrize('buffer_locally', [True, False])
@pytest.mark.parametrize('dictionary_source', ['DICT_FROM_REDO_LOGS', 'DICT_FROM_ONLINE_CATALOG'])
def test_disable_continuous_mine(sdc_builder, sdc_executor, database, keep_data, buffer_locally, dictionary_source):
    """Simple test to check "Disable Continuous Mine" stage option.

    The test just inserts a few records in the test table and check they are successfully consumed by Oracle
    CDC. This is tested with different configurations which affect how LogMiner sessions are set.

    Pipeline: oracle_cdc >> wiretap

    """
    table_name = f'STF_{get_random_string(string.ascii_uppercase)}'
    num_records = 10
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
                                                   buffer_locally=buffer_locally,
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

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(num_records, timeout_sec=360)
        sdc_executor.stop_pipeline(pipeline, force=True)

        output_values = [rec.field['ID'].value for rec in wiretap.output_records]
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


@sdc_min_version('3.22.0')
@database('oracle')
@pytest.mark.parametrize('action', ['TO_ERROR', 'DISCARD'])
def test_jdbc_52_error_format(sdc_builder, sdc_executor, database, action):
    """Basic test to check the proper formatting of error JDBC_52.

    The test creates a user guest/guest with just CONNECT privileges. This is enough to force an error when starting the Oracle Log Miner.
    Whw compare then the produced error with the expected one. The test finishes removing the guest user to reset the database state.

    Pipeline: oracle_cdc >> wiretap

    """

    try:
        logger.info("Starting test for JDBC_52 error")

        expected_message = "JDBC_52 - Error starting LogMiner: Action: Start generator thread - Message:"

        engine = database.engine
        connection = engine.connect()
        table = None
        pipeline = None

        # As the Oracle 19 environment is a multitenant container database, Oracle mandates to have the user name start with C##
        # https://community.oracle.com/tech/developers/discussion/4288833/ora-65096-invalid-common-user-or-role-name
        guest_username = f"C##_GUEST_{get_random_string(string.ascii_uppercase, 16)}"
        guest_password = f"C##_guest_{get_random_string(string.ascii_uppercase, 16)}"

        # Create guest user
        logger.info(f"Creating user {guest_username} in database...")
        connection.execute(f"CREATE USER {guest_username} IDENTIFIED BY {guest_password}")
        connection.execute(f"GRANT CONNECT TO {guest_username}")
        connection.execute(f"GRANT SELECT_CATALOG_ROLE TO {guest_username}")

        # Create source table
        src_table_name = get_random_string(string.ascii_uppercase, 9)
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
        pipeline = pipeline_builder.build("Oracle CDC: JDBC_52 error format").configure_for_environment(database)
        pipeline.configuration["shouldRetry"] = False

        # Assign a non privileged user to the pipeline
        oracle_cdc_client.set_attributes(username=guest_username,
                                         password=guest_password)

        # Create the pipeline
        sdc_executor.add_pipeline(pipeline)

        # Execute the pipeline (it should stop with StageExcception)
        with pytest.raises(Exception):
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
            if pipeline is not None:
                if sdc_executor.get_pipeline_status(pipeline).response.json().get("status") == "RUNNING":
                    sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get("status")
        message = sdc_executor.get_pipeline_status(pipeline).response.json().get("message")

        assert "RUN_ERROR" == status
        assert message.startswith(expected_message)
    finally:
        # Drop guest user
        try:
            logger.info(f"Dropping user {guest_username} in database...")
            connection.execute(f"DROP USER {guest_username}")
        finally:
            # In an Oracle multitenant container databse (CDB), the user or table will not be created dur
            # to enforced named constraints. This is not a real problem as we just want to fire somre
            # internal error starting CDC
            pass

        # Stop pipeline
        if pipeline is not None:
            if sdc_executor.get_pipeline_status(pipeline).response.json().get("status") == "RUNNING":
                sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        # Drop source table
        if table is not None:
            try:
                logger.info("Dropping source table table %s", src_table_name)
                table.drop(engine)
            finally:
                # Pass for the same reasons above
                pass

def _get_oracle_cdc_client_origin(connection, database, sdc_builder, pipeline_builder, buffer_locally,
                                  src_table_name=None, batch_size=BATCH_SIZE, **kwargs):
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
                                            max_batch_size_in_records=batch_size,
                                            **kwargs)


def _get_oracle_db_version(connection):
    """Returns an array of numbers where the first position is the major version"""
    db_version = connection.execute("SELECT version FROM product_component_version").fetchall()[0][0]
    str_version_list = db_version.split(".")
    version_list = [int(i) for i in str_version_list]
    return version_list


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
