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
from datetime import datetime
from time import sleep

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PRIMARY_KEY = 'ID'
OTHER_COLUMN = 'NAME'
BATCH_SIZE = 12
Operations = namedtuple('Operations', ['rows', 'cdc_op_types', 'sdc_op_types', 'change_count'])


# pylint: disable=pointless-statement, too-many-locals


def setup_table(database, table_name, create_primary_key=True):
    db_engine = database.engine
    logger.info('Creating source table %s in %s database ...', table_name, database.type)

    table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                             sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=create_primary_key),
                             sqlalchemy.Column(OTHER_COLUMN, sqlalchemy.String(20)))
    table.create(db_engine)
    return table


def get_oracle_cdc_client_origin(connection, database, sdc_builder, pipeline_builder, buffer_locally, src_table_name):
    oracle_cdc_client = pipeline_builder.add_stage('Oracle CDC Client')
    start = get_current_oracle_time(connection=connection)
    start_date = start.strftime('%d-%m-%Y %H:%M:%S')

    # The time at the oracle db and the node executing the test may not have the exact same time.
    # So wait until this node reaches that time (including the timezone offset),
    # otherwise validation will fail because the origin thinks the
    # start time is in the future.
    wait_until_time(time=start)

    logger.info('Start Date is %s', start_date)

    if Version(sdc_builder.version) >= Version('3.1.0.0'):
        tables = [{'schema': database.database, 'table': src_table_name, 'excludePattern': ''}]
    else:
        oracle_cdc_client.set_attributes(schema_name=database.database)
        tables = [src_table_name]

    return oracle_cdc_client.set_attributes(buffer_changes_locally=buffer_locally,
                                            db_time_zone='UTC',
                                            dictionary_source='DICT_FROM_ONLINE_CATALOG',
                                            initial_change='DATE',
                                            logminer_session_window='${5 * MINUTES}',
                                            max_batch_size_in_records=BATCH_SIZE,
                                            maximum_transaction_length='${4 * MINUTES}',
                                            start_date=start_date,
                                            tables=tables)


def get_current_oracle_time(connection):
    return connection.execute(sqlalchemy.sql.text('SELECT SYSDATE FROM DUAL')).fetchall()[0][0]


def wait_until_time(time):
    current_time = datetime.utcnow()
    if current_time < time:
        sleep((time - current_time).total_seconds() + 1)


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
                src_table_pattern = get_table_pattern(src_table_name)
            else:
                pytest.skip('Skipping test as SDC Builder version < 3.1.0.0')
        else:
            src_table_pattern = src_table_name

        connection = database.engine.connect()
        table = setup_table(database=database,
                            table_name=src_table_name)

        logger.info('Using table pattern %s', src_table_pattern)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = get_oracle_cdc_client_origin(connection=connection,
                                                         database=database,
                                                         sdc_builder=sdc_builder,
                                                         pipeline_builder=pipeline_builder,
                                                         buffer_locally=buffer_locally,
                                                         src_table_name=src_table_pattern)

        inserts = insert(connection=connection,
                         table=table)

        rows = inserts.rows
        cdc_op_types = inserts.cdc_op_types
        sdc_op_types = inserts.sdc_op_types
        change_count = inserts.change_count

        updates = update(connection=connection,
                         table=table)

        rows += updates.rows
        cdc_op_types += updates.cdc_op_types
        sdc_op_types += updates.sdc_op_types
        change_count += updates.change_count

        deletes = delete(connection=connection,
                         table=table)

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
        wait_until_time(get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot

        row_index = 0
        op_index = 0
        # assert all the data captured have the same raw_data
        for record in snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output:
            assert row_index == int(record.value['value']['ID']['value'])
            assert rows[op_index]['NAME'] == record.value['value']['NAME']['value']
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
                src_table_pattern = get_table_pattern(src_table_name)
            else:
                pytest.skip('Skipping test as SDC Builder version < 3.1.0.0')
        else:
            src_table_pattern = src_table_name

        connection = database.engine.connect()
        table = setup_table(database=database,
                            table_name=src_table_name,
                            create_primary_key=False)

        logger.info('Using table pattern %s', src_table_pattern)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = get_oracle_cdc_client_origin(connection=connection,
                                                         database=database,
                                                         sdc_builder=sdc_builder,
                                                         pipeline_builder=pipeline_builder,
                                                         buffer_locally=buffer_locally,
                                                         src_table_name=src_table_pattern)
        rows = [{'ID': 100, 'NAME': 'NULL'},
                {'ID': None, 'NAME': 'Whose Name?'},
                {'ID': 123, 'NAME': None},
                {'ID': None, 'NAME': None}]

        connection.execute(table.insert(), rows)

        txn = connection.begin()
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
        wait_until_time(get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot

        # assert all the data captured have the same raw_data
        output = snapshot.snapshot_batches[0][oracle_cdc_client.instance_name].output
        for i, record in enumerate(output):
            # In update records, values with NULLs in the row are not returned
            if 'ID' in record.value2:
                id_val = record.value2['ID']
                assert rows[i]['ID'] == None if id_val is None else int(id_val)
            assert rows[i]['NAME'] == record.value2['NAME']

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
        table = setup_table(database=database,
                            table_name=src_table_name,
                            create_primary_key=False)

        logger.info('Using table name %s', src_table_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        oracle_cdc_client = get_oracle_cdc_client_origin(connection=connection,
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
        wait_until_time(get_current_oracle_time(connection=connection))

        oracle_cdc_client >> trash
        pipeline = pipeline_builder.build('Oracle CDC Client Pipeline').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).wait_for_finished(60).snapshot
        sdc_executor.stop_pipeline(pipeline=pipeline,
                                   force=True)

        def compare_output(output_records, rows):
            assert len(output_records) == len(rows)
            for i, output_record in enumerate(output_records):
                assert output_record.value2['ID'] == rows[i]['ID']
                assert output_record.value2['NAME'] == rows[i]['NAME']

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
                src_table_pattern = get_table_pattern(src_table_name)
            else:
                pytest.skip('Skipping test as SDC Builder version < 3.1.0.0')
        else:
            src_table_pattern = src_table_name

        connection = database.engine.connect()
        src_table = setup_table(database, src_table_name)

        pipeline_builder = sdc_builder.get_pipeline_builder()

        logger.info('Using table pattern %s', src_table_pattern)
        oracle_cdc_client = get_oracle_cdc_client_origin(connection=connection,
                                                         database=database,
                                                         sdc_builder=sdc_builder,
                                                         pipeline_builder=pipeline_builder,
                                                         buffer_locally=buffer_locally,
                                                         src_table_name=src_table_pattern)

        dest_table_name = get_random_string(string.ascii_uppercase, 9)

        dest_table = setup_table(database, dest_table_name)
        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')

        jdbc_producer.set_attributes(table_name=dest_table_name,
                                     default_operation='INSERT',
                                     # A framework bug creates a 1-element array, so remove the entry
                                     field_to_column_mapping=[])

        oracle_cdc_client >> jdbc_producer

        pipeline = pipeline_builder.build('Oracle CDC Client to JDBC Producer').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        inserts = insert(connection=connection,
                         table=src_table,
                         count=BATCH_SIZE).rows

        sleep(5)

        start_pipeline_cmd = sdc_executor.start_pipeline(pipeline)
        start_pipeline_cmd.wait_for_pipeline_batch_count(1)

        assert [tuple(row.values()) for row in inserts] == select_from_table(db_engine=db_engine, dest_table=dest_table)

        updates = update(connection=connection,
                         table=src_table,
                         count=BATCH_SIZE).rows

        start_pipeline_cmd.wait_for_pipeline_batch_count(2)

        assert [tuple(row.values()) for row in updates] == select_from_table(db_engine=db_engine, dest_table=dest_table)

        delete(connection=connection,
               table=src_table,
               count=BATCH_SIZE)

        start_pipeline_cmd.wait_for_pipeline_batch_count(3)

        assert len(select_from_table(db_engine=db_engine, dest_table=dest_table)) == 0

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline,
                                       force=True)
        if src_table is not None:
            src_table.drop(db_engine)
        if dest_table is not None:
            dest_table.drop(db_engine)


def get_table_pattern(src_table_name):
    return f'{src_table_name[:-2]}%'


def insert(connection, table, count=3):
    rows = [{'ID': i, 'NAME': get_random_string(string.ascii_uppercase, 10)} for i in range(count)]
    sdc_op_types = [1 for i in range(count)]
    cdc_op_types = ['INSERT' for i in range(count)]

    connection.execute(table.insert(), rows)
    return Operations(rows=rows,
                      cdc_op_types=cdc_op_types,
                      sdc_op_types=sdc_op_types,
                      change_count=count)


def update(connection, table, count=3):
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


def delete(connection, table, count=3):
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


def select_from_table(db_engine, dest_table):
    target_result = db_engine.execute(dest_table.select().order_by(dest_table.c[PRIMARY_KEY]))
    target_result_list = target_result.fetchall()
    target_result.close()
    return target_result_list
