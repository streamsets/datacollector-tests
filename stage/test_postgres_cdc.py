# Copyright 2018 StreamSets Inc.
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
import threading
import time
from collections import namedtuple

import pytest
import sqlalchemy
from streamsets.testframework.environments import databases
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

PRIMARY_KEY = 'id'
NAME_COLUMN = 'name'
OperationsData = namedtuple('OperationsData', ['kind', 'table', 'columnnames', 'columnvalues', 'oldkeys'])
Oldkeys = namedtuple('Oldkeys', ['keynames', 'keyvalues'])

INSERT_ROWS = [
    {PRIMARY_KEY: 0, NAME_COLUMN: 'Mbappe'},
    {PRIMARY_KEY: 1, NAME_COLUMN: 'Kane'},
    {PRIMARY_KEY: 2, NAME_COLUMN: 'Griezmann'}
]
UPDATE_ROWS = [
    {PRIMARY_KEY: 0, NAME_COLUMN: 'Kylian Mbappe'},
    {PRIMARY_KEY: 1, NAME_COLUMN: 'Harry Kane'},
    {PRIMARY_KEY: 2, NAME_COLUMN: 'Antoine Griezmann'}
]
DELETE_ROWS = [{PRIMARY_KEY: 0}, {PRIMARY_KEY: 1}, {PRIMARY_KEY: 2}]
KIND_FOR_INSERT = 'insert'
KIND_FOR_UPDATE = 'update'
KIND_FOR_DELETE = 'delete'

CHECK_REP_SLOT_QUERY = 'select slot_name from pg_replication_slots;'

POLL_INTERVAL = "${1 * SECONDS}"

INSERTS_PER_THREAD = 20
NUM_THREADS = 10
TOTAL_THREADING_RECORDS = INSERTS_PER_THREAD * NUM_THREADS


def _create_table_in_database(table_name, database):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(PRIMARY_KEY, sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(NAME_COLUMN, sqlalchemy.String(20))
    )
    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table


def _insert(connection, table, insert_rows=INSERT_ROWS, create_txn=False):
    if create_txn:
        txn = connection.begin()

    connection.execute(table.insert(), insert_rows)

    if create_txn:
        txn.commit()

    # Prepare expected data to compare for verification against snapshot data.
    operations_data = []
    for row in insert_rows:
        operations_data.append(OperationsData(KIND_FOR_INSERT,
                                              table.name,
                                              [PRIMARY_KEY, NAME_COLUMN],
                                              list(row.values()),
                                              None))  # No oldkeys expected.
    return operations_data


def _update(connection, table, update_rows=UPDATE_ROWS):
    txn = connection.begin()

    try:
        for row in update_rows:
            connection.execute(table.update().where(table.c.id == row[PRIMARY_KEY]).values(name=row[NAME_COLUMN]))
        txn.commit()
    except:
        txn.rollback()
        raise

    # Prepare expected data to compare for verification against snapshot data.
    operations_data = []
    for row in update_rows:
        operations_data.append(OperationsData(KIND_FOR_UPDATE,
                                              table.name,
                                              [PRIMARY_KEY, NAME_COLUMN],
                                              list(row.values()),
                                              Oldkeys([PRIMARY_KEY], [row[PRIMARY_KEY]])))
    return operations_data


def _delete(connection, table, delete_rows=DELETE_ROWS):
    txn = connection.begin()

    try:
        for row in delete_rows:
            connection.execute(table.delete().where(table.c.id == row[PRIMARY_KEY]))
        txn.commit()
    except:
        txn.rollback()
        raise

    # Prepare expected data to compare for verification against snapshot data.
    operations_data = []
    for row in delete_rows:
        operations_data.append(OperationsData(KIND_FOR_DELETE,
                                              table.name,
                                              None,  # No columnnames expected.
                                              None,  # No columnvalues expected.
                                              Oldkeys([PRIMARY_KEY], [row[PRIMARY_KEY]])))
    return operations_data


@sdc_min_version('3.16.0')
@database('postgresql')
@pytest.mark.parametrize('poll_interval', [1, 5])
def test_stop_start(sdc_builder, sdc_executor, database, poll_interval):
    """Records are neither dropped, nor duplicated when a pipeline is stopped and then started in
    the midst of ingesting data. Repeat this a couple of times, and inbetween restart with no data
    and make sure offset can be read properly back.

    Runs with two poll intervals to verify that the Batch Wait Time (ms) configuration is respected.
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')

    SAMPLE_DATA = [dict(id=i, name=f'Martin_{i}') for i in range(40)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String(20)))
    replication_slot = get_random_string(string.ascii_lowercase, 10)

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        postgresql_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
        postgresql_cdc_client.set_attributes(batch_wait_time_in_ms=10000,
                                             max_batch_size_in_records=10,
                                             poll_interval=poll_interval,
                                             replication_slot=replication_slot)
        wiretap = pipeline_builder.add_wiretap()
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        # We want the pipeline to stop automatically part-way through processing batch 1 and at the end of batch 2.
        pipeline_finisher.set_attributes(preconditions=[
            "${record:value('/change[0]/columnvalues[0]') == 9"
            " or record:value('/change[0]/columnvalues[0]') == 19"
            " or record:value('/change[0]/columnvalues[0]') == 29"
            " or record:value('/change[0]/columnvalues[0]') == 39}"
        ])
        postgresql_cdc_client >> [wiretap.destination, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and add some data
        sdc_executor.start_pipeline(pipeline)

        table.create(database.engine)
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in SAMPLE_DATA[:30]:
                connection.execute(table.insert(), row)

        # Pipeline will stop once it sees id=5.
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED')

        # Since we stop gracefully, we expect to see the entire first batch (records with id=0 through id=9)
        # written to the destination.
        # Within the field, column names are stored in a list (e.g. ['id', 'name']) and so are
        # column values (e.g. [1, 'Martin_1']). We use zip to help us combine each instance into a dictionary.
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records] == SAMPLE_DATA[:10]
        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the second time ...')
        # Again, pipeline will stop on its own, this time when it processes the last record (id=19).
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # We expect to see records with id=10 through id=19 (i.e. no duplicated or missing records).
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records] == SAMPLE_DATA[10:20]

        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the second time ...')
        # Again, pipeline will stop on its own, this time when it processes the last record (id=19).
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # We expect to see records with id=10 through id=19 (i.e. no duplicated or missing records).
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records] == SAMPLE_DATA[20:30]

        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the third time ...')
        # Don't insert records and get an empty batch
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)
        metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
        assert metrics.counter("pipeline.batchOutputRecords.counter").count == 0

        # Add few records
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in SAMPLE_DATA[30:40]:
                connection.execute(table.insert(), row)
        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the fourth time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # We expect to see records with id=10 through id=19 (i.e. no duplicated or missing records).
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records] == SAMPLE_DATA[30:40]
    finally:
        table.drop(database.engine)
        database.deactivate_and_drop_replication_slot(replication_slot)


@sdc_min_version('3.16.0')
@database('postgresql')
@pytest.mark.parametrize('start_from', ['DATE', 'LSN'])
@pytest.mark.parametrize('create_slot', [True, False])
def test_start_not_from_latest(sdc_builder, sdc_executor, database, start_from, create_slot):
    """
    We test that start from LSN and Date works as expected, for that we insert some data, get the date/lsn and insert
    some more data, after that we verify that we only process the second batch inserted.

    After that we insert a third batch of records and start again the pipeline to verify that we don't read any
    duplicated record.

    Apart from that we also included a case where the replication slot is created by the pipeline it set during the
    start, in that case we need to insert new data after it gets created so, there is a fourth batch of records that is
    inserted and processed.
    """

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')

    if start_from is 'LSN' and database.database_server_version.major < 10:
        pytest.skip('LSN test cannot be executed in versions < 10.')

    SAMPLE_DATA = [dict(id=f'1{i}', name=f'Alex_{i}') for i in range(20)]
    SAMPLE_DATA_2 = [dict(id=f'2{i}', name=f'Martin_{i}') for i in range(20)]
    SAMPLE_DATA_3 = [dict(id=f'3{i}', name=f'Santhosh_{i}') for i in range(20)]
    SAMPLE_DATA_4 = [dict(id=f'4{i}', name=f'Tucu_{i}') for i in range(20)]

    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.String(20), primary_key=True),
                             sqlalchemy.Column('name', sqlalchemy.String(20)))
    replication_slot = get_random_string(string.ascii_lowercase, 10)

    try:
        table.create(database.engine)

        if create_slot:
            # create replication slot
            with database.engine.connect().execution_options(autocommit=True) as connection:
                connection.execute(
                    f'SELECT * FROM pg_create_logical_replication_slot(\'{replication_slot}\', \'wal2json\')')

        # insert first batch of data
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in SAMPLE_DATA:
                connection.execute(table.insert(), row)

        if start_from is 'DATE':
            # get timestamp from database and timezone
            time.sleep(5)
            with database.engine.connect().execution_options(autocommit=True) as connection:
                date = connection.execute('SELECT CURRENT_TIMESTAMP').first()[0]
                timezone = str(connection.execute('SHOW timezone').first()[0])
        else:
            # get current lsn from replication slot
            with database.engine.connect().execution_options(autocommit=True) as connection:
                start_lsn = str(connection.execute('select pg_current_wal_lsn()').first()[0])

        # insert second batch of data
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in SAMPLE_DATA_2:
                connection.execute(table.insert(), row)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        postgresql_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
        postgresql_cdc_client.set_attributes(replication_slot=replication_slot,
                                             initial_change=start_from,
                                             poll_interval=1)

        if start_from is 'DATE':
            postgresql_cdc_client.set_attributes(start_date=date.strftime('%m-%d-%Y %H:%M:%S'),
                                                 db_time_zone=timezone)
        else:
            postgresql_cdc_client.set_attributes(start_lsn=start_lsn)

        wiretap = pipeline_builder.add_wiretap()
        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        pipeline_finisher.set_attributes(preconditions=[
            "${record:value('/change[0]/columnvalues[0]') == 219"
            " or record:value('/change[0]/columnvalues[0]') == 319"
            " or record:value('/change[0]/columnvalues[0]') == 419}"])
        postgresql_cdc_client >> [wiretap.destination, pipeline_finisher]
        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        if create_slot:
            # We manually created the slot in the test so the data that we inserted after it will be available
            expected_data = SAMPLE_DATA_2
        else:
            # Since the pipeline has created the replication slot we are inserting data now to make it available
            expected_data = SAMPLE_DATA_4
            sdc_executor.wait_for_pipeline_status(pipeline, 'RUNNING', timeout_sec=120)
            # insert first batch of data
            with database.engine.connect().execution_options(autocommit=True) as connection:
                for row in SAMPLE_DATA_4:
                    connection.execute(table.insert(), row)

        # Pipeline will stop once it sees id=219 or 419 if the pipelines creates the replication slot
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED', timeout_sec=120)

        # Since we stop gracefully, we expect to see the entire first batch (records with id=0 through id=9)
        # written to the destination.
        # Within the field, column names are stored in a list (e.g. ['id', 'name']) and so are
        # column values (e.g. [1, 'Martin_1']). We use zip to help us combine each instance into a dictionary.
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records] == expected_data

        wiretap.reset()

        # insert second batch of data
        with database.engine.connect().execution_options(autocommit=True) as connection:
            for row in SAMPLE_DATA_3:
                connection.execute(table.insert(), row)

        sdc_executor.start_pipeline(pipeline)

        # Pipeline will stop once it sees id=319.
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED', timeout_sec=120)

        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records] == SAMPLE_DATA_3

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        table.drop(database.engine)
        database.deactivate_and_drop_replication_slot(replication_slot)


@database('postgresql')
@sdc_min_version('3.4.0')
def test_postgres_cdc_client_basic(sdc_builder, sdc_executor, database):
    """Basic test that inserts/updates/deletes to a Postgres table,
    and validates that they are read in the same order.
    Here `Initial Change` config. is at default value = `From the latest change`.
    With this, the origin processes all changes that occur after pipeline is started.

    The pipeline looks like:
        postgres_cdc_client >> trash
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(remove_replication_slot_on_close=True,
                                       max_batch_size_in_records=1,
                                       poll_interval=POLL_INTERVAL,
                                       replication_slot=replication_slot_name)
    trash = pipeline_builder.add_stage('Trash')
    postgres_cdc_client >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Create table and then perform insert, update and delete operations.
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)

        snapshot = snapshot_command.wait_for_finished().snapshot

        # Verify snapshot data is received in exact order as expected.
        operation_index = 0
        for record in snapshot[postgres_cdc_client.instance_name].output:
            # No need to worry about DDL related CDC records. e.g. table creation etc.
            if record.get_field_data('/change'):
                # Since we performed each operation (insert, update and delete) on 3 rows,
                # each CDC  record change contains a list of 3 elements.
                for i in range(3):
                    expected = expected_operations_data[operation_index]
                    assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                    assert expected.table == record.get_field_data(f'/change[{i}]/table')
                    # For delete operation there are no columnnames and columnvalues fields.
                    if expected.kind != KIND_FOR_DELETE:
                        assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                        assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                    if expected.kind != KIND_FOR_INSERT:
                        # For update and delete operations verify extra information about old keys.
                        assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                        assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                    operation_index += 1

    finally:
        if pipeline:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)


@database('postgresql')
@sdc_min_version('3.4.0')
def test_postgres_cdc_max_poll_attempts(sdc_builder, sdc_executor, database):
    """Test the delivery of a batch when the maximum poll attempts is reached.

    The condition to generate a new batch in PostgreSQL CDC Origin is a) to reach the maximum batch size; or b) to
    reach the maximum attempts to poll data from CDC. This test set a max batch size of 100 records and check a new
    batch is generated with only a few records because of hitting the max poll attempts.

    Pipeline:
        postgres_cdc_client >> trash

    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(remove_replication_slot_on_close=True,
                                       max_batch_size_in_records=100,
                                       poll_interval=POLL_INTERVAL,
                                       replication_slot=replication_slot_name)
    trash = pipeline_builder.add_stage('Trash')
    postgres_cdc_client >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Create table, perform a few insertions, and then wait for the pipeline a period of time enough to hit the
        # max poll attempts (max poll attempts == POLL_INTERVAL * 100).
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        snapshot = snapshot_command.wait_for_finished(120).snapshot

        # Verify snapshot data is received in exact order as expected.
        for record in snapshot[postgres_cdc_client.instance_name].output:
            # No need to worry about DDL related CDC records. e.g. table creation etc.
            if record.get_field_data('/change'):
                # Check that the CDC record change contains a list of 3 insertions.
                for i in range(len(INSERT_ROWS)):
                    expected = expected_operations_data[i]
                    assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                    assert expected.table == record.get_field_data(f'/change[{i}]/table')
                    assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                    assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')

    finally:
        if pipeline:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)


@database('postgresql')
@sdc_min_version('3.8.1')
def test_postgres_cdc_client_filtering_table(sdc_builder, sdc_executor, database):
    """
        Test filtering for inserts/updates/deletes to a Postgres table

        1. Random table names for "table_allow", "table_deny"
        2. Filter OUT anything for "table_deny"
        3. Insert/update/delete for both tables
        4. Should see updates for "table_allow" only

        The pipeline looks like:
        postgres_cdc_client >> trash
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')

    table_name_allow = get_random_string(string.ascii_lowercase, 20)
    table_name_deny = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)

    postgres_cdc_client.set_attributes(remove_replication_slot_on_close=True,
                                       replication_slot=replication_slot_name,
                                       max_batch_size_in_records=1,
                                       poll_interval=POLL_INTERVAL,
                                       tables=[{'schema': 'public',
                                                'excludePattern': table_name_deny,
                                                'table': table_name_allow}])
    trash = pipeline_builder.add_stage('Trash')
    postgres_cdc_client >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Create table and then perform insert, update and delete operations.
        table_allow = _create_table_in_database(table_name_allow, database)
        table_deny = _create_table_in_database(table_name_deny, database)
        connection = database.engine.connect()

        expected_operations_data = _insert(connection=connection, table=table_allow)
        expected_operations_data += _update(connection=connection, table=table_allow)
        expected_operations_data += _delete(connection=connection, table=table_allow)

        actual_operations_data = expected_operations_data.copy()

        actual_operations_data += _insert(connection=connection, table=table_deny)
        actual_operations_data += _update(connection=connection, table=table_deny)
        actual_operations_data += _delete(connection=connection, table=table_deny)

        snapshot = snapshot_command.wait_for_finished().snapshot

        # Verify snapshot data is received in exact order as expected.
        operation_index = 0

        for record in snapshot[postgres_cdc_client.instance_name].output:
            # No need to worry about DDL related CDC records. e.g. table creation etc.
            if record.get_field_data('/change'):
                # Since we performed each operation (insert, update and delete) on 3 rows,
                # each CDC  record change contains a list of 3 elements.
                for i in range(3):
                    if operation_index >= len(expected_operations_data):
                        break
                    expected = expected_operations_data[operation_index]
                    assert expected.kind == record.get_field_data(f'/change[{i}]/kind')
                    assert expected.table == record.get_field_data(f'/change[{i}]/table')
                    # For delete operation there are no columnnames and columnvalues fields.
                    if expected.kind != KIND_FOR_DELETE:
                        assert expected.columnnames == record.get_field_data(f'/change[{i}]/columnnames')
                        assert expected.columnvalues == record.get_field_data(f'/change[{i}]/columnvalues')
                    if expected.kind != KIND_FOR_INSERT:
                        # For update and delete operations verify extra information about old keys.
                        assert expected.oldkeys.keynames == record.get_field_data(f'/change[{i}]/oldkeys/keynames')
                        assert expected.oldkeys.keyvalues == record.get_field_data(f'/change[{i}]/oldkeys/keyvalues')
                    operation_index += 1

    finally:
        if pipeline:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table_allow is not None:
            table_allow.drop(database.engine)
            logger.info('Table: %s dropped.', table_name_allow)
        if table_deny is not None:
            table_deny.drop(database.engine)
            logger.info('Table: %s dropped.', table_name_deny)


@database('postgresql')
@sdc_min_version('3.4.0')
def test_postgres_cdc_client_remove_replication_slot(sdc_builder, sdc_executor, database):
    """
        Test the 'Remove replication slot on close' functionality

        1.  Initialize and start pipeline with specified replication slot
        2.  Pass some data
        3.  Stop the pipeline
        4.  Query postgres database for replication slots, checking removal
    """
    if database.database_server_version < databases.EARLIEST_POSTGRESQL_VERSION_WITH_ACTIVE_PID:
        # Test only runs against PostgreSQL version with active_pid column in pg_replication_slots.
        pytest.skip('Test only runs against PostgreSQL version >= '
                    f"{'.'.join(str(item) for item in databases.EARLIEST_POSTGRESQL_VERSION_WITH_ACTIVE_PID)}")
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)
    replication_slot = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    postgres_cdc_client.set_attributes(remove_replication_slot_on_close=True,
                                       max_batch_size_in_records=1,
                                       poll_interval=POLL_INTERVAL,
                                       replication_slot=replication_slot)
    trash = pipeline_builder.add_stage('Trash')
    postgres_cdc_client >> trash

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        snapshot_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        # Create table and then perform some operations to simulate activity
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)

        snapshot = snapshot_command.wait_for_finished().snapshot

        # Timeout is set as without SDC-11252, pipeline will get stuck in 'STOPPING' state forever
        sdc_executor.stop_pipeline(pipeline=pipeline).wait_for_stopped(timeout_sec=60)

        # After pipeline stoppage, check on the replication slots remaining
        listed_slots = connection.execute(CHECK_REP_SLOT_QUERY).fetchall()

        # Check that replication_slot is not in listed_slots
        logger.info('Replication slot:  ' + replication_slot)
        logger.info('List of current slots: ' + str(listed_slots))
        assert (replication_slot,) not in listed_slots

    finally:
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)


@database('postgresql')
@sdc_min_version('3.4.0')
@pytest.mark.parametrize(('batch_size'), [1,10,100,1000])
def test_postgres_cdc_client_multiple_concurrent_operations(sdc_builder, sdc_executor, database, batch_size):
    """Basic test that inserts/update/delete to a Postgres table with multiple threads,
    and validates using a wire tap the records processed.
    Here `Initial Change` config. is at default value = `From the latest change`.
    With this, the origin processes all changes that occur after pipeline is started.

    The pipeline looks like:
        postgres_cdc_client >> [pipeline_finesher, wiretap]
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_client = pipeline_builder.add_stage('PostgreSQL CDC Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_client.set_attributes(remove_replication_slot_on_close=False,
                                       max_batch_size_in_records=batch_size,
                                       poll_interval=POLL_INTERVAL,
                                       replication_slot=replication_slot_name,
                                       batch_wait_time_in_ms=3000
                                       )
    wiretap = pipeline_builder.add_wiretap()

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    # We want the pipeline to stop automatically part-way through processing batch 1 and at the end of batch 2.
    pipeline_finisher.set_attributes(preconditions=[
        "${record:value('/change[0]/columnvalues[0]') == -1}"
    ])

    postgres_cdc_client >> [wiretap.destination, pipeline_finisher]

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        pipeline_cmd = sdc_executor.start_pipeline(pipeline)

        # Create table and then perform insert operations with various threads at the same time.
        table = _create_table_in_database(table_name, database)
        connections = [database.engine.connect() for _ in range(NUM_THREADS)]

        expected = []

        def inserter_thread(connection, table, id, amount):
            for i in range(amount):
                insert_rows = [
                    {
                        PRIMARY_KEY: id * amount + i,
                        NAME_COLUMN: get_random_string(string.ascii_lowercase, 10)
                    }
                ]
                expected.append(_insert(
                    connection=connection,
                    table=table,
                    insert_rows=insert_rows,
                    create_txn=True
                ))
                insert_rows = [
                    {
                        PRIMARY_KEY: id * amount + i,
                        NAME_COLUMN: get_random_string(string.ascii_lowercase, 10)
                    }
                ]
                expected.append(_update(
                    connection=connection,
                    table=table,
                    update_rows=insert_rows
                ))
                expected.append(_delete(
                    connection=connection,
                    table=table,
                    delete_rows=insert_rows
                ))

        thread_pool = [
            threading.Thread(
                target=inserter_thread,
                args=(connections[i], table, i, INSERTS_PER_THREAD)
            )
            for i in range(NUM_THREADS)
        ]

        for thread in thread_pool:
            thread.start()

        for thread in thread_pool:
            thread.join()

        final_row = [{PRIMARY_KEY: -1, NAME_COLUMN: 'Last Record'}]
        expected.append(_insert(
            connection=connections[0],
            table=table,
            insert_rows=final_row,
            create_txn=True
        ))
        pipeline_cmd.wait_for_finished(timeout_sec=120)

        output = []
        for record in wiretap.output_records:
            if record.get_field_data('/change[0]/kind')=='delete':
                output.append({'type':'delete', 'value': record.get_field_data('/change[0]/oldkeys/keyvalues')})
            if record.get_field_data('/change[0]/kind')=='insert':
                output.append({'type':'insert', 'value': record.get_field_data('/change[0]/columnvalues')})
            if record.get_field_data('/change[0]/kind')=='update':
                output.append({'type':'update', 'value': record.get_field_data('/change[0]/columnvalues')})

        output_sorted_values = sorted(output, key=lambda key: f'{key["value"][0]}|{key["type"]}')


        expected_values = []
        for record in expected:
            if record[0].kind == 'delete':
                expected_values.append({'type':'delete', 'value': record[0].oldkeys.keyvalues})
            if record[0].kind == 'insert':
                expected_values.append({'type': 'insert', 'value': record[0].columnvalues})
            if record[0].kind == 'update':
                expected_values.append({'type': 'update', 'value': record[0].columnvalues})

        expected_sorted_values= sorted(expected_values, key=lambda key: f'{key["value"][0]}|{key["type"]}')

        assert len(expected_sorted_values) == len(output_sorted_values)
        assert expected_sorted_values == output_sorted_values

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
