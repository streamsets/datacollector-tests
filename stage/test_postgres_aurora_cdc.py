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
import threading
import time
from collections import namedtuple

import pytest
import sqlalchemy
from streamsets.sdk.utils import wait_for_condition
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
BLACKLISTED_WAL_SENDER_COLUMNS = set(["usename", "client_addr", "client_hostname"])
WAL_SENDER_COLUMNS_TO_COMPARE = set(["application_name", "pid"])


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
    logger.info('Inserting rows %s in %s table ...', insert_rows, table)

    if create_txn:
        txn.commit()

    # Prepare expected data to compare for verification against wiretap data.
    operations_data = []
    for row in insert_rows:
        operations_data.append(OperationsData(KIND_FOR_INSERT,
                                              table.name,
                                              [PRIMARY_KEY, NAME_COLUMN],
                                              list(row.values()),
                                              None))  # No oldkeys expected.
    if create_txn:
        txn.close()

    return operations_data


def _update(connection, table, update_rows=UPDATE_ROWS):
    operations_data = []
    txn = connection.begin()

    try:
        for row in update_rows:
            connection.execute(table.update().where(table.c.id == row[PRIMARY_KEY]).values(name=row[NAME_COLUMN]))
            logger.info('Updating row %s from %s table ...', row, table)
            # Prepare expected data to compare for verification against wiretap data.
            operations_data.append(OperationsData(KIND_FOR_UPDATE,
                                                  table.name,
                                                  [PRIMARY_KEY, NAME_COLUMN],
                                                  list(row.values()),
                                                  Oldkeys([PRIMARY_KEY], [row[PRIMARY_KEY]])))
        txn.commit()
    except:
        txn.rollback()
        pytest.fail('Error updating rows ...')

    finally:
        txn.close()

    return operations_data


def _delete(connection, table, delete_rows=DELETE_ROWS):
    txn = connection.begin()
    operations_data = []
    try:
        for row in delete_rows:
            connection.execute(table.delete().where(table.c.id == row[PRIMARY_KEY]))
            logger.info('Deleting row %s from %s table ...', row, table)
            # Prepare expected data to compare for verification against wiretap data.
            operations_data.append(OperationsData(KIND_FOR_DELETE,
                                                  table.name,
                                                  None,  # No column names expected.
                                                  None,  # No column values expected.
                                                  Oldkeys([PRIMARY_KEY], [row[PRIMARY_KEY]])))
        txn.commit()

    except:
        txn.rollback()
        pytest.fail('Error deleting rows ...')
    finally:
        txn.close()

    return operations_data


def _get_wal_sender_status(connection):
    results = connection.execute('select * from pg_stat_replication')
    wal_sender_statuses = [{c: v for c, v in r.items()} for r in results]
    return wal_sender_statuses[0] if len(wal_sender_statuses) > 0 else None


@sdc_min_version('4.5.0')
@database('postgresqlaurora')
@pytest.mark.parametrize('poll_interval', ["${1 * SECONDS}", "${5 * SECONDS}"])
def test_stop_start(sdc_builder, sdc_executor, database, poll_interval):
    """Records are neither dropped, nor duplicated when a pipeline is stopped and then started in
    the midst of ingesting data. Repeat this a couple of times, and inbetween restart with no data
    and make sure offset can be read properly back.

    Runs with two poll intervals to verify that the Batch Wait Time (ms) configuration is respected.

    The pipeline looks like:
        postgresql_cdc_aurora_client >> [wiretap.destination, pipeline_finisher]
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    connection = database.engine.connect().execution_options(autocommit=True)

    sample_data = [dict(id=i, name=f'Martin_{i}') for i in range(40)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    replication_slot = get_random_string(string.ascii_lowercase, 10)

    # Create table
    connection.execute(f"""
        CREATE TABLE {table_name}(
            id int primary key,
            name VARCHAR(20)
        )
    """)

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        postgresql_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
        postgresql_cdc_aurora_client.set_attributes(batch_wait_time_in_ms=10000,
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

        postgresql_cdc_aurora_client >> [wiretap.destination, pipeline_finisher]

        pipeline = pipeline_builder.build(title='test_stop_start').configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Start pipeline and add some data
        sdc_executor.start_pipeline(pipeline)

        for row in sample_data[:30]:
            connection.execute(f"INSERT INTO {table_name} VALUES(1, {row})")

        # Pipeline will stop once it sees id=9.
        sdc_executor.wait_for_pipeline_status(pipeline, 'FINISHED')

        # Since we stop gracefully, we expect to see the entire first batch (records with id=0 through id=9)
        # written to the destination.
        # Within the field, column names are stored in a list (e.g. ['id', 'name']) and so are
        # column values (e.g. [1, 'Martin_1']). We use zip to help us combine each instance into a dictionary.
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records] == sample_data[:10]
        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the second time ...')
        # Again, pipeline will stop on its own, this time when it processes the last record (id=19).
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # We expect to see records with id=10 through id=19 (i.e. no duplicated or missing records).
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records][:10] == sample_data[10:20]

        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the second time ...')
        # Again, pipeline will stop on its own, this time when it processes the last record (id=19).
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # We expect to see records with id=10 through id=19 (i.e. no duplicated or missing records).
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records][:10] == sample_data[20:30]

        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the third time ...')
        # Don't insert records and get an empty batch
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)
        metrics = sdc_executor.get_pipeline_history(pipeline).latest.metrics
        assert metrics.counter("pipeline.batchOutputRecords.counter").count == 0

        # Add few records
        for row in sample_data[30:40]:
            connection.execute(f"INSERT INTO {table_name} VALUES(1, {row})")
        # Reset the wiretap so that we don't see records we've collected up to this point when we access it next time.
        wiretap.reset()
        logger.info('Starting pipeline for the fourth time ...')
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # We expect to see records with id=10 through id=19 (i.e. no duplicated or missing records).
        assert [dict(zip(record.field['change'][0]['columnnames'], record.field['change'][0]['columnvalues']))
                for record in wiretap.output_records][:10] == sample_data[30:40]
    finally:
        logger.info('Dropping table %s in %s database ...', table_name, database.type)
        connection.execute(f'DROP TABLE {table_name}')
        database.deactivate_and_drop_replication_slot(replication_slot)
        database.engine.connect().close()


@sdc_min_version('4.5.0')
@database('postgresqlaurora')
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

    The pipeline looks like:
        postgresql_cdc_aurora_client >> [wiretap.destination, pipeline_finisher]
    """

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

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
        postgresql_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
        postgresql_cdc_aurora_client.set_attributes(replication_slot=replication_slot,
                                                    initial_change=start_from,
                                                    poll_interval=1)

        if start_from is 'DATE':
            postgresql_cdc_aurora_client.set_attributes(start_date=date.strftime('%m-%d-%Y %H:%M:%S'),
                                                        database_time_zone=timezone)
        else:
            postgresql_cdc_aurora_client.set_attributes(start_lsn=start_lsn)

        wiretap = pipeline_builder.add_wiretap()

        pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        pipeline_finisher.set_attributes(preconditions=[
            "${record:value('/change[0]/columnvalues[0]') == 219"
            " or record:value('/change[0]/columnvalues[0]') == 319"
            " or record:value('/change[0]/columnvalues[0]') == 419}"])

        postgresql_cdc_aurora_client >> [wiretap.destination, pipeline_finisher]

        pipeline = pipeline_builder.build(title='test_start_not_from_latest').configure_for_environment(database)
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
        database.engine.connect().close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
def test_postgres_cdc_aurora_client_basic(sdc_builder, sdc_executor, database):
    """Basic test that inserts/updates/deletes to a Postgres table,
    and validates that they are read in the same order.
    Here `Initial Change` config. is at default value = `From the latest change`.
    With this, the origin processes all changes that occur after pipeline is started.

    The pipeline looks like:
        postgresql_cdc_aurora_client >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgresql_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgresql_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                                max_batch_size_in_records=1,
                                                poll_interval=POLL_INTERVAL,
                                                replication_slot=replication_slot_name
                                                )
    wiretap = pipeline_builder.add_wiretap()

    postgresql_cdc_aurora_client >> wiretap.destination

    pipeline = pipeline_builder.build(title='test_postgres_cdc_aurora_client_basic').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        connection = database.engine.connect()
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        sdc_executor.start_pipeline(pipeline)

        # Create table and then perform insert, update and delete operations.
        table = _create_table_in_database(table_name, database)
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # Verify wiretap contains records
        assert len(wiretap.output_records) > 0

        # Verify wiretap data is received in exact order as expected.
        operation_index = 0
        for record in wiretap.output_records:
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
def test_postgres_cdc_aurora_max_poll_attempts(sdc_builder, sdc_executor, database):
    """Test the delivery of a batch when the maximum poll attempts is reached.

    The condition to generate a new batch in PostgreSQL CDC Aurora Origin is a) to reach the maximum batch size; or b) to
    reach the maximum attempts to poll data from CDC. This test set a max batch size of 100 records and check a new
    batch is generated with only a few records because of hitting the max poll attempts.

    Pipeline:
        postgres_cdc_aurora_client >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                              max_batch_size_in_records=100,
                                              poll_interval=POLL_INTERVAL,
                                              replication_slot=replication_slot_name)

    wiretap = pipeline_builder.add_wiretap()

    postgres_cdc_aurora_client >> wiretap.destination

    pipeline = pipeline_builder.build(title='test_postgres_cdc_aurora_max_poll_attempts').configure_for_environment(
        database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        sdc_executor.start_pipeline(pipeline)

        # Create table, perform a few insertions, and then wait for the pipeline a period of time enough to hit the
        # max poll attempts (max poll attempts == POLL_INTERVAL * 100).
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # Verify wiretap contains records
        assert len(wiretap.output_records) > 0

        # Verify wiretap data is received in exact order as expected.
        for record in wiretap.output_records:
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
def test_postgres_cdc_aurora_client_filtering_table(sdc_builder, sdc_executor, database):
    """
        Test filtering for inserts/updates/deletes to a Postgres table

        1. Random table names for "table_allow", "table_deny"
        2. Filter OUT anything for "table_deny"
        3. Insert/update/delete for both tables
        4. Should see updates for "table_allow" only

        The pipeline looks like:
            postgres_cdc_aurora_client >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name_allow = get_random_string(string.ascii_lowercase, 20)
    table_name_deny = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              poll_interval=POLL_INTERVAL,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': table_name_deny,
                                                       'table': table_name_allow}])

    wiretap = pipeline_builder.add_wiretap()

    postgres_cdc_aurora_client >> wiretap.destination

    pipeline = pipeline_builder.build(
        title='test_postgres_cdc_aurora_client_filtering_table').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    # Create tables before pipeline starts
    table_allow = _create_table_in_database(table_name_allow, database)
    table_deny = _create_table_in_database(table_name_deny, database)
    try:
        # Database operations done after pipeline start will be captured by CDC.
        sdc_executor.start_pipeline(pipeline)

        # Create table and then perform insert, update and delete operations.
        connection = database.engine.connect()

        _insert(connection=connection, table=table_deny)
        _update(connection=connection, table=table_deny)
        _delete(connection=connection, table=table_deny)

        expected_operations_data = _insert(connection=connection, table=table_allow)
        expected_operations_data += _update(connection=connection, table=table_allow)
        expected_operations_data += _delete(connection=connection, table=table_allow)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # Verify output_records data lenght * 3 - three inserts/updates/deletes per record - is equal
        # to wiretap length
        assert len(expected_operations_data) == len(wiretap.output_records) * 3

        # Verify wiretap data is received in exact order as expected.
        operation_index = 0
        for record in wiretap.output_records:
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table_allow is not None:
            table_allow.drop(database.engine)
            logger.info('Table: %s dropped.', table_name_allow)
        if table_deny is not None:
            table_deny.drop(database.engine)
            logger.info('Table: %s dropped.', table_name_deny)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
def test_postgres_cdc_aurora_client_remove_replication_slot(sdc_builder, sdc_executor, database):
    """
        Test the 'Remove replication slot on close' functionality

        1.  Initialize and start pipeline with specified replication slot
        2.  Pass some data
        3.  Stop the pipeline
        4.  Query postgres database for replication slots, checking removal

        The pipeline looks like:
            postgres_cdc_aurora_client >> wiretap
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)
    replication_slot = get_random_string(string.ascii_lowercase, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                              max_batch_size_in_records=1,
                                              poll_interval=POLL_INTERVAL,
                                              replication_slot=replication_slot)

    wiretap = pipeline_builder.add_wiretap()

    postgres_cdc_aurora_client >> wiretap.destination

    pipeline = pipeline_builder.build(
        title='test_postgres_cdc_aurora_client_remove_replication_slot').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        sdc_executor.start_pipeline(pipeline)

        # Create table and then perform some operations to simulate activity
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)

        # Timeout is set as without SDC-11252, pipeline will get stuck in 'STOPPING' state forever
        sdc_executor.stop_pipeline(pipeline=pipeline).wait_for_stopped(timeout_sec=60)

        # After pipeline stoppage, check on the replication slots remaining
        listed_slots = connection.execute(CHECK_REP_SLOT_QUERY).fetchall()

        # Check that replication_slot is not in listed_slots
        logger.info('Replication slot:  ' + replication_slot)
        logger.info('List of current slots: ' + str(listed_slots))
        assert (replication_slot,) not in listed_slots
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
@pytest.mark.parametrize('batch_size', [1, 10, 100, 1000])
def test_postgres_cdc_aurora_client_multiple_concurrent_operations(sdc_builder, sdc_executor, database, batch_size):
    """Basic test that inserts/update/delete to a Postgres table with multiple threads,
    and validates using a wire tap the records processed.
    Here `Initial Change` config. is at default value = `From the latest change`.
    With this, the origin processes all changes that occur after pipeline is started.

    The pipeline looks like:
        postgres_cdc_aurora_client >> [pipeline_finesher, wiretap]
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=False,
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

    postgres_cdc_aurora_client >> [wiretap.destination, pipeline_finisher]

    pipeline = pipeline_builder.build(
        title='test_postgres_cdc_aurora_client_multiple_concurrent_operations').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
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
            if record.get_field_data('/change[0]/kind') == 'delete':
                output.append({'type': 'delete', 'value': record.get_field_data('/change[0]/oldkeys/keyvalues')})
            if record.get_field_data('/change[0]/kind') == 'insert':
                output.append({'type': 'insert', 'value': record.get_field_data('/change[0]/columnvalues')})
            if record.get_field_data('/change[0]/kind') == 'update':
                output.append({'type': 'update', 'value': record.get_field_data('/change[0]/columnvalues')})

        output_sorted_values = sorted(output, key=lambda key: f'{key["value"][0]}|{key["type"]}')

        expected_values = []
        for record in expected:
            if record[0].kind == 'delete':
                expected_values.append({'type': 'delete', 'value': record[0].oldkeys.keyvalues})
            if record[0].kind == 'insert':
                expected_values.append({'type': 'insert', 'value': record[0].columnvalues})
            if record[0].kind == 'update':
                expected_values.append({'type': 'update', 'value': record[0].columnvalues})

        expected_sorted_values = sorted(expected_values, key=lambda key: f'{key["value"][0]}|{key["type"]}')

        assert len(expected_sorted_values) == len(output_sorted_values)
        assert expected_sorted_values == output_sorted_values

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)
        for conn in connections:
            conn.close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
def test_postgres_cdc_aurora_client_filtering_multiple_tables(sdc_builder, sdc_executor, database):
    """
    Test filtering for inserts/updates/deletes to multiple Postgres table
        1. Random table name for "table[1]", "table[2]", "table[3]", "table[4]"
        2. Filter in "table[1]", "table[2]", "table[3]"
        3. Insert/update/delete for the four tables
        4. Should see updates for "table[1], [2] and [3]" only

        The pipeline looks like:
            postgres_cdc_aurora_client >> wiretap
    """

    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name = [get_random_string(string.ascii_lowercase, 20) for _ in range(4)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              max_batch_size_in_records=1,
                                              poll_interval=POLL_INTERVAL,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': '',
                                                       'table': table_name[i]} for i in range(3)
                                                      ])
    wiretap = pipeline_builder.add_wiretap()

    postgres_cdc_aurora_client >> wiretap.destination

    pipeline = pipeline_builder.build(
        title='test_postgres_cdc_aurora_client_filtering_multiple_tables').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = []
    try:
        # Create tables and then perform insert, update and delete operations.
        table = [_create_table_in_database(name, database) for name in table_name]

        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        sdc_executor.start_pipeline(pipeline)

        with database.engine.connect().execution_options(autocommit=False) as connection:
            expected_operations_data = []
            for i in range(3):
                expected_operations_data += _insert(connection=connection, table=table[i], create_txn=True)
                time.sleep(1)
                expected_operations_data += _update(connection=connection, table=table[i])
                time.sleep(1)
                expected_operations_data += _delete(connection=connection, table=table[i])
                time.sleep(1)

            # inserts/updates/deletes in table 3 should not be processed
            _insert(connection=connection, table=table[3], create_txn=True)
            _update(connection=connection, table=table[3])
            _delete(connection=connection, table=table[3])

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 9)
        sdc_executor.stop_pipeline(pipeline)

        # Verify record data is received in exact order as expected.
        operation_index = 0

        # Each record record includes 3 records
        assert len(wiretap.output_records) * 3 == len(expected_operations_data)
        for record in wiretap.output_records:
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        for t in table:
            t.drop(database.engine)
            logger.info('Table: %s dropped.', t.name)
        database.engine.connect().close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
def test_postgres_cdc_aurora_wal_sender_status_metrics(sdc_builder, sdc_executor, database):
    """
    Test Wal Sender Status gauge. Test whether after pipeline stop the metrics captured
         has the right information from the database

        The pipeline looks like:
            postgres_cdc_aurora_client >> trash
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)

    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              max_batch_size_in_records=1,
                                              poll_interval=POLL_INTERVAL,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': '',
                                                       'table': table_name}])
    trash = pipeline_builder.add_stage('Trash')

    postgres_cdc_aurora_client >> trash

    pipeline = pipeline_builder.build(
        title='test_postgres_cdc_aurora_wal_sender_status_metrics').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)
    table = _create_table_in_database(table_name, database)
    try:
        start_command = sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()
        expected_operations_data = []

        expected_operations_data += _insert(connection=connection, table=table)
        time.sleep(1)

        expected_operations_data += _update(connection=connection, table=table)
        time.sleep(1)

        expected_operations_data += _delete(connection=connection, table=table)

        start_command.wait_for_pipeline_output_records_count(3)

        wal_sender_status_from_db = _get_wal_sender_status(connection)

        assert wal_sender_status_from_db is not None

        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        wal_sender_status_from_metrics = history.latest.metrics.gauge(
            'custom.PostgreSQLCDCAuroraClient_01.Wal Sender Status.0.gauge').value

        # Black listed fields should not be available in metrics
        assert all([k not in wal_sender_status_from_metrics for k in BLACKLISTED_WAL_SENDER_COLUMNS])
        # Assert fields from wal sender properly available in gauge
        assert ({k: str(v) for k, v in wal_sender_status_from_metrics.items() if k in WAL_SENDER_COLUMNS_TO_COMPARE}
                == {k: str(v) for k, v in wal_sender_status_from_db.items() if k in WAL_SENDER_COLUMNS_TO_COMPARE})
    finally:
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        table.drop(database.engine)
        logger.info('Table: %s dropped.', table.name)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
def test_postgres_cdc_aurora_queue_buffering_metrics(sdc_builder, sdc_executor, database):
    """
    Test Queue buffering Metrics are proper. Produce multiple batches and
        assert the question size metrics are proper.

        The pipeline looks like:
            postgres_cdc_aurora_client >> delay >> wiretap.destination
    """
    if not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC enabled.')

    table_names = [get_random_string(string.ascii_lowercase, 20) for _ in range(9)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    queue_size = 6
    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                              replication_slot=replication_slot_name,
                                              max_batch_size_in_records=1,
                                              cdc_generator_queue_size=queue_size,
                                              poll_interval=POLL_INTERVAL,
                                              tables=[{'schema': 'public',
                                                       'excludePattern': '',
                                                       'table': table_name} for table_name in table_names])
    delay = pipeline_builder.add_stage('Delay')
    delay.delay_between_batches = 5000

    wiretap = pipeline_builder.add_wiretap()

    postgres_cdc_aurora_client >> delay >> wiretap.destination

    pipeline = pipeline_builder.build(
        title='test_postgres_cdc_aurora_queue_buffering_metrics').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)
    tables = [_create_table_in_database(table_name, database) for table_name in table_names]
    try:
        start_command = sdc_executor.start_pipeline(pipeline)
        connection = database.engine.connect()

        expected_operations_data = []
        for i in range(9):
            expected_operations_data += _insert(connection=connection,
                                                insert_rows=[{PRIMARY_KEY: 1, NAME_COLUMN: 'dummy'}],
                                                table=tables[i])

        def condition():
            pipeline_metrics = sdc_executor.get_pipeline_metrics(pipeline)
            queue_metrics = pipeline_metrics.gauge('custom.PostgreSQLCDCAuroraClient_01.CDC Metrics.0.gauge').value
            output_records_from_origin = pipeline_metrics.counter(
                'stage.PostgreSQLCDCAuroraClient_01.outputRecords.counter').count
            if 0 < output_records_from_origin < len(expected_operations_data):
                assert 0 < queue_metrics['Queue Size'] <= queue_size
            assert queue_metrics['Queue Capacity'] == queue_size - queue_metrics['Queue Size']
            return output_records_from_origin >= len(expected_operations_data)

        def failure(timeout):
            pipeline_metrics = sdc_executor.get_pipeline_metrics(pipeline)
            output_records_from_origin = pipeline_metrics.counter(
                'stage.PostgreSQLCDCAuroraClient_01.outputRecords.counter').count
            raise Exception('Timed out after `{}` seconds waiting for Output record metrics `{}` to reach `{}` '.format(
                timeout, output_records_from_origin, len(expected_operations_data)))

        wait_for_condition(condition=condition, timeout=120, failure=failure)

        start_command.wait_for_pipeline_output_records_count(9)

        sdc_executor.stop_pipeline(pipeline=pipeline)

        assert len(wiretap.output_records) == len(expected_operations_data)
    finally:
        if sdc_executor.get_pipeline_status(pipeline) == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        for t in tables:
            t.drop(database.engine)
            logger.info('Table: %s dropped.', t.name)
        connection.close()


@database('postgresqlaurora')
@sdc_min_version('4.5.0')
@pytest.mark.parametrize('ssl_mode', [
    'REQUIRED',
    'VERIFY_CA',
    'VERIFY_FULL'
])
def test_postgres_cdc_ssl_enabled(sdc_builder, sdc_executor, database, ssl_mode):
    """
    Basic test for SSL enabled that inserts/updates/deletes to a Postgres table,
    and validates that they are read in the same order.

    The pipeline looks like:
        postgres_cdc_aurora_client >> wiretap
    """

    # skip the test if the PostgreSQL Aurora CDC client isn't ssl enabled.
    if not database.ca_certificate or not database.is_cdc_enabled:
        pytest.skip('Test only runs against PostgreSQL Aurora with CDC and SSL enabled.')

    table_name = get_random_string(string.ascii_lowercase, 20)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    postgres_cdc_aurora_client = pipeline_builder.add_stage('PostgreSQL CDC Aurora Client')
    replication_slot_name = get_random_string(string.ascii_lowercase, 10)
    postgres_cdc_aurora_client.set_attributes(remove_replication_slot_on_close=True,
                                              max_batch_size_in_records=1,
                                              poll_interval=POLL_INTERVAL,
                                              replication_slot=replication_slot_name,
                                              ssl_mode=ssl_mode
                                              )

    if ssl_mode != 'REQUIRED':
        postgres_cdc_aurora_client.set_attributes(ca_certificate_pem=database.ca_certificate_file_contents,
                                                  server_certificate_pem=database.server_certificate_file_contents
                                                  )

    wiretap = pipeline_builder.add_wiretap()

    postgres_cdc_aurora_client >> wiretap.destination

    pipeline = pipeline_builder.build(title='test_postgres_cdc_ssl_enabled').configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    table = None
    try:
        # Database operations done after pipeline start will be captured by CDC.
        # Hence start the pipeline but do not wait for the capture to be finished.
        sdc_executor.start_pipeline(pipeline)

        # Create table and then perform insert, update and delete operations.
        table = _create_table_in_database(table_name, database)
        connection = database.engine.connect()
        expected_operations_data = _insert(connection=connection, table=table)
        expected_operations_data += _update(connection=connection, table=table)
        expected_operations_data += _delete(connection=connection, table=table)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'data_batch_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # Verify wiretap contains records
        assert len(wiretap.output_records) > 0

        # Verify wiretap data is received in exact order as expected.
        operation_index = 0
        for record in wiretap.output_records:
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
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)
        database.deactivate_and_drop_replication_slot(replication_slot_name)
        if table is not None:
            table.drop(database.engine)
            logger.info('Table: %s dropped.', table_name)
        connection.close()
