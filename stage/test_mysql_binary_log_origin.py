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

import json
import logging
import pytest
import random
import sqlalchemy
import string
import time
import uuid

from streamsets.sdk.utils import Version
from streamsets.testframework.environments.databases import MySqlDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string
from time import sleep

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def cdc_check(database):
    if isinstance(database, MySqlDatabase) and not database.is_cdc_enabled:
        pytest.skip('Test only runs against MySQL with CDC enabled.')


@database('mysql')
def test_mysql_binary_log_json_column(sdc_builder, sdc_executor, database, keep_data):
    """Test that MySQL Binary Log Origin is able to correctly read a json column in a row coming from MySQL Binary Log
    (AKA CDC).

    Pipeline looks like:

        mysql_binary_log >> wiretap
    """
    table = None
    connection = None

    try:
        # Create table.
        connection = database.engine.connect()
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                                 sqlalchemy.Column('name', sqlalchemy.String(25)),
                                 sqlalchemy.Column('json_column', sqlalchemy.JSON))
        table.create(database.engine)

        # Create Pipeline.
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(initial_offset=_get_initial_offset(database),
                                        server_id=_get_server_id(),
                                        include_tables=database.database + '.' + table_name)
        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Run pipeline and verify output.
        sdc_executor.start_pipeline(pipeline)

        # Insert data into table.
        connection.execute(table.insert(), {'id': 100, 'name': 'a', 'json_column': {'a': 123, 'b': 456}})

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert 1 == len(wiretap.output_records)

        for record in wiretap.output_records:
            assert record.field['Data']['id'] == 100
            assert record.field['Data']['name'] == 'a'
            assert record.field['Data']['json_column'].value == '{"a":123,"b":456}'

    finally:
        if not keep_data:
            # Drop table and Connection.
            if table is not None:
                logger.info('Dropping table %s in %s database...', table, database.type)
                table.drop(database.engine)

            if connection is not None:
                connection.close()


# SDC-15872: Disconnect MySQL Bin Log client if if it's not connected
@database('mysql')
def test_disconnect_on_error(sdc_builder, sdc_executor, database, keep_data):
    """Verify that we properly disconnect from the database on error (such as second slave with the same id)."""
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    table = None

    try:
        server_id = _get_server_id()
        initial_offset = _get_initial_offset(database)

        # Create table.
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, autoincrement=False, quote=True),
                                 quote=True)
        table.create(database.engine)
        connection.execute(table.insert(), {'id': 0})
        first_insert_number_of_records = 1

        # We need two pipelines that are using the same server id
        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('MySQL Binary Log')
        origin.set_attributes(initial_offset=initial_offset,
                              server_id=server_id,
                              include_tables=database.database + '.' + table_name)
        wiretap1 = builder.add_wiretap()
        origin >> wiretap1.destination
        pipeline1 = builder.build().configure_for_environment(database)
        pipeline1.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline1)

        # Create second pipeline (same logical pipeline though)
        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('MySQL Binary Log')
        origin.set_attributes(initial_offset=initial_offset,
                              server_id=server_id,
                              include_tables=database.database + '.' + table_name)
        trash = builder.add_stage('Trash')
        origin >> trash
        pipeline2 = builder.build().configure_for_environment(database)
        pipeline2.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline2)

        # 1) Start pipeline 1, wait until it consumes at least that one record
        sdc_executor.start_pipeline(pipeline1).wait_for_pipeline_output_records_count(first_insert_number_of_records)

        # 2) Start pipeline 2 (while pipeline 1 is still running)
        sdc_executor.start_pipeline(pipeline2)

        # 3) Pipeline 1 should at this point fail (two different slaves with the same id)
        _wait_for_pipeline_statuses(sdc_executor, pipeline1, ["RUN_ERROR", "RUNNING_ERROR"])

        # 4) Pipeline 2 can be stopped at this point
        sdc_executor.stop_pipeline(pipeline2)

        # 5) We start the first pipeline and insert data in 10-second interval, the pipeline should read all of them
        # In order to see the problem with the left-over thread, we need to wait at least a minute.
        sdc_executor.start_pipeline(pipeline1)
        second_insert_number_of_records = 8
        for i in range(first_insert_number_of_records, first_insert_number_of_records + second_insert_number_of_records):
            time.sleep(10)
            logger.info(f"Inserting {i}")
            connection.execute(table.insert(), {'id': i})

        # 6) Finally the pipeline should still be running and should process all 10 rows. Here is where
        # a version without SDC-15872 should fail.
        assert sdc_executor.get_pipeline_status(pipeline1).response.json().get('status') == "RUNNING"

        # 7) Wait for the 8 records to be processed then
        #    stop pipeline 1 and validate that we got all the records into the wiretap
        sdc_executor.wait_for_pipeline_metric(pipeline1, 'input_record_count', second_insert_number_of_records,
                                              timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline1)

        records = wiretap1.output_records
        assert len(records) == first_insert_number_of_records + second_insert_number_of_records

        for i in range(0, first_insert_number_of_records + second_insert_number_of_records):
            assert records[i].field['Data']['id'] == i
    finally:
        if not keep_data:
            # Drop table and Connection.
            if table is not None:
                logger.info('Dropping table %s in %s database...', table, database.type)
                table.drop(database.engine)

            if connection is not None:
                connection.close()


@pytest.mark.parametrize('total_records', [512])
@pytest.mark.parametrize('commit_rate', [8])
@pytest.mark.parametrize('time_to_sleep_before_disconnect', [8, 64])
@pytest.mark.parametrize('time_to_sleep_after_disconnect', [8, 64])
@pytest.mark.parametrize('rate_limit', [32])
@pytest.mark.parametrize('batch_wait_time_in_ms', [1000])
@pytest.mark.parametrize('max_batch_size_in_records', [8])
@database('mysql')
def test_auto_recovery_from_lost_connectivity(sdc_builder,
                                              sdc_executor,
                                              database,
                                              keep_data,
                                              total_records,
                                              commit_rate,
                                              time_to_sleep_before_disconnect,
                                              time_to_sleep_after_disconnect,
                                              rate_limit,
                                              batch_wait_time_in_ms,
                                              max_batch_size_in_records):
    """Test "MySQL Binary Log" (origin)  auto recovery.

    Test checking that there is no data loss when connection is lost when running a pipeline using
    "MySQL Binary Log" as origin.

    A table is created injecting some records with a defined commit rate ($total_records and commit_rate).

    Then a simple pipeline having "MySQL Binary Log" as origin and "wiretap" as destination is build. This pipeline
    contains it its title relevant information about he parameters used to run it.

    The rate limit of the pipeline is configured through parameter rate_limit. This is criticat, as it is necessary
    for proper testing to make sure some way that not all records are consumed between the elapsed time from pipeline
    start and network disconnection.

    For detailed testing scenarios, it is also possible to parametrize the wait time between batches
    (batch_wait_time_in_ms) and the batch sizes (max_batch_size_in_records). The meaning of these parameters is being
    able to easily check no data loss happens in fail-over scenarios.

    Then the pipeline is started with no wait or ending condition.

    Them the pipeline is started. After a parametrized given time (time_to_sleep_before_disconnect) SDC is disconnected
    from its network. Then, after a second parametrized given time (time_to_sleep_after_disconnect) SDC is connected
    again to its network.

    Then a condition is specified to wait until the origin stage produces as many records as rows inserted into the
    database table.

    Then we check that the pipeline was able to consume all the changes made in the database. We do this checking the
    number of output records and verifying all generated id's are present  in the output.

    Finally we clean-up removinf the table created and stopping the pipeline if necessary.

    Pipeline: mysql_binary_log_origin >> wiretap.destination

    """
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    try:

        logger.info(f'Running test: test_auto_recovery_from_lost_connectivity...')

        table_records = total_records

        logger.info(f'Gathering global configuration...')

        initial_offset = _get_initial_offset(database)
        server_id = _get_server_id()

        logger.info(f'Creating the table...')

        table_name = get_random_string(string.ascii_lowercase, 16)
        include_tables = database.database + '.' + table_name
        table_metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(table_name,
                                 table_metadata,
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
                                 sqlalchemy.Column('name', sqlalchemy.String(16), primary_key=False, quote=True),
                                 sqlalchemy.Column('surname', sqlalchemy.String(32), primary_key=False, quote=True),
                                 quote=True)
        table.create(database.engine)

        logger.info(f'Inserting data into the table {table_name}...')

        transaction = connection.begin()
        partial_records = 0
        needs_commit = False
        for total_records in range(1, table_records + 1):
            partial_records = partial_records + 1
            connection.execute(table.insert(), [{'id': total_records,
                                                 'name': get_random_string(string.ascii_lowercase, 16),
                                                 'surname': get_random_string(string.ascii_lowercase, 32)}])
            needs_commit = True
            if partial_records == commit_rate:
                transaction.commit()
                needs_commit = False
                partial_records = 0
                transaction = connection.begin()
        if needs_commit:
            transaction.commit()

        logger.info(f'Creating the pipeline...')

        pipeline_name = f'r={total_records}.' \
                        f'c={commit_rate}.' \
                        f'b={time_to_sleep_before_disconnect}.' \
                        f'a={time_to_sleep_after_disconnect}.' \
                        f'l={rate_limit}.' \
                        f'w={batch_wait_time_in_ms}.' \
                        f's={max_batch_size_in_records}-' \
                        f'={get_random_string(string.ascii_lowercase, 8)}'
        pipeline_title = f'MySQL: {pipeline_name}'

        pipeline_builder = sdc_builder.get_pipeline_builder()

        mysql_binary_log_origin = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log_origin.set_attributes(initial_offset=initial_offset,
                                               server_id=server_id,
                                               include_tables=include_tables,
                                               batch_wait_time_in_ms=batch_wait_time_in_ms,
                                               max_batch_size_in_records=max_batch_size_in_records)

        wiretap = pipeline_builder.add_wiretap()
        mysql_binary_log_origin >> wiretap.destination

        pipeline = pipeline_builder.build(title=pipeline_title,
                                          rate_limit=rate_limit).configure_for_environment(database)
        pipeline.rate_limit = rate_limit

        sdc_executor.add_pipeline(pipeline)

        logger.info(f'Starting the pipeline...')
        sdc_executor.start_pipeline(pipeline)

        logger.info(f'SDC is running on a container')

        # TODO
        # For the time being the following comment block cannot be migrated to Next. We are in conversations with
        # EP (Dima & Kirti) to decide which would be the good way to do this test in the new platform.

        # logger.info(f'Waiting {time_to_sleep_before_disconnect} seconds to disconnect the network...')
        # time.sleep(time_to_sleep_before_disconnect)
        # logger.info(f'Waited {time_to_sleep_before_disconnect} seconds to disconnect the network!')
        # logger.info(f'Disconnecting the network...')
        # sdc_executor.container.network_disconnect()
        # logger.info(f'Network disconnected!')
        # logger.info(f'Keeping the network disconnected for {time_to_sleep_after_disconnect} seconds...')
        # time.sleep(time_to_sleep_after_disconnect)
        # logger.info(f'Kept the network disconnected for {time_to_sleep_before_disconnect} seconds!')
        # logger.info(f'Reconnecting the network...')
        # sdc_executor.container.network_reconnect()
        # logger.info(f'Network reconnected!')

        logger.info(f'Waiting for the pipeline to output all the expected records...')
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', table_records)

        output_records = wiretap.output_records
        assert len(output_records) == table_records, \
            f'Expected {table_records} output records for the same quantity of inserts'

        def sort_function(entry):
            return entry.field['Data']['id'].value

        output_records.sort(key=sort_function)
        expected_id = 1
        for record in output_records:
            assert record.field['Data']['id'] == expected_id, 'Missing id {id} from output records'
            expected_id = expected_id + 1

    finally:

        if not keep_data:
            logger.info(f'Dropping table {table_name} in %{database.type} database...')
            table.drop(database.engine)

        if pipeline is not None:
            logger.info(f'Stopping pipeline {pipeline_title}')
            sdc_executor.stop_pipeline(pipeline, force=True)


@database('mysql')
def test_rollback_to_savepoint(sdc_builder, sdc_executor, database):
    """
    Tests the rollback to savepoint mechanism. The test writes some data, creates a savepoint, writes some more
    data and then rolls back to the savepoint. Finally, it validates that only the data before the save point and
    after the rollback is read.
    """
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    pipeline = None
    table = None

    try:
        # Create the table
        table_name = get_random_string(string.ascii_uppercase, 20)
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String(64))
        )
        table.create(database.engine)

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        transaction = connection.begin()

        connection.execute(f"insert into {table_name} values (1, 'Tony Stark')")
        connection.execute(f"insert into {table_name} values (2, 'Peter Parker')")
        connection.execute(f"update {table_name} set name = 'Steven Rogers' where id = 1")
        connection.execute("savepoint test_savepoint")
        connection.execute(f"insert into {table_name} values (3, 'Thor')")
        connection.execute(f"update {table_name} set name = 'Loki' where id = 1")
        connection.execute(f"delete from {table_name} where id = 1")
        connection.execute("rollback to savepoint test_savepoint")
        connection.execute(f"update {table_name} set name = 'Bruce Banner' WHERE id = 2")
        connection.execute(f"insert into {table_name} values (3, 'Steven Strange')")

        transaction.commit()

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 5)

        output_records = wiretap.output_records
        assert len(output_records) == 5

        assert output_records[0].field['Type'] == 'INSERT'
        assert output_records[0].field['Data']['id'] == 1
        assert output_records[0].field['Data']['name'] == 'Tony Stark'

        assert output_records[1].field['Type'] == 'INSERT'
        assert output_records[1].field['Data']['id'] == 2
        assert output_records[1].field['Data']['name'] == 'Peter Parker'

        assert output_records[2].field['Type'] == 'UPDATE'
        assert output_records[2].field['Data']['id'] == 1
        assert output_records[2].field['Data']['name'] == 'Steven Rogers'

        assert output_records[3].field['Type'] == 'UPDATE'
        assert output_records[3].field['Data']['id'] == 2
        assert output_records[3].field['Data']['name'] == 'Bruce Banner'

        assert output_records[4].field['Type'] == 'INSERT'
        assert output_records[4].field['Data']['id'] == 3
        assert output_records[4].field['Data']['name'] == 'Steven Strange'

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        if table is not None:
            table.drop(database.engine)


@database('mysql')
def test_mysql_binary_log_bulk(sdc_builder, sdc_executor, database):
    """
    Tests inserts/updates/deletes to a table with bulk sentences and verifies that no record is lost .
    """
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    source_table = None
    target_table = None
    pipeline = None

    try:
        source_table_name = get_random_string(string.ascii_uppercase, 20)
        logger.info('Creating source table %s in %s database ...', source_table_name, database.type)
        source_table = sqlalchemy.Table(
            source_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column('ID', sqlalchemy.Integer),
            sqlalchemy.Column('NAME', sqlalchemy.String(32)),
            sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
            sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
            sqlalchemy.Column('CITY', sqlalchemy.String(3))
        )
        source_table.create(database.engine)

        target_table_name = get_random_string(string.ascii_uppercase, 16)
        logger.info('Creating target table %s in %s database ...', target_table_name, database.type)
        target_table = sqlalchemy.Table(
            target_table_name,
            sqlalchemy.MetaData(),
            sqlalchemy.Column('ID', sqlalchemy.Integer),
            sqlalchemy.Column('NAME', sqlalchemy.String(32)),
            sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
            sqlalchemy.Column('COUNTRY', sqlalchemy.String(2)),
            sqlalchemy.Column('CITY', sqlalchemy.String(3))
        )
        target_table.create(database.engine)

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + target_table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        number_of_rows = 100

        for id in range(0, number_of_rows):
            name = "'" + str(uuid.uuid4())[:32] + "'"
            surname = "'" + str(uuid.uuid4())[:64] + "'"
            country = "'" + str(uuid.uuid4())[:2] + "'"
            city = "'" + str(uuid.uuid4())[:3] + "'"
            connection.execute(f"insert into {source_table_name} values ({id}, {name}, {surname}, {country}, {city})")

        connection.execute(f'insert into {target_table_name} select * from {source_table_name}')
        connection.execute(f'update {target_table_name} set CITY = COUNTRY')
        connection.execute(f'delete from {target_table_name}')

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3 * number_of_rows)

        q_insert = sum(1 for record in wiretap.output_records if record.field['Type'] == 'INSERT')
        q_update = sum(1 for record in wiretap.output_records if record.field['Type'] == 'UPDATE')
        q_delete = sum(1 for record in wiretap.output_records if record.field['Type'] == 'DELETE')

        logger.info(f'Total INSERT sentences: {q_insert}')
        logger.info(f'Total UPDATE sentences: {q_update}')
        logger.info(f'Total DELETE sentences: {q_delete}')

        assert q_insert == number_of_rows
        assert q_update == number_of_rows
        assert q_delete == number_of_rows

    finally:
        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)

        if source_table is not None:
            source_table.drop(database.engine)

        if target_table is not None:
            target_table.drop(database.engine)


@database('mysql')
def test_mysql_binary_log_record_information(sdc_builder, sdc_executor, database):
    """
    Test to check the contents of the record field.
    """
    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        # Create the table
        connection = database.engine.connect()

        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String(64)),
            sqlalchemy.Column('code_name', sqlalchemy.String(64))
        )
        table.create(database.engine)

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        ironman = {'id': 1, 'name': 'Tony Stark', 'code_name': 'Iron Man'}
        spiderman = {'id': 2, 'name': 'Peter Parker', 'code_name': 'Spider-man'}

        connection.execute(f"""
            insert into {table_name}
            values ({ironman.get('id')}, '{ironman.get('name')}', '{ironman.get('code_name')}')
        """)
        connection.execute(f"""
            update {table_name}
            set id = {spiderman.get('id')}, name = '{spiderman.get('name')}', code_name = '{spiderman.get('code_name')}'
            where id = {ironman.get('id')}
        """)
        connection.execute(f"delete from {table_name} where id = {spiderman.get('id')}")

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 3)

        assert len(wiretap.output_records) == 3

        for record in wiretap.output_records:
            assert record.field is not None

            assert 'BinLogFilename' in record.field
            assert 'Type' in record.field
            assert 'Table' in record.field
            assert 'ServerId' in record.field
            assert 'BinLogPosition' in record.field
            assert 'Database' in record.field
            assert 'Timestamp' in record.field
            assert 'Offset' in record.field

            assert record.field['BinLogFilename'] is not None
            assert record.field['Type'] is not None
            assert record.field['Table'] is not None
            assert record.field['ServerId'] is not None
            assert record.field['BinLogPosition'] is not None
            assert record.field['Database'] is not None
            assert record.field['Timestamp'] is not None
            assert record.field['Offset'] is not None

            assert record.field['Table'] == table_name
            assert record.field['Type'] in ['INSERT', 'UPDATE', 'DELETE']

            assert 'sdc.operation.type' in record.header.values

            if record.field['Type'] == 'INSERT':
                assert record.header.values['sdc.operation.type'] == '1'

                assert 'OldData' not in record.field
                assert 'Data' in record.field

                assert record.field['Data'] is not None
                assert sorted(record.field['Data']) == sorted(ironman)

            elif record.field['Type'] == 'UPDATE':
                assert record.header.values['sdc.operation.type'] == '3'

                assert 'OldData' in record.field
                assert 'Data' in record.field

                assert record.field['OldData'] is not None
                assert record.field['Data'] is not None
                assert sorted(record.field['OldData']) == sorted(ironman)
                assert sorted(record.field['Data']) == sorted(spiderman)

            else:
                assert record.header.values['sdc.operation.type'] == '2'

                assert 'OldData' in record.field
                assert 'Data' not in record.field

                assert record.field['OldData'] is not None
                assert sorted(record.field['OldData']) == sorted(spiderman)

    finally:
        connection.execute(f'drop table if exists {table_name}')

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)


@database('mysql')
def test_mysql_binary_log_include_tables(sdc_builder, sdc_executor, database):
    """
    Test to check the 'Include Tables' filter.
    """
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    pipeline = None
    included_table_name = get_random_string(string.ascii_lowercase, 20)
    excluded_table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        logger.info('Creating table %s in %s database ...', included_table_name, database.type)
        included_table = sqlalchemy.Table(
            included_table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String(64))
        )
        included_table.create(database.engine)

        logger.info('Creating table %s in %s database ...', excluded_table_name, database.type)
        excluded_table = sqlalchemy.Table(
            excluded_table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String(64))
        )
        excluded_table.create(database.engine)

        # Create the pipeline setting the table to be included
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + included_table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        black_widow = {'id': 1, 'name': 'Black Widow'}
        hulk = {'id': 2, 'name': 'Hulk'}

        connection.execute(f"""
            insert into {excluded_table_name}
            values ({hulk.get('id')}, '{hulk.get('name')}')
        """)
        connection.execute(f"""
            insert into {included_table_name}
            values ({black_widow.get('id')}, '{black_widow.get('name')}')
        """)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        assert len(wiretap.output_records) == 1

        record = wiretap.output_records[0]
        assert 'Data' in record.field
        assert record.field['Data'] is not None
        assert sorted(record.field['Data']) == sorted(black_widow)

    finally:
        connection.execute(f'drop table if exists {included_table_name}')
        connection.execute(f'drop table if exists {excluded_table_name}')

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)


@database('mysql')
def test_mysql_binary_log_ignore_tables(sdc_builder, sdc_executor, database):
    """
    Test to check the 'Ignore Tables' filter.
    """
    connection = database.engine.connect()
    if Version(database.version) >= Version('8.0.0'):
        binlog_row_metadata = connection.execute("SELECT @@GLOBAL.binlog_row_metadata").fetchall()[0][0]
        if binlog_row_metadata == "FULL":
            pytest.skip('This test is only executed in environments with binlog_row_metadata set to MINIMAL to avoid'
                        ' unnecessary executions given the fact this variable is irrelevant to the test.')

    pipeline = None
    tables_prefix = "test_mysql_binary_log_ignore_tables_" + get_random_string(string.ascii_lowercase, 5)
    included_table_name = tables_prefix + get_random_string(string.ascii_lowercase, 5)
    excluded_table_name = tables_prefix + get_random_string(string.ascii_lowercase, 5)

    try:
        # Create the table
        logger.info('Creating source table %s in %s database ...', included_table_name, database.type)
        included_table = sqlalchemy.Table(
            included_table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String(64))
        )
        included_table.create(database.engine)

        logger.info('Creating source table %s in %s database ...', excluded_table_name, database.type)
        excluded_table = sqlalchemy.Table(
            excluded_table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String(64))
        )
        excluded_table.create(database.engine)

        # Create the pipeline setting the table to be ignored
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + tables_prefix + "%",
            ignore_tables=database.database + '.' + excluded_table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        black_widow = {'id': 1, 'name': 'Black Widow'}
        hulk = {'id': 2, 'name': 'Hulk'}

        connection.execute(f"""
            insert into {excluded_table_name}
            values ({hulk.get('id')}, '{hulk.get('name')}')
        """)
        connection.execute(f"""
            insert into {included_table_name}
            values ({black_widow.get('id')}, '{black_widow.get('name')}')
        """)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        assert len(wiretap.output_records) == 1

        record = wiretap.output_records[0]
        assert 'Data' in record.field
        assert record.field['Data'] is not None
        assert sorted(record.field['Data']) == sorted(black_widow)

    finally:
        connection.execute(f'drop table if exists {included_table_name}')
        connection.execute(f'drop table if exists {excluded_table_name}')

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)


@sdc_min_version('5.1.0')
@database('mysql')
def test_mysql_binary_log_primary_keys_headers(sdc_builder, sdc_executor, database):
    """
    Test to check the primary keys are present in the headers of the output records.
    """
    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        # Create the table
        connection = database.engine.connect()

        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(database.engine),
            sqlalchemy.Column('TYPE', sqlalchemy.String(64), primary_key=True),
            sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('NAME', sqlalchemy.String(64)),
            sqlalchemy.Column('SURNAME', sqlalchemy.String(64)),
            sqlalchemy.Column('ADDRESS', sqlalchemy.String(64))
        )
        table.create(database.engine)

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # Define the data for each statement
        name = 'Bilbo'
        surname = 'Baggins'
        column_types = [
            'Hobbit',
            'Fallohide',
            'Fallohide',
            'Hobbit - Fallohide',
            'Hobbit - Fallohide',
            'Hobbit - Fallohide',
            'Hobbit - Fallohide',
            'Hobbit'
        ]
        column_ids = [1, 1, 2, 3, 3, 4, 4, 5]
        index = 0

        # Insert data into the table
        connection.execute(f"""
            insert into {table_name}
            values ('{column_types[index]}', {column_ids[index]}, '{name}', '{surname}', 'Bag End {index}')
        """)
        index += 1

        connection.execute(f"""
            update {table_name}
            set ADDRESS = 'Bag End {index}', TYPE = '{column_types[index]}'
            where TYPE = '{column_types[index - 1]}' and ID = {column_ids[index - 1]}
        """)
        index += 1

        connection.execute(f"""
            update {table_name}
            set ADDRESS = 'Bag End {index}', ID = {column_ids[index]}
            where TYPE = '{column_types[index - 1]}' and ID = {column_ids[index - 1]}
        """)
        index += 1

        connection.execute(f"""
            update {table_name}
            set ADDRESS = 'Bag End {index}', TYPE = '{column_types[index]}', ID = {column_ids[index]}
            where TYPE = '{column_types[index - 1]}' and ID = {column_ids[index - 1]}
        """)
        index += 1

        connection.execute(f"""
            update {table_name}
            set ADDRESS = 'Bag End {index}', TYPE = '{column_types[index]}'
            where TYPE = '{column_types[index - 1]}'
        """)
        index += 1

        connection.execute(f"""
            update {table_name}
            set ADDRESS = 'Bag End {index}', ID = {column_ids[index]}
            where ID = {column_ids[index - 1]}
        """)
        index += 1

        connection.execute(f"""
            update {table_name}
            set ADDRESS = 'Bag End {index}'
            where ID = {column_ids[index - 1]}
        """)
        index += 1

        connection.execute(f"""
            update {table_name}
            set ADDRESS = 'Bag End {index}', ID = {column_ids[index]}, TYPE = '{column_types[index]}'
            where ADDRESS = 'Bag End {index - 1}'
        """)
        index += 1

        connection.execute(f"delete from {table_name}")
        index += 1

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', index)

        assert len(wiretap.output_records) == index

        primary_key_before_prefix = "jdbc.primaryKey.before."
        primary_key_after_prefix = "jdbc.primaryKey.after."

        for record in wiretap.output_records:
            if record.field['Type'] == "UPDATE":
                assert primary_key_before_prefix + "TYPE" in record.header.values
                assert primary_key_before_prefix + "ID" in record.header.values
                assert primary_key_after_prefix + "TYPE" in record.header.values
                assert primary_key_after_prefix + "ID" in record.header.values

                assert record.header.values[primary_key_before_prefix + "TYPE"] is not None
                assert record.header.values[primary_key_before_prefix + "ID"] is not None
                assert record.header.values[primary_key_after_prefix + "TYPE"] is not None
                assert record.header.values[primary_key_after_prefix + "ID"] is not None

                column_address = record.field['Data']['ADDRESS'].value
                index = int(column_address[-1])

                assert record.header.values[f"{primary_key_before_prefix}TYPE"] == column_types[index - 1]
                assert record.header.values[f"{primary_key_before_prefix}ID"] == f"{column_ids[index - 1]}"
                assert record.header.values[f"{primary_key_after_prefix}TYPE"] == column_types[index]
                assert record.header.values[f"{primary_key_after_prefix}ID"] == f"{column_ids[index]}"

            else:
                assert primary_key_before_prefix + "TYPE" not in record.header.values
                assert primary_key_before_prefix + "ID" not in record.header.values
                assert primary_key_after_prefix + "TYPE" not in record.header.values
                assert primary_key_after_prefix + "ID" not in record.header.values

    finally:
        connection.execute(f'drop table if exists {table_name}')

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)


@sdc_min_version('5.1.0')
@database('mysql')
def test_mysql_binary_log_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """
    Test to check the metadata of numeric primary keys are present in the headers of the output records with the
    expected values.
    """
    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        connection = database.engine.connect()

        # Create the table
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        connection.execute(f"""
            create table {table_name}(
                my_bit bit(2),
                my_tinyint tinyint,
                my_smallint smallint,
                my_int int,
                my_bigint bigint,
                my_decimal decimal(10, 5),
                my_numeric numeric(8, 4),
                my_float float,
                my_double double,
                primary key (
                    my_bit,
                    my_tinyint,
                    my_smallint,
                    my_int,
                    my_bigint,
                    my_decimal,
                    my_numeric,
                    my_float,
                    my_double
                )
            )
        """)

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # Add a record to the table
        connection.execute(f'insert into {table_name} values (1, 1, 1, 1, 1, 1.1, 1, 1.1, 1.1)')

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        assert len(wiretap.output_records) == 1

        record = wiretap.output_records[0]
        assert "jdbc.primaryKeySpecification" in record.header.values
        assert {record.header.values["jdbc.primaryKeySpecification"]} is not None

        primary_key_specification_json = json.dumps(
            json.loads(record.header.values["jdbc.primaryKeySpecification"]),
            sort_keys=True
        )

        if Version(database.version) < Version('8.0.0'):
            t_int_size = 4
            s_int_size = 6
            int_size = 11
            b_int_size = 20
            decimal_size = 12
            numeric_size = 10
            float_scale = 31
            double_scale = 31
        else:
            t_int_size = 3
            s_int_size = 5
            int_size = 10
            b_int_size = 19
            decimal_size = 10
            numeric_size = 8
            float_scale = 0
            double_scale = 0

        primary_key_specification_expected = f'''{{
            {_primary_key_specification_json("my_bit", -7, "BIT", 2, 2, 0, "false", "false")},
            {_primary_key_specification_json("my_tinyint", -6, "TINYINT", t_int_size, t_int_size, 0, "true", "false")},
            {_primary_key_specification_json("my_smallint", 5, "SMALLINT", s_int_size, s_int_size, 0, "true", "false")},
            {_primary_key_specification_json("my_int", 4, "INTEGER", int_size, int_size, 0, "true", "false")},
            {_primary_key_specification_json("my_bigint", -5, "BIGINT", b_int_size, b_int_size, 0, "true", "false")},
            {_primary_key_specification_json("my_decimal", 3, "DECIMAL", decimal_size, 10, 5, "true", "false")},
            {_primary_key_specification_json("my_numeric", 3, "DECIMAL", numeric_size, 8, 4, "true", "false")},
            {_primary_key_specification_json("my_float", 7, "REAL", 12, 12, float_scale, "true", "false")},
            {_primary_key_specification_json("my_double", 8, "DOUBLE", 22, 22, double_scale, "true", "false")}
        }}'''

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        connection.execute(f'drop table if exists {table_name}')

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)


@sdc_min_version('5.1.0')
@database('mysql')
def test_mysql_binary_log_non_numeric_primary_keys_metadata(sdc_builder, sdc_executor, database):
    """
    Test to check the metadata of non-numeric primary keys are present in the headers of the output records with the
    expected values.
    """
    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        connection = database.engine.connect()

        # Create the table
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        connection.execute(f"""
            create table {table_name}(
                my_boolean boolean,
                my_date date,
                my_datetime datetime,
                my_timestamp timestamp,
                my_time time,
                my_year year,
                my_char char(10),
                my_varchar varchar(32),
                my_varchar2 varchar(64),
                my_text text(16),
                primary key (
                    my_boolean,
                    my_date,
                    my_datetime,
                    my_timestamp,
                    my_time,
                    my_year,
                    my_char,
                    my_varchar,
                    my_varchar2,
                    my_text(16)
                )
            )
        """)

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # Add a record to the table
        connection.execute(f'''
            insert into {table_name}
            values (true, '2011-12-18', '2011-12-18 9:17:17', current_timestamp, current_time, 2022, ' ', ' ', ' ', ' ')
        ''')

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)

        assert len(wiretap.output_records) == 1

        # Check the primary keys metadata in the record header
        record = wiretap.output_records[0]
        assert "jdbc.primaryKeySpecification" in record.header.values
        assert {record.header.values["jdbc.primaryKeySpecification"]} is not None

        primary_key_specification_json = json.dumps(
            json.loads(record.header.values["jdbc.primaryKeySpecification"]),
            sort_keys=True
        )

        if Version(database.version) < Version('8.0.0'):
            text_datatype = "LONGVARCHAR"
            text_size = 255
            text_type = -1
        else:
            text_datatype = "VARCHAR"
            text_size = 63
            text_type = 12

        primary_key_specification_expected = f'''{{
            {_primary_key_specification_json("my_boolean", -7, "BIT", 1, 1, 0, "false", "false")},
            {_primary_key_specification_json("my_date", 91, "DATE", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_datetime", 93, "TIMESTAMP", 19, 19, 0, "false", "false")},
            {_primary_key_specification_json("my_timestamp", 93, "TIMESTAMP", 19, 19, 0, "false", "false")},
            {_primary_key_specification_json("my_time", 92, "TIME", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_year", 91, "DATE", 4, 4, 0, "false", "false")},
            {_primary_key_specification_json("my_char", 1, "CHAR", 10, 10, 0, "false", "false")},
            {_primary_key_specification_json("my_varchar", 12, "VARCHAR", 32, 32, 0, "false", "false")},
            {_primary_key_specification_json("my_varchar2", 12, "VARCHAR", 64, 64, 0, "false", "false")},
            {_primary_key_specification_json(
                "my_text",
                text_type,
                text_datatype,
                text_size,
                text_size,
                0,
                "false",
                "false"
            )}
        }}'''

        primary_key_specification_expected_json = json.dumps(
            json.loads(primary_key_specification_expected),
            sort_keys=True
        )

        assert primary_key_specification_json == primary_key_specification_expected_json

    finally:
        connection.execute(f"drop table if exists {table_name}")

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)


@sdc_min_version('5.1.0')
@database('mysql')
def test_mysql_binary_log_altering_columns(sdc_builder, sdc_executor, database):
    """
    Test to check the record values and primary keys headers after adding columns and deleting and changing the columns
    defined as primary keys. The rows added to the table before adding a column should have no value for the newly added
    columns and the primary key headers information should match the primary key constraints as they are changed.
    """
    if Version(database.version) < Version('8.0.0'):
        pytest.skip('Historical table structures tracking is unsupported for MySQL versions older than 8.0.0')

    pipeline = None
    table_name = get_random_string(string.ascii_lowercase, 20)

    try:
        connection = database.engine.connect()

        # Create the table
        logger.info('Creating source table %s in %s database ...', table_name, database.type)
        connection.execute(f"create table {table_name}(id int primary key, name varchar(32))")

        # Create the pipeline
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(
            initial_offset=_get_initial_offset(database),
            server_id=_get_server_id(),
            include_tables=database.database + '.' + table_name
        )

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build("MySQL Binary Log Client Pipeline").configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        connection.execute(f'insert into {table_name} values (1, "Tony Stark")')
        connection.execute(f'alter table {table_name} add column code_name varchar(64) after name')
        connection.execute(f'insert into {table_name} values (2, "Steve Rogers", "Captain America")')
        connection.execute(f'update {table_name} set id = 3 where id = 1')
        connection.execute(f'alter table {table_name} drop primary key')
        connection.execute(f'update {table_name} set id = 4 where id = 2')
        connection.execute(f'alter table {table_name} add primary key (id, name)')
        connection.execute(f'update {table_name} set id = 5 where id = 4')

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 5)
        assert len(wiretap.output_records) == 5

        primary_key_before_prefix = "jdbc.primaryKey.before."
        primary_key_after_prefix = "jdbc.primaryKey.after."
        primary_key_specification = "jdbc.primaryKeySpecification"
        id_metadata = _primary_key_specification_json("id", 4, "INTEGER", 10, 10, 0, "true", "false")
        name_metadata = _primary_key_specification_json("name", 12, "VARCHAR", 32, 32, 0, "false", "false")

        for record in wiretap.output_records:
            # Check the primary keys headers
            if record.field['Type'] == 'UPDATE' and record.field['Data']['id'] != 4:
                assert primary_key_before_prefix + "id" in record.header.values
                assert primary_key_after_prefix + "id" in record.header.values
                assert record.header.values[primary_key_before_prefix + "id"] is not None
                assert record.header.values[primary_key_after_prefix + "id"] is not None

                if record.field['Data']['id'] == 5:
                    assert primary_key_before_prefix + "name" in record.header.values
                    assert primary_key_after_prefix + "name" in record.header.values
                    assert record.header.values[primary_key_before_prefix + "name"] is not None
                    assert record.header.values[primary_key_after_prefix + "name"] is not None

                    assert record.header.values[primary_key_before_prefix + "id"] == "4"
                    assert record.header.values[primary_key_after_prefix + "id"] == "5"
                    assert record.header.values[primary_key_before_prefix + "name"] == "Steve Rogers"
                    assert record.header.values[primary_key_after_prefix + "name"] == "Steve Rogers"
                else:
                    assert primary_key_before_prefix + "name" not in record.header.values
                    assert primary_key_after_prefix + "name" not in record.header.values

                    assert record.header.values[primary_key_before_prefix + "id"] == "1"
                    assert record.header.values[primary_key_after_prefix + "id"] == "3"
            else:
                assert primary_key_before_prefix + "id" not in record.header.values
                assert primary_key_before_prefix + "name" not in record.header.values
                assert primary_key_after_prefix + "id" not in record.header.values
                assert primary_key_after_prefix + "name" not in record.header.values

            # Check the primary keys metadata
            assert primary_key_specification in record.header.values
            assert {record.header.values[primary_key_specification]} is not None

            if record.field['Data']['id'] == 4:
                primary_key_specification_expected = "{}"
            elif record.field['Data']['id'] == 5:
                primary_key_specification_expected = f'{{ {id_metadata}, {name_metadata} }}'
            else:
                primary_key_specification_expected = f'{{ {id_metadata} }}'

            primary_key_specification_json = json.dumps(
                json.loads(record.header.values[primary_key_specification]),
                sort_keys=True
            )
            primary_key_specification_expected_json = json.dumps(
                json.loads(primary_key_specification_expected),
                sort_keys=True
            )
            assert primary_key_specification_json == primary_key_specification_expected_json

            # Check the presence and value for the new column added halfway through
            if record.field['Data']['id'] == 1:
                assert 'code_name' not in record.field['Data']
            elif record.field['Data']['id'] == 3:
                assert 'code_name' in record.field['Data']
                assert record.field['Data']['code_name'] == None
            else:
                assert 'code_name' in record.field['Data']
                assert record.field['Data']['code_name'] == "Captain America"

    finally:
        connection.execute(f"drop table if exists {table_name}")

        if pipeline is not None:
            sdc_executor.stop_pipeline(pipeline=pipeline, force=True)


@database('mysql')
@sdc_min_version('5.7.0')
def test_mysql_binary_log_loaded_metadata_tables(sdc_builder, sdc_executor, database, keep_data):
    """Test that MySQL Binary Log Origin loaded metadata for more than 10 tables.

    Pipeline looks like:

        mysql_binary_log >> wiretap
    """
    tables = []
    connection = None

    try:
        # Create table.
        connection = database.engine.connect()
        table_names = [f'{get_random_string(string.ascii_lowercase, 8)}' for i in range(1, 21)]
        tables = []
        for table_name in table_names:
            table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                     sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=False),
                                     sqlalchemy.Column('number1', sqlalchemy.Integer),
                                     sqlalchemy.Column('number2', sqlalchemy.Integer),
                                     sqlalchemy.Column('number3', sqlalchemy.Integer))
            table.create(database.engine)
            tables.append(table)

        # Create Pipeline.
        pipeline_builder = sdc_builder.get_pipeline_builder()
        mysql_binary_log = pipeline_builder.add_stage('MySQL Binary Log')
        mysql_binary_log.set_attributes(initial_offset=_get_initial_offset(database),
                                        server_id=_get_server_id())

        wiretap = pipeline_builder.add_wiretap()

        mysql_binary_log >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Run pipeline and verify output.
        sdc_executor.start_pipeline(pipeline)

        # Insert data into table.
        for table in tables:
            connection.execute(table.insert(), {'id': 1, 'number1': 1, 'number2': 1, 'number3': 1})

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(tables))
        sdc_executor.stop_pipeline(pipeline)

        assert len(tables) == len(wiretap.output_records)

    finally:
        if not keep_data:
            # Drop table and Connection.
            for table in tables:
                logger.info('Dropping table %s in %s database...', table, database.type)
                table.drop(database.engine)

            if connection is not None:
                connection.close()


def _primary_key_specification_json(column_name, column_type, data_type, size, precision, scale, signed, currency):
    return f'''
        \"{column_name}\": {{
            \"type\": {column_type},
            \"datatype\": \"{data_type}\",
            \"size\": {size},
            \"precision\": {precision},
            \"scale\": {scale},
            \"signed\": {signed},
            \"currency\": {currency}
        }}
    '''


def _get_server_id():
    server_id = str(random.randint(1, 2147483647))
    logger.info(f"Generated server id {server_id}")
    return server_id


def _get_initial_offset(database):
    """Return current position of the bin log that can be used for Initial Offset configuration."""
    connection = database.engine.connect()
    rs = None

    try:
        rs = connection.execute("SHOW MASTER STATUS")
        rows = [row for row in rs]

        assert len(rows) == 1
        offset = f"{rows[0][0]}:{rows[0][1]}"
        logger.info(f"Generated starting offset: {offset}")
        return offset
    finally:
        if rs is not None:
            rs.close()

        if connection is not None:
            connection.close()

DEFAULT_WAIT_FOR_STATUS_TIMEOUT = 200
def _wait_for_pipeline_statuses(sdc_executor, pipeline, statuses, timeout_sec=DEFAULT_WAIT_FOR_STATUS_TIMEOUT):
    """Block until a pipeline reaches a status included in the list of desired status.

    Args:
        pipeline (:py:class:`streamsets.sdk.sdc_models.Pipeline`): The pipeline instance.
        status (:py:obj:`list`): The desired list of status to wait for.
        timeout_sec (:obj:`int`, optional): Timeout to wait for ``pipeline`` to reach ``status``, in seconds.
            Default: :py:const:`streamsets.sdk.sdc.DEFAULT_WAIT_FOR_STATUS_TIMEOUT`.

    Raises:
        TimeoutError: If ``timeout_sec`` passes without ``pipeline`` reaching ``status``.
    """
    logger.info('Waiting for pipeline to reach status %s ...', statuses)
    start_waiting_time = time.time()
    stop_waiting_time = start_waiting_time + timeout_sec

    while time.time() < stop_waiting_time:
        current_status = sdc_executor.get_pipeline_status(pipeline).response.json()['status']
        logger.debug('Pipeline has current status %s ...', current_status)
        if current_status in statuses:
            logger.info('Pipeline (%s) reached status %s (took %.2f s).',
                        pipeline.id,
                        current_status,
                        time.time() - start_waiting_time)
            break
        time.sleep(1)
    else:
        # We got out of the loop and did not get the status we were waiting for.
        raise TimeoutError('Pipeline did not reach status {} '
                           'after {} s (current status {})'.format(status, timeout_sec, current_status))