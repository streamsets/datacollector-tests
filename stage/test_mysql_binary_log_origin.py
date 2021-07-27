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
import random
import string
import time

import pytest
import sqlalchemy
from streamsets.testframework.environments.databases import MySqlDatabase
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

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
    table = None
    connection = None

    try:
        server_id = _get_server_id()
        initial_offset = _get_initial_offset(database)

        # Create table.
        connection = database.engine.connect()
        table_name = get_random_string(string.ascii_lowercase, 20)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, autoincrement=False, quote=True),
                                 quote=True)
        table.create(database.engine)
        connection.execute(table.insert(), {'id': 1})

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
        wiretap2 = builder.add_wiretap()
        origin >> wiretap2.destination
        pipeline2 = builder.build().configure_for_environment(database)
        pipeline2.configuration['shouldRetry'] = False
        sdc_executor.add_pipeline(pipeline2)

        # 1) Start pipeline 1, wait until it consumes at least that one record
        sdc_executor.start_pipeline(pipeline1).wait_for_pipeline_output_records_count(1)

        # 2) Start pipeline 2 (while pipeline 1 is still running)
        sdc_executor.start_pipeline(pipeline2)

        # 3) Pipeline 1 should at this point fail (two different slaves with the same id)
        time.sleep(5)
        assert sdc_executor.get_pipeline_status(pipeline1).response.json().get('status') in ("RUN_ERROR", "RUNNING_ERROR")

        # 4) Pipeline 2 can be stopped at this point
        sdc_executor.stop_pipeline(pipeline2)

        # 5) We start the first pipeline and insert data in 5-second interval, the pipeline should read all of them
        # In order to see the problem with the left-over thread, we need to wait at least a minute.
        sdc_executor.start_pipeline(pipeline1)
        for i in range(2, 10):
            time.sleep(10)
            logger.info(f"Inserting {i}")
            connection.execute(table.insert(), {'id': i})

        # 6) Finally the pipeline should still be running and should process all 10 rows. Here is where
        # a version without SDC-15872 should fail.
        assert sdc_executor.get_pipeline_status(pipeline1).response.json().get('status') == "RUNNING"

        # 7) Stop pipeline 1 and validate that we got all the records into the wiretap
        sdc_executor.stop_pipeline(pipeline1)

        records = wiretap1.output_records
        assert len(records) == 9

        for i in range(1, 10):
            assert records[i-1].field['Data']['id'] == i
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

        connection = database.engine.connect()
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

        logger.info(f'Waiting {time_to_sleep_before_disconnect} seconds to disconnect the network...')
        time.sleep(time_to_sleep_before_disconnect)
        logger.info(f'Waited {time_to_sleep_before_disconnect} seconds to disconnect the network!')
        logger.info(f'Disconnecting the network...')
        sdc_executor.container.network_disconnect()
        logger.info(f'Network disconnected!')
        logger.info(f'Keeping the network disconnected for {time_to_sleep_after_disconnect} seconds...')
        time.sleep(time_to_sleep_after_disconnect)
        logger.info(f'Kept the network disconnected for {time_to_sleep_before_disconnect} seconds!')
        logger.info(f'Reconnecting the network...')
        sdc_executor.container.network_reconnect()
        logger.info(f'Network reconnected!')

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
