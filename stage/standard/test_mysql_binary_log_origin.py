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

import docker
import logging
import random
import string
import time

import pytest
import sqlalchemy

from streamsets.testframework.environments.databases import MySqlDatabase, MemSqlDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def preflight_check(database):
    if isinstance(database, MySqlDatabase) and not database.is_cdc_enabled:
        pytest.skip('Test only runs against MySQL with CDC enabled.')
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")


# https://dev.mysql.com/doc/refman/8.0/en/data-types.html
# The underlying library we're using doesn't work properly with unsigned types:
#   https://github.com/shyiko/mysql-binlog-connector-java#implementation-notes
#   They are always returned as signed numbers, meaning that -1 is a max value of the column
# The underlying library is also not working properly with binary types - assuming that they are strings
#   unless a config is set (but then all strings are binaries), ... Seems like some inherent limitation
#   inside MySQL's bin log.
#   https://github.com/shyiko/mysql-binlog-connector-java/issues/276
# No good support for the poly type either (point, linestring, polygon).
DATA_TYPES = [
    ('TINYINT', '-128', 'INTEGER', '-128'),
    ('TINYINT UNSIGNED', '255', 'INTEGER', '-1'),
    ('SMALLINT', '-32768', 'INTEGER', '-32768'),
    ('SMALLINT UNSIGNED', '65535', 'INTEGER', '-1'),
    ('MEDIUMINT', '-8388608', 'INTEGER', '-8388608'),
    ('MEDIUMINT UNSIGNED', '16777215', 'INTEGER', '-1'),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('INT UNSIGNED', '4294967295', 'INTEGER', '-1'),
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('BIGINT UNSIGNED', '18446744073709551615', 'LONG', '-1'),
    ('DECIMAL(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('FLOAT', '5.2', 'FLOAT', '5.2'),
    ('DOUBLE', '5.2', 'DOUBLE', '5.2'),
    ('BIT(8)', "b'1000001'", 'STRING', '{0, 6}'), # I have no clue what this means and where it's coming from
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIMESTAMP', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIME', "'5:00:00'", 'TIME', 18000000),
    ('YEAR', "'2019'", 'INTEGER', '2019'),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('BINARY(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARBINARY(5)', "'Hello'", 'STRING', 'Hello'),
    ('BLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ("ENUM('a', 'b')", "'a'", 'INTEGER', '1'),
    ("set('a', 'b', 'c')", "'a,c'", 'LONG', '5'),
#    ("POINT", "POINT(1, 1)", 'STRING', 'AAAAAAEBAAAAAAAAAAAA8D8AAAAAAADwPw=='),
#    ("LINESTRING", "LineString(Point(0,0), Point(10,10), Point(20,25), Point(50,60))", 'STRING', 'AAAAAAECAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkQAAAAAAAACRAAAAAAAAANEAAAAAAAAA5QAAAAAAAAElAAAAAAAAATkA='),
#    ("POLYGON", "Polygon(LineString(Point(0,0),Point(10,0),Point(10,10),Point(0,10),Point(0,0)),LineString(Point(5,5),Point(7,5),Point(7,7),Point(5,7),Point(5,5)))", 'STRING', 'AAAAAAEDAAAAAgAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAAAAUQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAFEAAAAAAAAAUQA=='),
    ("JSON", "'{\"a\":\"b\"}'", 'STRING', '{\"a\":\"b\"}'),
]


@database('mysql')
@sdc_min_version('3.0.0.0')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES, ids=[i[0] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create Pipeline.
        builder = sdc_builder.get_pipeline_builder()
        origin = builder.add_stage('MySQL Binary Log')
        origin.initial_offset = _get_initial_offset(database)
        origin.server_id = _get_server_id()
        origin.include_tables = database.database + '.' + table_name

        wiretap = builder.add_wiretap()
        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                data_column {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        # Run pipeline and verify output.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
        record = wiretap.output_records[0]
        null_record = wiretap.output_records[1]

        # TLKT-177: Add ability for field to return raw value
        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.

        assert record.field['Data']['data_column'].type == expected_type
        assert null_record.field['Data']['data_column'].type == expected_type

        assert record.field['Data']['data_column']._data['value'] == expected_value
        assert null_record.field['Data']['data_column'] == None
    finally:
        if not keep_data:
            if connection is not None:
                logger.info('Dropping table %s in %s database ...', table_name, database.type)
                connection.execute(f"DROP TABLE IF EXISTS {table_name}")

            if connection is not None:
                connection.close()


# Rules: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
# Rules: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
OBJECT_NAMES = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 64), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 64)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "$_", get_random_string(string.ascii_letters, 5) + "$_"),
]


@database('mysql')
@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names(sdc_builder, sdc_executor, database, test_name, table_name, offset_name, keep_data):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('MySQL Binary Log')
    origin.initial_offset = _get_initial_offset(database)
    origin.server_id = _get_server_id()
    origin.include_tables = database.database + '.' + table_name

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(offset_name, sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{offset_name: 1}])

        sdc_executor.add_pipeline(pipeline)

        # Run pipeline and verify output.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        # Verify that we properly read that one record
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].field['Data'][offset_name] == 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database('mysql')
def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data):
    max_batch_size = 1000
    batches = 50
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('MySQL Binary Log')
    origin.initial_offset = _get_initial_offset(database)
    origin.server_id = _get_server_id()
    origin.include_tables = database.database + '.' + table_name

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        logger.info('Inserting data into %s', table_name)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'id': n} for n in range(1, max_batch_size * batches + 1)])

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(max_batch_size * batches)

        records = wiretap.output_records
        assert len(records) == max_batch_size * batches

        # Verify each record
        def sortFunc(entry):
            return entry.field['Data']['id'].value

        records.sort(key=sortFunc)

        expected_number = 1
        for record in records:
            assert record.field['Data']['id'] == expected_number
            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database('mysql')
def test_dataflow_events(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip('MySQL Binary Log Origin does not support events')


@database('mysql')
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("MySQL Origin doesn't deal with data formats")


@database('mysql')
def test_resume_offset(sdc_builder, sdc_executor, database, keep_data):
    iterations = 3
    records_per_iteration = 10
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('MySQL Binary Log')
    origin.initial_offset = _get_initial_offset(database)
    origin.server_id = _get_server_id()
    origin.include_tables = database.database + '.' + table_name

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            logger.info('Inserting data into %s', table_name)
            connection = database.engine.connect()
            connection.execute(table.insert(), [{'id': n} for n in range(iteration * records_per_iteration + 1,
                                                                         iteration * records_per_iteration + 1 + records_per_iteration)])

            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert record.field['Data']['id'].value == expected_number

                expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


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
            try:
                table.drop(database.engine)
            finally:
                pass

        if pipeline is not None:
            try:
                logger.info(f'Stopping pipeline {pipeline_title}')
                sdc_executor.stop_pipeline(pipeline, force=True)
            finally:
                pass


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
